// Copyright (C) 2026 The Syncthing Authors.
//
// Drip mode (patch/46): per-path coalescing of repeated small updates
// before they reach the database.
//
// Measured motivation (internal/db/sqlite amplification harness,
// bcf11ca04 + 189911571): one 1-block file change committed alone costs
// ~74-91 KB of WAL (~20 B-tree pages) regardless of content size, the cost
// is flat under a drip (zero amortization), and batching collapses it ~61x.
// A daemon rewriting a state file every few seconds therefore produces a
// constant ~80 KB transaction stream; the 2026-10-23 incident measured
// 106-134 MB/min of block-layer writes from a handful of such files.
//
// Drip mode detects a path committed repeatedly (detectN commits within
// window) and then holds its updates in a pending map (last version wins),
// flushing on a quiet period or a max-hold bound. The hold bounds are
// derived from the existing watch-aggregation staleness budget, not
// invented: quiet = 2x FSWatcherDelayS (20 s default), maxHold =
// watchaggregator.NotifyTimeout(FSWatcherDelayS) (60 s default) — the same
// bound the watcher already imposes on continuously-modified files. The
// tracker owns no timers: it takes an injected clock and reports what is
// due and when, so unit tests run on a fake clock and the folder Serve
// loop wires the same calls to a real timer.
//
// Scope: scanner-committed file updates only (updateLocalsFromScanning).
// Directories, deletes, and invalid entries always pass through; pull
// commits are untouched — receiver-side churn is announcement-driven, and
// holding the sender's announcements (only settled files get announced)
// removes it transitively.
//
// Env: STDRIPMODE=0 disables; STDRIPQUIETS / STDRIPMAXHOLDS /
// STDRIPDETECTN / STDRIPWINDOWS override the derived defaults (seconds /
// count).

package model

import (
	"os"
	"strconv"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/watchaggregator"
)

type dripParams struct {
	enabled bool
	detectN int           // commits within window that mark a path hot
	window  time.Duration // detection window
	quiet   time.Duration // flush after this long without a newer update
	maxHold time.Duration // flush regardless after this long held
}

type dripHeld struct {
	fi         protocol.FileInfo
	firstHeld  time.Time
	lastUpdate time.Time // last time a newer version arrived while held
	coalesced  int       // updates collapsed into this entry, incl. the first
}

// dripTracker is not internally synchronized; the folder Serve goroutine is
// the only caller (scan batch flushes and the flush timer both run there).
type dripTracker struct {
	churn  *churnTracker
	params dripParams
	now    func() time.Time

	recent map[string][]time.Time // commit timestamps within window, per path
	held   map[string]*dripHeld

	// logf receives one aggregated line per drip-mode transition and per
	// flush; wired to the folder's slog at INF.
	logf func(msg string, args ...any)
}

func defaultDripParams(cfg config.FolderConfiguration) dripParams {
	delayS := cfg.FSWatcherDelayS
	if delayS <= 0 {
		delayS = 10 // config default
	}
	p := dripParams{
		enabled: true,
		detectN: 3,
		window:  watchaggregator.NotifyTimeout(delayS), // 60 s default
		quiet:   time.Duration(2*delayS) * time.Second, // 20 s default
		maxHold: watchaggregator.NotifyTimeout(delayS), // 60 s default
	}
	if os.Getenv("STDRIPMODE") == "0" {
		p.enabled = false
	}
	if v, err := strconv.Atoi(os.Getenv("STDRIPDETECTN")); err == nil && v > 0 {
		p.detectN = v
	}
	if v, err := strconv.ParseFloat(os.Getenv("STDRIPWINDOWS"), 64); err == nil && v > 0 {
		p.window = time.Duration(v * float64(time.Second))
	}
	if v, err := strconv.ParseFloat(os.Getenv("STDRIPQUIETS"), 64); err == nil && v > 0 {
		p.quiet = time.Duration(v * float64(time.Second))
	}
	if v, err := strconv.ParseFloat(os.Getenv("STDRIPMAXHOLDS"), 64); err == nil && v > 0 {
		p.maxHold = time.Duration(v * float64(time.Second))
	}
	return p
}

func newDripTracker(cfg config.FolderConfiguration, now func() time.Time, logf func(string, ...any)) *dripTracker {
	return &dripTracker{
		params: defaultDripParams(cfg),
		now:    now,
		recent: make(map[string][]time.Time),
		held:   make(map[string]*dripHeld),
		logf:   logf,
	}
}

// filterScanUpdate partitions a scan batch into files to commit now and
// files to hold. Held entries are last-wins: a newer version of an already
// held path replaces the pending one. Detection and holding apply to plain
// files only; directories, deletes, and invalid entries pass unrecorded.
func (d *dripTracker) filterScanUpdate(fs []protocol.FileInfo) []protocol.FileInfo {
	if !d.params.enabled || len(fs) == 0 {
		return fs
	}
	now := d.now()
	commit := fs[:0]
	for _, fi := range fs {
		if fi.IsDirectory() || fi.Deleted || fi.IsInvalid() {
			commit = append(commit, fi)
			continue
		}
		if h, ok := d.held[fi.Name]; ok {
			// Newer version of a held path: collapse.
			h.fi = fi
			h.lastUpdate = now
			h.coalesced++
			d.recent[fi.Name] = append(d.pruned(fi.Name, now), now)
			continue
		}
		d.recent[fi.Name] = append(d.pruned(fi.Name, now), now)
		if len(d.recent[fi.Name]) >= d.params.detectN {
			d.held[fi.Name] = &dripHeld{fi: fi, firstHeld: now, lastUpdate: now, coalesced: 1}
			if d.logf != nil {
				d.logf("Frequently-changing file enters drip mode", "path", fi.Name,
					"commits", len(d.recent[fi.Name]), "windowS", d.params.window.Seconds(),
					"quietS", d.params.quiet.Seconds(), "maxHoldS", d.params.maxHold.Seconds())
			}
			continue
		}
		commit = append(commit, fi)
	}
	return commit
}

func (d *dripTracker) pruned(name string, now time.Time) []time.Time {
	ts := d.recent[name]
	cut := 0
	for cut < len(ts) && now.Sub(ts[cut]) > d.params.window {
		cut++
	}
	if cut == len(ts) {
		delete(d.recent, name)
		return nil
	}
	return ts[cut:]
}

// dripFlush enumerates held updates due at the time of the call: quiet for
// params.quiet, or held for params.maxHold regardless of activity.
func (d *dripTracker) dueLocked(now time.Time) (due []protocol.FileInfo, names []string, reasons []string) {
	for name, h := range d.held {
		var reason string
		switch {
		case now.Sub(h.lastUpdate) >= d.params.quiet:
			reason = "quiet"
		case now.Sub(h.firstHeld) >= d.params.maxHold:
			reason = "max-hold"
		default:
			continue
		}
		due = append(due, h.fi)
		names = append(names, name)
		reasons = append(reasons, reason)
	}
	return
}

// flushDue removes and returns the held updates due now, logging one
// aggregated line per flushed path. A quiet flush means the file stopped
// changing, so the path's detection history is cleared (it cools and must
// re-earn drip mode); a max-hold flush happens mid-drip, so the path stays
// hot and the next update is held immediately.
func (d *dripTracker) flushDue() []protocol.FileInfo {
	due, names, reasons := d.dueLocked(d.now())
	for i, name := range names {
		h := d.held[name]
		if d.churn != nil {
			d.churn.dripFlush(name, h.coalesced, d.now().Sub(h.firstHeld).Seconds(), reasons[i])
		} else if d.logf != nil {
			d.logf("Drip flush: coalesced updates committed", "path", name,
				"coalesced", h.coalesced, "heldS", d.now().Sub(h.firstHeld).Seconds(), "reason", reasons[i])
		}
		delete(d.held, name)
		if reasons[i] == "quiet" {
			delete(d.recent, name)
		}
	}
	return due
}

// flushAll removes and returns every held update (folder shutdown).
func (d *dripTracker) flushAll() []protocol.FileInfo {
	all := make([]protocol.FileInfo, 0, len(d.held))
	for name, h := range d.held {
		if d.churn != nil {
			d.churn.dripFlush(name, h.coalesced, d.now().Sub(h.firstHeld).Seconds(), "shutdown")
		} else if d.logf != nil {
			d.logf("Drip flush: coalesced updates committed", "path", name,
				"coalesced", h.coalesced, "heldS", d.now().Sub(h.firstHeld).Seconds(), "reason", "shutdown")
		}
		all = append(all, h.fi)
		delete(d.held, name)
	}
	return all
}

// nextIn returns the duration until the earliest pending flush deadline.
func (d *dripTracker) nextIn() (time.Duration, bool) {
	if len(d.held) == 0 {
		return 0, false
	}
	now := d.now()
	next := time.Time{}
	for _, h := range d.held {
		for _, dl := range [2]time.Time{h.lastUpdate.Add(d.params.quiet), h.firstHeld.Add(d.params.maxHold)} {
			if next.IsZero() || dl.Before(next) {
				next = dl
			}
		}
	}
	if !next.After(now) {
		return 0, true
	}
	return next.Sub(now), true
}

func (d *dripTracker) heldCount() int { return len(d.held) }
