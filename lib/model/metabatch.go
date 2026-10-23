// Copyright (C) 2026 The Syncthing Authors.
//
// Metadata-only update batching (patch/47): coalescing of scan-side updates
// that change nothing but the mtime, so that N such updates cost one
// transaction instead of N.
//
// Measured motivation: every index commit costs ~74-91 KB of WAL (~20
// B-tree pages) regardless of what changed (internal/db/sqlite
// amplification harness, bcf11ca04 + 189911571), so a daemon that rewrites
// a state file with identical content once a minute still produces a full
// ~80 KB transaction per rewrite. Drip mode (patch/46) only engages after
// detecting a burst (3 commits in 60 s); a sub-threshold stream of
// mtime-only bumps passes straight through. Content-identical updates are
// known-zero-information at scan time (the hash matches), so unlike drip
// they need no burst detection: they are always safe to hold.
//
// Held updates are last-wins per path and flush on three triggers:
// piggyback on the next real scan-side commit (one shared transaction, the
// common case), a latency bound derived from the same watch-aggregation
// staleness budget as drip (watchaggregator.NotifyTimeout, 60 s default —
// not invented), and folder shutdown. A hard cap bounds memory.
//
// Scope: scanner-committed updates only (commitLocalsFromScanning).
// Pull-side metadata application (shortcutFile) is NOT held: the puller's
// convergence bookkeeping compares index versions, and holding a pulled
// version would make the file perpetually "needed" (re-pull loop) and the
// folder perpetually out-of-sync until flush. Receiver-side churn from
// announced mtime bumps is a sender-side problem (the announcement itself
// must be held), out of scope here.
//
// Ordering: when a real update for a held path commits, the pending
// metadata-only entry is dropped, not piggybacked — committing the older
// mtime alongside the newer version would regress the index.
//
// Env: STMETABATCH=0 disables; STMETABATCHWINDOWS overrides the latency
// bound (seconds); STMETABATCHCAP overrides the pending-map cap.

package model

import (
	"os"
	"strconv"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/watchaggregator"
)

// metaBatchAnyModTime makes the equivalence check ignore mtime differences:
// everything else (size, blocks, type, perms, ownership, xattrs, symlink
// target) must still match exactly for an update to classify as
// metadata-only.
const metaBatchAnyModTime = 100 * 365 * 24 * time.Hour

type metaBatchParams struct {
	enabled bool
	window  time.Duration // latency bound: flush when oldest pending reaches this age
	cap     int           // hard flush when the pending map reaches this size
}

type metaBatchHeld struct {
	fi        protocol.FileInfo
	firstHeld time.Time
	coalesced int // updates collapsed into this entry, incl. the first
}

// metaBatchTracker is not internally synchronized; the folder Serve
// goroutine is the only caller (same call graph as the drip tracker).
type metaBatchTracker struct {
	params metaBatchParams
	now    func() time.Time

	pending map[string]*metaBatchHeld
	oldest  time.Time // firstHeld of the oldest pending entry; zero when empty

	logf func(msg string, args ...any)
}

func defaultMetaBatchParams(cfg config.FolderConfiguration) metaBatchParams {
	delayS := cfg.FSWatcherDelayS
	if delayS <= 0 {
		delayS = 10 // config default
	}
	p := metaBatchParams{
		enabled: true,
		window:  watchaggregator.NotifyTimeout(delayS), // 60 s default
		cap:     1000,
	}
	// Receive-only and receive-encrypted folders are excluded: a local
	// delta there is an aberration queued for revert, and holding it would
	// delay the repair bookkeeping (TestRecvOnly*), while the steady-state
	// mtime-churn cost this patch targets lives on normal folders.
	if cfg.Type == config.FolderTypeReceiveOnly || cfg.Type == config.FolderTypeReceiveEncrypted {
		p.enabled = false
	}
	if os.Getenv("STMETABATCH") == "0" {
		p.enabled = false
	}
	if v, err := strconv.ParseFloat(os.Getenv("STMETABATCHWINDOWS"), 64); err == nil && v > 0 {
		p.window = time.Duration(v * float64(time.Second))
	}
	if v, err := strconv.Atoi(os.Getenv("STMETABATCHCAP")); err == nil && v > 0 {
		p.cap = v
	}
	return p
}

func newMetaBatchTracker(cfg config.FolderConfiguration, now func() time.Time, logf func(string, ...any)) *metaBatchTracker {
	return &metaBatchTracker{
		params:  defaultMetaBatchParams(cfg),
		now:     now,
		pending: make(map[string]*metaBatchHeld),
		logf:    logf,
	}
}

// metadataOnlyUpdate reports whether fi differs from the current index
// entry cur only in mtime. The comparison is the walker's own strict
// equivalence with the mtime window opened up; blocks are compared via
// BlocksHash inside BlocksEqual, so content must be hash-identical.
func metadataOnlyUpdate(cur, fi protocol.FileInfo, comp protocol.FileInfoComparison) bool {
	if fi.IsDirectory() || fi.Deleted || fi.IsInvalid() {
		return false
	}
	comp.ModTimeWindow = metaBatchAnyModTime
	return cur.IsEquivalentOptional(fi, comp)
}

// hold records an mtime-only update, last version wins.
func (m *metaBatchTracker) hold(fi protocol.FileInfo) {
	now := m.now()
	if h, ok := m.pending[fi.Name]; ok {
		h.fi = fi
		h.coalesced++
		return
	}
	m.pending[fi.Name] = &metaBatchHeld{fi: fi, firstHeld: now, coalesced: 1}
	if len(m.pending) == 1 {
		m.oldest = now
	}
}

// nextIn reports when the oldest pending entry reaches the latency bound.
func (m *metaBatchTracker) nextIn() (time.Duration, bool) {
	if len(m.pending) == 0 {
		return 0, false
	}
	d := m.window() - m.now().Sub(m.oldest)
	if d < 0 {
		d = 0
	}
	return d, true
}

func (m *metaBatchTracker) window() time.Duration { return m.params.window }

func (m *metaBatchTracker) len() int { return len(m.pending) }

// due reports whether the batch should flush now: latency bound reached or
// cap hit.
func (m *metaBatchTracker) due() bool {
	if len(m.pending) == 0 {
		return false
	}
	return m.now().Sub(m.oldest) >= m.params.window || len(m.pending) >= m.params.cap
}

// drainExcept removes and returns all pending entries except those named in
// skip (a real update for those paths is committing in the same batch, so
// the pending older-mtime entry is dropped to avoid index regression).
// Dropped entries are still logged as coalesced.
func (m *metaBatchTracker) drainExcept(skip map[string]struct{}, reason string) []protocol.FileInfo {
	if len(m.pending) == 0 {
		return nil
	}
	out := make([]protocol.FileInfo, 0, len(m.pending))
	now := m.now()
	for name, h := range m.pending {
		_, superseded := skip[name]
		if m.logf != nil {
			r := reason
			if superseded {
				r = "superseded"
			}
			m.logf("Metabatch flush: metadata-only updates committed", "path", name,
				"coalesced", h.coalesced, "heldS", now.Sub(h.firstHeld).Seconds(), "reason", r)
		}
		if !superseded {
			out = append(out, h.fi)
		}
		delete(m.pending, name)
	}
	return out
}

// flushDue drains everything when due() holds.
func (m *metaBatchTracker) flushDue() []protocol.FileInfo {
	if !m.due() {
		return nil
	}
	return m.drainExcept(nil, "window")
}

// flushAll drains everything (folder shutdown).
func (m *metaBatchTracker) flushAll() []protocol.FileInfo {
	return m.drainExcept(nil, "shutdown")
}
