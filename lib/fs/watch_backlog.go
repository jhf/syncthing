// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !(solaris && !cgo) && !(darwin && !cgo) && !(darwin && kqueue)
// +build !solaris cgo
// +build !darwin cgo
// +build !darwin !kqueue

package fs

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/syncthing/notify"
)

// The watch backlog sits between the notify backend channel and the event
// translation loop. A pump goroutine does nothing but move events from the
// backend channel into an in-memory queue, so the backend never sees a full
// channel while the consumer is slow: notify drops on a full channel, and
// every drop used to cost a full-folder scan (measured 2026-11-24 on a
// 3.16M-file home tree: the boot journal replay pushed 69,773 events into
// the 65,536-entry channel 2 s after start, and a poll replay overflowed it
// again 11 min later; each was a full walk plus a full deletion sweep).
//
// Memory is spent for time: about 160 B per queued event (measured, 89-char
// path), so the boot limit of 4Mi events is ~640 MiB and the steady limit of
// 1Mi is ~160 MiB, held only while events are actually queued - the queue's
// backing array is released when it drains. The boot window covers the
// journal replay after a restart, when the backlog is largest.
//
// Past the limit nothing is dropped either: new events are absorbed into a
// BoundedPathSet, which widens paths to their ancestors to stay bounded,
// and is emitted as directory scans once the queue ahead of it has drained.
// Only when even the top-level directories exceed the set's limit does the
// backlog escalate to the folder root.
var (
	watchBacklogBootLimit   = 4 << 20
	watchBacklogSteadyLimit = 1 << 20
	watchBacklogBootWindow  = 30 * time.Minute
	// watchBacklogCoarseLimit bounds the overflow path set. Half the
	// aggregator's distinct-path window (512), so an emitted overflow set
	// never overflows the aggregator on its own.
	watchBacklogCoarseLimit = 256
	// watchBacklogLogThreshold is the peak queue length from which a
	// drained backlog episode logs one INFO line.
	watchBacklogLogThreshold = 65536
	// watchBacklogKeepCap is the largest backing array kept across drains.
	watchBacklogKeepCap = 4096
)

var (
	metricWatchBacklogEvents = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "syncthing",
		Subsystem: "fs",
		Name:      "watch_backlog_events",
		Help:      "Number of watch events queued between the notify backend and the event translation loop, per filesystem root",
	}, []string{"root"})
	metricWatchBacklogCoarsened = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "syncthing",
		Subsystem: "fs",
		Name:      "watch_backlog_coarsened_total",
		Help:      "Total number of watch backlog overflow episodes emitted as directory scans instead of individual events, per filesystem root",
	}, []string{"root"})
)

// watchBacklogLog emits backlog INFO lines. Variable for tests.
var watchBacklogLog = func(msg string, args ...any) {
	slog.Info(msg, args...)
}

type watchBacklog struct {
	root    string
	started time.Time
	gauge   prometheus.Gauge

	mu   sync.Mutex
	q    []notify.EventInfo
	head int
	// over is non-nil while new events are absorbed into the overflow set
	// rather than queued.
	over       *BoundedPathSet
	overEvents int
	overMaxID  uint64
	// overRootNoted is set when the set's root collapse was already counted
	// as an escalation where it originated (lost events, a drop flag).
	overRootNoted bool
	// overAfter holds the events that arrived during overflow but must
	// reach the translation loop themselves (barrier canaries, fatal flags,
	// paths outside the root). They are processed after the set's scan
	// events, preserving FIFO: a barrier canary observed after the set
	// still implies every earlier event was handed on.
	overAfter    []notify.EventInfo
	peak         int
	episodeStart time.Time
	signal       chan struct{}
}

func newWatchBacklog(root string) *watchBacklog {
	return &watchBacklog{
		root:    root,
		started: time.Now(),
		gauge:   metricWatchBacklogEvents.WithLabelValues(root),
		signal:  make(chan struct{}, 1),
	}
}

func (b *watchBacklog) limit() int {
	if time.Since(b.started) < watchBacklogBootWindow {
		return watchBacklogBootLimit
	}
	return watchBacklogSteadyLimit
}

func (b *watchBacklog) wake() {
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *watchBacklog) queued() int { return len(b.q) - b.head }

// pushRaw queues ev regardless of the limit.
func (b *watchBacklog) pushRaw(ev notify.EventInfo) {
	if b.queued() == 0 {
		b.episodeStart = time.Now()
	}
	b.q = append(b.q, ev)
	if n := b.queued(); n > b.peak {
		b.peak = n
	}
	if n := b.queued(); n&4095 == 0 {
		b.gauge.Set(float64(n))
	}
}

// enterOverflow starts an overflow episode if none is running.
func (b *watchBacklog) enterOverflow() {
	if b.over == nil {
		b.over = NewBoundedPathSet(watchBacklogCoarseLimit)
		b.overEvents, b.overMaxID, b.overRootNoted, b.overAfter = 0, 0, false, nil
	}
}

// absorb adds a path to the overflow set, entering overflow mode if needed.
func (b *watchBacklog) absorb(rel string, id uint64) {
	b.enterOverflow()
	b.over.Add(rel)
	b.overEvents++
	if id > b.overMaxID {
		b.overMaxID = id
	}
}

// overflowBatch is a drained overflow set with its bookkeeping.
type overflowBatch struct {
	set    *BoundedPathSet
	events int
	maxID  uint64
	// rootNoted: a root collapse was already counted as an escalation
	// where it originated (lost events, a root drop flag).
	rootNoted bool
	// after: raw events to process once the set's scan events are out.
	after []notify.EventInfo
}

// pop returns the next queued event, or the overflow set once the queue
// ahead of it is empty. ok is false when there is nothing to process.
func (b *watchBacklog) pop() (ev notify.EventInfo, over *overflowBatch, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queued() > 0 {
		ev = b.q[b.head]
		b.q[b.head] = nil
		b.head++
		switch n := b.queued(); {
		case n == 0:
			if cap(b.q) > watchBacklogKeepCap {
				b.q = nil
			} else {
				b.q = b.q[:0]
			}
			b.head = 0
			b.gauge.Set(0)
			if b.peak >= watchBacklogLogThreshold {
				watchBacklogLog("Watch backlog drained", "root", b.root, "peak", b.peak, "duration", time.Since(b.episodeStart))
			}
			b.peak = 0
		case b.head >= 1<<16 && b.head >= n:
			// Release the consumed half of the backing array.
			b.q = slices.Clone(b.q[b.head:])
			b.head = 0
			b.gauge.Set(float64(n))
		}
		return ev, nil, true
	}
	if b.over != nil {
		over = &overflowBatch{set: b.over, events: b.overEvents, maxID: b.overMaxID, rootNoted: b.overRootNoted, after: b.overAfter}
		b.over, b.overAfter = nil, nil
		return nil, over, true
	}
	return nil, nil, false
}

// watchPump moves events from the backend channel into the backlog until
// ctx is cancelled. It must stay cheap: every cycle it spends is a cycle
// the backend channel fills.
func (f *BasicFilesystem) watchPump(ctx context.Context, name string, roots []string, backendChan chan notify.EventInfo, b *watchBacklog, ignore Matcher) {
	for {
		// A full channel means the backend may have dropped events (notify
		// does not block on a full channel), so their paths are unknown.
		lost := len(backendChan) == backendBuffer
		select {
		case ev := <-backendChan:
			b.mu.Lock()
			if lost {
				NoteWatchRootEscalation(f.URI(), WatchEscalationBufferOverflow, "queued", b.queued(), "buffer", backendBuffer)
				b.absorb(name, 0)
				b.overRootNoted = true
			}
			if b.over == nil && b.queued() < b.limit() {
				b.pushRaw(ev)
			} else {
				f.absorbEvent(ev, name, roots, b, ignore)
			}
			b.mu.Unlock()
			b.wake()
		case <-ctx.Done():
			return
		}
	}
}

// absorbEvent adds ev to the overflow set. Events the translation loop must
// see itself (fatal backend flags, paths outside the root, barrier
// canaries) are kept in order behind the set.
func (f *BasicFilesystem) absorbEvent(ev notify.EventInfo, name string, roots []string, b *watchBacklog, ignore Matcher) {
	b.enterOverflow()
	switch watchEventClassify(ev) {
	case watchEventFatalClass:
		b.overAfter = append(b.overAfter, ev)
		return
	case watchEventOverflow:
		rel := f.dropFlagScope(ev, name, roots)
		if isRootPath(rel) {
			NoteWatchRootEscalation(f.URI(), WatchEscalationBackendDropped, "path", ev.Path(), "event", ev.Event().String(), "backlog", true)
			b.absorb(rel, 0)
			b.overRootNoted = true
			return
		}
		b.absorb(rel, 0)
		return
	}
	evPath := ev.Path()
	if !utf8.ValidString(evPath) {
		return
	}
	rel, err := f.unrootedChecked(evPath, roots)
	if err != nil || IsWatchBarrierCanary(rel) {
		b.overAfter = append(b.overAfter, ev)
		return
	}
	if ignore.Match(rel).IsIgnored() {
		return
	}
	b.absorb(rel, eventID(ev))
}

// dropFlagScope returns the subtree a backend drop flag asks to rescan: the
// flagged path (FSEvents MustScanSubDirs means "rescan this directory and
// everything below it"), or the watched root when the path is unusable.
func (f *BasicFilesystem) dropFlagScope(ev notify.EventInfo, name string, roots []string) string {
	p := ev.Path()
	if p == "" || !utf8.ValidString(p) {
		return name
	}
	rel, err := f.unrootedChecked(p, roots)
	if err != nil || isRootPath(rel) {
		return name
	}
	return rel
}

// eventID returns the backend journal position of ev, when the backend has
// one (FSEvents). Probed via an anonymous interface so this shared file
// builds for backends whose Sys() has no such concept.
func eventID(ev notify.EventInfo) uint64 {
	if withID, ok := ev.Sys().(interface{ EventID() uint64 }); ok {
		return withID.EventID()
	}
	return 0
}

// emitOverflowSet sends the overflow set's paths as scan events. Every
// event carries the highest journal position absorbed, so the replay
// watermark advances only behind a scan covering the whole set.
func (f *BasicFilesystem) emitOverflowSet(ctx context.Context, name string, over *overflowBatch, outChan chan<- Event) bool {
	paths := over.set.Paths()
	if len(paths) == 0 {
		// Only held raw events (canaries, out-of-root paths) arrived.
		return true
	}
	metricWatchBacklogCoarsened.WithLabelValues(f.URI()).Inc()
	if over.set.Root() {
		if !over.rootNoted {
			NoteWatchRootEscalation(f.URI(), WatchEscalationBacklogOverflow, "events", over.events, "coarseLimit", watchBacklogCoarseLimit)
		}
		paths = []string{name}
	} else {
		watchBacklogLog("Watch backlog over its limit, scanning affected directories instead", "root", f.URI(), "events", over.events, "paths", len(paths), "depthCap", over.set.DepthCap(), "first", paths[0])
	}
	for _, p := range paths {
		select {
		case outChan <- Event{Name: p, Type: NonRemove, ID: over.maxID}:
		case <-ctx.Done():
			return false
		}
	}
	return true
}
