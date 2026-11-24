// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !(solaris && !cgo) && !(darwin && !cgo) && !(darwin && kqueue)
// +build !solaris cgo
// +build !darwin cgo
// +build !darwin !kqueue

package fs

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/syncthing/notify"
)

// Notify does not block on sending to channel, so the channel must be buffered.
// 500 entries is a few seconds of headroom under burst load; while a long
// scan blocks the aggregator's flush, bursts overflowed this and each
// overflow forced a full-folder rescan (observed as a self-sustaining rescan
// loop, 2026-10-10). 65536 (1 MB) matches the notify fork's tree buffer.
// The watch pump (watch_backlog.go) drains it into a growable backlog, so it
// only has to absorb the pump's scheduling latency.
// Not meant to be changed, but must be changeable for tests
var backendBuffer = 65536

func (f *BasicFilesystem) Watch(name string, ignore Matcher, ctx context.Context, ignorePerms bool) (<-chan Event, <-chan error, error) {
	watchPath, roots, err := f.watchPaths(name)
	if err != nil {
		return nil, nil, err
	}
	RegisterWatchEscalationMetrics(f.URI())

	outChan := make(chan Event)
	backendChan := make(chan notify.EventInfo, backendBuffer)

	eventMask := subEventMask
	if !ignorePerms {
		eventMask |= permEventMask
	}

	absShouldIgnore := func(absPath string) bool {
		if !utf8.ValidString(absPath) {
			return true
		}

		rel, err := f.unrootedChecked(absPath, roots)
		if err != nil {
			return true
		}
		return ignore.Match(rel).CanSkipDir()
	}
	err = notify.WatchWithFilter(watchPath, backendChan, absShouldIgnore, eventMask)
	if err != nil {
		notify.Stop(backendChan)
		if reachedMaxUserWatches(err) {
			err = errors.New("failed to set up inotify handler. Please increase inotify limits, see https://docs.syncthing.net/users/faq.html#inotify-limits")
		}
		return nil, nil, err
	}

	errChan := make(chan error)
	go f.watchLoop(ctx, name, roots, backendChan, outChan, errChan, ignore)

	return outChan, errChan, nil
}

func (f *BasicFilesystem) watchLoop(ctx context.Context, name string, roots []string, backendChan chan notify.EventInfo, outChan chan<- Event, errChan chan<- error, ignore Matcher) {
	// The pump keeps the backend channel empty while this loop is blocked on
	// a slow consumer; see watch_backlog.go.
	backlog := newWatchBacklog(f.URI())
	pumpCtx, stopPump := context.WithCancel(ctx)
	defer stopPump()
	go f.watchPump(pumpCtx, name, roots, backendChan, backlog, ignore)
	// Raw events held behind a drained overflow set (see overflowBatch).
	var after []notify.EventInfo

	for {
		var ev notify.EventInfo
		if len(after) > 0 {
			ev, after = after[0], after[1:]
		} else {
			var over *overflowBatch
			var ok bool
			ev, over, ok = backlog.pop()
			if !ok {
				select {
				case <-backlog.signal:
					continue
				case <-ctx.Done():
					notify.Stop(backendChan)
					l.Debugln(f.Type(), f.URI(), "Watch: Stopped")
					return
				}
			}
			if over != nil {
				if !f.emitOverflowSet(ctx, name, over, outChan) {
					notify.Stop(backendChan)
					return
				}
				after = over.after
				continue
			}
		}

		// Backend failure-flag events (e.g. FSEvents journal problems)
		// are intercepted before the path checks below - they can
		// carry the watched root or an otherwise ignorable path.
		// Overflow means events were lost but the stream is alive:
		// heal by rescanning the flagged subtree (the watched root for
		// a dropped-events flag) and keep watching, like the inotify
		// queue overflow path. Fatal means the stream cannot be
		// trusted: report an error and tear the watch down.
		switch class := watchEventClassify(ev); class {
		case watchEventOverflow:
			scope := f.dropFlagScope(ev, name, roots)
			if scope == name {
				l.Debugln(f.Type(), f.URI(), "Watch: Backend dropped events, sending", name)
				NoteWatchRootEscalation(f.URI(), WatchEscalationBackendDropped, "path", ev.Path(), "event", ev.Event().String())
			}
			select {
			case outChan <- Event{Name: scope, Type: NonRemove}:
			case <-ctx.Done():
				notify.Stop(backendChan)
				return
			}
			continue
		case watchEventFatalClass:
			fatalErr := fatalEventError(ev)
			select {
			case errChan <- fatalErr:
				l.Debugln(f.Type(), f.URI(), "Watch: Sending fatal error", fatalErr)
			case <-ctx.Done():
			}
			notify.Stop(backendChan)
			return
		}

		evPath := ev.Path()

		if !utf8.ValidString(evPath) {
			l.Debugln(f.Type(), f.URI(), "Watch: Ignoring invalid UTF-8")
			continue
		}

		relPath, err := f.unrootedChecked(evPath, roots)
		if err != nil {
			select {
			case errChan <- err:
				l.Debugln(f.Type(), f.URI(), "Watch: Sending error", err)
			case <-ctx.Done():
			}
			notify.Stop(backendChan)
			l.Debugln(f.Type(), f.URI(), "Watch: Stopped due to", err)
			return
		}

		if ignore.Match(relPath).IsIgnored() && !IsWatchBarrierCanary(relPath) {
			l.Debugln(f.Type(), f.URI(), "Watch: Ignoring", relPath)
			continue
		}
		evType := f.eventType(ev.Event())
		select {
		case outChan <- Event{Name: relPath, Type: evType, ID: eventID(ev)}:
			l.Debugln(f.Type(), f.URI(), "Watch: Sending", relPath, evType)
		case <-ctx.Done():
			notify.Stop(backendChan)
			l.Debugln(f.Type(), f.URI(), "Watch: Stopped")
			return
		}
	}
}

func (*BasicFilesystem) eventType(notifyType notify.Event) EventType {
	if notifyType&rmEventMask != 0 {
		return Remove
	}
	return NonRemove
}
