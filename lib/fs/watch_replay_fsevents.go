// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build darwin && !kqueue && cgo && !ios

package fs

import (
	"errors"
	"fmt"

	"github.com/syncthing/notify"
)

// errWatchJournalIncomplete is delivered on the watch error channel when
// FSEvents reports the stream itself is broken (the watched root changed
// identity or journal IDs wrapped). Dropped-event flags do not arrive here:
// they are healed in place by a full-scan event (see watchOverflowMask). The
// watch is torn down; the consumer must do a full scan to re-establish a
// complete picture.
var errWatchJournalIncomplete = errors.New("filesystem journal does not cover all changes, full scan required")

// watchEventClass is the watch loop's verdict for a backend failure-flag
// event.
type watchEventClass int

const (
	watchEventOK watchEventClass = iota
	// watchEventOverflow means events were lost but the stream is alive:
	// heal by scanning the whole folder and keep watching (the same course
	// the inotify backend takes on queue overflow).
	watchEventOverflow
	// watchEventFatalClass means the stream can no longer be trusted at
	// all: report an error and tear the watch down.
	watchEventFatalClass
)

// watchOverflowMask are FSEvents flags meaning "some events were dropped,
// rescan, the stream continues". Per Apple's documentation the stream keeps
// delivering after all of these; the missed changes are healed by a full
// scan. MustScanSubDirs is in this group: it means "events were dropped,
// rescan everything below this path", both when a seeded journal position is
// not fully covered and when a live stream's delivery overflows (observed on
// a busy volume, 2026-10-08: an unseeded stream flagged it minutes into
// watching while fseventsd was overloaded). A full scan covers the gap in
// both cases, and watermarks only advance across scanned batches, so no
// persisted position can skip the gap afterwards.
const watchOverflowMask = notify.FSEventsMustScanSubDirs |
	notify.FSEventsUserDropped |
	notify.FSEventsKernelDropped

// watchFatalMask are FSEvents flags meaning the stream itself is
// untrustworthy: the watched root changed identity, or journal IDs are no
// longer comparable. (After an ID wrap the arming check in the model -
// persisted position <= current journal position - can never pass, so no
// stale watermark survives it either.)
const watchFatalMask = notify.FSEventsEventIdsWrapped |
	notify.FSEventsRootChanged

// watchEventClassify classifies a backend event. On a fatal verdict the
// journal replay seed is reset as a side effect, so the restarted stream
// does not replay the same failure in a loop.
func watchEventClassify(ev notify.EventInfo) watchEventClass {
	flags := ev.Event()
	switch {
	case flags&watchFatalMask != 0:
		ResetWatchReplay()
		return watchEventFatalClass
	case flags&watchOverflowMask != 0:
		return watchEventOverflow
	}
	return watchEventOK
}

// fatalEventError renders the error reported for a fatal backend event.
func fatalEventError(ev notify.EventInfo) error {
	return fmt.Errorf("%w (fsevents flags: %s)", errWatchJournalIncomplete, ev.Event())
}

// WatchReplaySupported reports whether this build can resume watching from a
// persisted journal position instead of scanning the whole tree at startup.
func WatchReplaySupported() bool { return true }

// SetWatchReplaySince seeds the journal position for watchers started after
// the call. The seed only moves backwards (minimum wins), so several folders
// calling with their own persisted positions cannot lose each other's
// events. Must be called before the corresponding Watch.
func SetWatchReplaySince(id uint64) { notify.SetSinceEventID(id) }

// ResetWatchReplay abandons replay: subsequently started watchers begin at
// the current journal position.
func ResetWatchReplay() { notify.ResetSinceEventID() }

// CurrentWatchEventID returns the journal's current position. A persisted
// position greater than this means the journal was reset (e.g. the volume
// was reformatted) and the persisted value is meaningless.
func CurrentWatchEventID() uint64 { return notify.CurrentEventID() }
