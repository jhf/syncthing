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
// FSEvents reports that its journal cannot account for every change (the
// seeded position was trimmed, events were dropped, IDs wrapped, or the
// watched root changed). The watch is torn down; the consumer must do a full
// scan to re-establish a complete picture.
var errWatchJournalIncomplete = errors.New("filesystem journal does not cover all changes, full scan required")

// watchFailureMask are FSEvents flags that mean "the event stream is no
// longer a complete account of what changed". They are requested as part of
// the event mask (basicfs_watch_eventtypes_darwin.go) and intercepted in the
// watch loop before regular event translation.
const watchFailureMask = notify.FSEventsMustScanSubDirs |
	notify.FSEventsUserDropped |
	notify.FSEventsKernelDropped |
	notify.FSEventsEventIdsWrapped |
	notify.FSEventsRootChanged

// watchEventFatal classifies a backend event: a non-nil return means the
// stream can no longer be trusted and the watch must be restarted with a
// full scan. Journal replay is reset as a side effect, so the restarted
// stream does not replay the same failure in a loop.
func watchEventFatal(ev notify.EventInfo) error {
	if ev.Event()&watchFailureMask == 0 {
		return nil
	}
	ResetWatchReplay()
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
