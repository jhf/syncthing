// Copyright (C) 2021 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build darwin && !kqueue && cgo && !ios

package fs

import "github.com/syncthing/notify"

const (
	// The watchFailureMask flags are requested so that an incomplete
	// journal (trimmed history, dropped events, ID wrap, root change) is
	// surfaced to the watch loop instead of being silently dropped by the
	// backend; they are intercepted before regular event translation and
	// turn into a watch error plus a full scan, never into fs.Events.
	subEventMask = notify.Create | notify.Remove | notify.Write | notify.Rename | notify.FSEventsInodeMetaMod | notify.FSEventsXattrMod |
		notify.FSEventsMustScanSubDirs | notify.FSEventsUserDropped | notify.FSEventsKernelDropped | notify.FSEventsEventIdsWrapped | notify.FSEventsRootChanged
	// FSEventsChangeOwner fires on permission change
	permEventMask = notify.FSEventsChangeOwner
	rmEventMask   = notify.Remove | notify.Rename
)
