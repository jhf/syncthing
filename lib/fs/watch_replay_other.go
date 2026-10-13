// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !darwin || kqueue || !cgo || ios

package fs

import (
	"time"

	"github.com/syncthing/notify"
)

// watchEventClass is the watch loop's verdict for a backend failure-flag
// event; only the FSEvents backend produces non-OK classes.
type watchEventClass int

const (
	watchEventOK watchEventClass = iota
	watchEventOverflow
	watchEventFatalClass
)

// watchEventClassify on platforms without a persistent journal never
// classifies an event as noteworthy; completeness gaps are handled by
// platform-specific means (e.g. the inotify queue overflow path injects a
// "." event).
func watchEventClassify(notify.EventInfo) watchEventClass { return watchEventOK }

// fatalEventError is never reached without journal support.
func fatalEventError(notify.EventInfo) error { return nil }

// WatchReplaySupported is false: only the FSEvents backend has a persistent
// journal to resume from.
func WatchReplaySupported() bool { return false }

// SetWatchReplaySince is a no-op without journal support.
func SetWatchReplaySince(uint64) {}

// ResetWatchReplay is a no-op without journal support.
func ResetWatchReplay() {}

// CurrentWatchEventID returns zero without journal support.
func CurrentWatchEventID() uint64 { return 0 }

// WatchReplayArmed is false without journal support.
func WatchReplayArmed() bool { return false }

// SetWatchPollOnly is a no-op without journal support.
func SetWatchPollOnly(time.Duration) {}
