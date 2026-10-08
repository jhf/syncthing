// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !darwin || kqueue || !cgo || ios

package fs

import "github.com/syncthing/notify"

// watchEventFatal on platforms without a persistent journal never classifies
// an event as fatal; completeness gaps are handled by platform-specific
// means (e.g. the inotify queue overflow path injects a "." event).
func watchEventFatal(notify.EventInfo) error { return nil }

// WatchReplaySupported is false: only the FSEvents backend has a persistent
// journal to resume from.
func WatchReplaySupported() bool { return false }

// SetWatchReplaySince is a no-op without journal support.
func SetWatchReplaySince(uint64) {}

// ResetWatchReplay is a no-op without journal support.
func ResetWatchReplay() {}

// CurrentWatchEventID returns zero without journal support.
func CurrentWatchEventID() uint64 { return 0 }
