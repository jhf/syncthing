// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"testing"
)

func TestBatchSyncfsGating(t *testing.T) {
	_, f := setupSendReceiveFolder(t)

	if got := f.batchSyncfs(); got != syncfsSupported {
		t.Errorf("default: batchSyncfs() = %v, want %v (platform support)", got, syncfsSupported)
	}

	f.DisableFsync = true
	if f.batchSyncfs() {
		t.Error("DisableFsync: batchSyncfs() = true, want false")
	}
	f.DisableFsync = false

	t.Setenv("STSYNCMODE", "fsync")
	if f.batchSyncfs() {
		t.Error("STSYNCMODE=fsync: batchSyncfs() = true, want false")
	}
}

func TestFlushPullDurabilitySelectsSyncfs(t *testing.T) {
	if !syncfsSupported {
		t.Skip("syncfs not supported on this platform")
	}
	_, f := setupSendReceiveFolder(t)

	var syncfsCalls int
	var syncfsRoot string
	var syncfsErr error
	old := syncfsFn
	defer func() { syncfsFn = old }()
	syncfsFn = func(root string) error {
		syncfsCalls++
		syncfsRoot = root
		return syncfsErr
	}

	changedDirs := map[string]struct{}{".": {}}

	// Default: one syncfs of the folder root, no per-directory fsyncs.
	f.flushPullDurability(changedDirs)
	if syncfsCalls != 1 {
		t.Fatalf("syncfs calls = %d, want 1", syncfsCalls)
	}
	if want := f.Filesystem().URI(); syncfsRoot != want {
		t.Errorf("syncfs root = %q, want %q", syncfsRoot, want)
	}

	// syncfs failure falls back to per-directory fsyncs without erroring.
	syncfsErr = errors.New("boom")
	f.flushPullDurability(changedDirs)
	if syncfsCalls != 2 {
		t.Fatalf("syncfs calls = %d, want 2", syncfsCalls)
	}

	// Kill switch: per-directory fsyncs only.
	t.Setenv("STSYNCMODE", "fsync")
	f.flushPullDurability(changedDirs)
	if syncfsCalls != 2 {
		t.Fatalf("syncfs calls = %d, want 2 (STSYNCMODE=fsync)", syncfsCalls)
	}

	// DisableFsync: no durability operations at all.
	f.DisableFsync = true
	f.flushPullDurability(changedDirs)
	if syncfsCalls != 2 {
		t.Fatalf("syncfs calls = %d, want 2 (DisableFsync)", syncfsCalls)
	}
}
