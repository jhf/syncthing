// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package fs

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestBtrfsScanDiff exercises the helper contract end to end on a real btrfs
// volume: refresh a baseline, change files, diff, promote, diff again. It is
// gated behind ST_BTRFS_TEST=1 because it OWNS the helper's baseline snapshot
// while it runs - never run it concurrently with a syncthing that uses the
// replay feature. Requires the helper + sudoers entry installed and a btrfs
// /home.
func TestBtrfsScanDiff(t *testing.T) {
	if os.Getenv("ST_BTRFS_TEST") != "1" {
		t.Skip("set ST_BTRFS_TEST=1 to run (takes over the helper baseline)")
	}
	if !BtrfsScanReplaySupported() {
		t.Skip("btrfs helper not installed")
	}

	dir, err := os.MkdirTemp("/home", "btrfs-replay-test-")
	if err != nil {
		t.Skipf("cannot create test dir on /home: %v", err)
	}
	defer os.RemoveAll(dir)
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Baseline, then changes of every flavour.
	if err := BtrfsScanRefresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "created.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "deleted.txt"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "sub dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "deleted.txt")); err != nil {
		t.Fatal(err)
	}

	got, err := BtrfsScanDiff(ctx, root)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	for _, want := range []string{"created.txt", "deleted.txt", "sub dir"} {
		if !slices.Contains(got, want) {
			t.Errorf("diff missing %q (got %q)", want, got)
		}
	}
	for _, p := range got {
		if filepath.IsAbs(p) || strings.HasPrefix(p, "..") {
			t.Errorf("diff returned non-relative path %q", p)
		}
	}

	if err := BtrfsScanPromote(ctx); err != nil {
		t.Fatalf("promote: %v", err)
	}

	// After promotion with no further changes the diff must not report the
	// same paths again. (Other processes may legitimately touch the dir;
	// only assert absence of OUR settled paths.)
	got2, err := BtrfsScanDiff(ctx, root)
	if err != nil {
		t.Fatalf("second diff: %v", err)
	}
	for _, settled := range []string{"created.txt", "deleted.txt", "sub dir"} {
		if slices.Contains(got2, settled) {
			t.Errorf("second diff re-reports settled path %q", settled)
		}
	}
	if err := BtrfsScanPromote(ctx); err != nil {
		t.Fatalf("second promote: %v", err)
	}
}

// TestBtrfsScanNestedSubvols verifies the nested-subvolume detection that
// keeps replay from arming when coverage would silently stop at a subvolume
// boundary. Same baseline-ownership caveat as TestBtrfsScanDiff.
func TestBtrfsScanNestedSubvols(t *testing.T) {
	if os.Getenv("ST_BTRFS_TEST") != "1" {
		t.Skip("set ST_BTRFS_TEST=1 to run")
	}
	if !BtrfsScanReplaySupported() {
		t.Skip("btrfs helper not installed")
	}
	dir, err := os.MkdirTemp("/home", "btrfs-nested-test-")
	if err != nil {
		t.Skipf("cannot create test dir on /home: %v", err)
	}
	defer os.RemoveAll(dir)
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	nested, err := BtrfsScanNestedSubvols(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(nested) != 0 {
		t.Fatalf("fresh dir reported nested subvolumes: %q", nested)
	}
}
