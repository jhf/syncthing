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

func btrfsTestTempDir(t *testing.T) string {
	t.Helper()
	// Prefer the subvolume root, fall back to the user's home (still on the
	// same btrfs subvolume; the test only needs same-subvol paths).
	for _, parent := range []string{"/home", os.Getenv("HOME")} {
		dir, err := os.MkdirTemp(parent, "btrfs-replay-test-")
		if err == nil {
			return dir
		}
	}
	t.Skip("cannot create test dir on the btrfs subvolume")
	return ""
}

// TestBtrfsScanDiff exercises the helper contract end to end on a real
// btrfs volume: set a baseline from a snapshot, change files, snapshot,
// diff baseline-vs-snapshot, promote via setbaseline, diff again. It is
// gated behind ST_BTRFS_TEST=1 because it OWNS the helper's baseline
// snapshot while it runs - never run it concurrently with a syncthing that
// uses the replay feature. Requires the helper + sudoers entry installed
// and a btrfs /home.
func TestBtrfsScanDiff(t *testing.T) {
	if os.Getenv("ST_BTRFS_TEST") != "1" {
		t.Skip("set ST_BTRFS_TEST=1 to run (takes over the helper baseline)")
	}
	if !BtrfsScanReplaySupported() {
		t.Skip("btrfs helper not installed")
	}

	dir := btrfsTestTempDir(t)
	defer os.RemoveAll(dir)
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	snap := func(name string) {
		t.Helper()
		if err := BtrfsScanSnapshot(ctx, name); err != nil {
			t.Fatalf("snap %s: %v", name, err)
		}
	}
	diff := func(name string) []string {
		t.Helper()
		paths, err := BtrfsScanDiffBaseline(ctx, name, root)
		if err != nil {
			t.Fatalf("diff-baseline %s: %v", name, err)
		}
		return paths
	}
	defer func() {
		for _, n := range []string{"sttest-a", "sttest-b", "sttest-c"} {
			_ = BtrfsScanRemoveSnapshot(ctx, n)
		}
	}()

	// Establish the baseline over the empty test dir.
	snap("sttest-a")
	if err := BtrfsScanSetBaseline(ctx, "sttest-a"); err != nil {
		t.Fatalf("setbaseline: %v", err)
	}

	// Changes of every flavour. deleted.txt is created AND deleted between
	// the two snapshots, so a state-based diff must NOT report it (this is
	// the whole reason for the shutdown pairing in folder_btrfs.go).
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

	snap("sttest-b")
	got := diff("sttest-b")
	for _, want := range []string{"created.txt", "sub dir"} {
		if !slices.Contains(got, want) {
			t.Errorf("diff missing %q (got %q)", want, got)
		}
	}
	if slices.Contains(got, "deleted.txt") {
		t.Errorf("diff reports create+delete-in-one-generation path %q", got)
	}
	for _, p := range got {
		if filepath.IsAbs(p) || strings.HasPrefix(p, "..") {
			t.Errorf("diff returned non-relative path %q", p)
		}
	}

	// Promote: after setbaseline with no further changes the diff must not
	// report the same paths again. (Other processes may legitimately touch
	// the dir; only assert absence of OUR settled paths.)
	if err := BtrfsScanSetBaseline(ctx, "sttest-b"); err != nil {
		t.Fatalf("second setbaseline: %v", err)
	}
	snap("sttest-c")
	got2 := diff("sttest-c")
	for _, settled := range []string{"created.txt", "sub dir"} {
		if slices.Contains(got2, settled) {
			t.Errorf("second diff re-reports settled path %q", settled)
		}
	}
	if err := BtrfsScanSetBaseline(ctx, "sttest-c"); err != nil {
		t.Fatalf("third setbaseline: %v", err)
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
	dir := btrfsTestTempDir(t)
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
