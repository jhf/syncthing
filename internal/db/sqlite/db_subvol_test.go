// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"os"
	"path/filepath"
	"testing"
)

// The rename choreography is filesystem-independent; injecting os.Mkdir
// as the subvolume creator exercises it on any platform.
func TestMigrateDirToSubvolume(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "index-v2")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"folder.a.db":     "data-a",
		"folder.a.db-wal": "wal-a",
		"main.db":         "main",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A subdirectory (index-v2/.tmp on a live system): btrfs cannot rename
	// directories across the subvolume boundary (EXDEV), so it is recreated
	// and its contents moved.
	if err := os.Mkdir(filepath.Join(dir, ".tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp", "scratch"), []byte("tmp"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := migrateDirToSubvolume(dir, func(p string) error { return os.Mkdir(p, 0o700) }); err != nil {
		t.Fatal(err)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != want {
			t.Errorf("%s: got %q, err %v; want %q", name, got, err, want)
		}
	}
	if got, err := os.ReadFile(filepath.Join(dir, ".tmp", "scratch")); err != nil || string(got) != "tmp" {
		t.Errorf(".tmp/scratch: got %q, err %v", got, err)
	}
	if _, err := os.Stat(dir + ".subvol-tmp"); !os.IsNotExist(err) {
		t.Error("temp subvolume dir left behind")
	}

	// Idempotent: a second run over the migrated directory is a no-op.
	if err := migrateDirToSubvolume(dir, func(p string) error { return os.Mkdir(p, 0o700) }); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "main.db")); string(got) != "main" {
		t.Error("second run lost data")
	}
}

func TestMigrateDirToSubvolumeCrashRecovery(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "index-v2")
	tmp := dir + ".subvol-tmp"

	// Crash between rmdir(dir) and rename(tmp, dir): dir gone, tmp
	// complete. Recovery is the rename alone.
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "folder.a.db"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateDirToSubvolume(dir, func(p string) error { return os.Mkdir(p, 0o700) }); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "folder.a.db")); err != nil || string(got) != "data" {
		t.Errorf("recovery: got %q, err %v", got, err)
	}

	// Crash mid-move: dir exists with a leftover file, tmp has the rest.
	// The retry must merge, not fail or recreate. (dir exists from the
	// recovery above.)
	if err := os.WriteFile(filepath.Join(dir, "b.db"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "a.db"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := migrateDirToSubvolume(dir, func(string) error {
		t.Fatal("create must not run when tmp already exists")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"a.db", "b.db"} {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("merge lost %s: %v", n, err)
		}
	}
}
