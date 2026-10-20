// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestShouldMigrateNoCOW(t *testing.T) {
	t.Parallel()
	cases := []struct {
		size, avail int64
		want        bool
	}{
		{size: 1000, avail: 2000, want: true},  // normal case
		{size: 1000, avail: 1100, want: true},  // exactly size + 10%
		{size: 1000, avail: 1099, want: false}, // one byte short
		{size: 0, avail: 99999, want: false},   // nothing to copy
		{size: 1000, avail: 0, want: false},    // unknown/full fs
	}
	for _, c := range cases {
		if got := shouldMigrateNoCOW(c.size, c.avail); got != c.want {
			t.Errorf("shouldMigrateNoCOW(%d, %d) = %v, want %v", c.size, c.avail, got, c.want)
		}
	}
}

func TestCopyReplaceAll(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	walPath := dbPath + "-wal"

	dbContent := make([]byte, 9<<20) // 9 MiB, larger than the copy buffer
	rand.Read(dbContent)
	walContent := []byte("wal frames")
	if err := os.WriteFile(dbPath, dbContent, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(walPath, walContent, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := copyReplaceAll([]string{dbPath, walPath}); err != nil {
		t.Fatal(err)
	}

	// Both files migrated with content and modes intact, temps gone.
	gotDB, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotDB, dbContent) {
		t.Fatal("db content mismatch after copyReplaceAll")
	}
	gotWAL, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotWAL, walContent) {
		t.Fatal("wal content mismatch after copyReplaceAll")
	}
	fi, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("db mode = %o, want 640", fi.Mode().Perm())
	}
	for _, p := range []string{dbPath, walPath} {
		if _, err := os.Stat(p + ".nocow-migrate"); !os.IsNotExist(err) {
			t.Fatalf("temp file left behind for %s", p)
		}
	}
}

// A temp file from an interrupted earlier attempt must not block migration.
func TestCopyReplaceAllCleansStaleTemp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	content := []byte("the real database")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".nocow-migrate", []byte("partial junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := copyReplaceAll([]string{path}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch after stale-temp cleanup")
	}
}

// A write failure mid-set must remove all temps and leave originals intact.
func TestCopyReplaceAllFailureCleansUp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	content := []byte("original")
	if err := os.WriteFile(dbPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Second path does not exist: copy phase fails after the first temp
	// was already written.
	missing := filepath.Join(dir, "missing-wal")
	err := copyReplaceAll([]string{dbPath, missing})
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	got, rerr := os.ReadFile(dbPath)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("original modified after failed migration")
	}
	if _, serr := os.Stat(dbPath + ".nocow-migrate"); !os.IsNotExist(serr) {
		t.Fatal("temp file left behind after failed migration")
	}
}
