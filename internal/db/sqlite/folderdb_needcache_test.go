// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"slices"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestNeedCountCache(t *testing.T) {
	t.Parallel()

	devA := protocol.DeviceID{1}
	devB := protocol.DeviceID{2}
	first := db.Counts{Files: 1}
	second := db.Counts{Files: 2}
	now := time.Unix(0, 0)

	c := new(needCountCache)

	if _, ok := c.get(devA, now); ok {
		t.Fatal("empty cache returned a value")
	}

	c.put(devA, first, now)

	// Without an invalidation the value holds.
	if got, ok := c.get(devA, now.Add(time.Second)); !ok || got.Files != first.Files {
		t.Fatalf("uninvalidated entry: got %v ok=%v", got, ok)
	}

	c.invalidate()

	// Immediately after invalidation the old value is still served, so that a
	// burst of writes coalesces into one recomputation.
	if got, ok := c.get(devA, now.Add(time.Second)); !ok || got.Files != first.Files {
		t.Fatalf("within the floor the old value should still be served: got %v ok=%v", got, ok)
	}

	// Past the floor it must be recomputed.
	if _, ok := c.get(devA, now.Add(needCountCacheMinRecompute)); ok {
		t.Fatal("past the floor an invalidated entry should miss")
	}

	// Invalidating because of one device invalidates all of them: another
	// device's index can change what this device is counted as needing.
	c.put(devB, first, now)
	c.invalidate()
	if _, ok := c.get(devB, now.Add(needCountCacheMinRecompute)); ok {
		t.Fatal("a write must invalidate every device's entry, not just one")
	}

	// An entry is dropped once it is old enough even if nothing invalidated it.
	c.put(devA, first, now)
	if _, ok := c.get(devA, now.Add(needCountCacheTTL)); ok {
		t.Fatal("an entry past its TTL should miss")
	}

	// Newer values replace older ones and clear the invalid flag.
	c.put(devA, second, now.Add(needCountCacheMinRecompute))
	if got, ok := c.get(devA, now.Add(needCountCacheMinRecompute)); !ok || got.Files != second.Files {
		t.Fatalf("replacement: got %v ok=%v", got, ok)
	}
}

// TestCountNeedCacheInvalidation exercises the cache through the real query: the
// value must be cached, and any write that can change the files table must make
// the next read recompute, including a write made on behalf of another device.
func TestCountNeedCacheInvalidation(t *testing.T) {
	// Not parallel: mutates needCountCacheMinRecompute.
	oldFloor := needCountCacheMinRecompute
	needCountCacheMinRecompute = 0
	defer func() { needCountCacheMinRecompute = oldFloor }()

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})

	other := protocol.DeviceID{2}
	third := protocol.DeviceID{3}

	// One local file that no other device has, so both of them need it.
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{genFile("a", 1, 1)}); err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB(folderID, false)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	counts, hit, err := fdb.countNeed(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("first read should not be a cache hit")
	}
	if counts.Files != 1 {
		t.Fatalf("expected 1 needed file, got %d", counts.Files)
	}

	_, hit, err = fdb.countNeed(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("second read should be a cache hit")
	}

	// The local device is never cached; its counts come from the counts table.
	if _, hit, err := fdb.countNeed(protocol.LocalDeviceID, now); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("the local device should never be served from this cache")
	}

	// A write on behalf of a *different* device can change what this device is
	// counted as needing, so it must invalidate too.
	if err := sdb.Update(folderID, third, []protocol.FileInfo{genFile("b", 1, 2)}); err != nil {
		t.Fatal(err)
	}
	counts, hit, err = fdb.countNeed(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("another device's write did not invalidate the cached value")
	}
	if counts.Files != 2 {
		t.Fatalf("expected 2 needed files after the other device announced one, got %d", counts.Files)
	}

	// And a local write does too.
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{genFile("c", 1, 3)}); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := fdb.countNeed(other, now); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("a local write did not invalidate the cached value")
	}

	// Dropping a device's files changes the answer as well.
	if _, hit, err := fdb.countNeed(other, now); err != nil {
		t.Fatal(err)
	} else if !hit {
		t.Fatal("expected a hit before the drop")
	}
	if err := sdb.DropDevice(third); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := fdb.countNeed(other, now); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("dropping a device did not invalidate the cached value")
	}
}

func TestNeedListCache(t *testing.T) {
	t.Parallel()

	devA := protocol.DeviceID{1}
	devB := protocol.DeviceID{2}
	first := []protocol.FileInfo{{Name: "a"}, {Name: "b"}}
	second := []protocol.FileInfo{{Name: "c"}}
	now := time.Unix(0, 0)

	c := new(needListCache)

	if _, ok := c.get(devA, now); ok {
		t.Fatal("empty cache returned a value")
	}

	// A cached empty list is still a hit. That is the common case: a folder that
	// is in sync has nothing to report, and recomputing seconds of query work
	// every time such a view is opened is the bug this cache exists to fix.
	c.put(devA, nil, now)
	if files, ok := c.get(devA, now.Add(time.Second)); !ok || len(files) != 0 {
		t.Fatalf("empty entry: got %v ok=%v", files, ok)
	}

	c.put(devB, first, now)
	if got, ok := c.get(devB, now.Add(time.Second)); !ok || len(got) != len(first) {
		t.Fatalf("uninvalidated entry: got %v ok=%v", got, ok)
	}

	c.invalidate()

	// Immediately after invalidation the old value is still served, so a burst
	// of index batches coalesces into one recomputation.
	if got, ok := c.get(devB, now.Add(time.Second)); !ok || len(got) != len(first) {
		t.Fatalf("within the floor the old value should still be served: got %v ok=%v", got, ok)
	}

	// Past the floor it must be recomputed.
	if _, ok := c.get(devB, now.Add(needCountCacheMinRecompute)); ok {
		t.Fatal("past the floor an invalidated entry should miss")
	}

	// Invalidating because of one device invalidates all of them: another
	// device's index decides what this one is missing.
	c.put(devA, first, now)
	c.invalidate()
	if _, ok := c.get(devA, now.Add(needCountCacheMinRecompute)); ok {
		t.Fatal("a write must invalidate every device's entry, not just one")
	}

	// An entry is dropped once it is old enough even if nothing invalidated it.
	c.put(devA, second, now)
	if _, ok := c.get(devA, now.Add(needCountCacheTTL)); ok {
		t.Fatal("an entry past its TTL should miss")
	}

	// A newer value replaces the old one and clears the invalid flag.
	c.put(devB, second, now.Add(needCountCacheMinRecompute))
	if got, ok := c.get(devB, now.Add(needCountCacheMinRecompute)); !ok || len(got) != 1 || got[0].Name != "c" {
		t.Fatalf("replacement: got %v ok=%v", got, ok)
	}
}

// TestNeedListCacheInvalidation exercises the list cache through the real query:
// the whole set must come back sorted by name, the second read must be a cache
// hit, and any write that can change the files table must make the next read
// recompute - including a write made on behalf of another device.
func TestNeedListCacheInvalidation(t *testing.T) {
	// Not parallel: mutates needCountCacheMinRecompute.
	oldFloor := needCountCacheMinRecompute
	needCountCacheMinRecompute = 0
	defer func() { needCountCacheMinRecompute = oldFloor }()

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})

	other := protocol.DeviceID{2}
	third := protocol.DeviceID{3}

	// Announced out of alphabetical order, so a list that is merely collected
	// from the query would not come back sorted.
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{
		genFile("c", 1, 3),
		genFile("a", 1, 1),
		genFile("b", 1, 2),
	}); err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB(folderID, false)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	files, hit, err := fdb.neededGlobalFilesCached(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("first read should not be a cache hit")
	}
	if got := fiNames(files); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("expected the whole set sorted by name, got %v", got)
	}

	if _, hit, err := fdb.neededGlobalFilesCached(other, now); err != nil {
		t.Fatal(err)
	} else if !hit {
		t.Fatal("second read should be a cache hit")
	}

	// A write on behalf of another device changes the global index, so it must
	// invalidate this device's cached list.
	if err := sdb.Update(folderID, third, []protocol.FileInfo{genFile("d", 1, 4)}); err != nil {
		t.Fatal(err)
	}
	files, hit, err = fdb.neededGlobalFilesCached(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("another device's write did not invalidate the cached list")
	}
	if got := fiNames(files); !slices.Equal(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("expected the new file in the list, got %v", got)
	}

	// And a local write does too.
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{genFile("e", 1, 5)}); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := fdb.neededGlobalFilesCached(other, now); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("a local write did not invalidate the cached list")
	}

	// Dropping a device changes the answer as well.
	if _, hit, err := fdb.neededGlobalFilesCached(other, now); err != nil {
		t.Fatal(err)
	} else if !hit {
		t.Fatal("expected a hit before the drop")
	}
	if err := sdb.DropDevice(third); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := fdb.neededGlobalFilesCached(other, now); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("dropping a device did not invalidate the cached list")
	}

	// Whatever the answer is now, it must be cached and served again: the point
	// of the cache is that repeated reads of the same unchanged state are free.
	if _, hit, err := fdb.neededGlobalFilesCached(other, now); err != nil {
		t.Fatal(err)
	} else if !hit {
		t.Fatal("the recomputed list was not cached")
	}
}
