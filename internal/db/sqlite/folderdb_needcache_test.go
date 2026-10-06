// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
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
