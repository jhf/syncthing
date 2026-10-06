// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

const (
	// needCountCacheTTL is the longest a cached need count is served without
	// being invalidated, as a safety net in case a write path forgets to
	// invalidate.
	needCountCacheTTL = 60 * time.Second
)

// needCountCacheMinRecompute is the shortest interval between two
// recomputations of the same entry. An index transfer arrives as many batches
// and every batch invalidates; without this floor each batch would pay for its
// own full recomputation. It is a var rather than a const so tests can remove
// the floor.
var needCountCacheMinRecompute = 5 * time.Second

type needCountEntry struct {
	counts  db.Counts
	at      time.Time
	invalid bool
}

// needCountCache holds the result of the remote-device need count. That query is
// a correlated NOT EXISTS aggregate over the files table, which takes seconds on
// a folder with millions of files, and the folder summary service asks for it
// repeatedly for every remote device. It is only correct as long as it is
// invalidated whenever the files table changes.
type needCountCache struct {
	mut     sync.Mutex
	entries map[protocol.DeviceID]needCountEntry
}

// get returns a usable entry. An invalidated entry is still served until
// needCountCacheMinRecompute has passed since it was computed, so that a burst
// of writes coalesces into one recomputation.
func (c *needCountCache) get(device protocol.DeviceID, now time.Time) (db.Counts, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	entry, ok := c.entries[device]
	if !ok {
		return db.Counts{}, false
	}
	age := now.Sub(entry.at)
	if age >= needCountCacheTTL {
		return db.Counts{}, false
	}
	if entry.invalid && age >= needCountCacheMinRecompute {
		return db.Counts{}, false
	}
	return entry.counts, true
}

func (c *needCountCache) put(device protocol.DeviceID, counts db.Counts, now time.Time) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.entries == nil {
		c.entries = make(map[protocol.DeviceID]needCountEntry)
	}
	c.entries[device] = needCountEntry{counts: counts, at: now}
}

// invalidate marks every entry as outdated. The values are kept rather than
// dropped so that the min-recompute floor still has something to serve while a
// recomputation is pending, and so that writes arriving in a burst coalesce.
func (c *needCountCache) invalidate() {
	c.mut.Lock()
	defer c.mut.Unlock()
	for device, entry := range c.entries {
		entry.invalid = true
		c.entries[device] = entry
	}
}

// invalidateNeedCounts must be called by every write path that can change the
// files table. Both the need count and the need list are derived from that
// table, so a write makes every cached value of either suspect. They are
// invalidated together here so a write path cannot remember one and forget the
// other. The local device's counts come from the maintained counts table and are
// not cached.
func (s *folderDB) invalidateNeedCounts() {
	s.needCache.invalidate()
	s.needListCache.invalidate()
}

// countNeed returns the need counts for a device, and whether they came from the
// cache. Testing this directly is how the cache behaviour is verified.
func (s *folderDB) countNeed(device protocol.DeviceID, now time.Time) (db.Counts, bool, error) {
	if device == protocol.LocalDeviceID {
		counts, err := s.needSizeLocal()
		return counts, false, err
	}

	if counts, ok := s.needCache.get(device, now); ok {
		return counts, true, nil
	}

	counts, err := s.needSizeRemote(device)
	if err != nil {
		return counts, false, err
	}
	s.needCache.put(device, counts, now)
	return counts, false, nil
}

type needListEntry struct {
	files   []protocol.FileInfo
	at      time.Time
	invalid bool
}

// needListCache holds the remote need list, which is what the "out of sync with
// device X" view asks for. Even without the SQL ORDER BY that query is seconds
// of work on a large folder, because it has to probe every global file against
// the device's entries to decide whether it is really missing; the list itself
// is usually empty or tiny. The GUI asks for it every time that view is opened.
//
// It shares the lifetime rules of needCountCache rather than inventing its own:
// same 60 s TTL as a safety net against a write path that forgets to invalidate,
// and the same minimum-recompute floor so that a burst of index batches
// coalesces into one recomputation instead of one per batch. Both are
// invalidated together, from the same call in every write path.
type needListCache struct {
	mut     sync.Mutex
	entries map[protocol.DeviceID]needListEntry
}

// get returns a usable entry, name-sorted. An invalidated entry is still served
// until needCountCacheMinRecompute has passed since it was computed.
func (c *needListCache) get(device protocol.DeviceID, now time.Time) ([]protocol.FileInfo, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	entry, ok := c.entries[device]
	if !ok {
		return nil, false
	}
	age := now.Sub(entry.at)
	if age >= needCountCacheTTL {
		return nil, false
	}
	if entry.invalid && age >= needCountCacheMinRecompute {
		return nil, false
	}
	return entry.files, true
}

// put stores a list. The caller's slice is kept as-is: it is built locally and
// never mutated afterwards, and copying a large list on every index update would
// cost more than the lookups it saves.
func (c *needListCache) put(device protocol.DeviceID, files []protocol.FileInfo, now time.Time) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.entries == nil {
		c.entries = make(map[protocol.DeviceID]needListEntry)
	}
	c.entries[device] = needListEntry{files: files, at: now}
}

func (c *needListCache) invalidate() {
	c.mut.Lock()
	defer c.mut.Unlock()
	for device, entry := range c.entries {
		entry.invalid = true
		c.entries[device] = entry
	}
}

// neededGlobalFilesCached returns the whole remote need list for a device,
// sorted by name, and whether it came from the cache. It is the form the
// out-of-sync view wants: complete, ordered, and pageable by the caller without
// asking the database again per page.
//
// The order is applied here rather than in SQL because ordering by name makes
// SQLite walk every name in the folder; see db.PullOrderUnspecified.
func (s *folderDB) neededGlobalFilesCached(device protocol.DeviceID, now time.Time) ([]protocol.FileInfo, bool, error) {
	if files, ok := s.needListCache.get(device, now); ok {
		return files, true, nil
	}

	it, errFn := s.AllNeededGlobalFiles(device, db.PullOrderUnspecified, 0, 0)
	files := slices.Collect(it)
	if err := errFn(); err != nil {
		return nil, false, err
	}
	slices.SortFunc(files, func(a, b protocol.FileInfo) int {
		return strings.Compare(a.Name, b.Name)
	})

	s.needListCache.put(device, files, now)
	return files, false, nil
}
