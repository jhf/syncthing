// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"testing"
	"time"
)

// The retired-blocklist GC must process the queue in batches and release
// updateLock between them. A single unbatched transaction over a large
// queue holds the lock (or, via the periodic service's caller-held lock,
// extends its hold) for many minutes on a multi-million-name folder,
// wedging every folderDB.Update behind it. Observed live on yarlung
// 2026-10-13 as a ~10 minute stall that parked a connection dispatcher,
// recreating the phantom-connection state of the 2026-10-12 incident.
//
// Teeth: the hook fires between batches and proves the lock is free by
// acquiring it, then revives H2 by attaching it to a file. With the
// unbatched implementation there are no inter-batch points: the hook never
// fires and H2's blocklist is deleted before the revival can interleave, so
// both assertions fail.
func TestGCRetiredBlocklistsReleasesLockBetweenBatches(t *testing.T) {
	// Not parallel: mutates gcRetiredBatchSize/gcRetiredBatchHook.

	sdb, err := Open(t.TempDir(), WithDeleteRetention(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}

	// Two retired hashes, H1 < H2 so H1 is collected in the first batch.
	h1 := []byte{0x10}
	h2 := []byte{0x20}
	for _, h := range [][]byte{h1, h2} {
		if _, err := fdb.sql.Exec(`INSERT INTO blocklists (blocklist_hash, blprotobuf) VALUES (?, ?)`, h, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, err := fdb.sql.Exec(`INSERT INTO blocks (hash, blocklist_hash, idx, offset, size) VALUES (?, ?, ?, ?, ?)`, []byte{h[0], byte(i)}, h, i, i*128, 128); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := fdb.sql.Exec(`INSERT INTO gc_retired_blocklists (blocklist_hash) VALUES (?)`, h); err != nil {
			t.Fatal(err)
		}
	}

	// One hash per batch forces the multi-batch path.
	oldSize := gcRetiredBatchSize
	gcRetiredBatchSize = 1
	defer func() { gcRetiredBatchSize = oldSize }()

	// Between batches: prove updateLock is free by taking it, then revive
	// H2 the way an Update would — a file referencing its blocklist.
	hookCalled := false
	gcRetiredBatchHook = func() {
		defer func() { gcRetiredBatchHook = nil }() // fire once
		hookCalled = true

		acquired := make(chan struct{})
		go func() {
			fdb.updateLock.Lock()
			defer fdb.updateLock.Unlock()
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(5 * time.Second):
			t.Error("updateLock not released between GC batches")
			return
		}

		fdb.updateLock.Lock()
		defer fdb.updateLock.Unlock()
		if _, err := fdb.sql.Exec(`INSERT INTO devices (idx, device_id) VALUES (999, 'test-device')`); err != nil {
			t.Error(err)
			return
		}
		if _, err := fdb.sql.Exec(`INSERT INTO file_names (idx, name) VALUES (999, 'revived')`); err != nil {
			t.Error(err)
			return
		}
		if _, err := fdb.sql.Exec(`INSERT INTO file_versions (idx, version) VALUES (999, 'v1')`); err != nil {
			t.Error(err)
			return
		}
		if _, err := fdb.sql.Exec(`INSERT INTO files (device_idx, name_idx, type, modified, size, version_idx, deleted, local_flags, blocklist_hash) VALUES (999, 999, 0, 0, 0, 999, 0, 0, ?)`, h2); err != nil {
			t.Error(err)
			return
		}
	}
	defer func() { gcRetiredBatchHook = nil }()

	conn, err := fdb.sql.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := garbageCollectRetiredBlocklists(context.Background(), fdb, conn); err != nil {
		t.Fatal(err)
	}

	if !hookCalled {
		t.Error("batch hook never fired: GC did not release the lock between batches")
	}

	// H1 was unreferenced: fully collected, queue row gone.
	var n int
	if err := fdb.sql.Get(&n, `SELECT count(*) FROM blocklists WHERE blocklist_hash = ?`, h1); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("unreferenced H1 blocklist survived: %d rows", n)
	}
	if err := fdb.sql.Get(&n, `SELECT count(*) FROM blocks WHERE blocklist_hash = ?`, h1); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("unreferenced H1 blocks survived: %d rows", n)
	}
	if err := fdb.sql.Get(&n, `SELECT count(*) FROM gc_retired_blocklists WHERE blocklist_hash = ?`, h1); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("collected H1 still queued: %d rows", n)
	}

	// H2 was revived mid-run: its blocklist and blocks must survive, and
	// its queue row remains for a later run.
	if err := fdb.sql.Get(&n, `SELECT count(*) FROM blocklists WHERE blocklist_hash = ?`, h2); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("revived H2 blocklist lost: %d rows", n)
	}
	if err := fdb.sql.Get(&n, `SELECT count(*) FROM blocks WHERE blocklist_hash = ?`, h2); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("revived H2 blocks lost: %d of 3 rows", n)
	}
}

// Multiple unreferenced hashes spread over several batches are all
// collected and the queue drains completely.
func TestGCRetiredBlocklistsMultiBatchCompletion(t *testing.T) {
	// Not parallel: mutates gcRetiredBatchSize.

	sdb, err := Open(t.TempDir(), WithDeleteRetention(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}

	const hashes = 5
	for i := 0; i < hashes; i++ {
		h := []byte{byte(i)}
		if _, err := fdb.sql.Exec(`INSERT INTO blocklists (blocklist_hash, blprotobuf) VALUES (?, ?)`, h, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
		if _, err := fdb.sql.Exec(`INSERT INTO blocks (hash, blocklist_hash, idx, offset, size) VALUES (?, ?, 0, 0, 128)`, []byte{0xaa, byte(i)}, h); err != nil {
			t.Fatal(err)
		}
		if _, err := fdb.sql.Exec(`INSERT INTO gc_retired_blocklists (blocklist_hash) VALUES (?)`, h); err != nil {
			t.Fatal(err)
		}
	}

	oldSize := gcRetiredBatchSize
	gcRetiredBatchSize = 2 // forces three batches over five hashes
	defer func() { gcRetiredBatchSize = oldSize }()

	conn, err := fdb.sql.Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := garbageCollectRetiredBlocklists(context.Background(), fdb, conn); err != nil {
		t.Fatal(err)
	}

	var n int
	for _, q := range []string{
		`SELECT count(*) FROM blocklists`,
		`SELECT count(*) FROM blocks`,
		`SELECT count(*) FROM gc_retired_blocklists`,
	} {
		if err := fdb.sql.Get(&n, q); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%s = %d, want 0", q, n)
		}
	}
}
