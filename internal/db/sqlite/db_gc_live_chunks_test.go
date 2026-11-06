// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

// The live-table GC (old deleted files, unreferenced names and versions)
// must hold updateLock for one bounded chunk at a time and stop at ctx
// cancellation between chunks.
//
// yarlung 2026-11-26: the periodic GC held the folder's updateLock in one
// statement per table over 18.8M files rows and 12.9M names. The folder's
// Serve loop sat in PendingInstalls waiting for the lock for over 30 min
// after a restart, the index receiver sat in Update, and a stop during an
// earlier run overran its budget: the statements ran without ctx.
//
// Teeth: the hook between chunks proves an Update can take the lock while
// GC is mid-run, then cancels ctx. Without chunking there is no point
// between chunks: the hook never fires, and the GC deletes every old row
// regardless of the cancellation.
func TestGCLiveTablesYieldLockAndHonourCtx(t *testing.T) {
	// Not parallel: mutates package-level GC knobs.
	for _, needBuilt := range []bool{false, true} {
		t.Run(fmt.Sprintf("needBuilt=%v", needBuilt), func(t *testing.T) {
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

			// 20 remote files, then deletions of them, then a newer local
			// file so the index sender's high-water mark is not one of them.
			const n = 20
			var files []protocol.FileInfo
			for i := range n {
				files = append(files, genFile(fmt.Sprintf("f%02d", i), 1, i+1))
			}
			if err := sdb.Update(folderID, protocol.DeviceID{1}, files); err != nil {
				t.Fatal(err)
			}
			for i := range files {
				files[i].SetDeleted(1)
				files[i].Version = files[i].Version.Update(1)
				files[i].Sequence = int64(n + i + 1)
			}
			if err := sdb.Update(folderID, protocol.DeviceID{1}, files); err != nil {
				t.Fatal(err)
			}
			if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{genFile("local", 1, 1)}); err != nil {
				t.Fatal(err)
			}
			// Age the deletions past retention.
			if _, err := fdb.sql.Exec(`UPDATE files SET modified = ? WHERE deleted`, time.Now().Add(-48*time.Hour).UnixNano()); err != nil {
				t.Fatal(err)
			}
			fdb.needBuilt.Store(needBuilt)
			old := countOldDeleted(t, fdb)
			if old != n {
				t.Fatalf("fixture: %d old deleted rows, want %d", old, n)
			}

			oldSpan, oldTarget, oldHook := gcLiveSpanStart, gcLiveHoldTarget, gcLiveChunkHook
			defer func() { gcLiveSpanStart, gcLiveHoldTarget, gcLiveChunkHook = oldSpan, oldTarget, oldHook }()
			gcLiveSpanStart, gcLiveHoldTarget = 1, 0

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			chunks := 0
			updated := false
			gcLiveChunkHook = func() {
				chunks++
				if countOldDeleted(t, fdb) == old {
					return // nothing dropped yet: wait for a chunk that did
				}
				if updated {
					return
				}
				updated = true
				done := make(chan error, 1)
				go func() {
					done <- fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{genFile("mid-gc", 1, 2)}, db.UpdateOptions{})
				}()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("an Update could not take updateLock between GC chunks")
				}
				cancel()
			}

			err = garbageCollectOldDeleted(ctx, fdb)
			if !updated {
				t.Fatalf("no point between GC chunks where an Update could run (chunks=%d): GC holds updateLock for the whole table", chunks)
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("GC returned %v after ctx was cancelled mid-run, want context.Canceled", err)
			}
			if left := countOldDeleted(t, fdb); left == 0 || left == old {
				t.Errorf("%d of %d old deleted rows left: want GC to stop part way at cancellation", left, old)
			}

			// A fresh run completes and drops the rest.
			gcLiveChunkHook = nil
			if err := garbageCollectOldDeleted(context.Background(), fdb); err != nil {
				t.Fatal(err)
			}
			if left := countOldDeleted(t, fdb); left != 0 {
				t.Errorf("%d old deleted rows left after a full run", left)
			}

			// Names and versions: chunked the same way.
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			nameChunks := 0
			gcLiveChunkHook = func() {
				nameChunks++
				if nameChunks == 2 {
					cancel2()
				}
			}
			if err := garbageCollectNamesAndVersions(ctx2, fdb); !errors.Is(err, context.Canceled) {
				t.Errorf("names/versions GC returned %v after ctx was cancelled between chunks (chunks=%d), want context.Canceled", err, nameChunks)
			}
		})
	}
}

func countOldDeleted(t *testing.T, fdb *folderDB) int {
	t.Helper()
	var c int
	if err := fdb.sql.Get(&c, `SELECT count(*) FROM files WHERE deleted AND modified < ?`, time.Now().Add(-24*time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	return c
}
