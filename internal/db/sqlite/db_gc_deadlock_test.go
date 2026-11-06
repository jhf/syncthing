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

// garbageCollectOldDeletedSpan is called by gcLiveChunks with updateLock
// already held. When the need tables are built it must not re-acquire that
// mutex: sync.Mutex is not reentrant,
// so re-locking self-deadlocks the GC service while it keeps holding the
// lock, and every later folderDB.Update (folder scans, incoming index
// updates) wedges behind it. Observed live on yarlung 2026-10-12: a
// connection whose dispatcherLoop was parked in Update when the peer went
// away could not finish internalClose (it waits for the dispatcher), so
// model.Closed never ran — a phantom "primary" connection with frozen
// counters and no kernel socket.
//
// Teeth: with the re-lock present the GC call never returns and the test
// fails on timeout.
func TestGCDeletedWithNeedTrackingDoesNotSelfDeadlock(t *testing.T) {
	t.Parallel()

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
	fdb.needBuilt.Store(true)

	// Production call pattern: the periodic service holds updateLock.
	fdb.updateLock.Lock()
	defer fdb.updateLock.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := garbageCollectOldDeletedSpan(context.Background(), fdb, 0, time.Now().UnixNano(), 0, 1<<62)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("garbageCollectOldDeletedSpan deadlocked: it must not re-acquire updateLock")
	}
}
