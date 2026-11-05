// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"runtime"
	"testing"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestPullCommitConnectionDurability(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	fdb, err := s.getFolderDB("pull-durable", true)
	if err != nil {
		t.Fatal(err)
	}
	fdb.updateLock.Lock()
	conn, release, err := fdb.pullCommitConnectionLocked()
	if err != nil {
		fdb.updateLock.Unlock()
		t.Fatal(err)
	}
	var synchronous, fullfsync int
	err = conn.GetContext(context.Background(), &synchronous, "PRAGMA synchronous")
	if err == nil {
		err = conn.GetContext(context.Background(), &fullfsync, "PRAGMA fullfsync")
	}
	release()
	fdb.updateLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if synchronous != 2 || runtime.GOOS == "darwin" && fullfsync != 1 {
		t.Fatalf("pull synchronous=%d fullfsync=%d", synchronous, fullfsync)
	}
	// Reserve every slot together, checking that no returned physical connection
	// leaked FULL or fullfsync into the ordinary writer pool.
	max := fdb.sql.Stats().MaxOpenConnections
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < max; i++ {
		conn, err := fdb.sql.Connx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, func() { conn.Close() })
		var sync int
		if err := conn.GetContext(context.Background(), &sync, "PRAGMA synchronous"); err != nil {
			t.Fatal(err)
		}
		if sync != 1 {
			t.Errorf("ordinary connection %d synchronous=%d", i, sync)
		}
	}
	for _, release := range releases {
		release()
	}
	releases = nil
	fi := genFile("pending", 1, 0)
	fi.LocalFlags |= protocol.FlagLocalInstallPending
	if err := s.Update("pull-durable", protocol.LocalDeviceID, []protocol.FileInfo{fi}, db.WithDurable()); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingInstalls("pull-durable")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || !pending[0].Version.Equal(fi.Version) || len(pending[0].Blocks) != len(fi.Blocks) {
		t.Fatalf("pending roundtrip: %+v", pending)
	}
	fi.LocalFlags &^= protocol.FlagLocalInstallPending
	if err := s.Update("pull-durable", protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
		t.Fatal(err)
	}
	pending, err = s.PendingInstalls("pull-durable")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending cleared: %+v %v", pending, err)
	}
	t.Logf("pull synchronous=%d fullfsync=%d ordinary connections=%d synchronous=1", synchronous, fullfsync, max)
}
