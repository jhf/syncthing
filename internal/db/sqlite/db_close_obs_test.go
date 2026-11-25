// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

// closeObsFiles returns n distinct small files.
func closeObsFiles(n int) []protocol.FileInfo {
	fs := make([]protocol.FileInfo, n)
	for i := range fs {
		fs[i] = genFile(fmt.Sprintf("f%04d", i), 2, i+1)
	}
	return fs
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// walBacklog reads the backlog from the -shm header; its numbers must match
// SQLite's own: mxFrame equals the log size a PASSIVE checkpoint reports,
// and after that checkpoint (nothing pins the WAL) the backlog is zero.
// Teeth: a wrong header offset reads garbage or zero frames here.
func TestWALBacklogMatchesSQLite(t *testing.T) {
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sdb.Close() })

	files := closeObsFiles(200)
	if err := sdb.Update("test", protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB("test", false)
	if err != nil {
		t.Fatal(err)
	}

	backlog, mx, ok := walBacklog(fdb.path)
	if !ok || mx == 0 || backlog == 0 {
		t.Fatalf("before checkpoint: backlog=%d mxFrame=%d ok=%v, want frames pending", backlog, mx, ok)
	}

	conn, err := fdb.sql.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var res, logPages, moved int
	if err := conn.QueryRowContext(context.Background(), `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&res, &logPages, &moved); err != nil {
		t.Fatal(err)
	}
	if uint32(logPages) != mx {
		t.Fatalf("mxFrame from -shm = %d, SQLite reports log=%d", mx, logPages)
	}
	if moved != logPages {
		t.Skipf("checkpoint did not complete (moved %d of %d); backlog check needs an unpinned WAL", moved, logPages)
	}
	if backlog, _, _ := walBacklog(fdb.path); backlog != 0 {
		t.Fatalf("after a complete checkpoint: backlog=%d, want 0", backlog)
	}
}

// Closing logs the WAL backlog at the start and one "done" line per step,
// so a close the app abandons after 10 s still shows which step it was in
// (yarlung: "Database failed to stop within 10s" on 4 of 10 stops).
// Teeth: drop the closeStep wrappers and the step assertions fail.
func TestCloseLogsSteps(t *testing.T) {
	var buf syncBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.Update("test", protocol.LocalDeviceID, closeObsFiles(50)); err != nil {
		t.Fatal(err)
	}
	if err := sdb.Close(); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, `msg="Closing database"`) || !strings.Contains(out, "walBacklogFrames=") {
		t.Fatalf("no WAL backlog line at close:\n%s", out)
	}
	for _, step := range []string{"mtime-flush", "need-build-wait", "write-lock", "statements", "connections"} {
		if !strings.Contains(out, "step="+step) {
			t.Errorf("close step %q not logged", step)
		}
	}
}
