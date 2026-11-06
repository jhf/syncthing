// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

// lockLogBuffer collects log output written from several goroutines.
type lockLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *lockLogBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *lockLogBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A tidy statement that holds the write lock past the bounded-step budget
// while an Update waits must leave INFO lines naming both: the holder (tidy)
// and the waiter (Update). yarlung 2026-11-28 07:35:47 logged only
// `PRAGMA wal_checkpoint(PASSIVE)` duration=12.66680797s; nothing said
// whether a writer stood behind it.
func TestWriteLockReportsHoldWithWaiters(t *testing.T) {
	var buf lockLogBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	_, fdb := openTestFolderDB(t)
	p := attachWriterProbe(t, fdb.baseDB)
	stall := 3 * gcLiveHoldTarget
	var once sync.Once
	done := make(chan error, 1)
	p.before = func(q string) {
		if !strings.Contains(strings.ToUpper(q), "WAL_CHECKPOINT") {
			return
		}
		once.Do(func() {
			go func() {
				done <- fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{genFile("behind-checkpoint", 1, 1)}, db.UpdateOptions{})
			}()
			// A checkpoint on slow disks, with the Update parked on the lock.
			time.Sleep(stall)
		})
	}
	if err := runProbedTidy(context.Background(), fdb.baseDB); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Update never completed")
	}
	out := buf.String()
	var held, waited string
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.Contains(line, "Database write lock held while writers waited"):
			held = line
		case strings.Contains(line, "Database write lock waited"):
			waited = line
		}
	}
	if held == "" {
		t.Fatalf("no INFO line for a %v hold with a writer waiting:\n%s", stall, out)
	}
	if !strings.Contains(held, "tidy") || !strings.Contains(held, "waiters=1") {
		t.Errorf("hold line does not name tidy with one waiter: %s", held)
	}
	if waited == "" || !strings.Contains(waited, "Update") {
		t.Errorf("wait line does not name the waiting Update: %q", waited)
	}
	t.Logf("%s\n%s", held, waited)
}

// Uncontended holds, however long, and short contended ones log nothing.
func TestWriteLockQuietWithoutContention(t *testing.T) {
	var buf lockLogBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	l := writeLock{db: "test.db"}
	l.Lock()
	time.Sleep(2 * gcLiveHoldTarget)
	l.Unlock()

	l.Lock()
	go func() { time.Sleep(gcLiveHoldTarget / 10); l.Unlock() }()
	l.Lock()
	l.Unlock()

	if s := buf.String(); strings.Contains(s, "Database write lock") {
		t.Fatalf("logged without a long contended hold:\n%s", s)
	}
}
