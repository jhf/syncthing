// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

// A reader that stays open keeps every PASSIVE checkpoint short of the
// WAL's end, so the WAL never resets and grows with the writes (yarlung
// 2026-11-07, 824 MiB). With patch/73 the backstop notices the size, waits
// while the reader stays, never blocks the writer meanwhile, and truncates
// the WAL as soon as the reader goes. Before patch/73 the WAL stays large.
func TestWALBackstopTruncatesAfterReaderGap(t *testing.T) {
	oldBytes, oldDelay, oldMax := walBackstopBytes, walBackstopFirstDelay, walBackstopMaxDelay
	walBackstopBytes, walBackstopFirstDelay, walBackstopMaxDelay = 1<<20, 20*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { walBackstopBytes, walBackstopFirstDelay, walBackstopMaxDelay = oldBytes, oldDelay, oldMax })

	const folderID = "test"
	dir := t.TempDir()
	sdb, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}
	walPath := fdb.path + "-wal"
	walMiB := func() float64 {
		fi, err := os.Stat(walPath)
		if err != nil {
			return 0
		}
		return float64(fi.Size()) / (1 << 20)
	}

	write := func(round int) {
		t.Helper()
		files := make([]protocol.FileInfo, 0, 500)
		for i := range 500 {
			files = append(files, genFile(fmt.Sprintf("r%03d/f%04d", round, i), 4, i+1))
		}
		start := time.Now()
		if err := sdb.Update(folderID, protocol.LocalDeviceID, files); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("writer blocked %v while the backstop was waiting", d)
		}
	}

	// A long reader: an open read transaction with a row pending, exactly
	// like a sweep iterating under a slow plan.
	write(0)
	rows, err := fdb.sql.Query(`SELECT name FROM file_names`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("no rows")
	}
	for round := 1; round <= 12; round++ {
		write(round)
	}
	pinned := walMiB()
	t.Logf("WAL with the reader open: %.1f MiB (limit %.1f MiB)", pinned, float64(walBackstopBytes)/(1<<20))
	if pinned*(1<<20) < float64(walBackstopBytes) {
		t.Fatalf("fixture did not grow the WAL past the limit: %.1f MiB", pinned)
	}

	// The reader goes away. The backstop must reset and truncate the WAL.
	rows.Close()
	deadline := time.Now().Add(10 * time.Second)
	for walMiB()*(1<<20) >= float64(walBackstopBytes) {
		if time.Now().After(deadline) {
			t.Fatalf("WAL still %.1f MiB 10 s after the reader closed: no backstop", walMiB())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("WAL after the reader closed: %.1f MiB", walMiB())
	if _, err := os.Stat(filepath.Join(dir)); err != nil {
		t.Fatal(err)
	}
}
