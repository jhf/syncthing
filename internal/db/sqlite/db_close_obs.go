// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"encoding/binary"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Close observability. The app gives the database 10 s to close and then
// exits anyway ("Database failed to stop within 10s"); on yarlung that
// happened on 4 of the last 10 stops, once with no shutdown work at all
// (2026-11-24 18:30), so the time goes into the close itself. Which step
// is not visible from outside: the close runs an mtime flush, waits for a
// background need build, closes cached statements under updateLock, and
// the last connection's sqlite3_close checkpoints the whole WAL backlog
// into the database. Each step logs its duration at INFO, together with
// the WAL backlog at the start (frames written but not yet copied into the
// database, from the -shm header), and a step that is still running after
// closeProgressInterval logs that too: a close abandoned by the 10 s exit
// still names the step it was in.
var closeProgressInterval = 2 * time.Second

// walBacklog reads the WAL index header from the -shm file: mxFrame (last
// valid frame) and nBackfill (frames already checkpointed). backlog is
// their difference: the frames a checkpoint still has to copy. Read-only
// and lock-free; the values may be one transaction stale, which is fine
// for a log line. ok is false when there is no WAL index (not in WAL mode,
// or already removed by a completed close).
func walBacklog(path string) (backlog, mxFrame uint32, ok bool) {
	f, err := os.Open(path + "-shm")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	// WalIndexHdr (48 bytes) at offset 0, mxFrame at 16; WalCkptInfo at
	// 96 with nBackfill first. Integers are in host byte order; every
	// platform we build for is little-endian.
	var b [100]byte
	if _, err := f.ReadAt(b[:], 0); err != nil {
		return 0, 0, false
	}
	mx := binary.LittleEndian.Uint32(b[16:])
	backfill := binary.LittleEndian.Uint32(b[96:])
	if backfill > mx {
		return 0, mx, true
	}
	return mx - backfill, mx, true
}

// closeStep runs fn as one named step of closing db, logging its duration
// and, while it runs past closeProgressInterval, that it is still running.
func closeStep(db, step string, fn func()) {
	start := time.Now()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(closeProgressInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				slog.Info("Database close step still running", slog.String("db", db), slog.String("step", step), slog.Duration("elapsed", time.Since(start).Round(time.Millisecond)))
			}
		}
	}()
	fn()
	close(done)
	wg.Wait()
	slog.Info("Database close step done", slog.String("db", db), slog.String("step", step), slog.Duration("took", time.Since(start).Round(time.Millisecond)))
}
