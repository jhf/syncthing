// Copyright (C) 2026 The Syncthing Authors.
//
// Scaled validation of the amplification measurement (2026-10-23): the base
// TestAmplificationSingleFile measured on a nearly-empty DB (18 WAL frames per
// change). This test pre-populates the folder DB to a realistic size and
// re-measures, and additionally measures the checkpoint side via the frame
// counts returned by PRAGMA wal_checkpoint (which the base test only assumed
// to be x2), and records PRAGMA synchronous (the fsync policy).
//
// Run: go test ./internal/db/sqlite -run TestAmplificationScaled -v

package sqlite

import (
	"fmt"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

func TestAmplificationScaled(t *testing.T) {
	const prepFiles = 20_000

	dir := t.TempDir()
	sdb, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	})

	// fsync policy actually in effect on the connection.
	fdb, err := sdb.getFolderDB(ampFolderID, true)
	if err != nil {
		t.Fatal(err)
	}
	var syncMode int
	if err := fdb.sql.QueryRow(`PRAGMA synchronous`).Scan(&syncMode); err != nil {
		t.Fatal(err)
	}
	t.Logf("PRAGMA synchronous = %d (0=OFF 1=NORMAL 2=FULL 3=EXTRA)", syncMode)

	setupSingleConn(t, sdb)

	// Pre-populate with realistic file count so B-trees have real depth.
	for i := 0; i < prepFiles; i += 1000 {
		fs := make([]protocol.FileInfo, 1000)
		for j := range fs {
			fs[j] = genFile(fmt.Sprintf("bulk/%05d-%s", i+j, rand.String(8)), 1, 0)
		}
		if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, fs); err != nil {
			t.Fatal(err)
		}
	}
	resetWAL(t, sdb) // zero the WAL so later file-size deltas are exact
	dbB, walB := dirSizes(t, dir)
	t.Logf("populated: files=%d db=%d wal=%d", prepFiles, dbB, walB)

	// One hot file, changed once, on the populated DB.
	name := "hot/heartbeat.json"
	fi := genFile(name, 1, 0)
	if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
		t.Fatal(err)
	}
	resetWAL(t, sdb)

	fi.Version = fi.Version.Update(42)
	fi.Blocks = genBlocks(name, 1, 1)
	_, walB = dirSizes(t, dir)
	if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
		t.Fatal(err)
	}
	_, walA := dirSizes(t, dir)
	frames := (walA - walB) / (4096 + 24)
	t.Logf("SCALED (%d files): ONE change = %d WAL bytes = %d frames", prepFiles, walA-walB, frames)

	// Measure the checkpoint multiplier directly: drip N changes, then force a
	// checkpoint and read how many frames it wrote into the main DB.
	const N = 100
	_, walB = dirSizes(t, dir)
	for i := 0; i < N; i++ {
		fi.Version = fi.Version.Update(42)
		fi.Blocks = genBlocks(name, 200+i, 1)
		if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
			t.Fatal(err)
		}
	}
	_, walA = dirSizes(t, dir)
	walGen := walA - walB
	t.Logf("SCALED drip %d changes: WAL grew %d bytes (%.0f KB/change, ~%.0f frames/change)",
		N, walGen, float64(walGen)/N/1024, float64(walGen)/N/4120)
	busy, log, ckpt := measureCheckpoint(t, sdb)
	t.Logf("checkpoint after drip (FULL): busy=%d wal_frames=%d checkpointed_frames=%d (%d bytes into main db)",
		busy, log, ckpt, int64(ckpt)*4096)
	t.Logf("checkpoint/write ratio: %.2f (checkpointed bytes / WAL bytes generated)",
		float64(int64(ckpt)*4096)/float64(walGen))
}

// resetWAL truncates the WAL so the next phase's file-size delta is exact.
// Note: mattn/sqlite reports bogus zero counts for TRUNCATE checkpoints
// (verified in isolation 2026-10-23), so use it only for resetting and use
// measureCheckpoint for frame counts.
func resetWAL(t *testing.T, sdb *DB) {
	t.Helper()
	fdb, err := sdb.getFolderDB(ampFolderID, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fdb.sql.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
}

// measureCheckpoint runs a FULL checkpoint and returns its result row:
// busy, total WAL frames, and frames written into the main DB. FULL reports
// real counts (TRUNCATE does not); the WAL file keeps its high-water size,
// so call resetWAL before any phase whose file-size delta matters.
func measureCheckpoint(t *testing.T, sdb *DB) (busy, logFrames, checkpointed int) {
	t.Helper()
	fdb, err := sdb.getFolderDB(ampFolderID, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := fdb.sql.QueryRow(`PRAGMA wal_checkpoint(FULL)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatal(err)
	}
	return
}

// setupSingleConn caps the folder DB pool at one connection with no idle
// expiry, so every statement (updates and checkpoints alike) lands on one
// SQLite connection and per-connection PRAGMAs are deterministic. It also
// disables autocheckpoint on that connection so no background checkpoint
// interferes with WAL size measurement. Call once, before populating.
func setupSingleConn(t *testing.T, sdb *DB) {
	t.Helper()
	fdb, err := sdb.getFolderDB(ampFolderID, true)
	if err != nil {
		t.Fatal(err)
	}
	fdb.sql.SetMaxOpenConns(1)
	fdb.sql.SetConnMaxIdleTime(0)
	if _, err := fdb.sql.Exec(`PRAGMA wal_autocheckpoint(0)`); err != nil {
		t.Fatal(err)
	}
}
