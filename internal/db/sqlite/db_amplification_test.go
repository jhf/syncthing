// Copyright (C) 2026 The Syncthing Authors.
//
// Amplification experiment (2026-10-23): measure exactly how many bytes land
// in the WAL and the main DB when ONE small file changes, via the real
// folderDB.Update path. Scratch DB in t.TempDir(); never touches live data.
//
// Measured (darwin/arm64, page_size 4096, WAL):
//   ONE change:        74,192 B WAL = 18 frames (~370x the ~200 B content)
//   DRIP 200 x 1 txn:  ~78,012 B/change, flat (no amortization)
//   BATCH 200 x 1 txn: ~1,277 B/change  -> batching collapses fixed cost ~61x
//   dbstat growth over drip (append-only tables): blocks, blocklists,
//   file_versions, blocks_blocklist_hash, gc_retired_blocklists all grow
//   (right-edge appends keyed by ever-new hash/sequence); files/fileinfos/
//   file_names dirty pages in the WAL but do not net-grow (INSERT OR REPLACE
//   reuses the row slot).
//
// dbstat attribution needs -tags sqlite_dbstat; without it the WAL/DB byte
// deltas still report and the per-table section is skipped.
//
// Run: go test ./internal/db/sqlite -run TestAmplificationSingleFile -v
//      go test -tags sqlite_dbstat ./internal/db/sqlite -run TestAmplificationSingleFile -v

package sqlite

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

// dirSizes returns the byte sizes of all .db and .wal files under dir,
// recursively (the folder DB lives in a subdirectory).
func dirSizes(t *testing.T, dir string) (dbBytes, walBytes int64) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		name := d.Name()
		switch {
		case strings.HasSuffix(name, "-wal"):
			walBytes += fi.Size()
		case strings.HasSuffix(name, ".db"):
			dbBytes += fi.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

const ampFolderID = "test"

func TestAmplificationSingleFile(t *testing.T) {
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

	// Simulate one hot file: small (a few hundred bytes of content = 1 block),
	// rewritten repeatedly with a new version each time, as the local device
	// (the sender side, which also populates the blocks table).
	name := "hot/heartbeat.json"
	fi := genFile(name, 1, 0)
	if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
		t.Fatal(err)
	}

	// Force any pending checkpoint so we start from a clean baseline.
	forceCheckpoint(t, sdb)
	db0, wal0 := dirSizes(t, dir)
	t.Logf("baseline after first insert+checkpoint: db=%d wal=%d", db0, wal0)

	// One single-change update.
	fi.Version = fi.Version.Update(42)
	fi.Blocks = genBlocks(name, 1, 1)
	dbB, walB := dirSizes(t, dir)
	if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
		t.Fatal(err)
	}
	dbA, walA := dirSizes(t, dir)
	t.Logf("ONE change: wal delta = %d bytes (%d frames @4096+24), db delta = %d",
		walA-walB, (walA-walB)/(4096+24), dbA-dbB)

	// N single-change updates (the "drip": one file, one txn each).
	const N = 200
	tablePagesBefore := tablePages(t, sdb)
	dbB, walB = dirSizes(t, dir)
	for i := 0; i < N; i++ {
		fi.Version = fi.Version.Update(42)
		fi.Blocks = genBlocks(name, 100+i, 1)
		if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, []protocol.FileInfo{fi}); err != nil {
			t.Fatal(err)
		}
	}
	dbA, walA = dirSizes(t, dir)
	t.Logf("DRIP %d changes, 1 txn each: wal delta = %d bytes (~%d frames/change), db delta = %d, db+wal total = %d (~%d bytes/change)",
		N, walA-walB, (walA-walB)/N/(4096+24), dbA-dbB, dbA+walA-dbB-walB, (dbA+walA-dbB-walB)/N)

	// Per-table page growth over the drip (which tables accumulate pages).
	reportTableGrowth(t, sdb, tablePagesBefore)

	// The same N changes batched into ONE transaction.
	dbB, walB = dirSizes(t, dir)
	batch := make([]protocol.FileInfo, N)
	for i := range batch {
		f := fi
		f.Name = fmt.Sprintf("batch/%s-%d", rand.String(6), i)
		f.Version = f.Version.Update(42)
		f.Blocks = genBlocks(f.Name, 1000+i, 1)
		batch[i] = f
	}
	if err := sdb.Update(ampFolderID, protocol.LocalDeviceID, batch); err != nil {
		t.Fatal(err)
	}
	dbA, walA = dirSizes(t, dir)
	t.Logf("BATCH %d changes, 1 txn: wal delta = %d bytes (~%d bytes/change), db delta = %d",
		N, walA-walB, (walA-walB)/N, dbA-dbB)
}

// forceCheckpoint runs a truncating checkpoint via the folderDB's own conn.
func forceCheckpoint(t *testing.T, sdb *DB) {
	t.Helper()
	fdb, err := sdb.getFolderDB(ampFolderID, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fdb.sql.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
}

// tablePages returns page count per table/index via the dbstat virtual table.
func tablePages(t *testing.T, sdb *DB) map[string]int {
	t.Helper()
	fdb, err := sdb.getFolderDB(ampFolderID, false)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := fdb.sql.Queryx(`SELECT name, sum(pgsize) FROM dbstat GROUP BY name`)
	if err != nil {
		t.Logf("dbstat unavailable: %v", err)
		return nil
	}
	defer rows.Close()
	m := make(map[string]int)
	var name string
	var bytes int
	for rows.Next() {
		if err := rows.Scan(&name, &bytes); err != nil {
			t.Fatal(err)
		}
		m[name] = bytes
	}
	return m
}

// reportTableGrowth logs per-table byte growth since the before snapshot.
func reportTableGrowth(t *testing.T, sdb *DB, before map[string]int) {
	t.Helper()
	if before == nil {
		return
	}
	after := tablePages(t, sdb)
	t.Log("per-table growth over drip (bytes):")
	for name, ab := range after {
		delta := ab - before[name]
		if delta != 0 {
			t.Logf("  %-28s %+10d bytes  (%d -> %d)", name, delta, before[name], ab)
		}
	}
}
