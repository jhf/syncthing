// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

const (
	internalMetaPrefix     = "dbsvc"
	lastMaintKey           = "lastMaint"
	lastSuccessfulGCSeqKey = "lastSuccessfulGCSeq"
	gcNeedsFullScanKey     = "gcNeedsFullScan"

	gcMinChunks = 5
	gcChunkSize = 100_000 // approximate number of rows to process in a single gc query
)

// gcMaxRuntime is the max time to spend on gc, per table, per run. It is a var
// rather than a const so tests can force the interrupted path.
var gcMaxRuntime = 5 * time.Minute

func (s *DB) Service(maintenanceInterval time.Duration) db.DBService {
	return newService(s, maintenanceInterval)
}

type Service struct {
	sdb                 *DB
	maintenanceInterval time.Duration
	internalMeta        *db.Typed
	start               chan chan error
}

func (s *Service) String() string {
	return fmt.Sprintf("sqlite.service@%p", s)
}

func newService(sdb *DB, maintenanceInterval time.Duration) *Service {
	return &Service{
		sdb:                 sdb,
		maintenanceInterval: maintenanceInterval,
		internalMeta:        db.NewTyped(sdb, internalMetaPrefix),
		start:               make(chan chan error),
	}
}

func (s *Service) StartMaintenance() <-chan error {
	finishChan := make(chan error, 1)
	select {
	case s.start <- finishChan:
	default:
	}
	return finishChan
}

func (s *Service) Serve(ctx context.Context) error {
	// Run periodic maintenance
	// Figure out when we last ran maintenance and schedule accordingly. If
	// it was never, do it now.
	lastMaint, _, _ := s.internalMeta.Time(lastMaintKey)
	nextMaint := lastMaint.Add(s.maintenanceInterval)
	wait := time.Until(nextMaint)
	if wait < 0 {
		wait = time.Minute
	}
	slog.DebugContext(ctx, "Next periodic run due", "after", wait)
	timer := time.NewTimer(wait)

	if s.maintenanceInterval == 0 {
		timer.Stop()
	}

	for {
		var finishChan chan error
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case finishChan = <-s.start:
		}

		err := s.periodic(ctx)
		if finishChan != nil {
			finishChan <- err
		}

		if err != nil {
			return wrap(err)
		}

		if s.maintenanceInterval != 0 {
			timer.Reset(s.maintenanceInterval)
			slog.DebugContext(ctx, "Next periodic run due", "after", s.maintenanceInterval)
		}

		_ = s.internalMeta.PutTime(lastMaintKey, time.Now())
	}
}

func (s *Service) LastMaintenanceTime() time.Time {
	lastMaint, _, _ := s.internalMeta.Time(lastMaintKey)
	return lastMaint
}

func (s *Service) periodic(ctx context.Context) error {
	t0 := time.Now()
	slog.DebugContext(ctx, "Periodic start")

	t1 := time.Now()
	defer func() { slog.DebugContext(ctx, "Periodic done in", "t1", time.Since(t1), "t0t1", t1.Sub(t0)) }()

	s.sdb.updateLock.Lock()
	err := tidy(ctx, s.sdb.sql)
	s.sdb.updateLock.Unlock()
	if err != nil {
		return err
	}

	return wrap(s.sdb.forEachFolder(func(fdb *folderDB) error {
		// Get the current device sequence, for comparison in the next step.
		seq, err := fdb.GetDeviceSequence(protocol.LocalDeviceID)
		if err != nil {
			return wrap(err)
		}
		// Get the last successful GC sequence. If it's the same as the
		// current sequence, nothing has changed and we can skip the GC
		// entirely.
		meta := db.NewTyped(fdb, internalMetaPrefix)
		if prev, _, err := meta.Int64(lastSuccessfulGCSeqKey); err != nil {
			return wrap(err)
		} else if seq == prev {
			var pending int
			if err := fdb.sql.Get(&pending, `SELECT count(*) FROM gc_retired_blocklists`); err != nil {
				return wrap(err)
			}
			if pending == 0 {
				slog.DebugContext(ctx, "Skipping unnecessary GC", "folder", fdb.folderID, "fdb", fdb.baseName)
				return nil
			}
		}

		// Run the GC steps, in a function to be able to use a deferred
		// unlock.
		if err := func() error {
			fdb.updateLock.Lock()
			defer fdb.updateLock.Unlock()

			if err := garbageCollectOldDeletedLocked(ctx, fdb); err != nil {
				return wrap(err)
			}
			if err := garbageCollectNamesAndVersions(ctx, fdb); err != nil {
				return wrap(err)
			}
			if err := garbageCollectBlocklistsAndBlocksLocked(ctx, fdb); err != nil {
				return wrap(err)
			}
			return tidy(ctx, fdb.sql)
		}(); err != nil {
			return wrap(err)
		}

		// Update the successful GC sequence.
		return wrap(meta.PutInt64(lastSuccessfulGCSeqKey, seq))
	}))
}

func tidy(ctx context.Context, db *sqlx.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close()
	_, _ = conn.ExecContext(ctx, `ANALYZE`)
	_, _ = conn.ExecContext(ctx, `PRAGMA optimize`)
	_, _ = conn.ExecContext(ctx, `PRAGMA incremental_vacuum`)
	_, _ = conn.ExecContext(ctx, `PRAGMA journal_size_limit = 8388608`)
	_, _ = conn.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return nil
}

func garbageCollectNamesAndVersions(ctx context.Context, fdb *folderDB) error {
	l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName)

	res, err := fdb.stmt(`
		DELETE FROM file_names
		WHERE NOT EXISTS (SELECT 1 FROM files f WHERE f.name_idx = idx)
	`).Exec()
	if err != nil {
		return wrap(err, "delete names")
	}
	if aff, err := res.RowsAffected(); err == nil {
		l.DebugContext(ctx, "Removed old file names", "affected", aff)
	}

	res, err = fdb.stmt(`
		DELETE FROM file_versions
		WHERE NOT EXISTS (SELECT 1 FROM files f WHERE f.version_idx = idx)
	`).Exec()
	if err != nil {
		return wrap(err, "delete versions")
	}
	if aff, err := res.RowsAffected(); err == nil {
		l.DebugContext(ctx, "Removed old file versions", "affected", aff)
	}

	return nil
}

func garbageCollectOldDeletedLocked(ctx context.Context, fdb *folderDB) error {
	l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName)
	if fdb.deleteRetention <= 0 {
		slog.DebugContext(ctx, "Delete retention is infinite, skipping cleanup")
		return nil
	}

	// Remove deleted files that are marked as not needed (we have processed
	// them) and they were deleted more than MaxDeletedFileAge ago.
	// Dropping rows can change what a device is counted as needing.
	defer fdb.invalidateNeedCounts()
	l.DebugContext(ctx, "Forgetting deleted files", "retention", fdb.deleteRetention)
	res, err := fdb.stmt(`
		DELETE FROM files
		WHERE deleted AND modified < ? AND local_flags & {{.FlagLocalNeeded}} == 0
	`).Exec(time.Now().Add(-fdb.deleteRetention).UnixNano())
	if err != nil {
		return wrap(err)
	}
	if aff, err := res.RowsAffected(); err == nil {
		l.DebugContext(ctx, "Removed old deleted file records", "affected", aff)
	}
	return nil
}

func garbageCollectBlocklistsAndBlocksLocked(ctx context.Context, fdb *folderDB) error {
	// Remove blocklists not referred to by any files, then the blocks that
	// belonged to those hashes. File updates record retired hashes in
	// gc_retired_blocklists so the common path does not scan live tables.
	//
	// Foreign keys are disabled for the connection so we can delete orphans
	// without paying the FK trigger cost. Each unit of work runs in its own
	// transaction so a 5 minute time limit does not leave a multi-GB WAL from
	// one uncommitted DELETE, and so the folder lock can be released between
	// chunks.
	fdb.updateLock.Unlock()
	defer fdb.updateLock.Lock()

	conn, err := fdb.sql.Connx(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = 0`); err != nil {
		return wrap(err)
	}
	defer func() { //nolint:contextcheck
		_, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys = 1`)
	}()

	var pending int64
	if err := conn.GetContext(ctx, &pending, `SELECT count(*) FROM gc_retired_blocklists`); err != nil {
		return wrap(err)
	}

	meta := db.NewTyped(fdb, internalMetaPrefix)
	needsFull, ok, err := meta.Bool(gcNeedsFullScanKey)
	if err != nil {
		return wrap(err)
	}
	if !ok {
		// Unset means a pre-queue database: one live-table scan, then queue-only.
		if _, hadSeq, err := meta.Int64(lastSuccessfulGCSeqKey); err != nil {
			return wrap(err)
		} else if hadSeq {
			needsFull = true
		}
	}

	if pending > 0 {
		if err := garbageCollectRetiredBlocklists(ctx, fdb, conn); err != nil {
			return err
		}
	}

	if !needsFull {
		// Queue path is the source of truth after the first full scan.
		return nil
	}

	completed, err := garbageCollectOrphanBlocklistsFull(ctx, fdb, conn, gcMaxRuntime)
	if err != nil {
		return err
	}
	if !completed {
		// Time limit hit. Leave the flag set so the next run continues, rather
		// than claiming a full scan we did not finish.
		return nil
	}
	return meta.PutBool(gcNeedsFullScanKey, false)
}

func garbageCollectRetiredBlocklists(ctx context.Context, fdb *folderDB, conn *sqlx.Conn) error {
	l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName, "table", "gc_retired_blocklists")
	t0 := time.Now()

	fdb.updateLock.Lock()
	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		fdb.updateLock.Unlock()
		return wrap(err)
	}

	res, err := tx.ExecContext(ctx, `
		DELETE FROM blocklists
		WHERE blocklist_hash IN (SELECT blocklist_hash FROM gc_retired_blocklists)
		  AND NOT EXISTS (
			SELECT 1 FROM files WHERE files.blocklist_hash = blocklists.blocklist_hash
		  )`)
	if err != nil {
		_ = tx.Rollback()
		fdb.updateLock.Unlock()
		return wrap(err, "delete retired blocklists")
	}
	deletedLists, _ := res.RowsAffected()

	res, err = tx.ExecContext(ctx, `
		DELETE FROM blocks
		WHERE blocklist_hash IN (SELECT blocklist_hash FROM gc_retired_blocklists)
		  AND NOT EXISTS (
			SELECT 1 FROM files WHERE files.blocklist_hash = blocks.blocklist_hash
		  )`)
	if err != nil {
		_ = tx.Rollback()
		fdb.updateLock.Unlock()
		return wrap(err, "delete retired blocks")
	}
	deletedBlocks, _ := res.RowsAffected()

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM gc_retired_blocklists
		WHERE NOT EXISTS (
			SELECT 1 FROM blocklists WHERE blocklists.blocklist_hash = gc_retired_blocklists.blocklist_hash
		)
		AND NOT EXISTS (
			SELECT 1 FROM blocks WHERE blocks.blocklist_hash = gc_retired_blocklists.blocklist_hash
		)`); err != nil {
		_ = tx.Rollback()
		fdb.updateLock.Unlock()
		return wrap(err, "delete retired queue")
	}

	if err := tx.Commit(); err != nil {
		fdb.updateLock.Unlock()
		return wrap(err, "commit retired gc")
	}
	fdb.updateLock.Unlock()

	l.DebugContext(ctx, "Retired GC", "runtime", time.Since(t0), "blocklists", deletedLists, "blocks", deletedBlocks)
	return nil
}

// garbageCollectOrphanBlocklistsFull scans every blocklist with the NOT EXISTS
// files predicate, in time-bounded chunks. It returns completed=false when the
// time limit stopped it early, so the caller can leave the "needs a full scan"
// flag set and try again next run. Clearing that flag on an interrupted scan
// would silently strand the orphans that predate the retirement queue.
func garbageCollectOrphanBlocklistsFull(ctx context.Context, fdb *folderDB, conn *sqlx.Conn, maxRuntime time.Duration) (bool, error) {
	var rows int64
	if err := conn.GetContext(ctx, &rows, `SELECT count(*) FROM blocklists`); err != nil {
		return false, wrap(err)
	}
	chunks := max(gcMinChunks, rows/gcChunkSize)
	l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName, "table", "blocklists", "rows", rows, "chunks", chunks)
	t0 := time.Now()

	for i, br := range randomBlobRanges(int(chunks)) {
		if d := time.Since(t0); d > maxRuntime {
			l.InfoContext(ctx, "GC was interrupted due to exceeding time limit", "processed", i, "runtime", time.Since(t0))
			return false, nil
		}

		q := fmt.Sprintf(`
			DELETE FROM blocklists
			WHERE %s AND NOT EXISTS (
				SELECT 1 FROM files WHERE files.blocklist_hash = blocklists.blocklist_hash
			)`, br.SQL("blocklists.blocklist_hash"))

		fdb.updateLock.Lock()
		tx, err := conn.BeginTxx(ctx, nil)
		if err != nil {
			fdb.updateLock.Unlock()
			return false, wrap(err)
		}
		res, err := tx.ExecContext(ctx, q)
		if err != nil {
			_ = tx.Rollback()
			fdb.updateLock.Unlock()
			return false, wrap(err, "delete from blocklists")
		}

		if _, err := tx.ExecContext(ctx, `
			DELETE FROM blocks
			WHERE NOT EXISTS (
				SELECT 1 FROM files WHERE files.blocklist_hash = blocks.blocklist_hash
			)
			AND NOT EXISTS (
				SELECT 1 FROM blocklists WHERE blocklists.blocklist_hash = blocks.blocklist_hash
			)`); err != nil {
			_ = tx.Rollback()
			fdb.updateLock.Unlock()
			return false, wrap(err, "delete orphan blocks")
		}

		if err := tx.Commit(); err != nil {
			fdb.updateLock.Unlock()
			return false, wrap(err, "commit blocklists")
		}
		fdb.updateLock.Unlock()

		l.DebugContext(ctx, "GC query result", "processed", i, "runtime", time.Since(t0), "result", slogutil.Expensive(func() any {
			n, err := res.RowsAffected()
			if err != nil {
				return slogutil.Error(err)
			}
			return slog.Int64("rows", n)
		}))
	}
	return true, nil
}

// blobRange defines a range for blob searching. A range is open ended if
// start or end is nil.
type blobRange struct {
	start, end []byte
}

// SQL returns the SQL where clause for the given range, e.g.
// `column >= x'49249248' AND column < x'6db6db6c'`
func (r blobRange) SQL(name string) string {
	var sb strings.Builder
	if r.start != nil {
		fmt.Fprintf(&sb, "%s >= x'%x'", name, r.start)
	}
	if r.start != nil && r.end != nil {
		sb.WriteString(" AND ")
	}
	if r.end != nil {
		fmt.Fprintf(&sb, "%s < x'%x'", name, r.end)
	}
	return sb.String()
}

// randomBlobRanges returns n blobRanges in random order
func randomBlobRanges(n int) []blobRange {
	ranges := blobRanges(n)
	rand.Shuffle(len(ranges), func(i, j int) { ranges[i], ranges[j] = ranges[j], ranges[i] })
	return ranges
}

// blobRanges returns n blobRanges
func blobRanges(n int) []blobRange {
	// We use three byte (24 bit) prefixes to get fairly granular ranges and easy bit
	// conversions.
	rangeSize := (1 << 24) / n
	ranges := make([]blobRange, 0, n)
	var prev []byte
	for i := range n {
		var pref []byte
		if i < n-1 {
			end := (i + 1) * rangeSize
			pref = intToBlob(end)
		}
		ranges = append(ranges, blobRange{prev, pref})
		prev = pref
	}
	return ranges
}

func intToBlob(n int) []byte {
	var pref [4]byte
	binary.BigEndian.PutUint32(pref[:], uint32(n)) //nolint:gosec
	// first byte is always zero and not part of the range
	return pref[1:]
}
