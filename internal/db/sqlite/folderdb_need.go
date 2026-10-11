// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

// Incrementally maintained remote-need tracking.
//
// Answering "what does device X still need" requires probing every global
// file against the device's rows, which takes seconds on a folder with
// millions of files. These tables keep the answer precomputed: need_detail
// holds one row per (device, name) currently needed, need_totals the
// aggregate per (device, type, deleted) behind CountNeed.
//
// recalcGlobalForFile already loads every device's row for the name it
// touches, so it maintains both tables for free on the write path. After an
// upgrade the tables start empty; a background builder fills them in chunks
// while reads keep using the full-scan fallback, updates journal the names
// they touch, and the builder's final pass (under updateLock) drains the
// journal and flips the needTrackingBuilt kv flag. From then on reads are
// served from the tables and writes maintain them directly.
//
// Operations that remove a whole device's rows at once (DropAllFiles,
// DropDevice) would make incremental maintenance complicated; instead they
// reset the tables and the flag, and the next read kicks off a fresh build.

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"log/slog"
	"sync"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/lib/protocol"
)

const needTrackingBuiltKey = "needTrackingBuilt"

// needBuildChunkNames bounds how much of the global name_idx space one build
// transaction covers, so the background build never holds the write lock or a
// long transaction against live updates.
const needBuildChunkNames = 100_000

type needBuildState struct {
	mu      sync.Mutex
	running bool
	// gen changes whenever the tables are reset (DropAllFiles/DropDevice); a
	// builder observing a gen change aborts, because its chunked work built
	// on a state that no longer exists.
	gen uint64
}

type needDetailRow struct {
	DeviceIdx int64                 `db:"device_idx"`
	NameIdx   int64                 `db:"name_idx"`
	Type      protocol.FileInfoType `db:"type"`
	Size      int64                 `db:"size"`
	Deleted   bool                  `db:"deleted"`
}

// recalcNeedForFile maintains need_detail and need_totals for one file name.
// es holds every device's current row for the name, sorted so that es[globIdx]
// is the elected global entry; globalValid is whether that entry is usable
// (recalcGlobalForFile falls back to an invalid entry when all are invalid,
// and an invalid global creates no need).
//
// The contribution rules mirror neededGlobalFilesRemote exactly:
//   - a valid, non-deleted global is needed by every device that has no row
//     with the same version;
//   - a valid, deleted global is needed (as a delete) by every device that
//     still has a valid, non-deleted row.
func (s *folderDB) recalcNeedForFile(txp *txPreparedStmts, nameIdx int64, es []fileRow, global fileRow, globalValid bool) error {
	var devIdxs []int64
	if err := txp.Select(&devIdxs, `SELECT idx FROM devices WHERE idx != ?`, s.localDeviceIdx); err != nil {
		return wrap(err, "select devices")
	}
	if len(devIdxs) == 0 {
		return nil
	}

	var olds []needDetailRow
	if err := txp.Select(&olds, `SELECT device_idx, name_idx, type, size, deleted FROM need_detail WHERE name_idx = ?`, nameIdx); err != nil {
		return wrap(err, "select need_detail")
	}
	oldByDevice := make(map[int64]needDetailRow, len(olds))
	for _, o := range olds {
		oldByDevice[o.DeviceIdx] = o
	}

	for _, devIdx := range devIdxs {
		want, wantOK := needContribution(devIdx, es, global, globalValid)
		old, hasOld := oldByDevice[devIdx]
		switch {
		case !wantOK && !hasOld:
			// no change
		case wantOK && !hasOld:
			if err := s.putNeedDetail(txp, want); err != nil {
				return err
			}
		case !wantOK && hasOld:
			if err := s.delNeedDetail(txp, old); err != nil {
				return err
			}
		case wantOK && hasOld && (old.Type != want.Type || old.Size != want.Size || old.Deleted != want.Deleted):
			if err := s.delNeedDetail(txp, old); err != nil {
				return err
			}
			if err := s.putNeedDetail(txp, want); err != nil {
				return err
			}
		}
	}
	return nil
}

// needContribution computes whether device devIdx needs the file, given every
// row for the name and the elected global.
func needContribution(devIdx int64, es []fileRow, global fileRow, globalValid bool) (needDetailRow, bool) {
	if !globalValid {
		return needDetailRow{}, false
	}
	if !global.Deleted {
		for _, e := range es {
			if e.DeviceIdx == devIdx && e.Version.Equal(global.Version.Vector) {
				return needDetailRow{}, false
			}
		}
		return needDetailRow{DeviceIdx: devIdx, NameIdx: global.NameIdx, Type: global.Type, Size: global.Size}, true
	}
	for _, e := range es {
		if e.DeviceIdx == devIdx && !e.Deleted && !e.IsInvalid() {
			return needDetailRow{DeviceIdx: devIdx, NameIdx: global.NameIdx, Type: global.Type, Size: global.Size, Deleted: true}, true
		}
	}
	return needDetailRow{}, false
}

func (s *folderDB) putNeedDetail(txp *txPreparedStmts, row needDetailRow) error {
	if _, err := txp.Exec(`
		INSERT OR REPLACE INTO need_detail (device_idx, name_idx, type, size, deleted)
		VALUES (?, ?, ?, ?, ?)
	`, row.DeviceIdx, row.NameIdx, row.Type, row.Size, row.Deleted); err != nil {
		return wrap(err, "insert need_detail")
	}
	if err := s.bumpNeedTotals(txp, row.DeviceIdx, row.Type, row.Deleted, 1, row.Size); err != nil {
		return err
	}
	return nil
}

func (s *folderDB) delNeedDetail(txp *txPreparedStmts, row needDetailRow) error {
	if _, err := txp.Exec(`
		DELETE FROM need_detail WHERE device_idx = ? AND name_idx = ?
	`, row.DeviceIdx, row.NameIdx); err != nil {
		return wrap(err, "delete need_detail")
	}
	if err := s.bumpNeedTotals(txp, row.DeviceIdx, row.Type, row.Deleted, -1, -row.Size); err != nil {
		return err
	}
	return nil
}

func (s *folderDB) bumpNeedTotals(txp *txPreparedStmts, devIdx int64, typ protocol.FileInfoType, deleted bool, countDelta int, sizeDelta int64) error {
	_, err := txp.Exec(`
		INSERT INTO need_totals (device_idx, type, deleted, count, size)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(device_idx, type, deleted) DO UPDATE
			SET count = count + excluded.count, size = size + excluded.size
	`, devIdx, typ, deleted, countDelta, sizeDelta)
	return wrap(err, "bump need_totals")
}

// clearNeedForName drops every detail row for a name whose file rows are all
// gone, keeping the totals in step.
func (s *folderDB) clearNeedForName(txp *txPreparedStmts, name string) error {
	var olds []needDetailRow
	if err := txp.Select(&olds, `
		SELECT d.device_idx, d.name_idx, d.type, d.size, d.deleted FROM need_detail d
		INNER JOIN file_names n ON n.idx = d.name_idx
		WHERE n.name = ?
	`, name); err != nil {
		return wrap(err, "select need_detail by name")
	}
	for _, o := range olds {
		if err := s.delNeedDetail(txp, o); err != nil {
			return err
		}
	}
	return nil
}

// journalNeedBuildName records that a name changed while the initial build is
// running, so the builder's final pass re-derives its contribution from the
// post-change state.
func (s *folderDB) journalNeedBuildName(txp *txPreparedStmts, nameIdx int64) error {
	_, err := txp.Exec(`INSERT OR IGNORE INTO need_build_journal (name_idx) VALUES (?)`, nameIdx)
	return wrap(err, "journal need build name")
}

func (s *folderDB) needTrackingActive() bool {
	s.needBuild.mu.Lock()
	defer s.needBuild.mu.Unlock()
	return s.needBuilt.Load() || s.needBuild.running
}

// resetNeedTracking drops the maintained tables and the built flag, forcing a
// fresh background build. Called by the whole-device drop paths, whose effect
// is too broad for per-name maintenance. Caller holds updateLock.
func (s *folderDB) resetNeedTracking(txp *txPreparedStmts) error {
	for _, q := range []string{
		`DELETE FROM need_detail`,
		`DELETE FROM need_totals`,
		`DELETE FROM need_build_journal`,
		`DELETE FROM need_devices`,
		`DELETE FROM kv WHERE key = '` + needTrackingBuiltKey + `'`,
	} {
		if _, err := txp.Exec(q); err != nil {
			return wrap(err, "reset need tracking")
		}
	}
	s.needBuilt.Store(false)
	s.needBuild.mu.Lock()
	s.needBuild.gen++
	s.needBuild.mu.Unlock()
	return nil
}

// needDeviceKnown reports whether the device's need_detail coverage is
// complete. Devices unknown to the tracking fall back to the full-scan
// queries; their next update backfills them.
func (s *folderDB) needDeviceKnown(device protocol.DeviceID) bool {
	var known bool
	if err := s.stmt(`
		SELECT count(*) > 0 FROM need_devices nd
		INNER JOIN devices d ON d.idx = nd.device_idx
		WHERE d.device_id = ?
	`).Get(&known, device.String()); err != nil {
		return false
	}
	return known
}

// backfillNeedForDevice gives a device that is new to the tracking its full
// detail set: every valid, non-deleted global it has no matching row for.
// Called from Update before the batch is applied, so the batch's per-name
// maintenance then adjusts the announced names. Idempotent. Caller holds
// updateLock.
func (s *folderDB) backfillNeedForDevice(txp *txPreparedStmts, deviceIdx int64) error {
	var known bool
	if err := txp.Get(&known, `SELECT count(*) > 0 FROM need_devices WHERE device_idx = ?`, deviceIdx); err != nil {
		return wrap(err, "check need_devices")
	}
	if known {
		return nil
	}
	if _, err := txp.Exec(`
		INSERT OR IGNORE INTO need_detail (device_idx, name_idx, type, size, deleted)
		SELECT ?, g.name_idx, g.type, g.size, 0 FROM files g
		WHERE g.local_flags & ? != 0 AND NOT g.deleted AND g.local_flags & ? = 0
			AND NOT EXISTS (
				SELECT 1 FROM files f
				WHERE f.device_idx = ? AND f.name_idx = g.name_idx AND f.version_idx = g.version_idx
			)
	`, deviceIdx, protocol.FlagLocalGlobal, protocol.LocalInvalidFlags, deviceIdx); err != nil {
		return wrap(err, "backfill need_detail")
	}
	if _, err := txp.Exec(`DELETE FROM need_totals WHERE device_idx = ?`, deviceIdx); err != nil {
		return wrap(err, "clear need_totals for backfill")
	}
	if _, err := txp.Exec(`
		INSERT INTO need_totals (device_idx, type, deleted, count, size)
		SELECT device_idx, type, deleted, COUNT(*), COALESCE(SUM(size), 0)
		FROM need_detail WHERE device_idx = ? GROUP BY device_idx, type, deleted
	`, deviceIdx); err != nil {
		return wrap(err, "backfill need_totals")
	}
	if _, err := txp.Exec(`INSERT OR IGNORE INTO need_devices (device_idx) VALUES (?)`, deviceIdx); err != nil {
		return wrap(err, "mark need device")
	}
	return nil
}

// --- Reads ---

// needTotalsFromDetail answers CountNeed from the maintained totals table.
func (s *folderDB) needTotalsFromDetail(device protocol.DeviceID) (db.Counts, error) {
	if !s.needDeviceKnown(device) {
		return s.needSizeRemote(device)
	}
	var res []countsRow
	err := s.stmt(`
		SELECT t.type, t.count, t.size, t.deleted, 0 as local_flags FROM need_totals t
		INNER JOIN devices d ON d.idx = t.device_idx
		WHERE d.device_id = ?
	`).Select(&res, device.String())
	if err != nil {
		return db.Counts{}, wrap(err)
	}
	return summarizeCounts(res), nil
}

// neededGlobalFilesFromDetail answers the remote need list from the
// maintained detail table.
func (s *folderDB) neededGlobalFilesFromDetail(device protocol.DeviceID, selectOpts string) (iter.Seq[protocol.FileInfo], func() error) {
	it, errFn := iterStructs[indirectFI](s.stmt(`
		SELECT fi.fiprotobuf, bl.blprotobuf, n.name, g.size, g.modified FROM need_detail nd
		INNER JOIN file_names n ON n.idx = nd.name_idx
		INNER JOIN files g ON g.name_idx = nd.name_idx AND g.local_flags & {{.FlagLocalGlobal}} != 0
		INNER JOIN fileinfos fi ON fi.sequence = g.sequence
		LEFT JOIN blocklists bl ON bl.blocklist_hash = g.blocklist_hash
		INNER JOIN devices d ON d.idx = nd.device_idx
		WHERE d.device_id = ?
	` + selectOpts).Queryx(device.String()))
	return itererr.Map(it, errFn, indirectFI.FileInfo)
}

// --- Background build ---

func (s *folderDB) ensureNeedBuild() {
	if s.needBuilt.Load() {
		return
	}
	s.needBuild.mu.Lock()
	if s.needBuild.running {
		s.needBuild.mu.Unlock()
		return
	}
	s.needBuild.running = true
	gen := s.needBuild.gen
	s.needBuild.mu.Unlock()
	go s.buildNeedTracking(gen)
}

func (s *folderDB) buildNeedTracking(gen uint64) {
	log := slog.With("folder", s.folderID, "db", s.baseName)
	defer func() {
		s.needBuild.mu.Lock()
		s.needBuild.running = false
		s.needBuild.mu.Unlock()
	}()

	aborted := func() bool {
		s.needBuild.mu.Lock()
		defer s.needBuild.mu.Unlock()
		return s.needBuild.gen != gen
	}

	// Chunk over the name_idx space of global rows, for the devices present
	// when the build started; devices created mid-build are simply not marked
	// known, and their first post-build update backfills them. Each chunk is
	// its own short transaction; updates racing the build journal the names
	// they touch, and the final pass re-derives those.
	var maxNameIdx, maxDeviceIdx int64
	if err := s.stmt(`SELECT COALESCE(MAX(name_idx), 0) FROM files`).Get(&maxNameIdx); err != nil {
		log.Warn("Need tracking build failed at range scan", "error", err)
		return
	}
	if err := s.stmt(`SELECT COALESCE(MAX(idx), 0) FROM devices`).Get(&maxDeviceIdx); err != nil {
		log.Warn("Need tracking build failed at device scan", "error", err)
		return
	}

	for lo := int64(0); lo < maxNameIdx && !aborted(); lo += needBuildChunkNames {
		hi := lo + needBuildChunkNames
		if err := func() error {
			tx, err := s.sql.BeginTxx(context.Background(), nil)
			if err != nil {
				return wrap(err)
			}
			defer tx.Rollback() //nolint:errcheck
			// Valid, non-deleted globals no device row matches.
			if _, err := tx.Exec(`
				INSERT OR REPLACE INTO need_detail (device_idx, name_idx, type, size, deleted)
				SELECT d.idx, g.name_idx, g.type, g.size, 0 FROM files g
				CROSS JOIN devices d
				WHERE g.name_idx > ? AND g.name_idx <= ?
					AND g.local_flags & ? != 0 AND NOT g.deleted AND g.local_flags & ? = 0
					AND d.idx != ? AND d.idx <= ?
					AND NOT EXISTS (
						SELECT 1 FROM files f
						WHERE f.device_idx = d.idx AND f.name_idx = g.name_idx AND f.version_idx = g.version_idx
					)
			`, lo, hi, protocol.FlagLocalGlobal, protocol.LocalInvalidFlags, s.localDeviceIdx, maxDeviceIdx); err != nil {
				return wrap(err, "build non-deleted chunk")
			}
			// Valid, deleted globals a device still holds a live row for.
			if _, err := tx.Exec(`
				INSERT OR REPLACE INTO need_detail (device_idx, name_idx, type, size, deleted)
				SELECT d.idx, g.name_idx, g.type, g.size, 1 FROM files g
				CROSS JOIN devices d
				WHERE g.name_idx > ? AND g.name_idx <= ?
					AND g.local_flags & ? != 0 AND g.deleted AND g.local_flags & ? = 0
					AND d.idx != ? AND d.idx <= ?
					AND EXISTS (
						SELECT 1 FROM files f
						WHERE f.device_idx = d.idx AND f.name_idx = g.name_idx AND NOT f.deleted AND f.local_flags & ? = 0
					)
			`, lo, hi, protocol.FlagLocalGlobal, protocol.LocalInvalidFlags, s.localDeviceIdx, maxDeviceIdx, protocol.LocalInvalidFlags); err != nil {
				return wrap(err, "build deleted chunk")
			}
			return wrap(tx.Commit())
		}(); err != nil {
			log.Warn("Need tracking build failed in chunk", "error", err)
			return
		}
	}

	// Final pass: rebuild the aggregates and re-derive every name that
	// changed during the build, then flip the flag. Holding updateLock makes
	// the journal drain and the flag flip atomic with respect to updates.
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	if aborted() {
		return
	}

	tx, err := s.sql.BeginTxx(context.Background(), nil)
	if err != nil {
		log.Warn("Need tracking build failed at final pass", "error", err)
		return
	}
	defer tx.Rollback() //nolint:errcheck
	txp := &txPreparedStmts{Tx: tx}

	if _, err := txp.Exec(`DELETE FROM need_totals`); err != nil {
		log.Warn("Need tracking build failed clearing totals", "error", err)
		return
	}
	if _, err := txp.Exec(`
		INSERT INTO need_totals (device_idx, type, deleted, count, size)
		SELECT device_idx, type, deleted, COUNT(*), COALESCE(SUM(size), 0)
		FROM need_detail GROUP BY device_idx, type, deleted
	`); err != nil {
		log.Warn("Need tracking build failed aggregating totals", "error", err)
		return
	}

	var journaled []int64
	if err := txp.Select(&journaled, `SELECT name_idx FROM need_build_journal`); err != nil {
		log.Warn("Need tracking build failed reading journal", "error", err)
		return
	}
	for _, nameIdx := range journaled {
		var name string
		if err := txp.Get(&name, `SELECT name FROM file_names WHERE idx = ?`, nameIdx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			log.Warn("Need tracking build failed resolving journal name", "error", err)
			return
		}
		if err := s.recalcGlobalForFile(txp, name); err != nil {
			log.Warn("Need tracking build failed draining journal", "error", err)
			return
		}
	}
	if _, err := txp.Exec(`DELETE FROM need_build_journal`); err != nil {
		log.Warn("Need tracking build failed clearing journal", "error", err)
		return
	}
	// Devices covered by the chunk pass are fully known from now on.
	if _, err := txp.Exec(`INSERT OR IGNORE INTO need_devices (device_idx) SELECT idx FROM devices WHERE idx != ? AND idx <= ?`, s.localDeviceIdx, maxDeviceIdx); err != nil {
		log.Warn("Need tracking build failed marking devices", "error", err)
		return
	}
	if _, err := txp.Exec(`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`, needTrackingBuiltKey, []byte{1}); err != nil {
		log.Warn("Need tracking build failed setting flag", "error", err)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Warn("Need tracking build failed committing", "error", err)
		return
	}
	s.needBuilt.Store(true)
	log.Info("Need tracking tables built", "journaledNames", len(journaled))
}
