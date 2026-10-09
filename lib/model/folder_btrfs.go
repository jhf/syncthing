// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/internal/slogutil"
)

// This file implements the Linux/btrfs counterpart of the FSEvents journal
// replay (see tryArmWatchReplay), skipping the initial full scan by applying
// a snapshot diff instead of walking the tree.
//
// Why the design is what it is: a snapshot diff is STATE-based, not
// event-based like the FSEvents journal. A file created, indexed by the
// watch, and deleted again between the baseline snapshot and the next boot
// leaves no trace in the diff (absent at both ends) while the index still
// holds it - and next boot would resurrect it to the peers. The only safe
// baseline is therefore one paired with a provably consistent index: the
// shutdown sequence below drains the watch completely, snapshots, and
// confirms no further events arrive afterwards, establishing index ==
// disk(snapshot). It then records a clean-shutdown marker KV, which a crash
// cannot leave behind, and boot arming requires that marker.
//
// Boot ordering invariant: the watch is started BEFORE the diff snapshot is
// taken, so live changes and the diff overlap instead of leaving a gap.

// btrfsBaselineKVPrefix namespaces the clean-shutdown marker: it exists iff
// the previous run paired the baseline snapshot with a provably consistent
// index (see above). Read and deleted at folder start, so a crashed run can
// never inherit it; rewritten by the shutdown sequence on success.
const btrfsBaselineKVPrefix = "model/btrfsScanBaseline/"

// maxBtrfsReplayPaths caps the targeted scan after a snapshot diff. A larger
// diff means the downtime was very busy and the regular full walk is the
// simpler course.
const maxBtrfsReplayPaths = 200_000

var btrfsSnapNameClean = regexp.MustCompile(`[^a-z0-9.-]+`)

// btrfsReplayEligible reports, once per folder lifetime, whether this folder
// can use btrfs scan replay: helper installed, folder on the helper's btrfs
// subvolume, and no nested subvolume inside the folder (a snapshot diff does
// not cross subvolume boundaries, so a nested subvolume would silently
// escape coverage).
func (f *folder) btrfsReplayEligible(ctx context.Context) bool {
	if f.btrfsChecked {
		return f.btrfsOK
	}
	f.btrfsChecked = true
	if !fs.BtrfsScanReplaySupported() {
		return false
	}
	nested, err := fs.BtrfsScanNestedSubvols(ctx, f.Filesystem().URI())
	if err != nil {
		f.sl.DebugContext(ctx, "Btrfs scan replay check failed", slogutil.Error(err))
		return false
	}
	if len(nested) > 0 {
		f.sl.InfoContext(ctx, "Btrfs scan replay disabled: folder contains nested subvolumes", "subvolumes", nested)
		return false
	}
	f.btrfsOK = true
	return true
}

// consumeBtrfsBaselineMarker reads and deletes the clean-shutdown marker,
// returning whether it was present. Must run once at folder start; deleting
// eagerly is what makes the marker crash-proof.
func (f *folder) consumeBtrfsBaselineMarker(ctx context.Context) bool {
	if !f.btrfsReplayEligible(ctx) {
		return false
	}
	_, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to read btrfs baseline marker", slogutil.Error(err))
		return false
	}
	if err := f.db.DeleteKV(btrfsBaselineKVPrefix + f.folderID); err != nil {
		f.sl.WarnContext(ctx, "Failed to drop btrfs baseline marker", slogutil.Error(err))
	}
	return true
}

// tryArmBtrfsScanReplay attempts to skip the initial full scan by applying a
// btrfs snapshot diff. Must be called after startWatch (see the ordering
// invariant above). On success the changed paths are scanned synchronously
// and the folder falls into the regular rescan schedule, keeping periodic
// full scans as the safety net.
func (f *folder) tryArmBtrfsScanReplay(ctx context.Context) bool {
	if !f.btrfsCleanStart {
		return false
	}
	if done, err := f.initialScanEverCompleted(); err != nil || !done {
		// Without a recorded complete index the diff has nothing to build on.
		return false
	}
	ok, err := fs.BtrfsScanBaselineOK(ctx)
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to check btrfs scan baseline", slogutil.Error(err))
		return false
	}
	if !ok {
		return false
	}
	snap := btrfsSnapName("boot", f.folderID)
	if err := fs.BtrfsScanSnapshot(ctx, snap); err != nil {
		f.sl.WarnContext(ctx, "Failed to snapshot for btrfs scan diff, falling back to full scan", slogutil.Error(err))
		return false
	}
	defer fs.BtrfsScanRemoveSnapshot(context.Background(), snap)
	paths, err := fs.BtrfsScanDiffBaseline(ctx, snap, f.Filesystem().URI())
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to compute btrfs snapshot diff, falling back to full scan", slogutil.Error(err))
		return false
	}
	if len(paths) > maxBtrfsReplayPaths {
		f.sl.InfoContext(ctx, "Btrfs snapshot diff too large, falling back to full scan", "paths", len(paths), "max", maxBtrfsReplayPaths)
		return false
	}
	if len(paths) > 0 {
		if err := f.scanSubdirs(ctx, paths); err != nil {
			f.sl.WarnContext(ctx, "Failed to scan btrfs snapshot diff, falling back to full scan", slogutil.Error(err))
			return false
		}
	}

	// Skip the pending initial scan and fall straight into the regular
	// rescan schedule (same as tryArmWatchReplay).
	if !f.scanTimer.Stop() {
		select {
		case <-f.scanTimer.C:
		default:
		}
	}
	f.Reschedule()
	select {
	case <-f.initialScanFinished:
	default:
		close(f.initialScanFinished)
	}
	f.sl.InfoContext(ctx, "Skipping initial scan, applied btrfs snapshot diff", "changedPaths", len(paths))
	return true
}

// shutdownBtrfsBaseline pairs the baseline snapshot with a provably
// consistent index at shutdown: drain the watch completely, snapshot, and
// confirm no further events arrive afterwards (then index == disk at the
// snapshot). Only then is the clean-shutdown marker written, arming the next
// boot's diff. Any failure or busy filesystem just means no marker: the next
// boot runs the regular full scan. The watch is still alive here (its
// context outlives Serve's); it is stopped by Serve's defer afterwards.
func (f *folder) shutdownBtrfsBaseline() {
	if !f.btrfsOK || !f.FSWatcherEnabled {
		return
	}
	if done, err := f.initialScanEverCompleted(); err != nil || !done {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	snap := btrfsSnapName("shutdown", f.folderID)
	defer fs.BtrfsScanRemoveSnapshot(context.Background(), snap)

	for attempt := 0; attempt < 3; attempt++ {
		// Drain every buffered watch event into the index.
		if !f.drainWatch(ctx) {
			f.sl.DebugContext(ctx, "Btrfs baseline: watch did not drain in time")
			return
		}
		// Pin the filesystem state.
		if err := fs.BtrfsScanSnapshot(ctx, snap); err != nil {
			f.sl.DebugContext(ctx, "Btrfs baseline: snapshot failed", slogutil.Error(err))
			return
		}
		// Changes after the drain must show up as new events; scanning them
		// would move the index past the snapshot, so discard instead and
		// retry with a fresh snapshot.
		if f.awaitWatchQuiet(ctx) {
			if err := fs.BtrfsScanSetBaseline(ctx, snap); err != nil {
				f.sl.DebugContext(ctx, "Btrfs baseline: setbaseline failed", slogutil.Error(err))
				return
			}
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(time.Now().Unix()))
			if err := f.db.PutKV(btrfsBaselineKVPrefix+f.folderID, b[:]); err != nil {
				f.sl.WarnContext(ctx, "Failed to record btrfs baseline marker", slogutil.Error(err))
				return
			}
			f.sl.DebugContext(ctx, "Btrfs scan baseline paired with index at shutdown")
			return
		}
		_ = fs.BtrfsScanRemoveSnapshot(ctx, snap)
	}
	f.sl.DebugContext(ctx, "Btrfs baseline: filesystem busy, no clean pairing")
}

// drainWatch flushes and scans pending watch batches until none arrive for
// btrfsWatchQuietPeriod after a flush. Returns false on timeout/cancel.
func (f *folder) drainWatch(ctx context.Context) bool {
	for {
		ack := make(chan struct{})
		select {
		case f.watchFlush <- ack:
		case <-ctx.Done():
			return false
		default:
			// Aggregator not running (watch failed or was never started):
			// no clean pairing is possible.
			return false
		}
		select {
		case <-ack:
		case <-ctx.Done():
			return false
		}
		select {
		case batch := <-f.watchChan:
			if err := f.scanSubdirs(ctx, batch.Paths); err != nil {
				return false
			}
		case <-time.After(btrfsWatchQuietPeriod):
			return true
		case <-ctx.Done():
			return false
		}
	}
}

// awaitWatchQuiet reports whether no watch events arrive for
// btrfsWatchQuietPeriod after a flush. Arriving batches are DISCARDED, not
// scanned: they postdate the snapshot, and leaving them out of the index is
// exactly what the next boot's diff expects.
func (f *folder) awaitWatchQuiet(ctx context.Context) bool {
	ack := make(chan struct{})
	select {
	case f.watchFlush <- ack:
	case <-ctx.Done():
		return false
	default:
		return false
	}
	select {
	case <-ack:
	case <-ctx.Done():
		return false
	}
	select {
	case batch := <-f.watchChan:
		f.sl.DebugContext(ctx, "Btrfs baseline: discarding post-snapshot batch", "paths", len(batch.Paths))
		return false
	case <-time.After(btrfsWatchQuietPeriod):
		return true
	case <-ctx.Done():
		return false
	}
}

// btrfsWatchQuietPeriod bounds kernel->aggregator->batch latency when proving
// that no change happened around the shutdown snapshot.
const btrfsWatchQuietPeriod = 2 * time.Second

// btrfsSnapName builds a helper-safe snapshot name.
func btrfsSnapName(prefix, folderID string) string {
	id := btrfsSnapNameClean.ReplaceAllString(folderID, "-")
	return fmt.Sprintf("%s-%s-%d", prefix, id, time.Now().UnixNano())
}
