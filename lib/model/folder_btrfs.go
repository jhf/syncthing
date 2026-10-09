// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/fs"
)

// This file implements the Linux/btrfs counterpart of the FSEvents journal
// replay (see tryArmWatchReplay): instead of seeding a watch stream with a
// journal position, a root-owned helper diffs two btrfs snapshots (one taken
// while the index was complete, one taken now) and the changed paths are
// scanned directly. The diff is exact, so every failure mode is operational
// (helper missing, baseline snapshot gone, nested subvolume inside the
// folder) and every failure falls back to the regular full scan.
//
// Ordering invariant: the watch must be started BEFORE the diff snapshot is
// taken. The diff covers everything up to the snapshot, the watch covers
// everything after its start; starting the watch first makes the two overlap
// instead of leaving a gap.

// maxBtrfsReplayPaths caps the targeted scan after a snapshot diff. A larger
// diff means the baseline is ancient and the regular full walk is the simpler
// course; the scan that then runs refreshes the baseline.
const maxBtrfsReplayPaths = 200_000

// btrfsBaselineRefreshInterval bounds how stale the baseline snapshot gets
// while the watch is healthy, so the next restart's diff stays small. The
// snapshot itself costs milliseconds (reflink metadata only).
const btrfsBaselineRefreshInterval = 10 * time.Minute

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
	defer func() {
		if !f.btrfsOK {
			f.sl.DebugContext(ctx, "Btrfs scan replay not available for this folder")
		}
	}()
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

// tryArmBtrfsScanReplay attempts to skip the initial full scan by applying a
// btrfs snapshot diff. Must be called after startWatch (see the ordering
// invariant above). On success the changed paths are scanned synchronously,
// the diff snapshot is promoted to the new baseline, and the folder falls
// into the regular rescan schedule, keeping periodic full scans as the
// safety net.
func (f *folder) tryArmBtrfsScanReplay(ctx context.Context) bool {
	if !f.btrfsReplayEligible(ctx) {
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
		// No baseline yet (first run with the helper installed); the full
		// scan that follows creates one via refreshBtrfsBaseline.
		return false
	}
	paths, err := fs.BtrfsScanDiff(ctx, f.Filesystem().URI())
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
	// The diff's snapshot becomes the baseline only now: a crash before this
	// point replays a superset next time, a crash after loses nothing.
	if err := fs.BtrfsScanPromote(ctx); err != nil {
		f.sl.WarnContext(ctx, "Failed to promote btrfs scan baseline", slogutil.Error(err))
	}
	f.lastBtrfsBaselineRefresh = time.Now()

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

// refreshBtrfsBaseline replaces the baseline snapshot with one of the
// current filesystem state. Called when the index is known complete: after a
// full scan (force) or after watch batches at a bounded rate. Snapshotting
// while watch events are still queued is safe: the snapshot then predates
// full index coverage and the next diff over-reports, never under-reports.
func (f *folder) refreshBtrfsBaseline(ctx context.Context, force bool) {
	if !f.btrfsReplayEligible(ctx) {
		return
	}
	if !force && time.Since(f.lastBtrfsBaselineRefresh) < btrfsBaselineRefreshInterval {
		return
	}
	if err := fs.BtrfsScanRefresh(ctx); err != nil {
		f.sl.WarnContext(ctx, "Failed to refresh btrfs scan baseline", slogutil.Error(err))
		return
	}
	f.lastBtrfsBaselineRefresh = time.Now()
	f.sl.DebugContext(ctx, "Refreshed btrfs scan baseline")
}
