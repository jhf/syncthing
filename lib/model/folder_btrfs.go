// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
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

// btrfsBaselineMinInterval rate-limits runtime baseline refreshes. A
// refresh costs one snapshot swap (O(1) metadata) plus freeing the old
// baseline's pinned blocks, so the limit exists only to guard against
// pathologically frequent full scans.
const btrfsBaselineMinInterval = 10 * time.Minute

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
	if ok, err := fs.BtrfsScanReplaySupportedErr(); !ok {
		if err != nil {
			f.sl.InfoContext(ctx, "Btrfs scan replay not available", slogutil.Error(err))
		}
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
	select {
	case <-f.initialScanFinished:
		// Another fast path (FSEvents journal replay) already armed and
		// closed this; a diff on top would redo work the replay covers.
		return false
	default:
	}
	if !f.btrfsCleanStart {
		// Not an error - an unclean shutdown or a skipped pairing lands
		// here - but a full 2M-file scan deserves a visible reason.
		f.sl.InfoContext(ctx, "Btrfs scan replay not armed: no pairing marker from previous run, doing a full scan")
		return false
	}
	if done, err := f.initialScanEverCompleted(); err != nil || !done {
		// Without a recorded complete index the diff has nothing to build on.
		f.sl.InfoContext(ctx, "Btrfs scan replay not armed: no completed initial scan on record, doing a full scan", slogutil.Error(err))
		return false
	}
	ok, err := fs.BtrfsScanBaselineOK(ctx)
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to check btrfs scan baseline", slogutil.Error(err))
		return false
	}
	if !ok {
		f.sl.InfoContext(ctx, "Btrfs scan replay not armed: helper reports no baseline, doing a full scan")
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
	f.fullScanOK.Store(true)

	// Re-arm the marker immediately. The boot consumed it, and without a
	// fresh pairing the next restart depends on the shutdown pairing,
	// which races the process-exit deadline (observed on yarlung
	// 2026-10-16: exit 11s after SIGTERM with the barrier still in
	// flight, marker lost, next boot full-scanned). The replay just
	// proved the index matches the filesystem, so pair a fresh baseline
	// now; then ANY restart is covered, clean or not.
	f.lastBtrfsRefresh = time.Now()
	f.pairBtrfsBaseline(ctx, "post-replay")
	return true
}

// pairBtrfsBaseline runs the baseline pairing ritual: flush the watch,
// snapshot, prove with a canary barrier that every change up to the
// snapshot has been scanned into the index, then make the snapshot the new
// baseline (deleting the old one, reclaiming its pinned blocks) and record
// the clean marker arming the next boot's replay. Must run on the folder's
// Serve goroutine (the sole watchChan consumer). Any failure is safe: the
// baseline and marker are untouched and the next boot falls back to a full
// scan.
//
// The marker is written on every successful pairing, not just at shutdown:
// a crash after a runtime pairing is still sound, because the state-based
// boot diff covers every change after the baseline no matter what the index
// did, and the barrier guarantees every change before the baseline is in
// the index.
func (f *folder) pairBtrfsBaseline(ctx context.Context, trigger string) bool {
	f.sl.InfoContext(ctx, "Btrfs baseline: attempting pairing", "trigger", trigger)

	// Drain every buffered watch event into the index. The barrier below is
	// what proves completeness; this flush just keeps the barrier short.
	ack, ok := f.requestWatchFlush(ctx)
	if !ok {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, watch flush unavailable (watch not running)")
		return false
	}
	if !f.scanUntilAck(ctx, ack) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, watch did not drain in time")
		return false
	}

	// Pin the filesystem state. Changes after this point are irrelevant to
	// the pairing: the next boot's diff against this baseline covers them.
	snap := btrfsSnapName(trigger, f.folderID)
	defer fs.BtrfsScanRemoveSnapshot(context.Background(), snap)
	if err := fs.BtrfsScanSnapshot(ctx, snap); err != nil {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, snapshot error", slogutil.Error(err))
		return false
	}

	// Prove that every change up to the snapshot is in the index.
	if !f.watchBarrier(ctx) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, watch barrier not reached")
		return false
	}

	if err := fs.BtrfsScanSetBaseline(ctx, snap); err != nil {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, setbaseline error", slogutil.Error(err))
		return false
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().Unix()))
	if err := f.db.PutKV(btrfsBaselineKVPrefix+f.folderID, b[:]); err != nil {
		f.sl.WarnContext(ctx, "Failed to record btrfs baseline marker", slogutil.Error(err))
		return false
	}
	f.sl.InfoContext(ctx, "Btrfs scan baseline paired with index", "trigger", trigger)
	return true
}

// watchBarrier proves that the watch has delivered, and we have scanned,
// every filesystem event generated before the barrier: inotify event
// delivery is FIFO per watch descriptor, so observing the canary's own
// event implies all earlier events have been observed too. This replaces a
// wall-clock "quiet period", which could only guess at the
// kernel->aggregator->batch latency and could declare quiet while events
// were still in flight. (A queue overflow voids the FIFO guarantee, but an
// overflow already forces a full scan, so it cannot combine with pairing.)
// The canary uses the temp-file prefix, so the scanner ignores it and it is
// never indexed or synced.
func (f *folder) watchBarrier(ctx context.Context) bool {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		f.sl.DebugContext(ctx, "Btrfs baseline: canary nonce failed", slogutil.Error(err))
		return false
	}
	canary := fs.WatchBarrierCanaryPrefix + hex.EncodeToString(nonce[:]) + ".tmp"
	file, err := f.Filesystem().Create(canary)
	if err != nil {
		f.sl.DebugContext(ctx, "Btrfs baseline: canary create failed", slogutil.Error(err))
		return false
	}
	file.Close()
	defer f.Filesystem().Remove(canary) // best effort

	// Flush once so a canary event already sitting in the aggregator is
	// emitted immediately; if it has not reached the aggregator yet, the
	// regular aggregation delay delivers it.
	ack, _ := f.requestWatchFlush(ctx)

	for {
		select {
		case batch := <-f.watchChan:
			if err := f.scanSubdirs(ctx, batch.Paths); err != nil {
				return false
			}
			if slices.Contains(batch.Paths, canary) {
				return true
			}
		case <-ack:
			ack = nil
		case <-ctx.Done():
			return false
		}
	}
}

// shutdownBtrfsBaseline pairs the baseline snapshot with a provably
// consistent index at shutdown, arming the next boot's snapshot-diff
// replay. Any failure or busy filesystem just means no pairing: the next
// boot runs the regular full scan. The watch is still alive here (its
// context outlives Serve's); it is stopped by Serve's defer afterwards.
func (f *folder) shutdownBtrfsBaseline() {
	if !f.btrfsOK || !f.FSWatcherEnabled {
		f.sl.InfoContext(context.Background(), "Btrfs baseline: skipping pairing", "btrfsOK", f.btrfsOK, "fsWatcher", f.FSWatcherEnabled)
		return
	}
	// The pairing is only sound when the index is provably complete NOW:
	// a run that neither armed the replay nor finished a full scan still
	// carries staleness the watch barrier cannot repair.
	if !f.fullScanOK.Load() {
		f.sl.InfoContext(context.Background(), "Btrfs baseline: skipping pairing, index not provably complete")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if !f.pairBtrfsBaseline(ctx, "shutdown") {
		f.sl.InfoContext(ctx, "Btrfs baseline: no clean shutdown pairing, next boot does a full scan")
	}
}

// refreshBtrfsBaseline advances the baseline snapshot at runtime, right
// after a completed full scan (which makes the index provably complete).
// Without it the baseline only moves at clean shutdown, so a months-long
// uptime would pin months of churned blocks in the old baseline; with it,
// pinned growth is bounded by the refresh cadence (every full scan, at most
// once per btrfsBaselineMinInterval). Must run on the Serve goroutine.
func (f *folder) refreshBtrfsBaseline(ctx context.Context) {
	if !f.btrfsOK || !f.FSWatcherEnabled {
		return
	}
	if time.Since(f.lastBtrfsRefresh) < btrfsBaselineMinInterval {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if f.pairBtrfsBaseline(ctx, "runtime") {
		f.lastBtrfsRefresh = time.Now()
	}
}

// watchFlushTimeout bounds how long requestWatchFlush waits for the
// aggregator to accept the flush before concluding it is dead.
var watchFlushTimeout = 15 * time.Second

// requestWatchFlush asks the aggregator to emit pending batches immediately.
// The send must wait: the aggregator can be alive but momentarily busy
// (blocked handing a batch to the scanner inside actOnTimer), and a
// non-blocking send with a default branch then aborts a perfectly valid
// pairing — observed on yarlung 2026-10-15, "watch flush unavailable,
// watch not running" at the runtime pairing while the watcher had been up
// for 45 minutes. The timeout distinguishes busy from dead.
func (f *folder) requestWatchFlush(ctx context.Context) (chan struct{}, bool) {
	ack := make(chan struct{})
	timer := time.NewTimer(watchFlushTimeout)
	defer timer.Stop()
	select {
	case f.watchFlush <- ack:
		return ack, true
	case <-timer.C:
		// Aggregator not running (watch failed or was never started):
		// no clean pairing is possible.
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

// scanUntilAck scans watch batches until the flush ack closes.
func (f *folder) scanUntilAck(ctx context.Context, ack chan struct{}) bool {
	for {
		select {
		case batch := <-f.watchChan:
			if err := f.scanSubdirs(ctx, batch.Paths); err != nil {
				return false
			}
		case <-ack:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

// btrfsSnapName builds a helper-safe snapshot name.
func btrfsSnapName(prefix, folderID string) string {
	id := btrfsSnapNameClean.ReplaceAllString(folderID, "-")
	return fmt.Sprintf("%s-%s-%d", prefix, id, time.Now().UnixNano())
}
