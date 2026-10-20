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
	"os"
	"regexp"
	"slices"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/syncthing/syncthing/lib/watchaggregator"
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

// btrfsBaselineKVPrefix namespaces the pairing marker: it exists iff a run
// paired the baseline snapshot with a provably consistent index (see
// above). Its value (btrfsMarker) records where the local index stood at
// the pairing, so a boot can rescan every name the index wrote since then
// on top of the snapshot diff. That makes the marker valid for any number
// of boots until the next pairing replaces it: a restart before the new
// pairing (the watch setup takes ~34 min on yarlung) still replays
// instead of full-scanning. A legacy 8-byte marker (no index position) is
// consumed at boot as before.
const btrfsBaselineKVPrefix = "model/btrfsScanBaseline/"

// btrfsMarker is the decoded pairing marker. seqBefore is the local index
// sequence read before the pairing's snapshot: every index write after it
// has a higher sequence, so names above it are what the index may hold
// that the snapshot diff cannot see (created, indexed and removed again
// after the baseline). seqAfter is read once the barrier proved the index
// complete: a boot finding the local sequence below it has lost index
// writes the pairing relied on, and full-scans. indexID binds the marker
// to this index: a recreated folder database starts a new ID and its
// sequences again at 1.
type btrfsMarker struct {
	pairedAt  time.Time
	seqBefore int64
	seqAfter  int64
	indexID   protocol.IndexID
	durable   bool // false for a legacy time-only marker
}

const btrfsMarkerLen = 32

func (m btrfsMarker) encode() []byte {
	b := make([]byte, btrfsMarkerLen)
	binary.BigEndian.PutUint64(b[0:], uint64(m.pairedAt.Unix()))
	binary.BigEndian.PutUint64(b[8:], uint64(m.seqBefore))
	binary.BigEndian.PutUint64(b[16:], uint64(m.seqAfter))
	binary.BigEndian.PutUint64(b[24:], uint64(m.indexID))
	return b
}

func decodeBtrfsMarker(b []byte) (btrfsMarker, bool) {
	switch len(b) {
	case 8:
		return btrfsMarker{pairedAt: time.Unix(int64(binary.BigEndian.Uint64(b)), 0)}, true
	case btrfsMarkerLen:
		return btrfsMarker{
			pairedAt:  time.Unix(int64(binary.BigEndian.Uint64(b[0:])), 0),
			seqBefore: int64(binary.BigEndian.Uint64(b[8:])),
			seqAfter:  int64(binary.BigEndian.Uint64(b[16:])),
			indexID:   protocol.IndexID(binary.BigEndian.Uint64(b[24:])),
			durable:   true,
		}, true
	}
	return btrfsMarker{}, false
}

// maxBtrfsReplayPaths caps the targeted scan after a snapshot diff. A larger
// diff means the downtime was very busy and the regular full walk is the
// simpler course.
const maxBtrfsReplayPaths = 200_000

// btrfsBaselineMinInterval rate-limits runtime baseline refreshes. A
// refresh costs one snapshot swap (O(1) metadata) plus freeing the old
// baseline's pinned blocks, so the limit exists only to guard against
// pathologically frequent full scans.
const btrfsBaselineMinInterval = 10 * time.Minute

// btrfsPairInterval paces the periodic runtime pairing. It makes the
// shutdown pairing a bonus rather than the mechanism: any restart, clean
// or not, finds a baseline at most this old, so the boot diff stays small
// (2026-11-24 on yarlung a 2.5 h old baseline meant an 81,002-path diff
// and a 50 min boot) and a failed shutdown pairing (reboot ordering,
// shutdown deadline) costs a slightly larger diff instead of a full scan.
// A pairing holds the Serve loop for the flush and barrier, seconds.
var btrfsPairInterval = 30 * time.Minute

// btrfsRearmInterval paces the post-replay re-arm retries, and
// btrfsRearmMaxTries bounds them. The first attempt waits for the watch
// session's ready signal instead of a timer: the pairing needs the watch
// aggregator accepting flushes, and on Linux notify first sets up one
// inotify watch per directory synchronously. That setup outlasted a
// timer-only budget twice on yarlung: 2026-10-16 ("watch not running"
// 15s after boot) and 2026-11-25, when 20 tries of 30s + the 15s flush
// timeout (02:18-02:32) ran out while 476k watches were still being
// added. The retries now count only real pairing failures (flush busy,
// barrier, helper). After they run out, the periodic or full-scan
// refresh or the shutdown pairing provides coverage instead.
const btrfsRearmInterval = 30 * time.Second
const btrfsRearmMaxTries = 20

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
		// A nested subvolume escapes the snapshot diff, so changes
		// under it would be missed by replay — unless the path is IGNORED,
		// in which case its changes never enter the index and nothing is
		// missed. The database directory (index-v2, under the ignored
		// /.config/syncthing) deliberately lives in a nested subvolume so
		// baseline snapshots stop re-sharing its extents (NOCOW cannot hold
		// on shared extents).
		var visible []string
		for _, n := range nested {
			if !f.ignores.Match(n).IsIgnored() {
				visible = append(visible, n)
			}
		}
		if len(visible) > 0 {
			f.sl.InfoContext(ctx, "Btrfs scan replay disabled: folder contains nested subvolumes", "subvolumes", visible)
			return false
		}
		f.sl.DebugContext(ctx, "Btrfs scan replay: nested subvolumes are all ignored paths", "subvolumes", nested)
	}
	f.btrfsOK = true

	// Sweep pairing-snapshot residue once per folder lifetime,
	// at boot before this run creates its own snapshots (the eligibility
	// check is the one guaranteed-early btrfs touch point). Anything
	// matching the pairing-name pattern in the helper's staging dir is
	// garbage by construction — completed pairings rename to "baseline",
	// failed ones self-remove — but a crash, SIGKILL, or helper hiccup
	// defeats the deferred removal, and every leftover pins the blocks it
	// shared with the live tree (observed on yarlung 2026-10-21: 12
	// leftovers in 8 days, one pinning a 9.6 GiB old database image).
	if os.Getenv("STBTRFSSWEEP") != "0" {
		if removed, err := fs.BtrfsScanSweepStaleSnapshots(ctx); err != nil {
			f.sl.WarnContext(ctx, "Failed to sweep stale btrfs pairing snapshots", "removed", removed, slogutil.Error(err))
		} else if len(removed) > 0 {
			f.sl.InfoContext(ctx, "Swept stale btrfs pairing snapshots", "snapshots", removed)
		}
	}
	return true
}

// consumeBtrfsBaselineMarker loads the pairing marker at folder start and
// reports whether a replay may build on it. A durable marker stays in
// place: it remains true for every later boot until a new pairing replaces
// it, because the boot rescans the index's writes since the pairing. It is
// dropped only when it no longer describes this index (other index ID, or
// local sequence below the pairing's: index writes were lost). A legacy
// marker is deleted on read, as before.
func (f *folder) consumeBtrfsBaselineMarker(ctx context.Context) bool {
	f.btrfsMarker = btrfsMarker{}
	if !f.btrfsReplayEligible(ctx) {
		return false
	}
	key := btrfsBaselineKVPrefix + f.folderID
	val, err := f.db.GetKV(key)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to read btrfs baseline marker", slogutil.Error(err))
		return false
	}
	drop := func() {
		if err := f.db.DeleteKV(key); err != nil {
			f.sl.WarnContext(ctx, "Failed to drop btrfs baseline marker", slogutil.Error(err))
		}
	}
	m, ok := decodeBtrfsMarker(val)
	if !ok {
		f.sl.InfoContext(ctx, "Btrfs baseline marker unreadable, dropping it", "bytes", len(val))
		drop()
		return false
	}
	if !m.durable {
		drop()
		f.btrfsMarker = m
		return true
	}
	id, err := f.db.GetIndexID(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to read local index ID for btrfs baseline marker", slogutil.Error(err))
		return false
	}
	seq, err := f.db.GetDeviceSequence(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to read local sequence for btrfs baseline marker", slogutil.Error(err))
		return false
	}
	if id != m.indexID || seq < m.seqAfter {
		f.sl.InfoContext(ctx, "Btrfs baseline marker does not match this index, dropping it", "indexID", id, "markerIndexID", m.indexID, "sequence", seq, "markerSequence", m.seqAfter)
		drop()
		return false
	}
	f.btrfsMarker = m
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
	defer func() {
		// Removal failures were silently discarded here, which
		// is how staging-dir residue accumulated invisibly.
		// The helper kills a send still reading the snapshot (an abandoned
		// diff) and waits for it before deleting; that lag is part of what
		// giving up costs, so it feeds the next decision.
		lag, err := fs.BtrfsScanRemoveSnapshotCancel(context.Background(), snap)
		if err != nil {
			f.sl.WarnContext(context.Background(), "Failed to remove btrfs pairing snapshot", "snapshot", snap, slogutil.Error(err))
			return
		}
		if lag >= 0 {
			lags := f.recordCostSample(btrfsCancelLagKVPrefix, lag)
			f.sl.InfoContext(context.Background(), "Btrfs snapshot diff cancelled, send exited after kill", "lag", lag.Round(time.Millisecond), "lagMedian", medianDuration(lags).Round(time.Millisecond), "samples", len(lags))
		}
	}()
	paths, err := f.btrfsBootDiff(ctx, snap)
	if err != nil {
		if errors.Is(err, errBtrfsDiffCostlierThanScan) {
			f.sl.InfoContext(ctx, "Btrfs snapshot diff gave up, falling back to full scan", slogutil.Error(err))
			return false
		}
		f.sl.WarnContext(ctx, "Failed to compute btrfs snapshot diff, falling back to full scan", slogutil.Error(err))
		return false
	}
	if len(paths) > maxBtrfsReplayPaths {
		f.sl.InfoContext(ctx, "Btrfs snapshot diff too large, falling back to full scan", "paths", len(paths), "max", maxBtrfsReplayPaths)
		return false
	}
	// The diff sees the filesystem only. A name the index wrote after the
	// pairing but that is absent at both ends (created, indexed, removed)
	// appears in no diff, yet the index may still hold it live; rescanning
	// every name written since the pairing settles each one against disk.
	indexPaths, err := f.btrfsIndexPathsSincePairing()
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to list index changes since the btrfs pairing, falling back to full scan", slogutil.Error(err))
		return false
	}
	diffPaths := len(paths)
	paths = mergeReplayPaths(paths, indexPaths)
	if len(paths) > maxBtrfsReplayPaths {
		f.sl.InfoContext(ctx, "Btrfs replay paths too many, falling back to full scan", "paths", len(paths), "diffPaths", diffPaths, "indexPaths", len(indexPaths), "max", maxBtrfsReplayPaths)
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
	f.sl.InfoContext(ctx, "Skipping initial scan, applied btrfs snapshot diff", "changedPaths", len(paths), "diffPaths", diffPaths, "indexPaths", len(indexPaths), "pairedAt", f.btrfsMarker.pairedAt.UTC().Format(time.RFC3339))
	f.fullScanOK.Store(true)

	// Pair a fresh baseline. The marker this boot replayed from stays valid
	// (a restart before the new pairing replays from it again, rescanning
	// a little more of the index), but every boot's diff and index rescan
	// grow with time since that pairing, so move it forward. The watch
	// aggregator is still in its setup pass here and cannot accept a flush
	// yet (observed on yarlung 2026-10-16: inline pairing failed "watch not
	// running" 15s after boot), so the pairing waits for the watch-ready
	// signal in the Serve loop, then retries on a timer.
	f.btrfsRearmPending = true
	f.btrfsRearmAwaitWatch = true
	f.btrfsRearmTries = 0
	return true
}

// btrfsIndexPathsSincePairing returns the names the local index wrote after
// the marker's pairing began (sequence above seqBefore). A legacy marker
// carries no position and yields none, as before this rule existed.
func (f *folder) btrfsIndexPathsSincePairing() ([]string, error) {
	if !f.btrfsMarker.durable {
		return nil, nil
	}
	return f.db.LocalNamesSinceSequence(f.folderID, protocol.LocalDeviceID, f.btrfsMarker.seqBefore)
}

// mergeReplayPaths returns a and b without duplicates, order of a first.
func mergeReplayPaths(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range [][]string{a, b} {
		for _, p := range s {
			if _, dup := seen[p]; !dup {
				seen[p] = struct{}{}
				out = append(out, p)
			}
		}
	}
	return out
}

// btrfsScanDiffBaseline is the helper diff; a variable so tests can stand
// in a slow diff without a btrfs filesystem.
var btrfsScanDiffBaseline = fs.BtrfsScanDiffBaseline

// btrfsBootDiff runs the boot snapshot diff under the give-up rule
// (folder_btrfs_cost.go): it runs until it has cost as much as the
// measured full scan it would fall back to, or without limit when no full
// scan has been measured. A "still running" line every
// btrfsDiffProgressInterval keeps a long diff visible.
func (f *folder) btrfsBootDiff(ctx context.Context, snap string) ([]string, error) {
	start := time.Now()
	giveUp, scan, lag, bounded := f.btrfsDiffGiveUpAfter()
	dctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if bounded {
		t := time.AfterFunc(giveUp, func() {
			cancel(fmt.Errorf("%w (ran %v; full scan median %v, cancel lag median %v)", errBtrfsDiffCostlierThanScan, time.Since(start).Round(time.Second), scan.Round(time.Second), lag.Round(time.Second)))
		})
		defer t.Stop()
	}
	f.sl.InfoContext(ctx, "Btrfs snapshot diff started", "giveUpAfter", giveUp.Round(time.Second), "bounded", bounded, "fullScanMedian", scan.Round(time.Second), "cancelLagMedian", lag.Round(time.Second))

	type result struct {
		paths []string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		p, err := btrfsScanDiffBaseline(dctx, snap, f.Filesystem().URI())
		done <- result{p, err}
	}()
	tick := time.NewTicker(btrfsDiffProgressInterval)
	defer tick.Stop()
	for {
		select {
		case r := <-done:
			if r.err == nil {
				f.sl.InfoContext(ctx, "Btrfs snapshot diff finished", "took", time.Since(start).Round(time.Millisecond), "changedPaths", len(r.paths))
			} else if cause := context.Cause(dctx); cause != nil && errors.Is(cause, errBtrfsDiffCostlierThanScan) {
				return nil, cause
			}
			return r.paths, r.err
		case <-tick.C:
			f.sl.InfoContext(ctx, "Btrfs snapshot diff still running", "elapsed", time.Since(start).Round(time.Second), "giveUpAfter", giveUp.Round(time.Second), "bounded", bounded)
		}
	}
}

// btrfsRearmWatchReady runs the first post-replay pairing once the watch
// session's aggregator accepts flushes. Serve goroutine.
func (f *folder) btrfsRearmWatchReady(ctx context.Context) {
	f.btrfsRearmAwaitWatch = false
	f.btrfsRearmTimerFired(ctx)
}

// btrfsRearmChans returns what the Serve loop waits on for a pending
// re-arm: the watch session's ready channel until the first attempt, the
// retry timer after it. Both nil when nothing is pending.
func (f *folder) btrfsRearmChans() (<-chan struct{}, <-chan time.Time) {
	if !f.btrfsRearmPending {
		return nil, nil
	}
	if f.btrfsRearmAwaitWatch {
		return f.watchReadyChan(), nil
	}
	return nil, f.btrfsRearmTimer.C
}

// btrfsRearmTimerFired retries the post-replay baseline pairing until the
// watch aggregator is up and the pairing succeeds. Runs on the Serve
// goroutine (the sole watchChan consumer). On success the retry clears and
// the refresh is recorded; after btrfsRearmMaxTries failures it gives up
// and the next scheduled full scan's refresh (or the shutdown pairing)
// provides coverage instead.
func (f *folder) btrfsRearmTimerFired(ctx context.Context) {
	f.btrfsRearmTries++
	if f.pairBtrfsBaseline(ctx, "post-replay") {
		f.btrfsRearmPending = false
		f.lastBtrfsRefresh = time.Now()
		return
	}
	if f.btrfsRearmTries >= btrfsRearmMaxTries {
		f.btrfsRearmPending = false
		f.sl.InfoContext(ctx, "Btrfs baseline: post-replay re-arm giving up, next full-scan refresh or shutdown pairing provides coverage", "tries", f.btrfsRearmTries)
		return
	}
	f.btrfsRearmTimer.Reset(btrfsRearmInterval)
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

	// Scans run below must not dequeue watch batches between their chunks:
	// the barrier has to see its canary batch itself, and every batch has
	// to be scanned before pairing, not deferred past it.
	f.watchHold = true
	defer func() { f.watchHold = false }()
	// Deferred batches were observed before anything still queued; scan
	// them first.
	if !f.pairingStep(ctx, trigger, "drain-pending", func() bool { return f.drainWatchPending(ctx) }) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, deferred watch batches did not scan")
		return false
	}

	// Drain every buffered watch event into the index. The barrier below is
	// what proves completeness; this flush just keeps the barrier short.
	var ack chan struct{}
	ok := f.pairingStep(ctx, trigger, "flush-request", func() bool {
		var ok bool
		ack, ok = f.requestWatchFlush(ctx)
		return ok
	})
	if !ok {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, watch flush unavailable (watch not running)")
		return false
	}
	if !f.pairingStep(ctx, trigger, "flush-drain", func() bool { return f.scanUntilAck(ctx, ack) }) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, watch did not drain in time")
		return false
	}

	// Pin the filesystem state. Changes after this point are irrelevant to
	// the pairing: the next boot's diff against this baseline covers them.
	// The index position is read first: every index write from here on has
	// a higher sequence, so the boot's index rescan (names above it) covers
	// whatever the index does after the snapshot.
	seqBefore, err := f.db.GetDeviceSequence(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, local sequence unreadable", slogutil.Error(err))
		return false
	}
	snap := btrfsSnapName(trigger, f.folderID)
	if !f.pairingStep(ctx, trigger, "snapshot", func() bool { err = fs.BtrfsScanSnapshot(ctx, snap); return err == nil }) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, snapshot error", slogutil.Error(err))
		return false
	}
	// setbaseline renames the staging snapshot to the baseline, so a
	// successful pairing leaves nothing under the staging name to remove;
	// the deferred cleanup must stand down or it fires at a name that no
	// longer exists (a false-failure WRN, and helper exit 1).
	consumed := false
	defer func() {
		if consumed {
			return
		}
		// Runs even when ctx (e.g. the shutdown budget) is spent, so it
		// gets its own bound: the supervisors' stop timeout reserves
		// ShutdownCleanupTimeout for exactly this.
		rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), svcutil.ShutdownCleanupTimeout)
		defer cancel()
		if err := fs.BtrfsScanRemoveSnapshot(rmCtx, snap); err != nil {
			f.sl.WarnContext(rmCtx, "Failed to remove btrfs pairing snapshot", "snapshot", snap, slogutil.Error(err))
		}
	}()

	// Prove that every change up to the snapshot is in the index.
	if !f.pairingStep(ctx, trigger, "barrier", func() bool { return f.watchBarrier(ctx) }) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, watch barrier not reached")
		return false
	}

	if !f.pairingStep(ctx, trigger, "setbaseline", func() bool { err = fs.BtrfsScanSetBaseline(ctx, snap); return err == nil }) {
		f.sl.InfoContext(ctx, "Btrfs baseline: pairing failed, setbaseline error", slogutil.Error(err))
		return false
	}
	consumed = true
	m := btrfsMarker{pairedAt: time.Now(), seqBefore: seqBefore}
	if !f.pairingStep(ctx, trigger, "marker", func() bool {
		if m.indexID, err = f.db.GetIndexID(f.folderID, protocol.LocalDeviceID); err != nil {
			return false
		}
		if m.seqAfter, err = f.db.GetDeviceSequence(f.folderID, protocol.LocalDeviceID); err != nil {
			return false
		}
		err = f.db.PutKV(btrfsBaselineKVPrefix+f.folderID, m.encode())
		return err == nil
	}) {
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
//
// The barrier has no deadline of its own (the runtime and post-replay
// pairings run without one), so it ends on the two conditions under which
// the canary stops being on its way: the watch died
// (watchDiedDuringPairing), or the aggregator escalated to a full-folder
// batch (deferRootBatchInPairing).
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
	// emitted immediately. If it reaches the aggregator only after that
	// flush, the aggregator would hold it for its regular delay (0.5-1.5x
	// fsWatcherDelayS, 10 s default; on yarlung's 476k-watch tree the
	// barrier took that long), so re-flush on a short ticker until the
	// canary's batch arrives. The re-flush is a select case, not a
	// blocking requestWatchFlush: the loop must keep consuming batches.
	ack, _ := f.requestWatchFlush(ctx)
	reflush := time.NewTicker(watchBarrierReflushInterval)
	defer reflush.Stop()
	var flushC chan chan struct{} // non-nil while a re-flush is offered
	var nextAck chan struct{}

	for {
		select {
		case batch := <-f.watchChan:
			if f.deferRootBatchInPairing(ctx, "barrier", batch) {
				return false
			}
			if err := f.pairingScan(ctx, "barrier", batch.Paths); err != nil {
				return false
			}
			if slices.Contains(batch.Paths, canary) {
				return true
			}
		case <-ack:
			ack = nil
		case <-reflush.C:
			if f.watchDiedDuringPairing(ctx, "barrier") {
				return false
			}
			if ack == nil && flushC == nil {
				flushC, nextAck = f.watchFlush, make(chan struct{})
			}
		case flushC <- nextAck:
			ack, flushC, nextAck = nextAck, nil, nil
		case <-ctx.Done():
			return false
		}
	}
}

// watchBarrierReflushInterval is how often watchBarrier re-flushes the
// aggregator while waiting for its canary.
var watchBarrierReflushInterval = 250 * time.Millisecond

// shutdownBtrfsBaseline pairs the baseline snapshot with a provably
// consistent index at shutdown, arming the next boot's snapshot-diff
// replay. Any failure or busy filesystem just means no pairing: the next
// boot runs the regular full scan. The watch is still alive here (its
// context outlives Serve's); it is stopped by Serve's defer afterwards.
// ctx is the folder's shared shutdown budget (shutdownWork): whatever the
// watch-queue drain left of it.
func (f *folder) shutdownBtrfsBaseline(ctx context.Context) {
	if !f.btrfsOK || !f.FSWatcherEnabled {
		f.sl.InfoContext(ctx, "Btrfs baseline: skipping pairing", "btrfsOK", f.btrfsOK, "fsWatcher", f.FSWatcherEnabled)
		return
	}
	// The pairing is only sound when the index is provably complete NOW:
	// a run that neither armed the replay nor finished a full scan still
	// carries staleness the watch barrier cannot repair.
	if !f.fullScanOK.Load() {
		f.sl.InfoContext(ctx, "Btrfs baseline: skipping pairing, index not provably complete")
		return
	}
	if ctx.Err() != nil {
		f.sl.InfoContext(ctx, "Btrfs baseline: skipping pairing, shutdown budget spent by the watch-queue drain")
	} else if f.pairBtrfsBaseline(ctx, "shutdown") {
		return
	}
	// A pairing earlier in this run, or one an earlier boot replayed from,
	// left a marker: the next boot diffs against that baseline and rescans
	// the index's writes since it, not a full scan.
	if val, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID); err == nil {
		m, _ := decodeBtrfsMarker(val)
		f.sl.InfoContext(ctx, "Btrfs baseline: no clean shutdown pairing, next boot diffs against the last paired baseline", "pairedAt", m.pairedAt.UTC().Format(time.RFC3339))
	} else {
		f.sl.InfoContext(ctx, "Btrfs baseline: no clean shutdown pairing, next boot does a full scan")
	}
}

// btrfsPeriodicPair runs on btrfsPairTimer and re-arms it. It pairs only
// when the index is provably complete, the watch session accepts flushes,
// and no post-replay re-arm owns the pairing. Serve goroutine.
func (f *folder) btrfsPeriodicPair(ctx context.Context) {
	defer f.btrfsPairTimer.Reset(btrfsPairInterval)
	if f.btrfsRearmPending {
		return
	}
	f.refreshBtrfsBaseline(ctx)
}

// refreshBtrfsBaseline advances the baseline snapshot at runtime: right
// after a completed full scan, and every btrfsPairInterval
// (btrfsPeriodicPair). Without it the baseline only moves at clean
// shutdown, so a months-long uptime would pin months of churned blocks in
// the old baseline and every unclean restart would full-scan. Must run on
// the Serve goroutine.
//
// A runtime pairing does no catch-up scanning of deferred watch batches:
// it stands down, marks itself due, and runDueBtrfsPairing runs it once the
// Serve loop has scanned them. Measured on yarlung 2026-11-27 16:28:34: the
// pairing right after a 2h2m full scan found 28 deferred directories and
// failed drain-pending at 2m1.343s against its 2-minute deadline; the
// Serve loop then scanned the same directories in 7m33s (changes=0) and
// the next pairing took about 2 s. Nor does it run under a deadline: its
// scans are ones the Serve loop would run anyway (07:32 the same day a
// flush-drain scan of one .git/objects directory was cut off at 2m5.961s),
// and every step that can wait for something that is not coming ends on
// that condition instead (flush-request: watchFlushTimeout; helper calls:
// btrfsHelperTimeout; flush-drain and barrier: watch death and root
// escalation, see watchBarrier).
func (f *folder) refreshBtrfsBaseline(ctx context.Context) {
	if !f.btrfsOK || !f.FSWatcherEnabled {
		return
	}
	if time.Since(f.lastBtrfsRefresh) < btrfsBaselineMinInterval {
		return
	}
	// Sound only while the index is provably complete (a dead watch
	// clears this, see monitorWatch), and only possible once the watch
	// session accepts flushes; skip without the 15 s flush timeout.
	if !f.fullScanOK.Load() {
		return
	}
	select {
	case <-f.watchReadyChan():
	default:
		return
	}
	if p := f.watchPending; p != nil {
		if !f.btrfsPairDue {
			f.sl.InfoContext(ctx, "Btrfs baseline: runtime pairing waits for deferred watch batches to be scanned", "batches", p.batches, "paths", p.set.Len())
		}
		f.btrfsPairDue = true
		return
	}
	f.btrfsPairDue = false
	if f.pairBtrfsBaseline(ctx, "runtime") {
		f.lastBtrfsRefresh = time.Now()
	}
}

// runDueBtrfsPairing runs a runtime pairing that stood down for deferred
// watch batches, once none are pending. The Serve loop calls it after each
// piece of work. A pending post-replay re-arm owns the pairing, as in
// btrfsPeriodicPair.
func (f *folder) runDueBtrfsPairing(ctx context.Context) {
	if !f.btrfsPairDue || f.watchPending != nil || ctx.Err() != nil {
		return
	}
	if f.btrfsRearmPending {
		f.btrfsPairDue = false
		return
	}
	f.refreshBtrfsBaseline(ctx)
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

// deferRootBatchInPairing hands a full-folder watch batch dequeued inside a
// pairing to the pending set instead of scanning it there: a full-tree walk
// (2h2m on yarlung 2026-11-27) is the Serve loop's work, and the
// aggregator's escalation dropped every event it tracked, so a barrier's
// canary may never arrive. The pairing fails; the next runtime pairing
// finds the pending set and waits for it (refreshBtrfsBaseline).
func (f *folder) deferRootBatchInPairing(ctx context.Context, step string, b watchaggregator.Batch) bool {
	if !slices.ContainsFunc(b.Paths, func(p string) bool { return p == "." || p == "" }) {
		return false
	}
	f.sl.InfoContext(ctx, "Btrfs baseline: full-folder watch batch during pairing, deferring it to the Serve loop", "step", step)
	f.deferWatchPaths(b.Paths, b.MaxEventID)
	return true
}

// watchDiedDuringPairing reports (and logs) that the watch died while a
// pairing waited on it: monitorWatch clears fullScanOK on watch death, and
// stops the aggregator, so neither a flush ack nor a canary batch comes.
func (f *folder) watchDiedDuringPairing(ctx context.Context, step string) bool {
	if f.fullScanOK.Load() {
		return false
	}
	f.sl.InfoContext(ctx, "Btrfs baseline: index stopped being provably complete during pairing (watch died)", "step", step)
	return true
}

// scanUntilAck scans watch batches until the flush ack closes.
func (f *folder) scanUntilAck(ctx context.Context, ack chan struct{}) bool {
	tick := time.NewTicker(watchBarrierReflushInterval)
	defer tick.Stop()
	for {
		select {
		case batch := <-f.watchChan:
			if f.deferRootBatchInPairing(ctx, "flush-drain", batch) {
				return false
			}
			if err := f.pairingScan(ctx, "flush-drain", batch.Paths); err != nil {
				return false
			}
		case <-tick.C:
			if f.watchDiedDuringPairing(ctx, "flush-drain") {
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
