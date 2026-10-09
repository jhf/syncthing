// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
	"github.com/syncthing/syncthing/lib/semaphore"
	"github.com/syncthing/syncthing/lib/stats"
	"github.com/syncthing/syncthing/lib/stringutil"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/syncthing/syncthing/lib/versioner"
	"github.com/syncthing/syncthing/lib/watchaggregator"
)

// Arbitrary limit that triggers a warning on kqueue systems
const kqueueItemCountThreshold = 10000

type folder struct {
	stateTracker
	config.FolderConfiguration
	*stats.FolderStatisticsReference

	ioLimiter *semaphore.Semaphore

	localFlags protocol.FlagLocal

	model         *model
	shortID       protocol.ShortID
	db            db.DB
	ignores       *ignore.Matcher
	mtimefs       fs.Filesystem
	modTimeWindow time.Duration
	done          chan struct{} // used externally, accessible regardless of serve
	sl            *slog.Logger

	scanInterval           time.Duration
	scanTimer              *time.Timer
	watchScanDepth         int // >0 while servicing a watch-triggered scan between chunks
	scanDelay              chan time.Duration
	initialScanFinished    chan struct{}
	scanScheduled          chan struct{}
	versionCleanupInterval time.Duration
	versionCleanupTimer    *time.Timer

	// btrfs scan replay state; serve goroutine only (folder_btrfs.go).
	btrfsChecked             bool
	btrfsOK                  bool
	lastBtrfsBaselineRefresh time.Time

	pullScheduled chan struct{}
	pullPause     time.Duration
	pullFailTimer *time.Timer

	scanErrors []FileError
	pullErrors []FileError
	errorsMut  sync.Mutex

	doInSyncChan chan syncRequest

	forcedRescanRequested chan struct{}
	forcedRescanPaths     map[string]struct{}
	forcedRescanPathsMut  sync.Mutex

	watchCancel      context.CancelFunc
	watchChan        chan watchaggregator.Batch
	restartWatchChan chan struct{}
	watchErr         error
	watchMut         sync.Mutex

	// fullScanCount counts completed-or-attempted full-tree scans
	// (scanSubdirs with nil subDirs). It exists so tests and diagnostics
	// can tell whether a startup did a full walk or resumed from the
	// filesystem journal.
	fullScanCount atomic.Int64
	// lastReplayWatermark is the last persisted FSEvents journal position,
	// used to throttle KV writes (persist only on advancement).
	lastReplayWatermark atomic.Uint64

	puller    puller
	versioner versioner.Versioner

	warnedKqueue bool
}

type syncRequest struct {
	fn  func(context.Context) error
	err chan error
}

type puller interface {
	pull(ctx context.Context) (bool, error) // true when successful and should not be retried
}

func newFolder(model *model, ignores *ignore.Matcher, cfg config.FolderConfiguration, evLogger events.Logger, ioLimiter *semaphore.Semaphore, ver versioner.Versioner) *folder {
	f := folder{
		stateTracker:              newStateTracker(cfg.ID, evLogger),
		FolderConfiguration:       cfg,
		FolderStatisticsReference: stats.NewFolderStatisticsReference(db.NewTyped(model.sdb, "folderstats/"+cfg.ID)),
		ioLimiter:                 ioLimiter,

		model:         model,
		shortID:       model.shortID,
		db:            model.sdb,
		ignores:       ignores,
		mtimefs:       cfg.Filesystem(fs.NewMtimeOption(model.sdb, cfg.ID)),
		modTimeWindow: cfg.ModTimeWindow(),
		done:          make(chan struct{}),
		sl:            slog.Default().With(cfg.LogAttr()),

		scanInterval:           time.Duration(cfg.RescanIntervalS) * time.Second,
		scanTimer:              time.NewTimer(0), // The first scan should be done immediately.
		scanDelay:              make(chan time.Duration),
		initialScanFinished:    make(chan struct{}),
		scanScheduled:          make(chan struct{}, 1),
		versionCleanupInterval: time.Duration(cfg.Versioning.CleanupIntervalS) * time.Second,
		versionCleanupTimer:    time.NewTimer(time.Duration(cfg.Versioning.CleanupIntervalS) * time.Second),

		pullScheduled: make(chan struct{}, 1), // This needs to be 1-buffered so that we queue a pull if we're busy when it comes.

		doInSyncChan: make(chan syncRequest),

		forcedRescanRequested: make(chan struct{}, 1),
		forcedRescanPaths:     make(map[string]struct{}),

		watchCancel:      func() {},
		restartWatchChan: make(chan struct{}, 1),

		versioner: ver,
	}
	f.pullPause = f.pullBasePause()
	f.pullFailTimer = time.NewTimer(0)
	<-f.pullFailTimer.C

	registerFolderMetrics(f.ID)

	return &f
}

func (f *folder) Serve(ctx context.Context) error {
	f.model.foldersRunning.Add(1)
	defer f.model.foldersRunning.Add(-1)

	f.sl.DebugContext(ctx, "Folder starting")
	defer f.sl.DebugContext(ctx, "Folder exiting")

	f.setState(FolderStarting)

	defer func() {
		f.scanTimer.Stop()
		f.versionCleanupTimer.Stop()
		f.setState(FolderIdle)
	}()

	if err := f.reconcileBlockIndex(ctx); err != nil {
		f.setError(ctx, err)
		return err // will get restarted by suture
	}

	if f.FSWatcherEnabled && f.getHealthErrorAndLoadIgnores() == nil {
		f.tryArmWatchReplay(ctx)
		f.startWatch(ctx)
		// Btrfs replay arms AFTER the watch: the diff covers everything up
		// to its snapshot, the watch everything after its start, and only
		// this order makes the two overlap instead of leaving a gap.
		f.tryArmBtrfsScanReplay(ctx)
	}

	// If we're configured to not do version cleanup, or we don't have a
	// versioner, cancel and drain that timer now.
	if f.versionCleanupInterval == 0 || f.versioner == nil {
		if !f.versionCleanupTimer.Stop() {
			<-f.versionCleanupTimer.C
		}
	}

	initialCompleted := f.initialScanFinished
	pullTimer := time.NewTimer(0)
	pullTimer.Stop()

	f.setState(FolderIdle)

	for {
		var err error

		select {
		case <-ctx.Done():
			close(f.done)
			return nil

		case <-f.pullScheduled:
			if f.PullerDelayS > 0 {
				// Wait for incoming updates to settle before doing the
				// actual pull.
				//
				// Only set the state to SyncWaiting if we are Idle (and
				// not, e.g., errored) and have reason to believe there is
				// something to sync, to avoid unnecessary flashing in the
				// GUI.
				if cur, _, _ := f.getState(); cur == FolderIdle {
					if needCount, err := f.db.CountNeed(f.folderID, protocol.LocalDeviceID); err == nil && needCount.TotalItems() > 0 {
						f.setState(FolderSyncWaiting)
					}
				}
				pullTimer.Reset(time.Duration(float64(time.Second) * f.PullerDelayS))
				continue
			}
			_, err = f.pull(ctx)

		case <-pullTimer.C:
			_, err = f.pull(ctx)

		case <-f.pullFailTimer.C:
			var success bool
			success, err = f.pull(ctx)
			if (err != nil || !success) && f.pullPause < 60*f.pullBasePause() {
				// Back off from retrying to pull
				f.pullPause *= 2
			}

		case <-initialCompleted:
			// Initial scan has completed, we should do a pull
			initialCompleted = nil // never hit this case again
			_, err = f.pull(ctx)

		case <-f.forcedRescanRequested:
			err = f.handleForcedRescans(ctx)

		case <-f.scanTimer.C:
			f.sl.DebugContext(ctx, "Scanning due to timer")
			err = f.scanTimerFired(ctx)

		case req := <-f.doInSyncChan:
			f.sl.DebugContext(ctx, "Running something due to request")
			err = req.fn(ctx)
			req.err <- err

		case next := <-f.scanDelay:
			f.sl.DebugContext(ctx, "Delaying scan")
			f.scanTimer.Reset(next)
			continue

		case <-f.scanScheduled:
			f.sl.DebugContext(ctx, "Scan was scheduled")
			f.scanTimer.Reset(0)
			continue

		case fsEvents := <-f.watchChan:
			f.sl.DebugContext(ctx, "Scan due to watcher")
			err = f.scanSubdirs(ctx, fsEvents.Paths)
			if err == nil {
				f.persistWatchReplayWatermark(fsEvents.MaxEventID)
				f.refreshBtrfsBaseline(ctx, false)
			}

		case <-f.restartWatchChan:
			f.sl.DebugContext(ctx, "Restart watcher")
			err = f.restartWatch(ctx)

		case <-f.versionCleanupTimer.C:
			if _, _, healthErr := f.getState(); healthErr != nil {
				continue
			}
			f.sl.DebugContext(ctx, "Doing version cleanup")
			f.versionCleanupTimerFired(ctx)
		}

		if svcutil.IsFatal(err) {
			return err
		}

		// Set the state to FolderError when err is non-nil, otherwise reset
		// it back to FolderIdle.
		f.setError(ctx, err)
	}
}

func (f *folder) reconcileBlockIndex(ctx context.Context) error {
	if !f.BlockIndexing {
		f.sl.DebugContext(ctx, "Dropping block index (block indexing disabled)")
		return f.db.DropBlockIndex(f.folderID)
	}
	f.sl.DebugContext(ctx, "Populating block index if empty")
	return f.db.PopulateBlockIndex(f.folderID)
}

func (*folder) BringToFront(string) {}

func (*folder) Override() {}

func (*folder) Revert() {}

func (f *folder) DelayScan(next time.Duration) {
	select {
	case f.scanDelay <- next:
	case <-f.done:
	}
}

func (f *folder) ScheduleScan() {
	// 1-buffered chan
	select {
	case f.scanScheduled <- struct{}{}:
	default:
	}
}

func (f *folder) ignoresUpdated() {
	if f.FSWatcherEnabled {
		f.scheduleWatchRestart()
	}
}

func (f *folder) SchedulePull() {
	select {
	case f.pullScheduled <- struct{}{}:
	default:
		// We might be busy doing a pull and thus not reading from this
		// channel. The channel is 1-buffered, so one notification will be
		// queued to ensure we recheck after the pull, but beyond that we must
		// make sure to not block index receiving.
	}
}

func (*folder) Jobs(_, _ int) ([]string, []string, int) {
	return nil, nil, 0
}

func (f *folder) Scan(subdirs []string) error {
	<-f.initialScanFinished
	return f.doInSync(func(ctx context.Context) error {
		return f.scanSubdirs(ctx, subdirs)
	})
}

// doInSync allows to run functions synchronously in folder.serve from exported,
// asynchronously called methods.
func (f *folder) doInSync(fn func(context.Context) error) error {
	req := syncRequest{
		fn:  fn,
		err: make(chan error, 1),
	}

	select {
	case f.doInSyncChan <- req:
		return <-req.err
	case <-f.done:
		return context.Canceled
	}
}

func (f *folder) Reschedule() {
	if f.scanInterval == 0 {
		return
	}
	// Sleep a random time between 3/4 and 5/4 of the configured interval.
	sleepNanos := (f.scanInterval.Nanoseconds()*3 + rand.Int63n(2*f.scanInterval.Nanoseconds())) / 4 //nolint:gosec
	interval := time.Duration(sleepNanos) * time.Nanosecond
	f.sl.Debug("Next rescan scheduled", slog.Duration("interval", interval))
	f.scanTimer.Reset(interval)
}

func (f *folder) getHealthErrorAndLoadIgnores() error {
	if err := f.getHealthErrorWithoutIgnores(); err != nil {
		return err
	}
	if f.Type != config.FolderTypeReceiveEncrypted {
		if err := f.ignores.Load(".stignore"); err != nil && !fs.IsNotExist(err) {
			return fmt.Errorf("loading ignores: %w", err)
		}
	}
	return nil
}

func (f *folder) getHealthErrorWithoutIgnores() error {
	// Check for folder errors, with the most serious and specific first and
	// generic ones like out of space on the home disk later.

	if err := f.CheckPath(); err != nil {
		return err
	}

	if minFree := f.model.cfg.Options().MinHomeDiskFree; minFree.Value > 0 {
		dbPath := locations.Get(locations.Database)
		if usage, err := fs.NewFilesystem(fs.FilesystemTypeBasic, dbPath).Usage("."); err == nil {
			if err = config.CheckFreeSpace(minFree, usage); err != nil {
				return fmt.Errorf("insufficient space on disk for database (%v): %w", dbPath, err)
			}
		}
	}

	return nil
}

func (f *folder) pull(ctx context.Context) (success bool, err error) {
	f.pullFailTimer.Stop()
	select {
	case <-f.pullFailTimer.C:
	default:
	}

	select {
	case <-f.initialScanFinished:
	default:
		// The initial scan is still running. Pulls are normally held until
		// it completes, because before the first scan ever completes we
		// cannot trust the local index: every remote file looks needed,
		// including files that already exist on disk but have not been
		// indexed yet. Once a folder has completed an initial scan at any
		// point (recorded in the database), the local index it holds was
		// complete when written, and everything the puller is about to
		// change is first checked against it (scanIfItemChanged), backing
		// off with errModified where the disk and the index disagree. So
		// after a restart we pull during the re-scan; during the first scan
		// ever we wait for it.
		if done, serr := f.initialScanEverCompleted(); serr != nil || !done {
			return true, nil
		}
	}

	defer func() {
		if success {
			// We're good, reset the pause interval.
			f.pullPause = f.pullBasePause()
		}
	}()

	// Abort early (before acquiring a limiter token) if there's a folder
	// error. This must happen before the "nothing to do" check below so
	// that up-to-date folders are also periodically health-checked.
	err = f.getHealthErrorWithoutIgnores()
	if err != nil {
		f.sl.DebugContext(ctx, "Skipping pull due to folder error", slogutil.Error(err))
		return false, err
	}

	// If there is nothing to do, don't even enter sync-waiting state.
	needCount, err := f.db.CountNeed(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		return false, err
	}
	if needCount.TotalItems() == 0 {
		// Clears pull failures on items that were needed before, but aren't anymore.
		f.errorsMut.Lock()
		f.pullErrors = nil
		f.errorsMut.Unlock()
		return true, nil
	}

	// Send only folder doesn't do any io, it only checks for out-of-sync
	// items that differ in metadata and updates those.
	if f.Type != config.FolderTypeSendOnly {
		f.setState(FolderSyncWaiting)

		if err := f.ioLimiter.TakeWithContext(ctx, 1); err != nil {
			return true, err
		}
		defer f.ioLimiter.Give(1)
	}

	startTime := time.Now()

	// Check if the ignore patterns changed.
	oldHash := f.ignores.Hash()
	defer func() {
		if f.ignores.Hash() != oldHash {
			f.ignoresUpdated()
		}
	}()
	err = f.getHealthErrorAndLoadIgnores()
	if err != nil {
		f.sl.DebugContext(ctx, "Skipping pull due to folder error", slogutil.Error(err))
		return false, err
	}
	f.setError(ctx, nil)

	success, err = f.puller.pull(ctx)

	if success && err == nil {
		return true, nil
	}

	// Pulling failed, try again later.
	delay := f.pullPause + time.Since(startTime)
	f.sl.InfoContext(ctx, "Folder failed to sync, will be retried", slog.String("wait", stringutil.NiceDurationString(delay)))
	f.pullFailTimer.Reset(delay)

	return false, err
}

// scanChunkBudget bounds how long one chunk of a walk may run before it yields. It is a var so
// tests can shrink it to force chunk boundaries; production wants it small enough that a pull is
// never waiting long, and large enough that the per-chunk overhead stays irrelevant.
var scanChunkBudget = 250 * time.Millisecond

// initialScanDoneKVPrefix namespaces the per-folder marker recording that this folder once
// completed an initial scan. The marker is what lets pulls run during the initial scan after a
// restart (see pull): before the first scan ever completes, every remote file looks needed,
// including files that exist on disk but are not indexed yet, so pulling then is not safe.
const initialScanDoneKVPrefix = "model/initialScanDone/"

// initialScanEverCompleted reports whether this folder has ever completed an initial scan, as
// recorded in the database. A missing marker means no; a database error is reported to the
// caller, whose conservative answer is also no.
func (f *folder) initialScanEverCompleted() (bool, error) {
	_, err := f.db.GetKV(initialScanDoneKVPrefix + f.folderID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// serviceQueuedWatch runs one queued batch of watch events, if any, and returns immediately
// otherwise. Watch events wait on the serve goroutine like pulls do, so a long scan otherwise
// delays noticing (and announcing) local changes until it finishes; on a busy machine that delay
// was measured at a full scan's duration, and the notify backlog can overflow and lose events
// entirely while it builds up. A watch-triggered scan does not itself service the watch channel:
// the batches it would find there were generated while it ran, and servicing them inline would
// never let the outer scan finish.
func (f *folder) serviceQueuedWatch(ctx context.Context) error {
	if f.watchScanDepth > 0 {
		return nil
	}
	select {
	case fsEvents := <-f.watchChan:
		f.sl.DebugContext(ctx, "Scan due to watcher (serviced between scan chunks)")
		f.watchScanDepth++
		defer func() { f.watchScanDepth-- }()
		err := f.scanSubdirs(ctx, fsEvents.Paths)
		if err == nil {
			f.persistWatchReplayWatermark(fsEvents.MaxEventID)
			f.refreshBtrfsBaseline(ctx, false)
		}
		return err
	default:
		return nil
	}
}

// serviceQueuedPull runs one pull iteration if the folder has one queued, and returns immediately
// otherwise. It exists so that a chunked scan does not starve the puller: the scan and the pull both
// run in the serve goroutine, so this is the same work the loop would have done between two selects.
func (f *folder) serviceQueuedPull(ctx context.Context) error {
	select {
	case <-f.pullScheduled:
		_, err := f.pull(ctx)
		return err
	default:
		return nil
	}
}

func (f *folder) scanSubdirs(ctx context.Context, subDirs []string) error {
	f.sl.DebugContext(ctx, "Scanning")
	if subDirs == nil {
		f.fullScanCount.Add(1)
	}

	oldHash := f.ignores.Hash()

	err := f.getHealthErrorAndLoadIgnores()
	if err != nil {
		return err
	}
	f.setError(ctx, nil)

	// Check on the way out if the ignore patterns changed as part of scanning
	// this folder. If they did we should schedule a pull of the folder so that
	// we request things we might have suddenly become unignored and so on.
	defer func() {
		if f.ignores.Hash() != oldHash {
			f.sl.DebugContext(ctx, "Ignore patterns change detected while scanning; triggering puller")
			f.ignoresUpdated()
			f.SchedulePull()
		}
	}()

	f.setState(FolderScanWaiting)

	if err := f.ioLimiter.TakeWithContext(ctx, 1); err != nil {
		return err
	}
	defer f.ioLimiter.Give(1)

	metricFolderScans.WithLabelValues(f.ID).Inc()
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go addTimeUntilCancelled(scanCtx, metricFolderScanSeconds.WithLabelValues(f.ID))

	for i := range subDirs {
		sub := osutil.NativeFilename(subDirs[i])

		if sub == "" {
			// A blank subdirs means to scan the entire folder. We can trim
			// the subDirs list and go on our way.
			subDirs = nil
			break
		}

		subDirs[i] = sub
	}

	// Clean the list of subitems to ensure that we start at a known
	// directory, and don't scan subdirectories of things we've already
	// scanned.
	subDirs = unifySubs(subDirs, func(file string) bool {
		_, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, file)
		return err == nil && ok
	})

	f.setState(FolderScanning)
	f.clearScanErrors(subDirs)

	batch := f.newScanBatch()

	// Schedule a pull after scanning, but only if we actually detected any
	// changes.
	changes := 0
	defer func() {
		f.sl.DebugContext(ctx, "Finished scanning", slog.Int("changes", changes))
		if changes > 0 {
			f.SchedulePull()
		}
	}()

	// The walk runs in bounded chunks, and a queued pull is serviced between them. This is the
	// whole point of the chunking: a scan holds the serve loop for its entire duration, and while
	// it holds it nothing is pulled. Measured on this folder, that was a 585-second scan with a
	// 300-file queue frozen behind it. Both the scan and the pull run in the serve goroutine, so
	// servicing a pull here is the same thing the loop would have done had it not been scanning.
	resumeFrom := ""
	budget := scanChunkBudget
	for {
		f.setState(FolderScanning)

		changesHere, last, more, progressed, err := f.scanSubdirsChangedAndNew(ctx, subDirs, batch, resumeFrom, budget)
		changes += changesHere
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if progressed {
			budget = scanChunkBudget
		} else {
			// The walk could not show any progress within the budget - something at
			// the resume point takes longer than the budget (a file still hashing, a
			// directory still enumerating). Restarting the same stretch with the same
			// budget would never finish, so give the next chunk longer, up to a cap.
			budget = min(max(2*budget, 100*time.Millisecond), 5*time.Minute)
			f.sl.DebugContext(ctx, "Scan chunk made no progress, growing the chunk budget", slog.Duration("budget", budget), slog.String("resumeFrom", resumeFrom))
		}
		resumeFrom = last

		if err := batch.Flush(); err != nil {
			return err
		}
		// The scan holds an I/O token for its whole duration. Hand it back
		// while servicing a pull, or the pull's own TakeWithContext would
		// deadlock against us when maxFolderConcurrency is 1 (or wait out
		// the rest of the scan when it equals the number of folders).
		f.ioLimiter.Give(1)
		pullErr := f.serviceQueuedPull(ctx)
		watchErr := f.serviceQueuedWatch(ctx)
		limitErr := f.ioLimiter.TakeWithContext(ctx, 1)
		// The next chunk iteration restores the FolderScanning label that a
		// serviced pull moved; the sweep loop below has no such per-iteration
		// restore and needs its own.
		if pullErr != nil {
			return pullErr
		}
		if watchErr != nil {
			return watchErr
		}
		if limitErr != nil {
			return limitErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	if len(subDirs) == 0 {
		// If we have no specific subdirectories to traverse, set it to one
		// empty prefix so we traverse the entire folder contents once.
		subDirs = []string{""}
	}

	// Do a scan of the database for each prefix, to check for deleted and
	// ignored files.

	changesHere, err := f.scanSubdirsDeletedAndIgnored(ctx, subDirs, batch)
	changes += changesHere
	if err != nil {
		return err
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	f.ScanCompleted()
	return nil
}

const maxToRemove = 1000

type scanBatch struct {
	f                *folder
	updateBatch      *FileInfoBatch
	toRemove         []string
	skipInFindRename map[string]struct{}
}

func (f *folder) newScanBatch() *scanBatch {
	b := &scanBatch{
		f:                f,
		toRemove:         make([]string, 0, maxToRemove),
		skipInFindRename: make(map[string]struct{}),
	}
	b.updateBatch = NewFileInfoBatch(func(fs []protocol.FileInfo) error {
		if err := b.f.getHealthErrorWithoutIgnores(); err != nil {
			b.f.sl.Debug("Stopping scan due to folder error", slogutil.Error(err))
			return err
		}
		b.f.updateLocalsFromScanning(fs)
		clear(b.skipInFindRename)
		return nil
	})
	return b
}

func (b *scanBatch) Remove(item string) {
	b.toRemove = append(b.toRemove, item)
}

func (b *scanBatch) flushToRemove() error {
	if len(b.toRemove) > 0 {
		if err := b.f.db.DropFilesNamed(b.f.folderID, protocol.LocalDeviceID, b.toRemove); err != nil {
			return err
		}
		b.toRemove = b.toRemove[:0]
	}
	return nil
}

func (b *scanBatch) Flush() error {
	if err := b.flushToRemove(); err != nil {
		return err
	}
	return b.updateBatch.Flush()
}

func (b *scanBatch) FlushIfFull() error {
	if len(b.toRemove) >= maxToRemove {
		if err := b.flushToRemove(); err != nil {
			return err
		}
	}
	return b.updateBatch.FlushIfFull()
}

func (b *scanBatch) markSkipInFindRename(name string) {
	b.skipInFindRename[name] = struct{}{}
}

func (b *scanBatch) shouldSkipInFindRenames(name string) bool {
	_, ok := b.skipInFindRename[name]
	return ok
}

// Update adds the fileinfo to the batch for updating, and does a few checks.
// It returns false if the checks result in the file not going to be updated or removed.
func (b *scanBatch) Update(fi protocol.FileInfo) (bool, error) {
	// Check for a "virtual" parent directory of encrypted files. We don't track
	// it, but check if anything still exists within and delete it otherwise.
	if b.f.Type == config.FolderTypeReceiveEncrypted && fi.IsDirectory() && protocol.IsEncryptedParent(fs.PathComponents(fi.Name)) {
		if names, err := b.f.mtimefs.DirNames(fi.Name); err == nil && len(names) == 0 {
			b.f.mtimefs.Remove(fi.Name)
		}
		return false, nil
	}
	// A deleted receive-only item whose global item is our own receive-only
	// change need not be tracked at all.
	if fi.IsDeleted() && (b.f.Type == config.FolderTypeReceiveOnly || b.f.Type == config.FolderTypeReceiveEncrypted) {
		switch gf, ok, err := b.f.db.GetGlobalFile(b.f.folderID, fi.Name); {
		case err != nil:
			return false, err
		case ok && gf.IsReceiveOnlyChanged():
			b.Remove(fi.Name)
			b.f.sl.Debug("Deleting deleted receive-only local-changed file", slogutil.FilePath(fi.Name))
			return true, nil
		}
	}
	b.updateBatch.Append(fi)
	return true, nil
}

// scanSubdirsChangedAndNew walks the given subs, reporting entries that are new or changed.
//
// It runs for at most budget and then returns with more set and lastPath being the resume
// marker for the next chunk. The marker is the minimum of the last applied path and the first
// aborted path seen in this chunk: results arrive out of order, so the set of applied entries has
// holes, and everything the walk handed over but did not apply is reported as an error result by
// the scanner. Everything strictly below the marker is then known to be applied, and nothing below
// it was silently dropped, so resuming the walk from the marker loses nothing. The re-walked window
// is bounded by the hashers in flight at the cut, not by the size of the chunk.
//
// The budget is a timer, not a per-result check: the scanner reports nothing at all for unchanged
// files, so on a re-scan of a mostly unchanged tree results may not arrive for minutes at a time.
// A deadline checked only when a result arrives then never fires - measured on a 2.35M-file tree,
// where the first chunk boundary came 2m53s into the scan, when one changed file happened to
// produce a result. The timer cuts the walk regardless, and the abort reports of whatever was in
// flight at the cut keep the resume marker honest.
//
// progressed reports whether the marker moved forward. A chunk can fail to progress when the walk
// could not hand anything over within the budget (a file still hashing, a directory still
// enumerating); the caller responds by giving the next chunk a longer budget rather than
// restarting the same stretch forever.
func (f *folder) scanSubdirsChangedAndNew(ctx context.Context, subDirs []string, batch *scanBatch, resumeFrom string, budget time.Duration) (changes int, lastPath string, more, progressed bool, err error) {
	// If we return early e.g. due to a folder health error, the scan needs
	// to be cancelled.
	scanCtx, scanCancel := context.WithCancel(ctx)
	defer scanCancel()

	scanConfig := scanner.Config{
		Folder:                f.ID,
		Subs:                  subDirs,
		Matcher:               f.ignores,
		TempLifetime:          time.Duration(f.model.cfg.Options().KeepTemporariesH) * time.Hour,
		CurrentFiler:          cFiler{db: f.db, folder: f.folderID},
		Filesystem:            f.mtimefs,
		IgnorePerms:           f.IgnorePerms,
		AutoNormalize:         f.AutoNormalize,
		Hashers:               f.model.numHashers(f.ID),
		ShortID:               f.shortID,
		ProgressTickIntervalS: -1, // see below
		LocalFlags:            f.localFlags,
		ModTimeWindow:         f.modTimeWindow,
		EventLogger:           f.evLogger,
		ScanOwnership:         f.SendOwnership || f.SyncOwnership,
		ScanXattrs:            f.SendXattrs || f.SyncXattrs,
		XattrFilter:           f.XattrFilter,
		ResumeFrom:            resumeFrom,
	}
	// ProgressTickIntervalS -1 selects the streaming pipeline: handovers feed the hashers
	// while the walk runs. The default pipeline instead buffers every handover until the
	// walk ends - it needs the full list to compute the FolderScanProgress total - and only
	// then starts hashing, which defeats chunking outright: a budget cut during the walk
	// aborts every changed file found so far, and the earliest of them (typically the resume
	// entry itself, which by definition was never applied) holds the marker where it
	// started. Measured on the Mac: every cut chunk aborted its entire handover list, the
	// marker pinned on the first changed file, and the budget ballooned to the cap while
	// the scan crawled. The price is that chunked scans emit no FolderScanProgress events.

	var fchan chan scanner.ScanResult
	if f.Type == config.FolderTypeReceiveEncrypted {
		fchan = scanner.WalkWithoutHashing(scanCtx, scanConfig)
	} else {
		fchan = scanner.Walk(scanCtx, scanConfig)
	}

	// stopped records that the budget cut the walk short. The walk is cancelled there, but results
	// already in the hashing pipeline are still consumed and applied rather than thrown away. They
	// have to be: results do not arrive in walk order, so an entry whose result is still in flight
	// may sort before the resume marker, and resuming would then skip it for good. Draining in
	// flight work is bounded by the hashers and the queue depth, not by the size of the folder.
	stopped := false
	// The marker starts at the resume point and advances over what this chunk applies. Abort
	// reports are tracked separately in firstAborted and minned in only at the end: they arrive
	// out of order against applied results, so folding them into the marker directly would let
	// the next applied result advance past an aborted entry and lose it for good.
	marker := resumeFrom
	// firstAborted is the smallest path of any abort report in this chunk. Aborts are the
	// handed-over files the budget cut cancelled before they could be applied; they are not
	// recorded anywhere else, so the resume marker must stay behind all of them: an entry the
	// marker moved past would never be visited again. Real scan errors are not here - they are
	// surfaced to the user and retried by the next scan, and holding the marker for a
	// persistently erroring file would wedge the scan at it.
	firstAborted := ""
	// walkFrontier is how far the walk enumerated before the budget cut it, reported by the
	// scanner because unchanged entries produce no result of their own. Every applied result
	// and every abort sorts at or below it, so it is an upper bound the marker may advance to.
	walkFrontier := ""
	// The budget is enforced two ways: the timer covers silence (unchanged files produce no
	// results, so there is nothing to check against for minutes at a time), and the deadline
	// check covers bursts (a timer is delivered asynchronously, so a result stream can otherwise
	// outrun it arbitrarily long, and with a zero budget no cut would be guaranteed at all).
	deadline := time.Now().Add(budget)
	budgetTimer := time.NewTimer(budget)
	defer budgetTimer.Stop()
	budgetC := budgetTimer.C

loop:
	for {
		select {
		case <-budgetC:
			// The budget expired. Cut the walk here, whether or not any result
			// has arrived in this chunk - see the function comment.
			stopped = true
			budgetC = nil
			scanCancel()
		case res, ok := <-fchan:
			if !ok {
				break loop
			}

			if res.WalkFrontier {
				// The walk's enumeration frontier at the cut: an upper bound
				// for the marker, not an abort. It does not enter firstAborted,
				// whose minimum must stay behind unhandled work only.
				if walkFrontier == "" || scanner.WalkOrderLess(walkFrontier, res.Path) {
					walkFrontier = res.Path
				}
				continue
			}

			if res.Err != nil {
				if !errors.Is(res.Err, context.Canceled) {
					// A real scan error is user-visible bookkeeping and nothing
					// more: it must not hold the resume marker. A file that
					// errors persistently (it is being rewritten faster than it
					// can be hashed, say) would otherwise pin the marker at
					// itself forever, and the chunk after would error on it
					// again - measured on the Mac, where a skills manifest
					// rewritten every few minutes wedged the scan at itself
					// while the walk frontier sailed past it. The next scan
					// retries the file, as it always has.
					f.newScanError(res.Path, res.Err)
					continue
				}
				// An abort (the chunk budget cancelled the scan under this entry) is
				// bookkeeping for the resume marker, not a user-visible scan error: the
				// entry is retried by the next chunk.
				// The minimum must be in walk order for the same reason as below.
				if firstAborted == "" || scanner.WalkOrderLess(res.Path, firstAborted) {
					firstAborted = res.Path
				}
				continue
			}

			if !stopped && !time.Now().Before(deadline) {
				// The budget expired while results were flowing. Stop walking
				// here and let the loop finish what is already in flight.
				stopped = true
				budgetC = nil
				scanCancel()
			}

			if err := batch.FlushIfFull(); err != nil {
				// Prevent a race between the scan aborting due to context
				// cancellation and releasing the snapshot in defer here.
				scanCancel()
				for range fchan {
				}
				return changes, lastPath, false, false, err
			}

			if ok, err := batch.Update(res.File); err != nil {
				// The scanner's abort reports cannot be abandoned to a cancelled context,
				// so the channel must be drained before returning, or the scanner blocks
				// on a send forever.
				scanCancel()
				for range fchan {
				}
				return changes, lastPath, false, false, err
			} else if ok {
				changes++
			}

			switch f.Type {
			case config.FolderTypeReceiveOnly, config.FolderTypeReceiveEncrypted:
			default:
				// Rename detection is comparatively expensive, so only attempt
				// it for files that appeared as new on disk during this scan. A
				// rename that overwrites an existing file (the destination path
				// already had an entry, so it scans as an update rather than a
				// new file) is not optimised as a rename.
				if res.File.New && res.File.Size > 0 {
					if nf, ok := f.findRename(ctx, res.File, batch); ok {
						if ok, err := batch.Update(nf); err != nil {
							scanCancel()
							for range fchan {
							}
							return changes, lastPath, false, false, err
						} else if ok {
							changes++
						}
					}
				}
			}

			marker = res.File.Name
		}
	}

	// The marker stays behind every reported abort: the minimum of what was applied and what was
	// reported aborted. The minimum is in walk order, not string order, because that is the order
	// the scanner's resume predicate speaks: "doc01.txt" < "doc01/file" as strings (0x2E < 0x2F),
	// but the walk visits "doc01/file" first, so a string minimum would let the marker jump past an
	// aborted file that was never applied. Nothing below the marker is then unaccounted for. An
	// abort report with nothing applied after it leaves the marker where it started, which the
	// caller sees as no progress.
	if walkFrontier != "" && (marker == "" || scanner.WalkOrderLess(marker, walkFrontier)) {
		// Everything up to the frontier was enumerated this chunk. Unchanged entries produce
		// no result, so without this a chunk cut in a silent stretch would leave the marker
		// where it started and rewalk the same ground with an ever-growing budget. Applied
		// results and aborts all sort at or below the frontier, so advancing to it skips
		// nothing unhandled. It can only advance the marker, never move it back: the resumed
		// walk re-enumerates the covered region to reach new ground, and a budget that
		// expires during that catch-up reports a frontier behind the marker.
		marker = walkFrontier
	}
	if firstAborted != "" && (marker == "" || scanner.WalkOrderLess(firstAborted, marker)) {
		marker = firstAborted
	}
	lastPath = marker

	if stopped {
		f.sl.DebugContext(ctx, "Scan chunk cut", slog.String("resumeFrom", resumeFrom), slog.String("marker", marker), slog.String("walkFrontier", walkFrontier), slog.String("firstAborted", firstAborted), slog.Int("changes", changes))
	}

	return changes, lastPath, stopped, marker != resumeFrom, nil
}

func (f *folder) scanSubdirsDeletedAndIgnored(ctx context.Context, subDirs []string, batch *scanBatch) (int, error) {
	var toIgnore []protocol.FileInfo
	ignoredParent := ""
	changes := 0

	// On a full scan this sweep iterates every local database entry, which takes
	// the majority of a big scan's wall time (measured: about 15 of 22 minutes
	// on a 2.35M-entry folder). It runs in the serve goroutine like the walk
	// above, so without servicing it starves queued pulls and watch events just
	// the same - a removal watched at the start of the sweep was indexed only
	// when the whole scan ended. The database iterator is a snapshot, so it
	// stays valid across servicing, and unlike the walk there is nothing to
	// resume: the loop simply continues after the queued work is done.
	budget := time.NewTimer(scanChunkBudget)
	defer budget.Stop()

outer:
	for _, sub := range subDirs {
		for fi, err := range itererr.Zip(f.db.AllLocalFilesWithPrefix(f.folderID, protocol.LocalDeviceID, sub)) {
			if err != nil {
				return changes, err
			}

			select {
			case <-ctx.Done():
				break outer
			default:
			}

			select {
			case <-budget.C:
				// Hand back the scan's I/O token while servicing a pull, for the
				// same deadlock reason as in the walk loop above.
				f.ioLimiter.Give(1)
				pullErr := f.serviceQueuedPull(ctx)
				watchErr := f.serviceQueuedWatch(ctx)
				limitErr := f.ioLimiter.TakeWithContext(ctx, 1)
				budget.Reset(scanChunkBudget)
				// The serviced pull moved the folder state (sync-preparing
				// etc.), but this sweep is still running and, unlike the walk
				// loop, does not set FolderScanning per iteration: restore the
				// label here or the GUI reports e.g. "Preparing to Sync" for
				// the rest of the sweep (observed: label stuck for the
				// remaining 862s of a full sweep while all work was scan work).
				f.setState(FolderScanning)
				if pullErr != nil {
					return changes, pullErr
				}
				if watchErr != nil {
					return changes, watchErr
				}
				if limitErr != nil {
					return changes, limitErr
				}
			default:
			}

			if err := batch.FlushIfFull(); err != nil {
				return 0, err
			}

			if ignoredParent != "" && !fs.IsParent(fi.Name, ignoredParent) {
				for _, file := range toIgnore {
					f.sl.DebugContext(ctx, "Marking file as ignored", slogutil.FilePath(file.Name))
					nf := file
					nf.SetIgnored()
					if ok, err := batch.Update(nf); err != nil {
						return 0, err
					} else if ok {
						changes++
					}
					if err := batch.FlushIfFull(); err != nil {
						return 0, err
					}
				}
				toIgnore = toIgnore[:0]
				ignoredParent = ""
			}

			switch ignored := f.ignores.Match(fi.Name).IsIgnored(); {
			case fi.IsIgnored() && ignored:
				continue
			case !fi.IsIgnored() && ignored:
				// File was not ignored at last pass but has been ignored.
				if fi.IsDirectory() {
					// Delay ignoring as a child might be unignored.
					toIgnore = append(toIgnore, fi)
					if ignoredParent == "" {
						// If the parent wasn't ignored already, set
						// this path as the "highest" ignored parent
						ignoredParent = fi.Name
					}
					continue
				}

				f.sl.DebugContext(ctx, "Marking file as ignored", slogutil.FilePath(fi.Name))
				nf := fi
				nf.SetIgnored()
				if ok, err := batch.Update(nf); err != nil {
					return 0, err
				} else if ok {
					changes++
				}

			case fi.IsIgnored() && !ignored:
				// Successfully scanned items are already un-ignored during
				// the scan, so check whether it is deleted.
				fallthrough
			case !fi.IsIgnored() && !fi.IsDeleted() && !fi.IsUnsupported():
				// The file is not ignored, deleted or unsupported. Lets check if
				// it's still here. Simply stat:ing it won't do as there are
				// tons of corner cases (e.g. parent dir->symlink, missing
				// permissions)
				if !osutil.IsDeleted(f.mtimefs, fi.Name) {
					if ignoredParent != "" {
						// Don't ignore parents of this not ignored item
						toIgnore = toIgnore[:0]
						ignoredParent = ""
					}
					if changed, err := f.reconcileReceiveOnlyToGlobal(fi, batch); err != nil {
						return 0, err
					} else if changed {
						changes++
					}
					continue
				}
				nf := fi
				nf.SetDeleted(f.shortID)
				nf.LocalFlags = f.localFlags
				if fi.ShouldConflict() {
					// We do not want to override the global version with
					// the deleted file. Setting to an empty version makes
					// sure the file gets in sync on the following pull.
					nf.Version = protocol.Vector{}
				}
				f.sl.DebugContext(ctx, "Marking file as deleted", slogutil.FilePath(nf.Name))
				if ok, err := batch.Update(nf); err != nil {
					return 0, err
				} else if ok {
					changes++
				}
			case fi.IsDeleted() && fi.IsReceiveOnlyChanged():
				switch f.Type {
				case config.FolderTypeReceiveOnly, config.FolderTypeReceiveEncrypted:
					switch gf, ok, err := f.db.GetGlobalFile(f.folderID, fi.Name); {
					case err != nil:
						return 0, err
					case !ok:
					case gf.IsReceiveOnlyChanged():
						f.sl.DebugContext(ctx, "Removing deleted receive-only item that is globally receive-only from db", slogutil.FilePath(fi.Name))
						batch.Remove(fi.Name)
						changes++
					case gf.IsDeleted():
						// Our item is deleted and the global item is deleted too. We just
						// pretend it is a normal deleted file (nobody cares about that).
						f.sl.DebugContext(ctx, "Marking globally deleted item as not locally changed", slogutil.FilePath(fi.Name))
						fi.LocalFlags &^= protocol.FlagLocalReceiveOnly
						if ok, err := batch.Update(fi); err != nil {
							return 0, err
						} else if ok {
							changes++
						}
					}
				default:
					// No need to bump the version for a file that was and is
					// deleted and just the folder type/local flags changed.
					fi.LocalFlags &^= protocol.FlagLocalReceiveOnly
					f.sl.DebugContext(ctx, "Removing receive-only flag on deleted item", slogutil.FilePath(fi.Name))
					if ok, err := batch.Update(fi); err != nil {
						return 0, err
					} else if ok {
						changes++
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return changes, ctx.Err()
		default:
		}

		if len(toIgnore) > 0 {
			for _, file := range toIgnore {
				f.sl.DebugContext(ctx, "Marking file as ignored", slogutil.FilePath(file.Name))
				nf := file
				nf.SetIgnored()
				if ok, err := batch.Update(nf); err != nil {
					return 0, err
				} else if ok {
					changes++
				}
				if err := batch.FlushIfFull(); err != nil {
					return 0, err
				}
			}
			toIgnore = toIgnore[:0]
		}
	}

	return changes, nil
}

// reconcileReceiveOnlyToGlobal merges a locally receive-only changed item
// back into an equivalent global item, when there is no relevant difference
// between the two.
func (f *folder) reconcileReceiveOnlyToGlobal(fi protocol.FileInfo, batch *scanBatch) (bool, error) {
	if f.Type != config.FolderTypeReceiveOnly && f.Type != config.FolderTypeReceiveEncrypted {
		return false, nil
	}
	if !fi.IsReceiveOnlyChanged() {
		return false, nil
	}
	gf, ok, err := f.db.GetGlobalFile(f.folderID, fi.Name)
	if err != nil {
		return false, err
	}
	if !ok || gf.IsReceiveOnlyChanged() {
		// No global to adopt, or the global item is our own receive-only
		// change (nothing to reconcile against).
		return false, nil
	}
	if !gf.IsEquivalentOptional(fi, protocol.FileInfoComparison{
		ModTimeWindow:   f.modTimeWindow,
		IgnorePerms:     f.IgnorePerms,
		IgnoreBlocks:    true,
		IgnoreFlags:     protocol.FlagLocalReceiveOnly,
		IgnoreOwnership: !f.SyncOwnership && !f.SendOwnership,
		IgnoreXattrs:    !f.SyncXattrs && !f.SendXattrs,
	}) {
		return false, nil
	}
	f.sl.Debug("Merging identical locally changed item with global", slogutil.FilePath(fi.Name))
	return batch.Update(gf)
}

func (f *folder) findRename(ctx context.Context, file protocol.FileInfo, batch *scanBatch) (protocol.FileInfo, bool) {
	if len(file.Blocks) == 0 || file.Size == 0 {
		return protocol.FileInfo{}, false
	}

	found := false
	nf := protocol.FileInfo{}

loop:
	for fi, err := range itererr.Zip(f.db.AllLocalFilesWithBlocksHash(f.folderID, file.BlocksHash)) {
		if err != nil {
			return protocol.FileInfo{}, false
		}

		select {
		case <-ctx.Done():
			break loop
		default:
		}

		if fi.Name == file.Name {
			continue
		}

		if batch.shouldSkipInFindRenames(fi.Name) {
			continue
		}

		if fi.ShouldConflict() {
			continue
		}

		if f.ignores.Match(fi.Name).IsIgnored() {
			continue
		}

		// Only check the size.
		// No point checking block equality, as that uses BlocksHash comparison if that is set (which it will be).
		// No point checking BlocksHash comparison as WithBlocksHash already does that.
		if file.Size != fi.Size {
			continue
		}

		if !osutil.IsDeleted(f.mtimefs, fi.Name) {
			batch.markSkipInFindRename(fi.Name)
			continue
		}

		var ok bool
		nf, ok, err = f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, fi.Name)
		if err != nil || !ok || nf.Sequence != fi.Sequence {
			continue
		}
		nf.SetDeleted(f.shortID)
		nf.LocalFlags = f.localFlags
		found = true
		batch.markSkipInFindRename(fi.Name)
		break
	}

	return nf, found
}

func (f *folder) scanTimerFired(ctx context.Context) error {
	err := f.scanSubdirs(ctx, nil)

	select {
	case <-f.initialScanFinished:
	default:
		switch {
		case err == nil:
			f.sl.InfoContext(ctx, "Completed initial scan")
			// The index is now complete; pin the btrfs baseline here so a
			// later restart can diff against it.
			f.refreshBtrfsBaseline(ctx, true)
			// Record that this folder once completed an initial scan. After a
			// restart this is what lets pulls run while the re-scan is still
			// in progress (see pull); a failure to record it costs one more
			// gated scan, nothing else.
			if perr := f.db.PutKV(initialScanDoneKVPrefix+f.folderID, []byte{1}); perr != nil {
				f.sl.WarnContext(ctx, "Failed to record initial scan completion", slogutil.Error(perr))
			}
		case errors.Is(err, context.Canceled):
			// Suppressed: context was canceled (e.g. by user)
		default:
			f.sl.ErrorContext(ctx, "Failed initial scan", slogutil.Error(err))
		}
		close(f.initialScanFinished)
	}

	f.Reschedule()

	return err
}

func (f *folder) versionCleanupTimerFired(ctx context.Context) {
	f.setState(FolderCleanWaiting)

	if err := f.ioLimiter.TakeWithContext(ctx, 1); err != nil {
		return
	}
	defer f.ioLimiter.Give(1)

	f.setState(FolderCleaning)

	if err := f.versioner.Clean(ctx); err != nil {
		f.sl.WarnContext(ctx, "Failed to clean versions", slogutil.Error(err))
	}

	f.versionCleanupTimer.Reset(f.versionCleanupInterval)
}

func (f *folder) WatchError() error {
	f.watchMut.Lock()
	defer f.watchMut.Unlock()
	return f.watchErr
}

// stopWatch immediately aborts watching and may be called asynchronously
func (f *folder) stopWatch() {
	f.watchMut.Lock()
	f.watchCancel()
	f.watchMut.Unlock()
	f.setWatchError(nil, 0)
}

// scheduleWatchRestart makes sure watching is restarted from the main for loop
// in a folder's Serve and thus may be called asynchronously (e.g. when ignores change).
func (f *folder) scheduleWatchRestart() {
	select {
	case f.restartWatchChan <- struct{}{}:
	default:
		// We might be busy doing a pull and thus not reading from this
		// channel. The channel is 1-buffered, so one notification will be
		// queued to ensure we recheck after the pull.
	}
}

// restartWatch should only ever be called synchronously. If you want to use
// this asynchronously, you should probably use scheduleWatchRestart instead.
func (f *folder) restartWatch(ctx context.Context) error {
	f.stopWatch()
	f.startWatch(ctx)
	return f.scanSubdirs(ctx, nil)
}

// startWatch should only ever be called synchronously. If you want to use
// this asynchronously, you should probably use scheduleWatchRestart instead.
func (f *folder) startWatch(ctx context.Context) {
	watchCtx, cancel := context.WithCancel(ctx)
	f.watchMut.Lock()
	f.watchChan = make(chan watchaggregator.Batch)
	f.watchCancel = cancel
	f.watchMut.Unlock()
	go f.monitorWatch(watchCtx)
}

// monitorWatch starts the filesystem watching and retries every minute on failure.
// It should not be used except in startWatch.
func (f *folder) monitorWatch(ctx context.Context) {
	failTimer := time.NewTimer(0)
	aggrCtx, aggrCancel := context.WithCancel(ctx)
	var err error
	var eventChan <-chan fs.Event
	var errChan <-chan error
	warnedOutside := false
	var lastWatch time.Time
	pause := time.Minute
	// Subscribe to folder summaries only on kqueue systems, to warn about potential high resource usage
	var summarySub events.Subscription
	var summaryChan <-chan events.Event
	if fs.WatchKqueue && !f.warnedKqueue {
		summarySub = f.evLogger.Subscribe(events.FolderSummary)
		summaryChan = summarySub.C()
	}
	defer func() {
		aggrCancel() // aggrCancel might e re-assigned -> call within closure
		if summaryChan != nil {
			summarySub.Unsubscribe()
		}
	}()
	for {
		select {
		case <-failTimer.C:
			eventChan, errChan, err = f.mtimefs.Watch(".", f.ignores, ctx, f.IgnorePerms)
			// We do this once per minute initially increased to
			// max one hour in case of repeat failures.
			f.scanOnWatchErr()
			f.setWatchError(err, pause)
			if err != nil {
				failTimer.Reset(pause)
				if pause < 60*time.Minute {
					pause *= 2
				}
				continue
			}
			lastWatch = time.Now()
			watchaggregator.Aggregate(aggrCtx, eventChan, f.watchChan, f.FolderConfiguration, f.model.cfg, f.evLogger)
			f.sl.DebugContext(ctx, "Started filesystem watcher")
		case err = <-errChan:
			// The watch is dead; events between its death and the re-watch
			// are lost. Schedule a full scan to re-establish completeness,
			// and abandon any journal replay seed so the re-watch does not
			// replay the same gap (or a long history) again - the full
			// scan covers it.
			f.scanOnWatchErr()
			fs.ResetWatchReplay()
			var next time.Duration
			if dur := time.Since(lastWatch); dur > pause {
				pause = time.Minute
				next = 0
			} else {
				next = pause - dur
				if pause < 60*time.Minute {
					pause *= 2
				}
			}
			failTimer.Reset(next)
			f.setWatchError(err, next)
			// This error was previously a panic and should never occur, so generate
			// a warning, but don't do it repetitively.
			var errOutside *fs.WatchEventOutsideRootError
			if errors.As(err, &errOutside) {
				if !warnedOutside {
					slog.WarnContext(ctx, err.Error()) //nolint:sloglint
					warnedOutside = true
				}
				f.evLogger.Log(events.Failure, "watching for changes encountered an event outside of the filesystem root")
			}
			aggrCancel()
			errChan = nil
			aggrCtx, aggrCancel = context.WithCancel(ctx)
		case ev := <-summaryChan:
			if data, ok := ev.Data.(FolderSummaryEventData); !ok {
				f.evLogger.Log(events.Failure, "Unexpected type of folder-summary event in folder.monitorWatch")
			} else if data.Folder == f.folderID && data.Summary.LocalTotalItems-data.Summary.LocalDeleted > kqueueItemCountThreshold {
				f.warnedKqueue = true
				summarySub.Unsubscribe()
				summaryChan = nil
				slog.WarnContext(ctx, "Filesystem watching (kqueue) is enabled with a lot of files/directories, which requires a lot of resources and might slow down your system significantly", f.LogAttr())
			}
		case <-ctx.Done():
			aggrCancel() // for good measure and keeping the linters happy
			return
		}
	}
}

// setWatchError sets the current error state of the watch and should be called
// regardless of whether err is nil or not.
func (f *folder) setWatchError(err error, nextTryIn time.Duration) {
	f.watchMut.Lock()
	prevErr := f.watchErr
	f.watchErr = err
	f.watchMut.Unlock()
	if err != prevErr { //nolint:errorlint
		data := map[string]any{
			"folder": f.ID,
		}
		if prevErr != nil {
			data["from"] = prevErr.Error()
		}
		if err != nil {
			data["to"] = err.Error()
		}
		f.evLogger.Log(events.FolderWatchStateChanged, data)
	}
	if err == nil {
		return
	}
	if prevErr != err { //nolint:errorlint
		f.sl.Warn("Failed to start filesystem watcher", slog.String("wait", nextTryIn.String()), slogutil.Error(err))
	} else {
		f.sl.Debug("Failed to start filesystem watcher", slog.String("wait", nextTryIn.String()), slogutil.Error(err))
	}
}

// watchReplayWatermarkKVPrefix namespaces the per-folder journal position
// recording how far local filesystem changes have been accounted for in the
// database. On startup a watcher seeded with it replays every change since,
// which makes the initial full walk unnecessary (see tryArmWatchReplay).
const watchReplayWatermarkKVPrefix = "model/fseventsEventID/"

// maxWatchReplayAge bounds how old a persisted journal position may be to
// still arm a replay. The bound exists because a journal gap is NOT
// detectable from the stream: probing on macOS 26 (2026-10-08) showed that
// seeding a stream with an ID the journal no longer holds is silently
// clamped to the oldest available record, without the documented
// MustScanSubDirs flag, so a too-old watermark would mean lost changes. The
// journal only churns while the volume is active, so downtime while the
// machine sleeps barely ages it; the periodic rescan is the backstop for
// the remaining risk window.
const maxWatchReplayAge = 12 * time.Hour

// tryArmWatchReplay attempts to resume filesystem watching from the
// persisted journal position instead of running the initial full scan. When
// it succeeds the initial scan timer is replaced by the regular rescan
// schedule and the initial scan is marked finished: a complete database
// index (recorded by a previous run) plus a journal replay since the
// watermark is equivalent to a completed initial scan. Every failure path
// here leaves the folder on the regular course; every journal integrity
// failure after arming is reported by the watcher as a fatal watch error,
// which schedules a full scan via the watch error path. Must be called
// before startWatch so the seed is in place when the stream starts.
func (f *folder) tryArmWatchReplay(ctx context.Context) bool {
	if !fs.WatchReplaySupported() {
		return false
	}
	if done, err := f.initialScanEverCompleted(); err != nil || !done {
		// Without a recorded complete index, replay cannot substitute for
		// the first walk.
		return false
	}
	w, recordedAt, err := f.watchReplayWatermark()
	if err != nil {
		f.sl.WarnContext(ctx, "Failed to read watch replay watermark", slogutil.Error(err))
		return false
	}
	if w == 0 {
		return false
	}
	if age := time.Since(recordedAt); age > maxWatchReplayAge {
		// See maxWatchReplayAge: an over-old journal position cannot be
		// trusted to still be covered, and the stream will not say so.
		f.sl.InfoContext(ctx, "Watch replay watermark too old, falling back to full scan", "age", age, "max", maxWatchReplayAge)
		return false
	}
	if cur := fs.CurrentWatchEventID(); cur < w {
		// The journal starts over when e.g. the volume is reformatted; a
		// watermark ahead of its current position is meaningless.
		f.sl.WarnContext(ctx, "Watch replay watermark is ahead of the filesystem journal, falling back to full scan", "watermark", w, "journal", cur)
		return false
	}
	fs.SetWatchReplaySince(w)
	// Skip the pending initial scan and fall straight into the regular
	// rescan schedule, keeping periodic scans as the safety net.
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
	f.lastReplayWatermark.Store(w)
	f.sl.InfoContext(ctx, "Skipping initial scan, resuming filesystem watching from the journal", "sinceEventID", w)
	return true
}

// watchReplayWatermark reads the persisted journal position and when it was
// recorded; an ID of 0 means none was recorded yet.
func (f *folder) watchReplayWatermark() (uint64, time.Time, error) {
	b, err := f.db.GetKV(watchReplayWatermarkKVPrefix + f.folderID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, time.Time{}, nil
	}
	if err != nil {
		return 0, time.Time{}, err
	}
	if len(b) != 16 {
		return 0, time.Time{}, nil
	}
	return binary.BigEndian.Uint64(b[:8]), time.Unix(int64(binary.BigEndian.Uint64(b[8:])), 0), nil
}

// persistWatchReplayWatermark records that every local change up to the
// given journal position has been scanned into the database. Writes are
// throttled to advancements only; the value is only ever consumed by the
// next process start, and over-persisting is impossible because the caller
// scans the corresponding batch first.
func (f *folder) persistWatchReplayWatermark(id uint64) {
	if id == 0 || id <= f.lastReplayWatermark.Load() {
		return
	}
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], id)
	binary.BigEndian.PutUint64(b[8:], uint64(time.Now().Unix()))
	if err := f.db.PutKV(watchReplayWatermarkKVPrefix+f.folderID, b[:]); err != nil {
		f.sl.Warn("Failed to record watch replay watermark", slogutil.Error(err))
		return
	}
	f.lastReplayWatermark.Store(id)
}

// scanOnWatchErr schedules a full scan immediately if an error occurred while watching.
func (f *folder) scanOnWatchErr() {
	f.watchMut.Lock()
	err := f.watchErr
	f.watchMut.Unlock()
	if err != nil {
		f.DelayScan(0)
	}
}

func (f *folder) setError(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	state, _, oldErr := f.getState()
	switch {
	case err != nil && oldErr != nil && oldErr.Error() == err.Error():
		// The error is the same as the old error, no change is required
		return
	case err == nil && oldErr == nil && state == FolderIdle:
		// No error before or now, and we are already Idle
		return
	}

	if err != nil {
		if oldErr == nil {
			f.sl.WarnContext(ctx, "Error on folder", slogutil.Error(err))
		} else {
			f.sl.InfoContext(ctx, "Folder error changed", slogutil.Error(err), slog.Any("previously", oldErr))
		}
	} else if oldErr != nil {
		f.sl.InfoContext(ctx, "Folder error cleared")
		f.SchedulePull()
	}

	if f.FSWatcherEnabled {
		if err != nil {
			f.stopWatch()
		} else if oldErr != nil {
			f.scheduleWatchRestart()
		}
	}

	f.stateTracker.setError(err)
}

func (f *folder) pullBasePause() time.Duration {
	if f.PullerPauseS == 0 {
		return defaultPullerPause
	}
	return time.Duration(f.PullerPauseS) * time.Second
}

func (f *folder) String() string {
	return fmt.Sprintf("%s/%s@%p", f.Type, f.folderID, f)
}

func (f *folder) newScanError(path string, err error) {
	f.errorsMut.Lock()
	f.sl.Warn("Failed to scan", slogutil.FilePath(path), slogutil.Error(err))
	f.scanErrors = append(f.scanErrors, FileError{
		Err:  err.Error(),
		Path: path,
	})
	f.errorsMut.Unlock()
}

func (f *folder) clearScanErrors(subDirs []string) {
	f.errorsMut.Lock()
	defer f.errorsMut.Unlock()
	if len(subDirs) == 0 {
		f.scanErrors = nil
		return
	}
	filtered := f.scanErrors[:0]
outer:
	for _, fe := range f.scanErrors {
		for _, sub := range subDirs {
			if fe.Path == sub || fs.IsParent(fe.Path, sub) {
				continue outer
			}
		}
		filtered = append(filtered, fe)
	}
	f.scanErrors = filtered
}

func (f *folder) Errors() []FileError {
	f.errorsMut.Lock()
	defer f.errorsMut.Unlock()
	scanLen := len(f.scanErrors)
	errors := make([]FileError, scanLen+len(f.pullErrors))
	copy(errors[:scanLen], f.scanErrors)
	copy(errors[scanLen:], f.pullErrors)
	slices.SortFunc(errors, func(a, b FileError) int {
		return strings.Compare(a.Path, b.Path)
	})
	return errors
}

// ScheduleForceRescan marks the file such that it gets rehashed on next scan, and schedules a scan.
func (f *folder) ScheduleForceRescan(path string) {
	f.forcedRescanPathsMut.Lock()
	f.forcedRescanPaths[path] = struct{}{}
	f.forcedRescanPathsMut.Unlock()

	select {
	case f.forcedRescanRequested <- struct{}{}:
	default:
	}
}

func (f *folder) updateLocalsFromScanning(fs []protocol.FileInfo) error {
	if err := f.updateLocals(fs); err != nil {
		return err
	}
	f.emitDiskChangeEvents(fs, events.LocalChangeDetected)
	return nil
}

func (f *folder) updateLocalsFromPulling(fs []protocol.FileInfo) error {
	if err := f.updateLocals(fs); err != nil {
		return err
	}
	f.emitDiskChangeEvents(fs, events.RemoteChangeDetected)
	return nil
}

func (f *folder) updateLocals(fs []protocol.FileInfo) error {
	var opts []db.UpdateOption
	if !f.BlockIndexing {
		opts = append(opts, db.WithSkipBlockIndex())
	}
	if err := f.db.Update(f.folderID, protocol.LocalDeviceID, fs, opts...); err != nil {
		return err
	}

	filenames := make([]string, len(fs))
	f.forcedRescanPathsMut.Lock()
	for i, file := range fs {
		filenames[i] = file.Name
		// No need to rescan a file that was changed since anyway.
		delete(f.forcedRescanPaths, file.Name)
	}
	f.forcedRescanPathsMut.Unlock()

	seq, err := f.db.GetDeviceSequence(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		return err
	}
	f.evLogger.Log(events.LocalIndexUpdated, map[string]any{
		"folder":    f.ID,
		"items":     len(fs),
		"filenames": filenames,
		"sequence":  seq,
		"version":   seq, // legacy for sequence
	})
	return nil
}

func (f *folder) emitDiskChangeEvents(fs []protocol.FileInfo, typeOfEvent events.EventType) {
	for _, file := range fs {
		if file.IsInvalid() {
			continue
		}

		objType := "file"
		action := "modified"

		if file.IsDeleted() {
			action = "deleted"
		}

		if file.IsSymlink() {
			objType = "symlink"
		} else if file.IsDirectory() {
			objType = "dir"
		}

		// Two different events can be fired here based on what EventType is passed into function
		f.evLogger.Log(typeOfEvent, map[string]string{
			"folder":     f.ID,
			"folderID":   f.ID, // incorrect, deprecated, kept for historical compliance
			"label":      f.Label,
			"action":     action,
			"type":       objType,
			"path":       filepath.FromSlash(file.Name),
			"modifiedBy": file.ModifiedBy.String(),
		})
	}
}

func (f *folder) handleForcedRescans(ctx context.Context) error {
	f.forcedRescanPathsMut.Lock()
	paths := make([]string, 0, len(f.forcedRescanPaths))
	for path := range f.forcedRescanPaths {
		paths = append(paths, path)
	}
	f.forcedRescanPaths = make(map[string]struct{})
	f.forcedRescanPathsMut.Unlock()
	if len(paths) == 0 {
		return nil
	}

	batch := NewFileInfoBatch(func(fs []protocol.FileInfo) error {
		return f.db.Update(f.folderID, protocol.LocalDeviceID, fs)
	})

	for _, path := range paths {
		if err := batch.FlushIfFull(); err != nil {
			return err
		}

		fi, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, path)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		fi.SetMustRescan()
		batch.Append(fi)
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	return f.scanSubdirs(ctx, paths)
}

// The exists function is expected to return true for all known paths
// (excluding "" and ".")
func unifySubs(dirs []string, exists func(dir string) bool) []string {
	if len(dirs) == 0 {
		return nil
	}
	slices.Sort(dirs)
	if dirs[0] == "" || dirs[0] == "." || dirs[0] == string(fs.PathSeparator) {
		return nil
	}
	prev := "./" // Anything that can't be parent of a clean path
	for i := 0; i < len(dirs); {
		dir, err := fs.Canonicalize(dirs[i])
		if err != nil {
			slog.Debug("Skipping directory for scan", slog.String("dir", dirs[i]), slogutil.Error(err))
			dirs = append(dirs[:i], dirs[i+1:]...)
			continue
		}
		if dir == prev || fs.IsParent(dir, prev) {
			dirs = append(dirs[:i], dirs[i+1:]...)
			continue
		}
		parent := filepath.Dir(dir)
		for parent != "." && parent != string(fs.PathSeparator) && !exists(parent) {
			dir = parent
			parent = filepath.Dir(dir)
		}
		dirs[i] = dir
		prev = dir
		i++
	}
	return dirs
}

type cFiler struct {
	db     db.DB
	folder string
}

// Implements scanner.CurrentFiler
func (cf cFiler) CurrentFile(file string) (protocol.FileInfo, bool) {
	fi, ok, err := cf.db.GetDeviceFile(cf.folder, protocol.LocalDeviceID, file)
	if err != nil || !ok {
		return protocol.FileInfo{}, false
	}
	return fi, true
}
