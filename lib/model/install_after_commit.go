// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

type pendingInstall struct {
	file, previous protocol.FileInfo
	hasPrevious    bool
	temp           string
	scans          chan<- string
	state          *sharedPullerState // owned by the committer after enqueue
	done           chan error
}

func (f *sendReceiveFolder) retainInstallTemp(p *pendingInstall) {
	// Prepared temps carry the sender's potentially ancient mtime. After a
	// failed/cancelled precommit wait they are downloads again, not installed
	// files, and must not immediately expire in the next temporary-file scan.
	now := time.Now()
	_ = f.mtimefs.Chtimes(p.temp, now, now)
}

func (f *sendReceiveFolder) installTargetStat(name string, previous protocol.FileInfo, hasPrevious bool) (fs.FileInfo, error) {
	stat, err := f.mtimefs.Lstat(name)
	var caseErr *fs.CaseConflictError
	if errors.As(err, &caseErr) && hasPrevious && caseErr.Real == previous.Name {
		return f.mtimefs.Lstat(previous.Name)
	}
	return stat, err
}

// queueInstall prepares durable bytes without changing the destination. Only
// the single updater may publish them, after its durable transaction returns.
func (f *sendReceiveFolder) queueInstall(file, previous protocol.FileInfo, hasPrevious bool, temp string, jobs chan<- dbUpdateJob, scans chan<- string) (*pendingInstall, error) {
	return f.queueInstallState(file, previous, hasPrevious, temp, jobs, scans, nil)
}

func (f *sendReceiveFolder) queueInstallState(file, previous protocol.FileInfo, hasPrevious bool, temp string, jobs chan<- dbUpdateJob, scans chan<- string, state *sharedPullerState) (*pendingInstall, error) {
	if !f.IgnorePerms && !file.NoPermissions {
		if err := f.mtimefs.Chmod(temp, fs.FileMode(file.Permissions&0o777)); err != nil {
			return nil, fmt.Errorf("setting permissions: %w", err)
		}
	}
	if err := f.setPlatformData(&file, temp); err != nil {
		return nil, fmt.Errorf("setting metadata: %w", err)
	}
	if err := f.mtimefs.Chtimes(temp, file.ModTime(), file.ModTime()); err != nil {
		return nil, err
	}
	if !f.DisableFsync && !f.installBarrier() {
		fd, err := f.mtimefs.Open(temp)
		if err != nil {
			return nil, err
		}
		err = fd.Sync()
		closeErr := fd.Close()
		if err != nil {
			return nil, fmt.Errorf("syncing temporary file: %w", err)
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	// Check once before committing, then again immediately before publication.
	if stat, err := f.installTargetStat(file.Name, previous, hasPrevious); err == nil {
		checkName := file.Name
		if hasPrevious {
			checkName = previous.Name
		}
		if err = f.scanIfItemChanged(checkName, stat, previous, hasPrevious, false, scans); err != nil {
			return nil, fmt.Errorf("checking existing file: %w", err)
		}
	} else if !fs.IsNotExist(err) {
		return nil, err
	}
	file.LocalFlags |= protocol.FlagLocalInstallPending
	pending := &pendingInstall{file: file, previous: previous, hasPrevious: hasPrevious, temp: temp, scans: scans, state: state, done: make(chan error, 1)}
	jobs <- dbUpdateJob{file: file, jobType: dbUpdateHandleFile, install: pending}
	return pending, nil
}

// precommitInstallFence makes a group's prepared temps durable before their
// index commit. In barrier mode (Linux and macOS, real local filesystem) the
// cost model picks per fence between one full sync per temp and directory,
// and the platform's group barrier (installBarrierFn) over all of them, plus
// the legacy (non-install) jobs' directories. queueInstallState skipped the
// per-temp sync in that mode, so a failed barrier falls back to syncing each
// temp here: it never commits over unsynced temp bytes.
func (f *sendReceiveFolder) precommitInstallFence(temps []string, installDirs, legacyDirs map[string]struct{}) error {
	if f.DisableFsync || len(temps)+len(installDirs)+len(legacyDirs) == 0 {
		return nil
	}
	if f.installBarrier() {
		items := len(temps) + len(installDirs) + len(legacyDirs)
		method := fencePerItem
		if len(temps) > 0 {
			method = f.installCost.pre.choose(items)
		}
		if method == fenceSampleBarrier {
			// Refresh the per-item estimate on a few temps; the barrier
			// below covers them and everything else.
			t0 := time.Now()
			for _, temp := range temps[:min(installCostSampleItems, len(temps))] {
				if err := f.fsyncInstallFile(temp); err != nil {
					return err
				}
			}
			f.installCost.pre.observeItems(time.Since(t0), min(installCostSampleItems, len(temps)))
		}
		if method != fencePerItem {
			t0 := time.Now()
			err := f.runInstallBarrier(installBarrierPaths(temps, installDirs, legacyDirs))
			if err == nil {
				f.installCost.pre.observeBarrier(time.Since(t0))
				return nil
			}
			f.sl.Warn("Install group barrier failed, falling back to per-file syncs", slog.String("barrier", installBarrierName), slogutil.Error(err))
		}
		// Per-item fence (chosen, or a failed barrier): a full sync of each
		// temp and directory. Never commit over unsynced temp bytes.
		t0 := time.Now()
		for _, temp := range temps {
			if err := f.fsyncInstallFile(temp); err != nil {
				return err
			}
		}
		if len(legacyDirs) > 0 {
			f.fsyncDirs(legacyDirs)
		}
		if err := f.syncInstallDirs(installDirs); err != nil {
			return err
		}
		if method == fencePerItem {
			f.installCost.pre.observeItems(time.Since(t0), items)
		}
		return nil
	}
	if len(legacyDirs) > 0 {
		f.flushPullDurability(legacyDirs)
	}
	return f.syncInstallDirs(installDirs)
}

// postrenameInstallFence makes the group's renames durable before the
// pending bits are cleared: the cost model's choice between one full sync
// per distinct directory and the platform's group barrier over them.
func (f *sendReceiveFolder) postrenameInstallFence(installDirs map[string]struct{}) error {
	if f.DisableFsync || len(installDirs) == 0 {
		return nil
	}
	method := fencePerItem
	if f.installBarrier() {
		method = f.installCost.post.choose(len(installDirs))
	}
	if method == fenceSampleBarrier {
		dirs := make(map[string]struct{}, installCostSampleItems)
		for d := range installDirs {
			if len(dirs) == installCostSampleItems {
				break
			}
			dirs[d] = struct{}{}
		}
		t0 := time.Now()
		if err := f.syncInstallDirs(dirs); err != nil {
			return err
		}
		f.installCost.post.observeItems(time.Since(t0), len(dirs))
	}
	if method != fencePerItem {
		t0 := time.Now()
		err := f.runInstallBarrier(installBarrierPaths(nil, installDirs, nil))
		if err == nil {
			f.installCost.post.observeBarrier(time.Since(t0))
			return nil
		}
		f.sl.Warn("Install group barrier failed, falling back to per-directory syncs", slog.String("barrier", installBarrierName), slogutil.Error(err))
	}
	t0 := time.Now()
	if err := f.syncInstallDirs(installDirs); err != nil {
		return err
	}
	if f.installBarrier() && method == fencePerItem {
		f.installCost.post.observeItems(time.Since(t0), len(installDirs))
	}
	return nil
}

// installBarrierPaths lists what a group barrier must make durable: the
// temps, then every directory (sorted, so the order is deterministic).
// syncfs ignores the list; macOS fsyncs each entry before its one flush.
func installBarrierPaths(temps []string, dirSets ...map[string]struct{}) []string {
	paths := append([]string(nil), temps...)
	start := len(paths)
	for _, dirs := range dirSets {
		for d := range dirs {
			paths = append(paths, d)
		}
	}
	slices.Sort(paths[start:])
	return paths
}

// installSyncCost chooses, per fence, between the platform's group barrier
// (one syncfs(2) of the folder's filesystem on Linux; plain fsyncs plus one
// F_FULLFSYNC on macOS) and one full sync per item (temp file or directory)
// by what each has recently cost on this machine (patch/74):
//
//   - It keeps the last installCostSamples durations of each: barrier time,
//     and per-item time (a per-item fence's total divided by its items).
//   - Until each ring holds installCostWarmup (3) samples it explores,
//     alternating 50/50 by the golden ratio: fence k uses the barrier iff
//     frac(k*phi) < 1/2, a well-spread deterministic sequence. A group of
//     one item always uses per-item.
//   - Then it uses the barrier iff n * median(perItem) > median(barrier),
//     over the samples each ring holds: 3 at first, growing to
//     installCostSamples (5) through the refresh. Simulated with measured
//     costs over 60 fences, deciding from 3 instead of 5 cut the warm-up's
//     excess from 18.9 s to 12.2 s in total over five cases; the median of
//     3 still ignores a single outlier such as the first syncfs after
//     startup flushing everything already dirty (1.19 s against a 131 ms
//     median on yarlung). Lopsided 80/20 or 20/80 splits were worse (53.6 s
//     and 33.2 s): the rarer method takes longer to sample.
//   - Every installCostRefresh-th fence makes a refresh of the losing
//     method due, so both rings stay current as the disk's load changes;
//     no clock, no expiry, no fixed fallback. A per-item refresh runs at
//     once (a few fsyncs). A barrier refresh waits for a group where the
//     barrier costs at most twice the per-item fence, so it never adds a
//     whole syncfs to a single-file group.
//   - Exploring per-item on a group larger than installCostSampleItems
//     fsyncs only that many items, then barriers the group: a sample costs
//     at most a few fsyncs, never n.
//
// Measured with the same calls (scratch fsprobe, 2026-11-07): yarlung
// (btrfs, 2x HDD) fsync median 25.8 ms, syncfs 131.0 ms, so the barrier
// wins from n > 5.08; the rule predicted the faster method for n = 1, 2,
// 4, 8, 16 and 32 in all six cases. Under overnight sync load syncfs rose
// to 2.6 s and the break-even with it, which the refresh follows.
const (
	installCostSamples     = 5  // ring size; decisions use the median of what the ring holds
	installCostWarmup      = 3  // samples of each method before measurements decide
	installCostRefresh     = 17 // every 17th fence measures the losing method
	installCostSampleItems = 3  // per-item fsyncs when exploring a large group
)

// installGoldenRatio is phi; frac(k*phi) is the warm-up alternation.
const installGoldenRatio = 1.6180339887498949

type costRing struct {
	v [installCostSamples]time.Duration
	n int // samples recorded, saturating at installCostSamples
	i int // next slot
}

func (r *costRing) add(d time.Duration) {
	r.v[r.i] = d
	r.i = (r.i + 1) % installCostSamples
	if r.n < installCostSamples {
		r.n++
	}
}

func (r *costRing) median() time.Duration {
	s := r.v // copy
	slices.Sort(s[:r.n])
	return s[r.n/2]
}

// fenceMethod is what one fence does.
type fenceMethod int

const (
	fencePerItem       fenceMethod = iota // fsync every item
	fenceBarrier                          // one syncfs for the group
	fenceSampleBarrier                    // fsync installCostSampleItems items (timed), then one syncfs
)

type installSyncCost struct {
	mu         sync.Mutex
	barrier    costRing
	perItem    costRing
	fences     uint64
	refreshDue bool // a refresh of the losing method is waiting for a cheap group
	lastLogged int  // break-even group size last logged, 0 = none
	logf       func(breakEven int, barrier, perItem time.Duration)
}

type installSyncCosts struct{ pre, post installSyncCost }

// choose returns the method for a fence over n items.
func (c *installSyncCost) choose(n int) fenceMethod {
	if n < 2 {
		return fencePerItem
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fences++
	k := c.fences
	explorePerItem := func() fenceMethod {
		if n > installCostSampleItems {
			return fenceSampleBarrier
		}
		return fencePerItem
	}
	if c.barrier.n < installCostWarmup || c.perItem.n < installCostWarmup {
		_, frac := math.Modf(float64(k) * installGoldenRatio)
		if frac < 0.5 {
			return fenceBarrier
		}
		return explorePerItem()
	}
	barrierWins := time.Duration(n)*c.perItem.median() > c.barrier.median()
	// The losing method's ring grows from the warm-up's 3 samples to 5 at
	// the refresh rate, then stays current at the same rate.
	if k%installCostRefresh == 0 {
		c.refreshDue = true
	}
	if c.refreshDue {
		if barrierWins {
			// Re-measure per-item: a sample of at most
			// installCostSampleItems fsyncs, cheap on any group.
			c.refreshDue = false
			return explorePerItem()
		}
		// Re-measure the barrier only on a group where it is at most twice
		// the per-item cost, so a refresh never costs a full syncfs on top
		// of a single-file group (patch/74 trickle bench on yarlung: 0.3 s
		// each). Until such a group comes, stay with per-item, which is
		// always durable; a busy disk keeps the barrier dear anyway.
		if 2*time.Duration(n)*c.perItem.median() >= c.barrier.median() {
			c.refreshDue = false
			return fenceBarrier
		}
	}
	if barrierWins {
		return fenceBarrier
	}
	return fencePerItem
}

func (c *installSyncCost) observeBarrier(d time.Duration) {
	c.mu.Lock()
	c.barrier.add(d)
	c.noteLocked()
	c.mu.Unlock()
}

func (c *installSyncCost) observeItems(d time.Duration, n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	c.perItem.add(d / time.Duration(n))
	c.noteLocked()
	c.mu.Unlock()
}

// noteLocked reports the break-even group size once both rings hold enough
// samples to decide, and whenever it has moved by a factor of two or more
// since last reported, so the log shows what the decisions rest on without
// a line per group.
func (c *installSyncCost) noteLocked() {
	if c.logf == nil || c.barrier.n < installCostWarmup || c.perItem.n < installCostWarmup {
		return
	}
	b, p := c.barrier.median(), c.perItem.median()
	if p <= 0 {
		return
	}
	be := max(int(b/p)+1, 2)
	if c.lastLogged != 0 && be < 2*c.lastLogged && 2*be > c.lastLogged {
		return
	}
	c.lastLogged = be
	c.logf(be, b, p)
}

// installBarrierForTest forces barrier mode on the install path in tests,
// whose fake filesystem otherwise disables it.
var installBarrierForTest = false

// installBarrier reports whether install groups may use the platform's group
// barrier (installBarrierFn: syncfs on Linux, plain fsyncs plus one
// F_FULLFSYNC on macOS) instead of one full sync per temp and directory. It
// must be decided consistently for the per-temp skip in queueInstallState
// and the group fences. A real local filesystem is required; STSYNCMODE=fsync
// turns it off.
func (f *sendReceiveFolder) installBarrier() bool {
	if f.DisableFsync {
		return false
	}
	if installBarrierForTest {
		return true
	}
	return installBarrierSupported && f.Filesystem().Type() == fs.FilesystemTypeBasic && os.Getenv("STSYNCMODE") != "fsync"
}

// runInstallBarrier makes paths (relative to the folder) durable with the
// platform's group barrier.
func (f *sendReceiveFolder) runInstallBarrier(paths []string) error {
	return installBarrierFn(f.Filesystem().URI(), paths)
}

func (f *sendReceiveFolder) fsyncInstallFile(name string) error {
	fd, err := f.mtimefs.Open(name)
	if err != nil {
		return err
	}
	err = fd.Sync()
	closeErr := fd.Close()
	if err != nil {
		return fmt.Errorf("syncing temporary file: %w", err)
	}
	return closeErr
}

func (f *sendReceiveFolder) syncInstallDirs(dirs map[string]struct{}) error {
	if f.DisableFsync || f.Filesystem().Type() == fs.FilesystemTypeFake {
		return nil
	}
	return f.fsyncInstallDirs(dirs)
}

func (f *sendReceiveFolder) fsyncInstallDirs(dirs map[string]struct{}) error {
	for dir := range dirs {
		fd, err := f.mtimefs.Open(dir)
		if err != nil {
			return err
		}
		err = fd.Sync()
		closeErr := fd.Close()
		if err != nil {
			return fmt.Errorf("syncing install directory %s: %w", dir, err)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (f *sendReceiveFolder) commitInstallGroup(ctx context.Context, group []dbUpdateJob, observer *pullDBObserver) (result error) {
	results := make([]error, len(group))
	committed, completed := false, false
	defer func() {
		for i, job := range group {
			if job.install != nil {
				if !committed {
					f.retainInstallTemp(job.install)
				}
				err := results[i]
				if err == nil && !completed {
					err = result
				}
				f.completeInstall(ctx, job.install, err)
			}
		}
	}()
	files := make([]protocol.FileInfo, len(group))
	installDirs := make(map[string]struct{})
	legacyDirs := make(map[string]struct{})
	var temps []string
	durable := false
	for i, job := range group {
		files[i] = job.file
		files[i].Sequence = 0
		if job.install != nil {
			durable = true
			installDirs[filepath.Dir(job.install.temp)] = struct{}{}
			temps = append(temps, job.install.temp)
		} else {
			switch job.jobType {
			case dbUpdateHandleFile, dbUpdateShortcutFile:
				legacyDirs[filepath.Dir(job.file.Name)] = struct{}{}
			case dbUpdateHandleDir:
				legacyDirs[job.file.Name] = struct{}{}
			}
		}
	}
	// The recovery temp's directory entry must itself survive a power failure.
	// A temp-file fsync alone does not portably persist its name. Group this
	// precommit directory fence, and retain the existing postrename fence too.
	if err := f.precommitInstallFence(temps, installDirs, legacyDirs); err != nil {
		return err
	}
	if err := f.updatePullDBWithRetry(ctx, files, observer, durable); err != nil {
		return err
	}
	committed = true
	var acknowledged []protocol.FileInfo
	var emitted []protocol.FileInfo
	for i, job := range group {
		if job.install != nil {
			if err := f.publishInstall(job.install); err != nil {
				results[i] = err
				if result == nil {
					result = err
				}
				continue
			}
			clean := job.file
			clean.LocalFlags &^= protocol.FlagLocalInstallPending
			acknowledged = append(acknowledged, clean)
		}
		emitted = append(emitted, job.file)
	}
	if err := f.postrenameInstallFence(installDirs); err != nil {
		return err
	}
	// Clearing a pending bit needs no additional WAL sync: losing this normal
	// commit merely repeats idempotent recovery after restart. Never clear it
	// before the published directory entry has been synced.
	if len(acknowledged) > 0 {
		if err := f.updateLocals(acknowledged, db.WithSkipBlockIndex()); err != nil {
			return err
		}
	}
	for _, file := range emitted {
		f.retry.clear(file.Name, !file.IsDeleted() && !file.IsInvalid() && file.Type == protocol.FileInfoTypeFile)
		if !file.IsInvalid() && (!file.IsDirectory() && !file.IsSymlink() || file.IsDeleted()) {
			f.ReceivedFile(file.Name, file.IsDeleted())
		}
	}
	f.emitDiskChangeEvents(emitted, events.RemoteChangeDetected)
	completed = true
	return result
}

func (f *sendReceiveFolder) publishInstall(p *pendingInstall) error {
	if err := osutil.TraversesSymlink(f.mtimefs, filepath.Dir(p.file.Name)); err != nil {
		return err
	}
	if stat, err := f.installTargetStat(p.file.Name, p.previous, p.hasPrevious); err == nil {
		checkName := p.file.Name
		if p.hasPrevious {
			checkName = p.previous.Name
		}
		err = f.scanIfItemChanged(checkName, stat, p.previous, p.hasPrevious, false, p.scans)
		if errors.Is(err, errModified) {
			// A genuine local edit won the race. Keep it at the requested path and
			// preserve the incoming bytes under patch/60's bounded conflict name.
			// Persist the resolution before moving the only deterministic recovery
			// temp. A crash thereafter may scan the genuine edit, never predecessor
			// content. This rare conflict path alone pays a second durable commit.
			return f.resolveEditedInstall(p)
		}
		if err != nil {
			return err
		}
		if !p.previous.IsDirectory() && !p.previous.IsSymlink() && p.file.InConflictWith(p.previous) {
			err = f.inWritableDir(func(name string) error { return f.moveForConflict(name, p.file.ModifiedBy.String(), p.scans) }, p.file.Name)
		} else {
			err = f.deleteItemOnDisk(p.previous, p.scans)
		}
		if err != nil {
			return err
		}
	} else if !fs.IsNotExist(err) {
		return err
	} else if p.hasPrevious && !p.previous.IsDeleted() {
		// Deleting the old target during the commit is a local change too.
		// Preserve its absence and ask the scanner to record the deletion.
		p.scans <- p.file.Name
		return f.resolveEditedInstall(p)
	}
	if err := osutil.RenameOrCopy(f.CopyRangeMethod.ToFS(), f.mtimefs, f.mtimefs, p.temp, p.file.Name); err != nil {
		// RenameOrCopy makes a read-only parent writable, but a concurrent
		// inWritableDir in the same directory (a copier creating its temp
		// file) can restore the read-only mode in between: it is not
		// serialized with the rename lock. Retry once with the directory
		// held writable around the rename itself.
		if !fs.IsPermission(err) {
			return err
		}
		if err := f.inWritableDir(func(name string) error { return f.mtimefs.Rename(p.temp, name) }, p.file.Name); err != nil {
			return err
		}
	}
	// The bytes already carry this timestamp durably. Reapply it at the new
	// name so mtimeFS transfers its advisory virtual-time mapping as well.
	return f.mtimefs.Chtimes(p.file.Name, p.file.ModTime(), p.file.ModTime())
}

func (f *sendReceiveFolder) resolveEditedInstall(p *pendingInstall) error {
	clean := p.file
	clean.LocalFlags &^= protocol.FlagLocalInstallPending
	if err := f.updateLocals([]protocol.FileInfo{clean}, db.WithDurable()); err != nil {
		return err
	}
	return f.preserveIncomingConflict(p)
}

func (f *sendReceiveFolder) preserveIncomingConflict(p *pendingInstall) error {
	name := conflictName(p.file.Name, p.file.ModifiedBy.String())
	if _, err := f.mtimefs.Lstat(name); !fs.IsNotExist(err) {
		if err == nil {
			return fmt.Errorf("conflict destination already exists: %s", name)
		}
		return err
	}
	if err := f.inWritableDir(func(string) error { return f.mtimefs.Rename(p.temp, name) }, name); err != nil {
		return err
	}
	p.scans <- name
	return nil
}
