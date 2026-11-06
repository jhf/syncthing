// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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
	if !f.DisableFsync && !f.installSyncfs() {
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
// index commit. In syncfs mode (Linux default) one syncfs(2) of the folder
// filesystem covers every temp's contents and directory entry, plus the
// legacy (non-install) jobs' directories: one barrier per group instead of
// one fsync per file. queueInstallState skipped the per-temp fsync in that
// mode, so a failed syncfs must fall back to fsyncing each temp here; it is
// never allowed to commit over unsynced temp bytes.
func (f *sendReceiveFolder) precommitInstallFence(temps []string, installDirs, legacyDirs map[string]struct{}) error {
	if f.DisableFsync || len(temps)+len(installDirs)+len(legacyDirs) == 0 {
		return nil
	}
	if f.installSyncfs() {
		items := len(temps) + len(installDirs) + len(legacyDirs)
		if len(temps) > 0 && f.installCost.pre.useSyncfs(items) {
			t0 := time.Now()
			err := installSyncfsFn(f.Filesystem().URI())
			if err == nil {
				f.installCost.pre.observeSyncfs(time.Since(t0))
				return nil
			}
			f.sl.Warn("syncfs failed, falling back to per-file fsyncs for install group", slogutil.Error(err))
		}
		// Small group (or failed syncfs): per-file fsyncs are cheaper than one
		// syncfs, which flushes every dirty page of the whole filesystem.
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
		f.installCost.pre.observeItems(time.Since(t0), items)
		return nil
	}
	if len(legacyDirs) > 0 {
		f.flushPullDurability(legacyDirs)
	}
	return f.syncInstallDirs(installDirs)
}

// postrenameInstallFence makes the group's renames durable before the
// pending bits are cleared. One syncfs in syncfs mode, else one fsync per
// distinct directory.
func (f *sendReceiveFolder) postrenameInstallFence(installDirs map[string]struct{}) error {
	if f.DisableFsync || len(installDirs) == 0 {
		return nil
	}
	if f.installSyncfs() && f.installCost.post.useSyncfs(len(installDirs)) {
		t0 := time.Now()
		err := installSyncfsFn(f.Filesystem().URI())
		if err == nil {
			f.installCost.post.observeSyncfs(time.Since(t0))
			return nil
		}
		f.sl.Warn("syncfs failed, falling back to per-directory fsyncs for install group", slogutil.Error(err))
	}
	t0 := time.Now()
	if err := f.syncInstallDirs(installDirs); err != nil {
		return err
	}
	if f.installSyncfs() {
		f.installCost.post.observeItems(time.Since(t0), len(installDirs))
	}
	return nil
}

// installSyncfsMinGroup is the group size from which one syncfs(2) is used
// while there is no measurement to decide by (patch/67's fixed rule). It is
// only the starting point: installSyncCost replaces it with measured costs.
const installSyncfsMinGroup = 8

// installCostWindow is how long a cost measurement stays valid. A syncfs
// costs whatever the whole filesystem has dirty at that moment, so an old
// reading says little; an expired estimate falls back to the fixed rule,
// which also re-measures the method that had stopped being chosen.
const installCostWindow = 10 * time.Minute

// installSyncCost chooses, per fence, between one syncfs(2) and one fsync
// per item (temp file or directory) by what each has actually cost here
// lately: syncfs when n * perItem > syncfs. Both are exponentially weighted
// moving averages (alpha 1/4) of measured durations. patch/67 used a fixed n
// >= 8 from an inferred cost ratio. Measured on yarlung (btrfs, 2x HDD, live
// Syncthing, 2026-11-06 20:47 UTC, 15 samples): small-file fsync median
// 30.5 ms, syncfs median 226 ms (176..749 ms), break-even 7.4 files. The
// syncfs cost is whatever the whole filesystem has dirty at that moment, so
// it moves: on a quiet disk syncfs pays from n=2, while it is busy per-item
// fsyncs win for much larger groups. A group of one always fsyncs.
type installSyncCost struct {
	mu                  sync.Mutex
	now                 func() time.Time
	syncfs, perItem     time.Duration
	syncfsAt, perItemAt time.Time
	lastLogged          int // break-even group size last logged, 0 = none
	logf                func(breakEven int, syncfs, perItem time.Duration)
}

type installSyncCosts struct{ pre, post installSyncCost }

func (c *installSyncCost) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// useSyncfs reports whether a fence over n items should use one syncfs.
func (c *installSyncCost) useSyncfs(n int) bool {
	if n < 2 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	syncfsOK := !c.syncfsAt.IsZero() && now.Sub(c.syncfsAt) < installCostWindow
	perItemOK := !c.perItemAt.IsZero() && now.Sub(c.perItemAt) < installCostWindow
	if !syncfsOK || !perItemOK {
		return n >= installSyncfsMinGroup
	}
	return time.Duration(n)*c.perItem > c.syncfs
}

func ewma(old, sample time.Duration, fresh bool) time.Duration {
	if !fresh {
		return sample
	}
	return old + (sample-old)/4
}

func (c *installSyncCost) observeSyncfs(d time.Duration) {
	c.mu.Lock()
	now := c.clock()
	c.syncfs = ewma(c.syncfs, d, !c.syncfsAt.IsZero() && now.Sub(c.syncfsAt) < installCostWindow)
	c.syncfsAt = now
	c.noteLocked()
	c.mu.Unlock()
}

func (c *installSyncCost) observeItems(d time.Duration, n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	now := c.clock()
	c.perItem = ewma(c.perItem, d/time.Duration(n), !c.perItemAt.IsZero() && now.Sub(c.perItemAt) < installCostWindow)
	c.perItemAt = now
	c.noteLocked()
	c.mu.Unlock()
}

// noteLocked reports the break-even group size when it has moved by a
// factor of two or more since last reported, so the log shows what the
// decision rests on without a line per group.
func (c *installSyncCost) noteLocked() {
	if c.logf == nil || c.syncfsAt.IsZero() || c.perItemAt.IsZero() || c.perItem <= 0 {
		return
	}
	be := int(c.syncfs/c.perItem) + 1
	if be < 2 {
		be = 2
	}
	if c.lastLogged != 0 && be < 2*c.lastLogged && 2*be > c.lastLogged {
		return
	}
	c.lastLogged = be
	c.logf(be, c.syncfs, c.perItem)
}

// installSyncfsFn is the install path's syncfs hook. It is distinct from
// syncfsFn so tests can count install-group barriers independently.
var installSyncfsFn = func(root string) error { return syncfsFn(root) }

// installSyncfsForTest forces syncfs mode on the install path in tests, whose
// fake filesystem otherwise disables it.
var installSyncfsForTest = false

// installSyncfs reports whether install groups use syncfs barriers instead of
// per-temp and per-directory fsyncs. It must be decided consistently for both
// the per-temp skip in queueInstallState and the group fences.
func (f *sendReceiveFolder) installSyncfs() bool {
	return !f.DisableFsync && (installSyncfsForTest || f.batchSyncfs())
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
