// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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
		if len(temps) >= installSyncfsMinGroup {
			err := installSyncfsFn(f.Filesystem().URI())
			if err == nil {
				return nil
			}
			f.sl.Warn("syncfs failed, falling back to per-file fsyncs for install group", slogutil.Error(err))
		}
		// Small group (or failed syncfs): per-file fsyncs are cheaper than one
		// syncfs, which flushes every dirty page of the whole filesystem.
		for _, temp := range temps {
			if err := f.fsyncInstallFile(temp); err != nil {
				return err
			}
		}
		if len(legacyDirs) > 0 {
			f.fsyncDirs(legacyDirs)
		}
		return f.syncInstallDirs(installDirs)
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
	if f.installSyncfs() && len(installDirs) >= installSyncfsMinGroup {
		err := installSyncfsFn(f.Filesystem().URI())
		if err == nil {
			return nil
		}
		f.sl.Warn("syncfs failed, falling back to per-directory fsyncs for install group", slogutil.Error(err))
	}
	return f.syncInstallDirs(installDirs)
}

// installSyncfsMinGroup is the group size from which one syncfs(2) beats
// individual fsyncs. syncfs flushes every dirty page of the folder's whole
// filesystem, including other writers' data: measured ~0.5 s on yarlung
// (btrfs, live Syncthing) against ~33 ms per small-file fsync. Below this
// size the precommit fence fsyncs each temp and the postrename fence each
// directory instead.
const installSyncfsMinGroup = 8

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
		return err
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
	if err := f.mtimefs.Rename(p.temp, name); err != nil {
		return err
	}
	p.scans <- name
	return nil
}
