// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

type installBarrierDB struct {
	db.DB
	before func()
	after  func() error
	fail   error
}

func commitQueuedFinishForTest(t *testing.T, f *sendReceiveFolder, job dbUpdateJob) {
	t.Helper()
	var observer pullDBObserver
	defer observer.close()
	must(t, f.commitInstallGroup(t.Context(), []dbUpdateJob{job}, &observer))
}

func (d *installBarrierDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	if d.before != nil {
		d.before()
	}
	if d.fail != nil {
		return d.fail
	}
	if err := d.DB.Update(folder, device, files, opts...); err != nil {
		return err
	}
	if d.after != nil {
		return d.after()
	}
	return nil
}

func TestInstallAfterCommitBarrier(t *testing.T) {
	f, fi, data, jobs := intentFixture(t, "barrier")
	calls := 0
	f.db = &installBarrierDB{DB: f.db, before: func() {
		calls++
		if calls == 1 {
			if _, err := f.mtimefs.Lstat(fi.Name); !fs.IsNotExist(err) {
				t.Error("rename happened before durable commit returned")
			}
		}
	}}
	close(jobs)
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	must(t, err)
	if calls == 0 {
		t.Fatal("no database boundary")
	}
	fd, err := f.mtimefs.Open(fi.Name)
	must(t, err)
	defer fd.Close()
	got, err := io.ReadAll(fd)
	must(t, err)
	if !bytes.Equal(got, data) {
		t.Fatal("file not published after commit")
	}
}

func TestInstallAfterCommitNonLockError(t *testing.T) {
	f, fi, _, jobs := intentFixture(t, "non-lock")
	terminal := errors.New("structural database failure")
	f.db = &installBarrierDB{DB: f.db, fail: terminal}
	close(jobs)
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	if !errors.Is(err, terminal) {
		t.Fatalf("error=%v", err)
	}
	if _, err := f.mtimefs.Lstat(fi.Name); !fs.IsNotExist(err) {
		t.Fatal("failed commit published file")
	}
	if _, err := f.mtimefs.Lstat(fs.TempName(fi.Name)); err != nil {
		t.Fatal("failed commit lost reusable temp")
	}
}

func physicalInstallFixture(t *testing.T, name string) (*sendReceiveFolder, protocol.FileInfo, []byte, string) {
	t.Helper()
	f, fi, data, _ := intentFixture(t, name)
	f.FilesystemType = config.FilesystemTypeBasic
	f.Path = t.TempDir()
	f.mtimefs = f.Filesystem()
	must(t, f.CreateMarker())
	f.DisableFsync = false
	must(t, f.mtimefs.MkdirAll(filepath.Dir(name), 0o755))
	temp := fs.TempName(name)
	writeFile(t, f.mtimefs, temp, data)
	return f, fi, data, temp
}

func readInstallBytes(t *testing.T, f *sendReceiveFolder, name string) []byte {
	t.Helper()
	fd, err := f.mtimefs.Open(name)
	must(t, err)
	defer fd.Close()
	data, err := io.ReadAll(fd)
	must(t, err)
	return data
}

func existingInstallTarget(t *testing.T, f *sendReceiveFolder, name, kind string) protocol.FileInfo {
	t.Helper()
	switch kind {
	case "directory":
		must(t, f.mtimefs.Mkdir(name, 0o755))
	case "symlink":
		must(t, f.mtimefs.CreateSymlink("unrelated", name))
	default:
		writeFile(t, f.mtimefs, name, []byte("old sender bytes\n"))
	}
	stat, err := f.mtimefs.Lstat(name)
	must(t, err)
	fi, err := scanner.CreateFileInfo(stat, name, f.mtimefs, false, false, nil)
	must(t, err)
	fi.Version = protocol.Vector{}.Update(device1.Short())
	fi.ModifiedBy = device1.Short()
	if kind == "file" || kind == "conflict" {
		fi.Blocks, err = scanner.Blocks(t.Context(), bytes.NewReader([]byte("old sender bytes\n")), protocol.MinBlockSize, fi.Size, nil)
		must(t, err)
		fi.BlocksHash = protocol.BlocksHash(fi.Blocks)
	}
	if kind == "conflict" {
		fi.Version = protocol.Vector{}.Update(f.shortID)
		fi.ModifiedBy = f.shortID
	}
	must(t, f.db.Update(f.ID, protocol.LocalDeviceID, []protocol.FileInfo{fi}))
	return fi
}

func TestInstallAfterCommitLocalEdit(t *testing.T) {
	f, fi, data, temp := physicalInstallFixture(t, "local-edit")
	old := existingInstallTarget(t, f, fi.Name, "file")
	jobs := make(chan dbUpdateJob, 1)
	scans := make(chan string, 8)
	must(t, f.performFinish(fi, old, true, temp, jobs, scans))
	close(jobs)
	local := []byte("genuine local edit made after WAL sync\n")
	hooked := false
	f.db = &installBarrierDB{DB: f.db, after: func() error {
		if !hooked {
			hooked = true
			writeFile(t, f.mtimefs, fi.Name, local)
		}
		return nil
	}}
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	must(t, err)
	if !bytes.Equal(readInstallBytes(t, f, fi.Name), local) {
		t.Fatal("local edit overwritten")
	}
	conflicts := existingConflicts(fi.Name, f.mtimefs)
	if len(conflicts) != 1 || !bytes.Equal(readInstallBytes(t, f, conflicts[0]), data) {
		t.Fatal("incoming bytes not preserved under bounded conflict name")
	}
	if len(filepath.Base(conflicts[0])) > 255 {
		t.Fatal("unbounded conflict name")
	}
}

func TestInstallAfterCommitReplacementBarrier(t *testing.T) {
	for _, kind := range []string{"file", "directory", "symlink", "conflict"} {
		t.Run(kind, func(t *testing.T) {
			f, fi, data, temp := physicalInstallFixture(t, "replace")
			f.MaxConflicts = -1
			old := existingInstallTarget(t, f, fi.Name, kind)
			jobs := make(chan dbUpdateJob, 1)
			scans := make(chan string, 8)
			must(t, f.performFinish(fi, old, true, temp, jobs, scans))
			close(jobs)
			checked := false
			f.db = &installBarrierDB{DB: f.db, before: func() {
				if checked {
					return
				}
				checked = true
				stat, err := f.mtimefs.Lstat(fi.Name)
				if err != nil || stat.IsDir() != (kind == "directory") || stat.IsSymlink() != (kind == "symlink") {
					t.Error("target moved before commit")
				}
				if len(existingConflicts(fi.Name, f.mtimefs)) != 0 {
					t.Error("conflict move preceded commit")
				}
			}}
			_, err := f.dbUpdaterRoutine(t.Context(), jobs)
			must(t, err)
			if !checked || !bytes.Equal(readInstallBytes(t, f, fi.Name), data) {
				t.Fatal("replacement failed")
			}
			if kind == "conflict" && len(existingConflicts(fi.Name, f.mtimefs)) != 1 {
				t.Fatal("old conflict not preserved")
			}
		})
	}
}

func TestInstallAfterCommitLocalDelete(t *testing.T) {
	f, fi, data, temp := physicalInstallFixture(t, "local-delete")
	old := existingInstallTarget(t, f, fi.Name, "file")
	jobs := make(chan dbUpdateJob, 1)
	must(t, f.performFinish(fi, old, true, temp, jobs, make(chan string, 8)))
	close(jobs)
	hooked := false
	f.db = &installBarrierDB{DB: f.db, after: func() error {
		if !hooked {
			hooked = true
			return f.mtimefs.Remove(fi.Name)
		}
		return nil
	}}
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	must(t, err)
	if _, err := f.mtimefs.Lstat(fi.Name); !fs.IsNotExist(err) {
		t.Fatal("local deletion overwritten")
	}
	conflicts := existingConflicts(fi.Name, f.mtimefs)
	if len(conflicts) != 1 || !bytes.Equal(readInstallBytes(t, f, conflicts[0]), data) {
		t.Fatal("incoming data lost after local deletion")
	}
}

func TestInstallAfterCommitRejectCorruptTemp(t *testing.T) {
	f, fi, data, temp := physicalInstallFixture(t, "corrupt-temp")
	existingInstallTarget(t, f, fi.Name, "file")
	fi.LocalFlags |= protocol.FlagLocalInstallPending
	must(t, f.db.Update(f.ID, protocol.LocalDeviceID, []protocol.FileInfo{fi}, db.WithDurable()))
	corrupt := bytes.Clone(data)
	corrupt[0] ^= 0xff
	writeFile(t, f.mtimefs, temp, corrupt)
	if err := f.scanSubdirs(t.Context(), []string{fi.Name}); err == nil {
		t.Fatal("scanner accepted corrupt recovery temp")
	}
	got, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if !ok || !got.Version.Equal(fi.Version) || got.Version.Counter(f.shortID) != 0 || got.LocalFlags&protocol.FlagLocalInstallPending == 0 {
		t.Fatal("failed recovery invented receiver authorship or lost pending evidence")
	}
	if !bytes.Equal(readInstallBytes(t, f, fi.Name), []byte("old sender bytes\n")) {
		t.Fatal("corrupt recovery changed target")
	}
}

func TestInstallAfterCommitCrashRecovery(t *testing.T) {
	for _, kind := range []string{"absent", "file", "directory", "symlink", "conflict"} {
		t.Run(kind, func(t *testing.T) {
			f, fi, data, temp := physicalInstallFixture(t, "pending")
			dir := t.TempDir()
			original, err := sqlite.Open(dir)
			must(t, err)
			f.db = original
			var old protocol.FileInfo
			if kind != "absent" {
				old = existingInstallTarget(t, f, fi.Name, kind)
			}
			jobs := make(chan dbUpdateJob, 1)
			must(t, f.performFinish(fi, old, kind != "absent", temp, jobs, make(chan string, 8)))
			close(jobs)
			crash := errors.New("simulated death after real FULL commit, before rename")
			f.db = &installBarrierDB{DB: original, after: func() error { return crash }}
			_, err = f.dbUpdaterRoutine(t.Context(), jobs)
			if !errors.Is(err, crash) {
				t.Fatal(err)
			}
			must(t, original.Close())
			reopened, err := sqlite.Open(dir)
			must(t, err)
			defer reopened.Close()
			restarted := newSendReceiveFolder(f.model, f.ignores, f.FolderConfiguration, nil, f.evLogger, f.ioLimiter).(*sendReceiveFolder)
			restarted.db = reopened
			restarted.mtimefs = f.mtimefs
			must(t, restarted.scanSubdirs(t.Context(), []string{fi.Name}))
			got, ok, err := reopened.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
			must(t, err)
			if !ok || !got.Version.Equal(fi.Version) || got.Version.Counter(f.shortID) != 0 || got.LocalFlags&protocol.FlagLocalInstallPending != 0 || !bytes.Equal(readInstallBytes(t, restarted, fi.Name), data) {
				t.Fatalf("recovery changed sender identity: %+v", got)
			}
			if kind != "absent" && len(existingConflicts(fi.Name, f.mtimefs)) != 1 {
				t.Fatal("recovery discarded uncertain old target")
			}
			must(t, restarted.RecoverPendingInstalls(t.Context()))
		})
	}
}

func TestInstallAfterCommitCancelRetainsTemp(t *testing.T) {
	f, fi, _, temp := physicalInstallFixture(t, "cancel")
	old := existingInstallTarget(t, f, fi.Name, "file")
	jobs := make(chan dbUpdateJob, 1)
	must(t, f.performFinish(fi, old, true, temp, jobs, make(chan string, 8)))
	close(jobs)
	original := f.db
	f.db = &patch58BusyDB{DB: original, fail: errors.New("SQLITE_BUSY: database is locked"), failures: -1}
	ctx, cancel := context.WithCancel(t.Context())
	f.pullDBRetryPolicy = &pullDBRetryPolicy{wait: func(context.Context, time.Duration) error { cancel(); return context.Canceled }}
	_, err := f.dbUpdaterRoutine(ctx, jobs)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !bytes.Equal(readInstallBytes(t, f, fi.Name), []byte("old sender bytes\n")) {
		t.Fatal("cancellation changed target")
	}
	f.db = original
	must(t, f.scanSubdirs(t.Context(), []string{fi.Name}))
	if _, err := f.mtimefs.Lstat(temp); err != nil {
		t.Fatal("cancellation temp expired during scan")
	}
	got, _, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if !got.Version.Equal(old.Version) || got.Version.Counter(f.shortID) != 0 {
		t.Fatal("cancelled install created receiver work")
	}
	jobs = make(chan dbUpdateJob, 1)
	must(t, f.performFinish(fi, old, true, temp, jobs, make(chan string, 8)))
	close(jobs)
	_, err = f.dbUpdaterRoutine(t.Context(), jobs)
	must(t, err)
}

func TestInstallAfterCommitEncryptedRecovery(t *testing.T) {
	f, fi, data, temp := physicalInstallFixture(t, "encrypted")
	f.Type = config.FolderTypeReceiveEncrypted
	fi.Encrypted = []byte("opaque authenticated sender FileInfo")
	// Tokens deliberately are not SHA256(data), just like the encrypted wire.
	fi.Blocks[0].Hash = bytes.Repeat([]byte{0x7f}, 48)
	fi.BlocksHash = protocol.BlocksHash(fi.Blocks)
	fd, err := f.mtimefs.OpenFile(temp, fs.OptReadWrite, 0o600)
	must(t, err)
	n, err := writeEncryptionTrailer(fi, fd)
	must(t, err)
	must(t, fd.Close())
	fi.Size += n
	fi.EncryptionTrailerSize = int(n)
	fi.LocalFlags |= protocol.FlagLocalInstallPending
	must(t, f.db.Update(f.ID, protocol.LocalDeviceID, []protocol.FileInfo{fi}, db.WithDurable()))
	// A different intended identity must not adopt this temp.
	other := fi
	other.Encrypted = []byte("different authenticated sender FileInfo")
	match, err := f.installContentMatches(t.Context(), temp, other)
	must(t, err)
	if match {
		t.Fatal("recovery accepted another encrypted identity")
	}
	must(t, f.RecoverPendingInstalls(t.Context()))
	got := readInstallBytes(t, f, fi.Name)
	if !bytes.Equal(got[:len(data)], data) {
		t.Fatal("encrypted payload changed")
	}
	indexed, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if !ok || indexed.LocalFlags&protocol.FlagLocalInstallPending != 0 || !indexed.Version.Equal(fi.Version) {
		t.Fatal("encrypted recovery changed identity")
	}
	must(t, f.RecoverPendingInstalls(t.Context()))
}

func TestInstallAfterCommitRenameSourceRetained(t *testing.T) {
	f, _, _, _ := physicalInstallFixture(t, "unused")
	cur := existingInstallTarget(t, f, "source", "file")
	source := cur
	source.SetDeleted(device1.Short())
	target := cur
	target.Name = "destination"
	target.Version = target.Version.Update(device1.Short())
	terminal := errors.New("source retention commit failure")
	f.db = &installBarrierDB{DB: f.db, before: func() {
		if !bytes.Equal(readInstallBytes(t, f, cur.Name), []byte("old sender bytes\n")) {
			t.Error("rename shortcut removed source before target commit")
		}
	}, fail: terminal}
	jobs := make(chan dbUpdateJob, 2)
	done := make(chan error, 1)
	go func() { _, err := f.dbUpdaterRoutine(t.Context(), jobs); done <- err }()
	err := f.renameFile(cur, source, target, jobs, make(chan string, 8))
	close(jobs)
	if !errors.Is(err, terminal) || !errors.Is(<-done, terminal) {
		t.Fatalf("expected terminal failure, got %v", err)
	}
	if !bytes.Equal(readInstallBytes(t, f, cur.Name), []byte("old sender bytes\n")) {
		t.Fatal("failed rename shortcut lost source")
	}
	if _, err := f.mtimefs.Lstat(target.Name); !fs.IsNotExist(err) {
		t.Fatal("failed rename shortcut published target")
	}
}

type slowInstallDB struct {
	db.DB
	active, maximum, calls atomic.Int32
	started                chan struct{}
	sizes                  []int
}

func (d *slowInstallDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	var options db.UpdateOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.Durable {
		active := d.active.Add(1)
		for max := d.maximum.Load(); active > max; max = d.maximum.Load() {
			if d.maximum.CompareAndSwap(max, active) {
				break
			}
		}
		if d.calls.Add(1) == 1 {
			close(d.started)
		}
		d.sizes = append(d.sizes, len(files))
		time.Sleep(500 * time.Millisecond)
		defer d.active.Add(-1)
	}
	return d.DB.Update(folder, device, files, opts...)
}

func TestInstallAfterCommitSlowFlush(t *testing.T) {
	f, fi, data, first := intentFixture(t, "slow-first")
	jobs := make(chan dbUpdateJob, 32)
	jobs <- <-first
	d := &slowInstallDB{DB: f.db, started: make(chan struct{})}
	f.db = d
	done := make(chan error, 1)
	start := time.Now()
	go func() { _, err := f.dbUpdaterRoutine(t.Context(), jobs); done <- err }()
	<-d.started
	for i := 0; i < 31; i++ {
		next := fi
		next.Name = fmt.Sprintf("slow-%02d", i)
		temp := fs.TempName(next.Name)
		writeFile(t, f.mtimefs, temp, data)
		must(t, f.performFinish(next, protocol.FileInfo{}, false, temp, jobs, make(chan string, 2)))
	}
	close(jobs)
	must(t, <-done)
	elapsed := time.Since(start)
	if d.maximum.Load() != 1 || d.active.Load() != 0 || d.calls.Load() > int32(elapsed/(500*time.Millisecond))+1 {
		t.Fatalf("flushes=%d maxInFlight=%d elapsed=%v", d.calls.Load(), d.maximum.Load(), elapsed)
	}
	if len(d.sizes) != 2 || d.sizes[0] != 1 || d.sizes[1] != 31 {
		t.Fatalf("queued files were not drained into next group: %v", d.sizes)
	}
	t.Logf("slowFlush=500ms flushes=%d maxInFlight=%d groups=%v elapsed=%v", d.calls.Load(), d.maximum.Load(), d.sizes, elapsed)
}
