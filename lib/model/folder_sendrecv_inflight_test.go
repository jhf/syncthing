// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// patch56Private exposes only addressable test synchronization fields. SQLite's
// package-private lock and timer cannot otherwise be reached from a model test.
// Keep this deliberately coupled to their names/types, rather than adding any
// production hook. Access to maps/timers is under their actual owning mutexes.
func patch56Private[T any](v reflect.Value) *T {
	return (*T)(unsafe.Pointer(v.UnsafeAddr()))
}

type patch56ScanDB struct {
	db.DB
	generated    chan protocol.FileInfo
	resume       chan struct{}
	pendingRead  chan struct{}
	continueScan chan struct{}
}

func (d *patch56ScanDB) PendingInstalls(folder string) ([]protocol.FileInfo, error) {
	files, err := d.DB.PendingInstalls(folder)
	close(d.pendingRead)
	<-d.continueScan
	return files, err
}

func (d *patch56ScanDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	for _, fi := range files {
		if device == protocol.LocalDeviceID && fi.Name == "lib.rs" {
			d.generated <- fi
			<-d.resume
		}
	}
	return d.DB.Update(folder, device, files, opts...)
}

// TestPostPullInFlightMtimeStopsAtFinish integrates the real finish, SQLite
// batch flush and rescan without editing the receiver's installed file. The
// requested sequence stops at the next finish: the restored virtual timestamp
// differs from the physical timestamp just indexed by the scanner, so the
// safety check refuses replacement. No conflict exists to scan or announce,
// therefore no fake-peer exchange is attempted. This is NOT an explanation of
// the 2026-11-03 incident. scanSubdirs is the body of the post-pull Scan call,
// invoked directly because the event loop is stopped. Index commit necessarily
// waits for the same write lock as flush: observe the scanner-generated
// FileInfo before releasing it, then commit unchanged.
func TestPostPullInFlightMtimeStopsAtFinish(t *testing.T) {
	t.Setenv("STMETABATCH", "0")
	w, cfg := newDefaultCfgWrapper(t)
	cfg.Path += "&timeprecisionsecond=true"
	_, err := w.Modify(func(c *config.Configuration) { c.SetFolder(cfg) })
	must(t, err)
	m := setupModel(t, w)
	m.cancel()
	<-m.stopped
	runner, _ := m.folderRunners.Get(cfg.ID)
	f := runner.(*sendReceiveFolder)
	f.MaxConflicts = -1
	const name = "lib.rs"
	oldContent := []byte("// sender v1, never edited on receiver\n")
	newContent := []byte("// sender v4, later content\n")
	stamp := time.Unix(1793660546, 123456789)
	makeInfo := func(content []byte, version uint64) protocol.FileInfo {
		blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(content), protocol.MinBlockSize, int64(len(content)), nil)
		must(t, err)
		return protocol.FileInfo{Name: name, Type: protocol.FileInfoTypeFile, Size: int64(len(content)), Permissions: 0o640,
			ModifiedS: stamp.Unix(), ModifiedNs: int32(stamp.Nanosecond()), ModifiedBy: device1.Short(),
			Version: protocol.Vector{Counters: []protocol.Counter{{ID: device1.Short(), Value: version}}},
			Blocks:  blocks, BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize}
	}
	current := func(path string) protocol.FileInfo {
		t.Helper()
		fi, ok, err := f.db.GetDeviceFile(cfg.ID, protocol.LocalDeviceID, path)
		must(t, err)
		if !ok {
			t.Fatalf("missing local index entry %q", path)
		}
		return fi
	}
	read := func(path string) []byte {
		t.Helper()
		fd, err := f.mtimefs.Open(path)
		must(t, err)
		data, err := io.ReadAll(fd)
		must(t, err)
		must(t, fd.Close())
		return data
	}

	// Disarm the latency trigger before the pull. An inert, stopped timer also
	// prevents add from installing a new timer. The size threshold will invoke
	// the exact flushLocked used by the timer, with no scheduling-time race.
	root := reflect.ValueOf(m.sdb).Elem()
	foldersMut := patch56Private[sync.RWMutex](root.FieldByName("folderDBsMut"))
	foldersMut.RLock()
	folderDB := root.FieldByName("folderDBs").MapIndex(reflect.ValueOf(cfg.ID)).Elem()
	foldersMut.RUnlock()
	batch := folderDB.FieldByName("mtimes").Elem()
	batchMut := patch56Private[sync.Mutex](batch.FieldByName("mut"))
	batchMut.Lock()
	timer := patch56Private[*time.Timer](batch.FieldByName("timer"))
	if *timer != nil {
		(*timer).Stop()
	}
	inert := time.NewTimer(time.Hour)
	inert.Stop()
	*timer = inert
	batchMut.Unlock()

	v1 := makeInfo(oldContent, 1)
	jobs := make(chan dbUpdateJob, 1)
	scans := make(chan string, 4)
	temp := fs.TempName(name)
	writeFile(t, f.mtimefs, temp, oldContent)
	must(t, f.performFinish(v1, protocol.FileInfo{}, false, temp, jobs, scans))
	commitQueuedFinishForTest(t, f, <-jobs)
	if !current(name).Version.Equal(v1.Version) {
		t.Fatal("pull changed sender vector")
	}
	raw, err := cfg.Filesystem().Lstat(name)
	must(t, err)
	if !raw.ModTime().Equal(stamp.Truncate(time.Second)) {
		t.Fatal("filesystem did not truncate mtime")
	}
	disk, virtual := m.sdb.GetMtime(cfg.ID, name)
	if !disk.Equal(raw.ModTime()) || !virtual.Equal(stamp) {
		t.Fatal("pull did not create required mapping")
	}
	// Let the real pending-install precheck finish before starting the advisory
	// mtime flush. Its new updateLock scope otherwise blocks this intentionally
	// in-flight scan before it can exercise the older mapping visibility race.
	originalDB := f.db
	probe := &patch56ScanDB{DB: originalDB, generated: make(chan protocol.FileInfo, 1), resume: make(chan struct{}), pendingRead: make(chan struct{}), continueScan: make(chan struct{})}
	f.db = probe
	scanDone := make(chan error, 1)
	go func() { scanDone <- f.scanSubdirs(t.Context(), []string{name}) }()
	allowWalk := sync.OnceFunc(func() { close(probe.continueScan) })
	resume := sync.OnceFunc(func() { close(probe.resume) })
	release := func() {}
	defer func() { release(); allowWalk(); resume(); <-scanDone; f.db = originalDB }()
	select {
	case <-probe.pendingRead:
	case <-time.After(5 * time.Second):
		t.Fatal("scanner did not finish pending-install precheck")
	}

	lock := patch56Private[sync.Mutex](folderDB.FieldByName("updateLock"))
	lock.Lock()
	release = sync.OnceFunc(lock.Unlock)
	defer release()
	batchMut.Lock()
	pending := batch.FieldByName("pending").Len()
	batchMut.Unlock()
	// 512 is the production mtimeBatchMaxOps threshold. Filler mappings touch
	// only advisory DB state, never the receiver file or its existing mapping.
	flushed := make(chan struct{})
	go func(initialPending int) {
		defer close(flushed)
		for i := initialPending; i < 512; i++ {
			if err := m.sdb.PutMtime(cfg.ID, fmt.Sprintf("patch56-unused-%d", i), stamp, stamp); err != nil {
				panic(err)
			}
		}
	}(pending)
	defer func() { release(); <-flushed }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		batchMut.Lock()
		pending = batch.FieldByName("pending").Len()
		batchMut.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flush did not reach write lock")
		}
		runtime.Gosched()
	}
	disk, virtual = m.sdb.GetMtime(cfg.ID, name)
	if !disk.IsZero() || !virtual.IsZero() {
		t.Fatal("mapping remained visible during flush")
	}
	stat, err := f.mtimefs.Lstat(name)
	must(t, err)
	if !stat.ModTime().Equal(raw.ModTime()) {
		t.Fatal("real scanner filesystem did not expose physical timestamp")
	}

	allowWalk()
	var generated protocol.FileInfo
	select {
	case generated = <-probe.generated:
	case err := <-scanDone:
		scanDone <- err
		t.Fatalf("scan stopped before generating local version: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("scanner did not reach index write")
	}
	if generated.Version.Counter(f.shortID) == 0 || generated.Version.Counter(device1.Short()) != 1 ||
		generated.ModifiedBy != f.shortID || !generated.ModTime().Equal(raw.ModTime()) || !bytes.Equal(generated.BlocksHash, v1.BlocksHash) ||
		!bytes.Equal(generated.PreviousBlocksHash, v1.BlocksHash) || !bytes.Equal(read(name), oldContent) {
		t.Fatalf("scanner did not generate byte-identical receiver version: %+v", generated)
	}
	if !current(name).Version.Equal(v1.Version) {
		t.Fatal("index unexpectedly committed through held lock")
	}
	release()
	<-flushed
	disk, virtual = m.sdb.GetMtime(cfg.ID, name)
	if !disk.Equal(raw.ModTime()) || !virtual.Equal(stamp) {
		t.Fatal("flush did not restore mapping")
	}
	resume()
	err = <-scanDone
	scanDone <- err // leave completion available for unconditional cleanup
	must(t, err)
	f.db = originalDB
	cur := current(name)
	if !cur.Version.Equal(generated.Version) {
		t.Fatal("scan did not commit generated vector")
	}
	t.Log("steps 1-3: real pull mapping disappears in flush; scanner generates receiver counter on unchanged bytes and commits after unlock")

	v4 := makeInfo(newContent, 4)
	v3 := makeInfo([]byte("// sender v3, never received\n"), 3)
	v4.PreviousBlocksHash = v3.BlocksHash
	if !v4.InConflictWith(cur) {
		t.Fatal("expected concurrent conflict")
	}
	// The committed scan recorded the physical timestamp. Once flush restores
	// the virtual timestamp, performFinish's safety check sees a NEW metadata
	// mismatch. Do not erase the mapping or bypass this real safety check.
	writeFile(t, f.mtimefs, temp, newContent)
	err = f.performFinish(v4, cur, true, temp, jobs, scans)
	if !errors.Is(err, errModified) {
		t.Fatalf("expected step 4 to stop at restored-mtime safety check, got %v", err)
	}
	if len(jobs) != 0 || len(existingConflicts(name, f.mtimefs)) != 0 || !bytes.Equal(read(name), oldContent) || !current(name).Version.Equal(cur.Version) {
		t.Fatal("rejected finish changed content/index or created conflict")
	}
	if len(scans) != 1 || <-scans != name {
		t.Fatal("rejected finish did not request lib.rs rescan")
	}
	stat, err = f.mtimefs.Lstat(name)
	must(t, err)
	if !stat.ModTime().Equal(stamp) || cur.ModTime().Equal(stat.ModTime()) {
		t.Fatal("restored mapping does not explain safety-check mismatch")
	}
	t.Log("step 4 stops: checking existing file: file modified but not rescanned; will try again later")
	t.Log("steps 5-6 not reached: no conflict file, no conflict index or fake-peer announcement")
}
