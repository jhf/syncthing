// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
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

// unexportedField exposes only addressable test synchronization fields. SQLite's
// package-private lock and timer cannot otherwise be reached from a model test.
// Keep this deliberately coupled to their names/types, rather than adding any
// production hook. Access to maps/timers is under their actual owning mutexes.
func unexportedField[T any](v reflect.Value) *T {
	return (*T)(unsafe.Pointer(v.UnsafeAddr()))
}

type inflightScanDB struct {
	db.DB
	generated    chan protocol.FileInfo
	resume       chan struct{}
	pendingRead  chan struct{}
	continueScan chan struct{}
}

func (d *inflightScanDB) PendingInstalls(folder string) ([]protocol.FileInfo, error) {
	files, err := d.DB.PendingInstalls(folder)
	close(d.pendingRead)
	<-d.continueScan
	return files, err
}

func (d *inflightScanDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	for _, fi := range files {
		if device == protocol.LocalDeviceID && fi.Name == "lib.rs" {
			d.generated <- fi
			<-d.resume
		}
	}
	return d.DB.Update(folder, device, files, opts...)
}

// TestPostPullScanDuringMtimeFlushKeepsSenderVersion drives the real finish,
// SQLite mtime batch flush and post-pull rescan on a whole-second
// filesystem, without editing the receiver's installed file. The flush of
// the pull's virtual-mtime mapping is held at the folder write lock while
// the scan runs. The mapping must stay visible, so the scan sees the
// sender's timestamp and writes nothing, and the next sender version
// installs as a plain successor. Before the fix the mapping vanished for the
// flush's wait, the scan minted a receiver version of unchanged sender bytes
// (receiver counter, receiver ModifiedBy), and the next sender version was in
// conflict; only the restored-mtime safety check at finish stopped a
// conflict copy. scanSubdirs is the body of the post-pull Scan call, invoked
// directly because the event loop is stopped.
func TestPostPullScanDuringMtimeFlushKeepsSenderVersion(t *testing.T) {
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
	foldersMut := unexportedField[sync.RWMutex](root.FieldByName("folderDBsMut"))
	foldersMut.RLock()
	folderDB := root.FieldByName("folderDBs").MapIndex(reflect.ValueOf(cfg.ID)).Elem()
	foldersMut.RUnlock()
	batch := folderDB.FieldByName("mtimes").Elem()
	batchMut := unexportedField[sync.Mutex](batch.FieldByName("mut"))
	batchMut.Lock()
	timer := unexportedField[*time.Timer](batch.FieldByName("timer"))
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
	probe := &inflightScanDB{DB: originalDB, generated: make(chan protocol.FileInfo, 1), resume: make(chan struct{}), pendingRead: make(chan struct{}), continueScan: make(chan struct{})}
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

	lock := unexportedField[sync.Mutex](folderDB.FieldByName("updateLock"))
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
			if err := m.sdb.PutMtime(cfg.ID, fmt.Sprintf("inflight-unused-%d", i), stamp, stamp); err != nil {
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
	if !disk.Equal(raw.ModTime()) || !virtual.Equal(stamp) {
		t.Errorf("mapping not visible while its flush waits for the write lock: %s -> %s", disk, virtual)
	}
	stat, err := f.mtimefs.Lstat(name)
	must(t, err)
	if !stat.ModTime().Equal(stamp) {
		t.Errorf("scanner filesystem exposed %s during flush, want virtual %s", stat.ModTime(), stamp)
	}

	// Let the real scan run while the flush still waits for the write lock.
	// It must see the sender's timestamp, find nothing changed and finish
	// without writing: before the batcher kept in-flight batches visible it
	// saw the physical whole-second timestamp here and minted a receiver
	// version of the unchanged sender bytes.
	allowWalk()
	select {
	case generated := <-probe.generated:
		resume()
		t.Fatalf("scan during mtime flush minted a receiver version of unchanged sender content: version=%v ModifiedBy=%v", generated.Version, generated.ModifiedBy)
	case err := <-scanDone:
		scanDone <- err // leave completion available for unconditional cleanup
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not finish while the flush waited")
	}
	release()
	<-flushed
	f.db = originalDB
	cur := current(name)
	if !cur.Version.Equal(v1.Version) || cur.ModifiedBy != device1.Short() {
		t.Fatalf("sender v1 identity changed: version=%v ModifiedBy=%v", cur.Version, cur.ModifiedBy)
	}
	disk, virtual = m.sdb.GetMtime(cfg.ID, name)
	if !disk.Equal(raw.ModTime()) || !virtual.Equal(stamp) {
		t.Fatal("flush did not commit mapping")
	}

	// The next sender version is a plain successor: it installs, with no
	// safety-check refusal and no conflict.
	v4 := makeInfo(newContent, 4)
	if v4.InConflictWith(cur) {
		t.Fatal("sender successor is in conflict with sender v1")
	}
	writeFile(t, f.mtimefs, temp, newContent)
	must(t, f.performFinish(v4, cur, true, temp, jobs, scans))
	commitQueuedFinishForTest(t, f, <-jobs)
	if !bytes.Equal(read(name), newContent) || !current(name).Version.Equal(v4.Version) {
		t.Fatal("sender v4 not installed")
	}
	if len(existingConflicts(name, f.mtimefs)) != 0 || len(scans) != 0 {
		t.Fatal("sender v4 created a conflict or a rescan")
	}
}
