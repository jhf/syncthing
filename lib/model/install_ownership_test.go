// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestInstallAfterCommitFinisherOwnership(t *testing.T) {
	const n = 8
	f, fi, data, _ := intentFixture(t, "ownership-fixture")
	before, err := f.GetLastFile()
	must(t, err)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	terminal := errors.New("ownership terminal commit failure")
	d := &growthInstallDB{DB: f.db, fail: terminal, before: func() { close(started); <-release }}
	f.db = d
	jobs := make(chan dbUpdateJob, 8)
	finish := make(chan *sharedPullerState, 8)
	pulls := make(chan pullBlockState, n)
	copies, copyWG := startCopier(t.Context(), f, pulls, finish)
	var finishWG sync.WaitGroup
	for range 8 {
		finishWG.Go(func() { f.finisherRoutine(t.Context(), finish, jobs, make(chan string, n)) })
	}
	updater := make(chan error, 1)
	go func() { _, err := f.dbUpdaterRoutine(t.Context(), jobs); updater <- err }()
	for i := range n {
		file := fi
		file.Name = fmt.Sprintf("ownership-%d", i)
		file.Blocks = append([]protocol.BlockInfo(nil), fi.Blocks...)
		f.queue.Push(file.Name, file.Size, file.ModTime())
		f.queue.Pop()
		writeFile(t, f.mtimefs, fs.TempName(file.Name), data)
		must(t, f.handleFile(t.Context(), file, copies))
	}
	close(copies)
	copyWG.Wait()
	close(finish)
	finishersDone := make(chan struct{})
	go func() { finishWG.Wait(); close(finishersDone) }()
	<-started
	select {
	case <-finishersDone:
	case <-time.After(5 * time.Second):
		unblock()
		<-finishersDone
		close(jobs)
		<-updater
		t.Fatal("finishers waited for durable publication")
	}
	// Prepared work is still in progress, not reported as success or failure.
	if f.queue.lenProgress() != n {
		t.Errorf("queue completed before commit: %d", f.queue.lenProgress())
	}
	if len(f.tempPullErrors) != 0 {
		t.Error("error reported before commit returned")
	}
	after, err := f.GetLastFile()
	must(t, err)
	if after != before {
		t.Error("ReceivedFile before durable publication")
	}
	close(jobs)
	unblock()
	if err := <-updater; !errors.Is(err, terminal) {
		t.Fatalf("updater: %v", err)
	}
	if f.queue.lenProgress() != 0 || f.model.progressEmitter.lenRegistry() != 0 {
		t.Error("committer did not finish queue/progress ownership")
	}
	if len(f.tempPullErrors) != n {
		t.Fatalf("per-file errors=%d want=%d", len(f.tempPullErrors), n)
	}
	after, err = f.GetLastFile()
	must(t, err)
	if after != before {
		t.Error("failed commit emitted ReceivedFile")
	}
	for i := range n {
		name := fmt.Sprintf("ownership-%d", i)
		if _, err := f.mtimefs.Lstat(name); !fs.IsNotExist(err) {
			t.Errorf("failed commit published %s", name)
		}
		if got := readInstallBytes(t, f, fs.TempName(name)); string(got) != string(data) {
			t.Errorf("lost temp %s", name)
		}
	}
	t.Logf("finishers returned before commit; committer drained %d per-file errors and queue/progress entries", n)
}

func TestInstallAfterCommitDirectoryFencesDeduplicated(t *testing.T) {
	// Per-item mode: this pins one directory sync per distinct directory per
	// fence. Barrier mode (patch/74; Linux and now macOS) is covered by the
	// install_syncfs and install_barrier tests.
	t.Setenv("STSYNCMODE", "fsync")
	f, fi, data, _ := physicalInstallFixture(t, "one/fixture")
	measured := &installBenchFS{Filesystem: f.mtimefs, flushed: make(map[string]time.Time), visible: make(chan string, 6)}
	f.mtimefs = measured
	must(t, measured.MkdirAll("two", 0o755))
	jobs := make(chan dbUpdateJob, 6)
	for _, dir := range []string{"one", "two"} {
		for i := range 3 {
			file := fi
			file.Name = fmt.Sprintf("%s/file-%d", dir, i)
			temp := fs.TempName(file.Name)
			writeFile(t, measured, temp, data)
			must(t, f.performFinish(file, protocol.FileInfo{}, false, temp, jobs, make(chan string, 1)))
		}
	}
	close(jobs)
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	must(t, err)
	if measured.dirSync != 4 {
		t.Fatalf("directory syncs=%d want=4 (one precommit and one postrename per directory)", measured.dirSync)
	}
	t.Logf("files=6 directories=2 groups=1 directorySyncs=%d", measured.dirSync)
}
