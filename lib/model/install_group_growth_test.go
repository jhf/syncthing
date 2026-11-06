// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

type growthInstallDB struct {
	db.DB
	active, maximum atomic.Int32
	sizes           []int // read only after the updater joins
	before          func()
	fail            error
}

func (d *growthInstallDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
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
		d.sizes = append(d.sizes, len(files))
		if d.before != nil && len(d.sizes) == 1 {
			d.before()
		}
		time.Sleep(200 * time.Millisecond)
		defer d.active.Add(-1)
		if d.fail != nil {
			return d.fail
		}
	}
	return d.DB.Update(folder, device, files, opts...)
}

// Exercise the real copier and eight finishers, not direct performFinish calls.
// In particular the DB channel is only eight deep, as in pullerIteration. Slow
// commits must allow the collector's bounded next group to grow beyond that.
func TestInstallAfterCommitSlowFlushGroupGrowth(t *testing.T) {
	const n = 200
	f, fi, data, _ := intentFixture(t, "growth-fixture")
	f.Copiers = 8
	d := &growthInstallDB{DB: f.db}
	f.db = d
	jobs := make(chan dbUpdateJob, 8)
	finish := make(chan *sharedPullerState, 8)
	pulls := make(chan pullBlockState, n)
	copies, copyWG := startCopier(t.Context(), f, pulls, finish)
	scans := make(chan string, n)
	var finishWG sync.WaitGroup
	for range 8 {
		finishWG.Go(func() { f.finisherRoutine(t.Context(), finish, jobs, scans) })
	}
	updater := make(chan error, 1)
	go func() { _, err := f.dbUpdaterRoutine(t.Context(), jobs); updater <- err }()
	start := time.Now()
	for i := range n {
		file := fi
		file.Name = fmt.Sprintf("growth-%03d", i)
		file.Blocks = append([]protocol.BlockInfo(nil), fi.Blocks...)
		writeFile(t, f.mtimefs, fs.TempName(file.Name), data)
		must(t, f.handleFile(t.Context(), file, copies))
	}
	close(copies)
	copyWG.Wait()
	close(finish)
	finishWG.Wait()
	close(jobs)
	must(t, <-updater)
	elapsed := time.Since(start)
	if len(pulls) != 0 {
		t.Fatalf("unexpected downloads: %d", len(pulls))
	}
	maxGroup, total := 0, 0
	for _, size := range d.sizes {
		total += size
		if size > maxGroup {
			maxGroup = size
		}
	}
	t.Logf("slowFlush=200ms files=%d finishers=8 flushes=%d maxInFlight=%d maxGroup=%d groups=%v elapsed=%v", n, len(d.sizes), d.maximum.Load(), maxGroup, d.sizes, elapsed)
	if total != n || maxGroup <= 8 || len(d.sizes) >= n/8 {
		t.Fatalf("slow flush did not grow groups beyond finishers: total=%d maxGroup=%d flushes=%d", total, maxGroup, len(d.sizes))
	}
	if d.maximum.Load() != 1 || d.active.Load() != 0 || len(d.sizes) > int(elapsed/(200*time.Millisecond))+1 {
		t.Fatal("flush concurrency/rate invariant violated")
	}
	for i := range n {
		name := fmt.Sprintf("growth-%03d", i)
		indexed, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, name)
		must(t, err)
		if !ok || indexed.LocalFlags&protocol.FlagLocalInstallPending != 0 {
			t.Fatalf("incomplete install: %s", name)
		}
		if got := readInstallBytes(t, f, name); string(got) != string(data) {
			t.Fatalf("wrong bytes: %s", name)
		}
	}
	if len(f.tempPullErrors) != 0 {
		t.Fatalf("pull errors: %v", f.tempPullErrors)
	}
}
