// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

type retryObserver struct {
	version                 uint64
	queries, closed, failAt int
}

func (o *retryObserver) DataVersion(context.Context) (uint64, error) {
	o.queries++
	if o.queries == o.failAt {
		return 0, errors.New("observer read failed")
	}
	return o.version, nil
}
func (o *retryObserver) Close() error { o.closed++; return nil }

type observedRetryDB struct {
	db.DB
	observer               *retryObserver
	openErr                error
	opens, failures, calls int
	first                  *protocol.FileInfo
	changedBatch           bool
	bounded                bool
}

func (d *observedRetryDB) OpenDataVersionObserver(ctx context.Context, _ string) (db.DataVersionObserver, error) {
	d.opens++
	deadline, ok := ctx.Deadline()
	d.bounded = ok && time.Until(deadline) <= time.Second
	if d.openErr != nil {
		return nil, d.openErr
	}
	return d.observer, nil
}

func (d *observedRetryDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	d.calls++
	if d.first == nil {
		d.first = &files[0]
	} else if d.first != &files[0] {
		d.changedBatch = true
	}
	if d.failures != 0 {
		d.failures--
		return errors.New("SQLITE_BUSY: database is locked")
	}
	return d.DB.Update(folder, device, files, opts...)
}

func TestPullDBWaitWarnings(t *testing.T) {
	for _, mode := range []string{"moving", "unchanged", "read_error", "open_error"} {
		t.Run(mode, func(t *testing.T) {
			f, _, _, jobs := intentFixture(t, ".hermes/gateway_state.json")
			f.churn = churnTestTracker()
			var logs bytes.Buffer
			f.sl = slog.New(slog.NewTextHandler(&logs, nil))
			o := &retryObserver{version: 1}
			d := &observedRetryDB{DB: f.db, observer: o, failures: 25}
			if mode == "read_error" {
				o.failAt = 2
			}
			if mode == "open_error" {
				d.openErr = errors.New("observer pool exhausted")
			}
			f.db = d
			now := time.Unix(0, 0)
			f.pullDBRetryPolicy = &pullDBRetryPolicy{initial: 30 * time.Second, maximum: 30 * time.Second, now: func() time.Time { return now }, wait: func(ctx context.Context, delay time.Duration) error {
				now = now.Add(delay)
				if mode == "moving" {
					o.version++
				}
				return ctx.Err()
			}}
			close(jobs)
			_, err := f.dbUpdaterRoutine(t.Context(), jobs)
			must(t, err)
			wantMoved := "false"
			if mode == "moving" {
				wantMoved = "true"
			}
			if mode == "read_error" || mode == "open_error" {
				wantMoved = "unknown"
			}
			text := logs.String()
			if strings.Count(text, "Pull database update still waiting") != 2 || strings.Count(text, "data_version_moved="+wantMoved) != 2 || !strings.Contains(text, "elapsed=5m0s attempts=11") || !strings.Contains(text, "elapsed=10m0s attempts=21") || !strings.Contains(text, "level=WARN") {
				t.Fatalf("incorrect rate-limited warning/heartbeat: %s", text)
			}
			if d.opens != 1 || !d.bounded || d.calls != 26 || d.changedBatch {
				t.Fatalf("opens=%d bounded=%v attempts=%d changedBatch=%v", d.opens, d.bounded, d.calls, d.changedBatch)
			}
			if mode != "open_error" && (o.queries != 3 || o.closed != 1) {
				t.Fatalf("observer queries=%d closes=%d", o.queries, o.closed)
			}
			if got := f.churn.total.values[churnIndex("db_update.lock_wait_warning")]; got != 2 {
				t.Fatalf("warning churn=%d", got)
			}
		})
	}
}

func TestPullDBCancelWait(t *testing.T) {
	f, fi, _, jobs := intentFixture(t, "ssb/jsonb_stats/src/lib.rs")
	original := f.db
	f.db = &patch58BusyDB{DB: original, fail: errors.New("database is locked"), failures: -1}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := make(chan struct{})
	f.pullDBRetryPolicy = &pullDBRetryPolicy{wait: func(ctx context.Context, _ time.Duration) error { close(waiting); <-ctx.Done(); return ctx.Err() }}
	close(jobs)
	done := make(chan error, 1)
	go func() { _, err := f.dbUpdaterRoutine(ctx, jobs); done <- err }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("never entered retry wait")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry ignored cancellation")
	}
	_, ok, err := original.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if ok {
		t.Fatal("cancelled batch reported a commit")
	}
}

var retryStateSink pullDBRetryState

func TestPullDBCancelDrainsProducers(t *testing.T) {
	f, fi, _, _ := intentFixture(t, ".hermes/gateway_state.json")
	original := f.db
	f.db = &patch58BusyDB{DB: original, fail: errors.New("SQLITE_BUSY: database is locked"), failures: -1}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := make(chan struct{})
	f.pullDBRetryPolicy = &pullDBRetryPolicy{wait: func(ctx context.Context, _ time.Duration) error { close(waiting); <-ctx.Done(); return ctx.Err() }}
	jobs := make(chan dbUpdateJob)
	type result struct {
		changed int
		err     error
	}
	done := make(chan result, 1)
	go func() { n, err := f.dbUpdaterRoutine(ctx, jobs); done <- result{n, err} }()
	produced := make(chan struct{})
	go func() {
		defer close(produced)
		defer close(jobs)
		for range dbUpdateBatchSizeFiles + 10 {
			jobs <- dbUpdateJob{fi, dbUpdateHandleFile}
		}
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("full batch never entered wait")
	}
	cancel()
	select {
	case <-produced:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation stranded producers")
	}
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) || got.changed != dbUpdateBatchSizeFiles {
			t.Fatalf("cancelled drain: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drained updater did not exit")
	}
}

// Shared real-finisher fixture, independent of any journal or recovery format.
func intentFixture(t *testing.T, name string) (*sendReceiveFolder, protocol.FileInfo, []byte, chan dbUpdateJob) {
	t.Helper()
	t.Setenv("STDRIPMODE", "0")
	t.Setenv("STMETABATCH", "0")
	_, f := setupSendReceiveFolder(t)
	must(t, f.mtimefs.MkdirAll(path.Dir(name), 0o755))
	data := []byte("sender install which must survive the lost updater\n")
	blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(data), protocol.MinBlockSize, int64(len(data)), nil)
	must(t, err)
	fi := protocol.FileInfo{Name: name, Type: protocol.FileInfoTypeFile, Size: int64(len(data)), Permissions: 0o640,
		ModifiedS: 1793916000, ModifiedBy: device1.Short(), Version: protocol.Vector{}.Update(device1.Short()),
		Blocks: blocks, BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize}
	jobs := make(chan dbUpdateJob, 1)
	temp := fs.TempName(name)
	writeFile(t, f.mtimefs, temp, data)
	must(t, f.performFinish(fi, protocol.FileInfo{}, false, temp, jobs, make(chan string, 1)))
	return f, fi, data, jobs
}

func TestPullDBRetryStateConstant(t *testing.T) {
	for _, n := range []int{10, 1000000} {
		allocs := testing.AllocsPerRun(10, func() {
			s := pullDBRetryState{delay: 250 * time.Millisecond, maximum: 30 * time.Second}
			for range n {
				s.nextDelay()
				s.attempted()
				s.heartbeat(1, nil)
			}
			retryStateSink = s
		})
		if allocs != 0 {
			t.Fatalf("%d retries allocated %v times", n, allocs)
		}
		if retryStateSink.delay != 30*time.Second || retryStateSink.attempts != uint64(n) {
			t.Fatalf("retry state after %d: %+v", n, retryStateSink)
		}
	}
	s := pullDBRetryState{delay: 30 * time.Second, maximum: 30 * time.Second, attempts: ^uint64(0)}
	s.attempted()
	if s.attempts != ^uint64(0) || s.nextDelay() != 30*time.Second {
		t.Fatal("retry state wrapped instead of saturating")
	}
}

func TestPullDBNeverDropsHeldBatch(t *testing.T) {
	shrinkLockBackoff(t)
	f, fi, _, jobs := intentFixture(t, ".hermes/gateway_state.json")
	f.pullDBRetryPolicy = &pullDBRetryPolicy{wait: func(context.Context, time.Duration) error { return nil }}
	original := f.db
	busy := &patch58BusyDB{DB: original, fail: errors.New("SQLITE_BUSY: database is locked"), failures: len(indexLockBackoff) + 3}
	f.db = busy
	close(jobs)
	changed, err := f.dbUpdaterRoutine(t.Context(), jobs)
	if err != nil {
		t.Fatalf("completed pull dropped after %d attempts: %v", busy.calls, err)
	}
	if changed != 1 || busy.calls != len(indexLockBackoff)+4 {
		t.Fatalf("changed=%d attempts=%d", changed, busy.calls)
	}
	got, ok, err := original.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if !ok || !got.Version.Equal(fi.Version) || got.Version.Counter(f.shortID) != 0 {
		t.Fatalf("same sender batch not retained: %+v", got)
	}
	must(t, f.scanSubdirs(t.Context(), []string{fi.Name}))
	got, _, err = original.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if !got.Version.Equal(fi.Version) || got.Version.Counter(f.shortID) != 0 {
		t.Fatal("receiver counter after prolonged lock")
	}
}
