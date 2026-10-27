// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestChurnSuccessFailureCancellationReconcile(t *testing.T) {
	c := churnTestTracker()
	c.failureSamples = churnSamples
	f := &sendReceiveFolder{folder: &folder{churn: c}}
	fi := protocol.FileInfo{Name: "file"}
	c.admit(fi.Name)
	c.complete(fi.Name)
	c.admit(fi.Name)
	f.churnFailure(fi.Name, errModified, &fi)
	c.admit(fi.Name)
	f.newPullError(fi.Name, context.Canceled, fi)
	if c.total.values[churnIndex("attempts")] != c.total.values[churnIndex("completed")]+c.total.values[churnIndex("failures")]+c.total.values[churnIndex("cancelled")] {
		t.Fatal("drained attempts do not reconcile")
	}
	if c.total.values[churnIndex("queue.clear_success")] != 1 {
		t.Fatal("invented queue clear")
	}
}

func TestChurnBoundedStringsAndSampleEvictions(t *testing.T) {
	c := churnTestTracker()
	path := strings.Repeat("x", 10000) + "/file"
	for range 1000 {
		c.observe(churnSample{Path: path, Event: "source_reject"}, "source.traversal")
	}
	if len(c.parents) != 0 || c.other.sampleN != churnSamples || c.other.overflow != 1000 || c.other.truncated != churnSamples || c.other.omitted != 996 {
		t.Fatalf("bounds/diagnostics: %+v", c.other)
	}
	for _, s := range c.other.samples {
		if len(s.Path) > churnStringBytes {
			t.Fatal("oversized retained path")
		}
	}
	// Failure observations displace earlier queue evidence, with explicit loss.
	c2 := churnTestTracker()
	for range churnSamples {
		c2.observe(churnSample{Path: "dir/file", Event: "need_revalidation"}, "need.queue_pop.not_checked")
	}
	fi := protocol.FileInfo{Name: "dir/file"}
	d := &churnReadDB{global: fi, exists: true}
	f := &sendReceiveFolder{folder: &folder{churn: c2, db: d}}
	for range churnSamples {
		f.churnFailure(fi.Name, errModified, &fi)
	}
	if c2.total.evicted != churnSamples {
		t.Fatalf("sample evictions=%d", c2.total.evicted)
	}
	for _, s := range c2.total.samples {
		if s.Event != "pull_failure" || s.Outcome != "still_needed" {
			t.Fatal("checked failure sample discarded")
		}
	}
}

func TestChurnConcurrentProducersAndFlush(t *testing.T) {
	c := churnTestTracker()
	h := new(churnTestHandler)
	sl := slog.New(h)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 1000 {
				c.admit("dir/file")
				c.complete("dir/file")
			}
		})
	}
	for range 20 {
		c.flush(time.Now(), sl, c.budget)
	}
	wg.Wait()
	var attempts, completed uint64
	for _, r := range h.records {
		if r.Message == "Pull churn totals" {
			attempts += churnAttr(r, "attempts").Uint64()
			completed += churnAttr(r, "completed").Uint64()
		}
	}
	attempts += c.total.values[churnIndex("attempts")]
	completed += c.total.values[churnIndex("completed")]
	if attempts != 10000 || completed != 10000 {
		t.Fatalf("concurrent totals=%d/%d", attempts, completed)
	}
}

func TestChurnShutdownBudgetLossIsVisible(t *testing.T) {
	c := churnTestTracker()
	now := time.Now()
	c.budget.take(now, churnProcessLines)
	c.count("dir/file", "attempts", 3)
	h := new(churnTestHandler)
	sl := slog.New(h)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.serve(ctx, sl)
	if len(h.records) != 0 || c.budget.shutdownUnemitted.Load() != 1 || c.budget.shutdownCounts[churnIndex("attempts")].Load() != 3 {
		t.Fatal("terminal refused batch was silently lost")
	}
	next := newChurnTracker(now)
	next.budget = c.budget
	next.count("dir/file", "attempts", 1)
	next.flush(now.Add(time.Minute), sl, next.budget)
	totals := h.records[len(h.records)-1]
	if churnAttr(totals, "process_shutdown_unemitted.attempts").Uint64() != 3 {
		t.Fatal("terminal counter diagnostic not emitted")
	}
}

func TestChurnRecoveryRequiresBacklogEvidence(t *testing.T) {
	c := churnTestTracker()
	c.active = true
	if !c.wantsBacklogRead() || c.wantsBacklogRead() {
		t.Fatal("recovery read not bounded")
	}
	c.noteBacklog(1, nil)
	c.complete("dir/file")
	if c.settled {
		t.Fatal("successful file mislabeled an outstanding backlog settled")
	}
	c.noteBacklog(0, nil)
	if !c.settled {
		t.Fatal("real zero-need observation ignored")
	}
}

func TestChurnPartialWindowDoesNotExtrapolate(t *testing.T) {
	c := churnTestTracker()
	c.count("dir/file", "failures", 1)
	h := new(churnTestHandler)
	c.flush(time.Now(), slog.New(h), c.budget)
	if c.active {
		t.Fatal("one failure in a partial shutdown window extrapolated into a storm")
	}
	for _, r := range h.records {
		if r.Message == "Pull churn detected" {
			t.Fatal("partial-window false detection")
		}
	}
}

func TestChurnFinalCloseFailureStage(t *testing.T) {
	for _, saved := range []bool{false, true} {
		s := &sharedPullerState{
			fs:       fs.NewFilesystem(fs.FilesystemTypeBasic, t.TempDir()),
			tempName: "missing-parent/file",
		}
		want := "finalize"
		if saved {
			s.fail(churnAt("block_request", protocol.ErrNoSuchFile))
			want = "block_request"
		}
		closed, err := s.finalClose()
		if !closed || err == nil || churnStage(err, "unknown") != want {
			t.Fatalf("saved=%v closed=%v stage=%s err=%v", saved, closed, churnStage(err, "unknown"), err)
		}
	}
}

func TestChurnQueuePopValidNeedNotChecked(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	f.churn = churnTestTracker()
	f.churn.failureSamples = churnSamples
	fi := protocol.FileInfo{Name: "file", Version: protocol.Vector{}.Update(1)}
	must(t, m.sdb.Update(f.folderID, device1, []protocol.FileInfo{fi}))
	f.queue.Push(fi.Name, 0, time.Now())
	must(t, f.processQueue(t.Context(), nil, nil, make(chan copyBlocksState, 1), make(chan dbUpdateJob, 1), make(chan string, 1)))
	if f.churn.total.values[churnIndex("need.queue_pop.not_checked")] != 1 || f.churn.total.values[churnIndex("need.queue_pop.still_needed")] != 0 {
		t.Fatal("global existence mislabeled full need check")
	}
}
