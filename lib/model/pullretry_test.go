// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"fmt"
	"github.com/syncthing/syncthing/lib/protocol"
	"strings"
	"sync"
	"testing"
	"time"
)

func retryFixture(t *testing.T) (*pullRetryTracker, *time.Time) {
	t.Helper()
	now := time.Unix(1000, 0)
	r := newPullRetryTracker(func() time.Duration { return time.Minute }, newChurnTracker(now))
	r.budget = new(pullRetryBudget)
	r.now = func() time.Time { return now }
	t.Cleanup(r.clearAll)
	r.begin()
	r.beginWalk()
	return r, &now
}
func TestPullRetryFixedDeadline(t *testing.T) {
	r, now := retryFixture(t)
	r.admit("a/file")
	r.failure("a/file", "remote_no_such_file", "block_request", "real error")
	deadline := now.Add(time.Minute)
	for range 10 {
		*now = now.Add(time.Second)
		if !r.gate("a/file") || r.gate("a/sibling") {
			t.Fatal("gate must be exact path")
		}
		if !r.entries["a/file"].nextEligible.Equal(deadline) {
			t.Fatal("encounter extended deadline")
		}
	}
	r.finishWalk(true)
	s, errs := r.publish()
	if s.deferredNeeded != 1 || s.attempted != 1 || s.actualFailures != 1 || errs["a/file"].text != "real error" {
		t.Fatalf("status=%+v errors=%v", s, errs)
	}
	r.begin()
	r.beginWalk()
	r.gate("a/file")
	r.finishWalk(true)
	s, _ = r.publish()
	if !s.pureDeferral() {
		t.Fatalf("not pure deferral: %+v", s)
	}
	*now = deadline
	if r.gate("a/file") {
		t.Fatal("deadline did not admit")
	}
	r.admit("a/file")
	r.failure("a/file", "local_modified", "finalize", "changed")
	if !r.entries["a/file"].nextEligible.Equal(deadline.Add(time.Minute)) {
		t.Fatal("new failure must start one fixed interval")
	}
	r.clear("a/file", true)
	if len(r.entries) != 0 || r.budget.paths.Load() != 0 || r.budget.bytes.Load() != 0 {
		t.Fatal("success leaked state")
	}
}
func TestPullRetryEligibilityAndPruning(t *testing.T) {
	for _, tc := range []struct {
		class, stage string
		eligible     bool
	}{
		{"remote_no_such_file", "block_request", true}, {"remote_no_such_file", "local_copy", false},
		{"local_modified", "shortcut_check", true}, {"local_modified", "finalize", true}, {"local_modified", "local_copy", false},
		{"no_source", "block_request", false}, {"local_io", "finalize", false}, {"other", "finalize", false},
	} {
		t.Run(tc.class+tc.stage, func(t *testing.T) {
			r, _ := retryFixture(t)
			r.failure("d/f", tc.class, tc.stage, "failure")
			if (len(r.entries) == 1) != tc.eligible {
				t.Fatal("wrong eligibility")
			}
		})
	}
	r, _ := retryFixture(t)
	r.failure("seen", "local_modified", "shortcut_check", "one")
	r.failure("absent", "local_modified", "shortcut_check", "two")
	r.beginWalk()
	r.gate("seen")
	r.finishWalk(false)
	if len(r.entries) != 2 {
		t.Fatal("incomplete walk pruned")
	}
	r.beginWalk()
	r.gate("seen")
	r.finishWalk(true)
	if len(r.entries) != 1 {
		t.Fatal("complete walk did not prune")
	}
	r.failure("seen", "local_io", "finalize", "replacement")
	if len(r.entries) != 0 {
		t.Fatal("noneligible replacement retained deadline")
	}
	for _, name := range []string{"", ".", "..", "../file", "/absolute", "a/../b", "a//b"} {
		r.failure(name, "local_modified", "finalize", "bad")
		if len(r.entries) != 0 {
			t.Fatalf("retained invalid path %q", name)
		}
	}
}

func TestPullRetryTypedAttemptOrigin(t *testing.T) {
	r, _ := retryFixture(t)
	id := r.admit("file")
	err := fmt.Errorf("finishing: %w", retryAttemptError(churnAt("block_request", protocol.ErrNoSuchFile), id))
	if !errors.Is(err, protocol.ErrNoSuchFile) || churnStage(err, "finalize") != "block_request" || retryAttemptID(err) != id {
		t.Fatal("attempt wrapper lost error origin")
	}
	r.failure("file", churnClass(err), churnStage(err, "finalize"), err.Error(), retryAttemptID(err))
	if r.entries["file"].attempt != id {
		t.Fatal("terminal attempt ID lost")
	}
	if next := r.admit("sibling"); next <= id {
		t.Fatal("attempt IDs not unique and monotonic")
	}
}
func TestPullRetryCapacityFailOpen(t *testing.T) {
	r, _ := retryFixture(t)
	for i := range pullRetryPaths + 100 {
		r.failure(fmt.Sprint(i), "remote_no_such_file", "block_request", strings.Repeat("e", 1000))
	}
	if len(r.entries) != pullRetryPaths || r.gate(fmt.Sprint(pullRetryPaths+1)) {
		t.Fatal("path budget not fail-open")
	}
	if r.budget.paths.Load() != pullRetryPaths {
		t.Fatal("process accounting")
	}
	if r.churn.total.values[churnIndex("queue.state_capacity_bypass")] != 100 {
		t.Fatal("capacity count")
	}
	for _, e := range r.entries {
		if len(e.message) > churnStringBytes {
			t.Fatal("unbounded diagnostic")
		}
	}
	r.clearAll()
	r.budget.bytes.Store(pullRetryProcessBytes)
	r.failure("new", "local_modified", "finalize", "x")
	if len(r.entries) != 0 || r.budget.paths.Load() != 0 {
		t.Fatal("process byte capacity must roll back path reservation")
	}
	r.budget.bytes.Store(0)
	r.failure(strings.Repeat("x", pullRetryBytes+1), "local_modified", "finalize", "x")
	if len(r.entries) != 0 {
		t.Fatal("folder byte budget")
	}
}
func TestPullRetryConcurrentReports(t *testing.T) {
	r, _ := retryFixture(t)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			name := fmt.Sprintf("dir/%d", i)
			for range 100 {
				r.admit(name)
				r.failure(name, "remote_no_such_file", "block_request", "missing")
				r.gate(name)
			}
		})
	}
	wg.Wait()
	r.finishWalk(true)
	s, _ := r.publish()
	if s.attempted != 2000 || s.actualFailures != 2000 || s.deferredNeeded != 20 {
		t.Fatalf("lost outcomes %+v", s)
	}
}

func TestPullRetryExpiryDuringWalkStaysPending(t *testing.T) {
	r, now := retryFixture(t)
	r.failure("held", "remote_no_such_file", "block_request", "missing")
	r.begin()
	r.beginWalk()
	if !r.gate("held") {
		t.Fatal("not held")
	}
	*now = now.Add(time.Minute)
	r.finishWalk(true)
	s, messages := r.publish()
	if s.deferredNeeded != 1 || len(messages) != 1 {
		t.Fatal("expired held file falsely disappeared from pending status")
	}
	if again, _ := r.publish(); again.deferredNeeded != 1 {
		t.Fatal("publication not idempotent")
	}
	r.otherFailure()
	if r.snapshot().pureDeferral() {
		t.Fatal("unrelated failure incorrectly suppressed backoff")
	}
}
func TestPullRetryHeldQueueMembership(t *testing.T) {
	q := newJobQueue()
	q.HoldDuringBuild("tree/first")
	q.Push("tree/second", 1, time.Time{})
	name, ok := q.Pop()
	if !ok {
		t.Fatal("pop")
	}
	q.HoldInProgress(name)
	q.Done(name)
	q.Done("tree/first")
	if !q.ContainsPathUnder("tree") || q.ContainsPathUnder("tre") || q.ContainsPathUnder("tree/first") {
		t.Fatal("component membership")
	}
	progress, pending, _ := q.Jobs(1, 10)
	if len(progress) != 0 || strings.Join(pending, ",") != "tree/first,tree/second" {
		t.Fatalf("jobs %v %v", progress, pending)
	}
	_, page, skipped := q.Jobs(2, 1)
	if len(page) != 1 || page[0] != "tree/second" || skipped != 1 {
		t.Fatal("held pagination")
	}
	q.HoldDuringBuild("tree/first")
	if len(q.held) != 2 {
		t.Fatal("duplicate held")
	}
	q.Reset()
	if q.ContainsPathUnder("tree") {
		t.Fatal("reset")
	}
}

func BenchmarkPullRetryCapacity(b *testing.B) {
	now := time.Unix(1000, 0)
	b.ReportAllocs()
	for b.Loop() {
		r := newPullRetryTracker(func() time.Duration { return time.Minute }, nil)
		r.now = func() time.Time { return now }
		r.budget = new(pullRetryBudget)
		for i := range pullRetryPaths {
			r.failure(fmt.Sprintf("parent/child-%04d", i), "remote_no_such_file", "block_request", "saved terminal failure")
		}
		b.ReportMetric(float64(r.bytes), "retained_bytes")
		r.clearAll()
	}
}
