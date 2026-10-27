// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"testing"
	"time"
)

func dirFixture(t *testing.T) (*pullRetryTracker, *time.Time) {
	r, now := retryFixture(t)
	r.dir = newPullDirState()
	r.dir.budget = new(pullDirBudget)
	return r, now
}
func qualifyDir(r *pullRetryTracker, parent string) {
	for i := range 3 {
		name := fmt.Sprintf("%s/f%d", parent, i)
		r.admit(name)
		r.failure(name, "remote_no_such_file", "block_request", "remote missing")
	}
}
func TestPullDirProbePermitsExpireAndLateConsumption(t *testing.T) {
	r, now := dirFixture(t)
	qualifyDir(r, "top/branch")
	r.gate("top/branch/old")
	r.gate("top/branch/young")
	*now = now.Add(10 * time.Second)
	r.beginWalk()
	b := r.dir.buckets["top/branch"]
	if b.probeName != "top/branch/old" {
		t.Fatal("missing initial reservation")
	}
	*now = now.Add(10 * time.Second)
	// Even before the next walk, the old permit is no longer usable.
	if !r.gate("top/branch/old") || b.probeName != "" {
		t.Fatal("old-window permit was carried across rollover")
	}
	r.beginWalk()
	if r.dir.probeUsed != 1 || b.probeName != "top/branch/old" {
		t.Fatal("rollover did not re-reserve exactly one current permit")
	}
	*now = now.Add(9 * time.Second)
	if r.gate("top/branch/old") {
		t.Fatal("current late permit held")
	}
	r.admit("top/branch/old")
	admitted := *now
	*now = now.Add(time.Second)
	r.beginWalk()
	if b.probeName != "" {
		t.Fatal("reservation cadence used reservation time instead of admission time")
	}
	*now = admitted.Add(10 * time.Second)
	r.beginWalk()
	if b.probeName != "top/branch/young" {
		t.Fatal("next oldest not reserved at admission+10s")
	}
}

func TestPullDirExpiredPermitAtMaxHoldIsNotAProbe(t *testing.T) {
	r, now := dirFixture(t)
	qualifyDir(r, "top/branch")
	name := "top/branch/waiter"
	r.gate(name)
	*now = now.Add(pullDirProbe)
	r.beginWalk()
	if r.dir.buckets["top/branch"].probeName != name {
		t.Fatal("no initial reservation")
	}
	// Simulate an unpreemptible chunk without another reservation pass.
	*now = now.Add(pullDirEpoch - pullDirProbe)
	if r.gate(name) {
		t.Fatal("expired epoch/max hold blocked admission")
	}
	r.admit(name)
	var probe, maxHold, stale uint64
	for i, key := range pullDirKeys {
		switch key {
		case "dir.probe_admit":
			probe = r.dir.counts[i]
		case "dir.max_hold_admit":
			maxHold = r.dir.counts[i]
		case "dir.probe_stale":
			stale = r.dir.counts[i]
		}
	}
	if probe != 0 || maxHold != 1 || stale != 1 {
		t.Fatalf("probe=%d max_hold=%d stale=%d", probe, maxHold, stale)
	}
}

func TestPullDirEpochRemovalReleasesCurrentWindowPermit(t *testing.T) {
	r, now := dirFixture(t)
	qualifyDir(r, "top/branch")
	r.gate("top/branch/waiter")
	*now = now.Add(55 * time.Second)
	r.beginWalk()
	if r.dir.probeUsed != 1 {
		t.Fatal("no permit reserved in late folder window")
	}
	*now = now.Add(5 * time.Second)
	r.beginWalk()
	if r.dir.buckets["top/branch"] != nil || r.dir.probeUsed != 0 {
		t.Fatal("epoch removal stranded a current-window reservation")
	}
}

func TestPullDirRolloverFolderReservationCap(t *testing.T) {
	r, now := dirFixture(t)
	for i := range 20 {
		parent := fmt.Sprintf("top/dir%02d", i)
		qualifyDir(r, parent)
		r.gate(parent + "/wait")
	}
	for range 4 {
		*now = now.Add(10 * time.Second)
		r.beginWalk()
		n := 0
		for _, b := range r.dir.buckets {
			if b.probeName != "" {
				n++
				if !b.probeWindow.Equal(r.dir.probeWindow) {
					t.Fatal("old-window reservation survived")
				}
			}
		}
		if n != 8 || r.dir.probeUsed != 8 {
			t.Fatalf("reservation inflation: n=%d used=%d", n, r.dir.probeUsed)
		}
	}
}

func TestPullDirDistinctEvidenceScopeAndEdges(t *testing.T) {
	r, now := dirFixture(t)
	for range 50 {
		r.failure("top/branch/hot", "remote_no_such_file", "block_request", "missing")
	}
	if r.dir.buckets["top/branch"].childN != 1 || !r.dir.buckets["top/branch"].holdUntil.IsZero() {
		t.Fatal("one hot name qualified a directory")
	}
	for i := range 3 {
		r.failure(fmt.Sprintf("top/local/f%d", i), "local_modified", "shortcut_check", "changed")
	}
	if r.dir.buckets["top/local"] != nil {
		t.Fatal("local modifications escalated")
	}
	qualifyDir(r, "top/branch")
	deadline := r.dir.buckets["top/branch"].holdUntil
	for _, name := range []string{"top/branch/fresh", "top/branch/sub/fresh"} {
		if !r.gate(name) {
			t.Fatalf("eligible edge not gated %s", name)
		}
	}
	for _, name := range []string{"top/branch/sub/deeper/fresh", "top/brancher/fresh", "top/sibling/fresh", "top/fresh", "fresh"} {
		if r.gate(name) {
			t.Fatalf("collateral edge or prefix violation %s", name)
		}
	}
	*now = now.Add(5 * time.Second)
	r.failure("top/branch/freshFailure", "remote_no_such_file", "block_request", "missing")
	if !r.dir.buckets["top/branch"].holdUntil.Equal(deadline) {
		t.Fatal("epoch extended")
	}
	qualifyDir(r, "single")
	if r.dir.buckets["single"] != nil || r.dir.buckets["."] != nil {
		t.Fatal("top-level or root bucket retained")
	}
}
func TestPullDirIndependentPromotion(t *testing.T) {
	r, _ := dirFixture(t)
	qualifyDir(r, "top/grand/one")
	grand := r.dir.buckets["top/grand"]
	if grand == nil || !grand.holdUntil.IsZero() {
		t.Fatal("single branch promoted")
	}
	for range 10 {
		r.failure("top/grand/one/f0", "remote_no_such_file", "block_request", "missing")
	}
	if !grand.holdUntil.IsZero() {
		t.Fatal("repeated branch promoted")
	}
	qualifyDir(r, "top/grand/two")
	if grand.holdUntil.IsZero() || grand.qualifiedN != 2 {
		t.Fatal("independent branches did not promote")
	}
	if _, b, depth := r.dir.match("top/grand/other/file", r.now()); b != grand || depth != 2 {
		t.Fatal("grandparent gate missing")
	}
	if r.dir.buckets["top"] != nil {
		t.Fatal("promotion escaped two evidence edges")
	}
}
func TestPullDirOldestProbeAndADeadline(t *testing.T) {
	r, now := dirFixture(t)
	qualifyDir(r, "top/branch")
	if !r.gate("top/branch/z-oldest") || !r.gate("top/branch/a-younger") {
		t.Fatal("fresh siblings not held")
	}
	r.finishWalk(true)
	s, messages := r.publish()
	if s.dirDeferred != 2 || len(messages) != 3 || !s.nextEligible.Equal(now.Add(pullDirProbe)) {
		t.Fatalf("fresh siblings fabricated errors or wrong wake: %+v %v", s, messages)
	}
	*now = now.Add(9 * time.Second)
	r.beginWalk()
	if r.dir.buckets["top/branch"].probeName != "" {
		t.Fatal("probe before qualification+10s")
	}
	*now = now.Add(time.Second)
	r.beginWalk()
	b := r.dir.buckets["top/branch"]
	if b.probeName != "top/branch/z-oldest" {
		t.Fatalf("probe chose sort order, not oldest: %q", b.probeName)
	}
	if r.gate(b.probeName) {
		t.Fatal("reserved probe held")
	}
	r.admit(b.probeName)
	if r.entries["top/branch/z-oldest"] != nil {
		t.Fatal("fresh admitted sibling retained fake error state")
	}
	if !r.gate("top/branch/a-younger") {
		t.Fatal("second probe in same cadence")
	}
	// An A deadline on the waiting path takes precedence over every B probe.
	r.failure("top/branch/a-younger", "remote_no_such_file", "block_request", "new real failure")
	*now = now.Add(10 * time.Second)
	r.beginWalk()
	if b.probeName == "top/branch/a-younger" || !r.gate("top/branch/a-younger") {
		t.Fatal("B probe bypassed A")
	}
}
func TestPullDirFolderProbeCapAndStaleReservation(t *testing.T) {
	r, now := dirFixture(t)
	for i := range 10 {
		parent := fmt.Sprintf("top/d%02d", i)
		qualifyDir(r, parent)
		r.gate(parent + "/waiting")
	}
	*now = now.Add(pullDirProbe)
	r.beginWalk()
	reserved := 0
	for _, b := range r.dir.buckets {
		if b.probeName != "" {
			reserved++
		}
	}
	if reserved != 8 || r.dir.probeUsed != 8 {
		t.Fatalf("folder cap reserved=%d used=%d", reserved, r.dir.probeUsed)
	}
	for _, b := range r.dir.buckets {
		if b.probeName != "" {
			r.clear(b.probeName, false)
			break
		}
	}
	if r.dir.probeUsed != 7 {
		t.Fatal("stale reservation did not release quota")
	}
	r.beginWalk()
	if r.dir.probeUsed != 8 {
		t.Fatal("released reservation did not permit next oldest due directory")
	}
}
func TestPullDirMaxHoldSurvivesOverlapRequalificationAndA(t *testing.T) {
	r, now := dirFixture(t)
	qualifyDir(r, "top/grand/one")
	name := "top/grand/one/waiter"
	if !r.gate(name) {
		t.Fatal("not held")
	}
	first := r.entries[name].firstDirDeferred
	*now = now.Add(30 * time.Second)
	qualifyDir(r, "top/grand/two")
	if !r.entries[name].firstDirDeferred.Equal(first) {
		t.Fatal("overlap reset first deferral")
	}
	r.failure(name, "remote_no_such_file", "block_request", "real failure")
	*now = first.Add(time.Minute)
	r.beginWalk()
	if !r.entries[name].mustAdmit || !r.gate(name) {
		t.Fatal("must-admit latch must survive but not bypass A")
	}
	// Requalify the immediate directory after its first epoch ends.
	qualifyDir(r, "top/grand/one")
	*now = first.Add(90 * time.Second)
	r.beginWalk()
	if r.gate(name) {
		t.Fatal("B requalification defeated max-hold latch")
	}
	r.admit(name)
	if e := r.entries[name]; e != nil && (!e.firstDirDeferred.IsZero() || e.mustAdmit) {
		t.Fatal("admission did not clear B wait state")
	}
}
func TestPullDirCapacityFailOpenAndWindowExpiry(t *testing.T) {
	r, now := dirFixture(t)
	for i := range 300 {
		qualifyDir(r, fmt.Sprintf("top/d%03d", i))
	}
	if len(r.dir.buckets) > pullDirBuckets || r.dir.budget.buckets.Load() > pullDirBuckets {
		t.Fatal("unbounded bucket state")
	}
	qualifyDir(r, "top/overflow")
	if r.gate("top/overflow/fresh") {
		t.Fatal("directory bucket capacity did not fail open")
	}
	// Path capacity is shared with A. A fresh sibling cannot be held without
	// bounded first-deferral bookkeeping even when its directory qualifies.
	qualifyDir(r, "top/d000")
	r.budget.paths.Store(pullRetryProcessPaths)
	if r.gate("top/d000/capacity-bypass") {
		t.Fatal("B held a path without fairness state")
	}
	r.budget.paths.Store(int64(len(r.entries)))
	r.clearAll()
	// Byte exhaustion does not let an unrecorded name qualify a bucket.
	d := newPullDirState()
	d.budget = new(pullDirBudget)
	r.dir = d
	t.Cleanup(func() {
		for name, b := range d.buckets {
			d.remove(name, b)
		}
	})
	d.budget.bytes.Store(pullDirProcessBytes)
	d.failure("top/bytes/file", *now)
	if len(d.buckets) != 0 || d.budget.buckets.Load() != 0 {
		t.Fatal("byte overflow retained or promoted evidence")
	}
	d.budget.bytes.Store(0)
	for range 10 {
		d.failure("top/one/file", *now)
	}
	*now = now.Add(time.Minute)
	d.failure("top/one/second", *now)
	if b := d.buckets["top/one"]; b.childN != 1 || !b.holdUntil.IsZero() {
		t.Fatal("evidence window extended with repeated failures")
	}
}
