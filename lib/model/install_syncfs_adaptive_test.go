// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"sync/atomic"
	"testing"
	"time"
)

// Decision table for the measured choice. patch/67's fixed rule chose syncfs
// exactly when n >= 8, whatever the disk was doing.
func TestInstallSyncCostDecision(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	c := &installSyncCost{now: func() time.Time { return now }}

	// No measurements: the fixed starting rule.
	for n, want := range map[int]bool{1: false, 2: false, 7: false, 8: true, 100: true} {
		if got := c.useSyncfs(n); got != want {
			t.Errorf("unmeasured n=%d: got %v want %v", n, got, want)
		}
	}

	// Quiet disk: syncfs 20 ms, fsync 15 ms per item -> syncfs from n=2.
	c.observeSyncfs(20 * time.Millisecond)
	c.observeItems(45*time.Millisecond, 3)
	for n, want := range map[int]bool{1: false, 2: true, 7: true} {
		if got := c.useSyncfs(n); got != want {
			t.Errorf("quiet disk n=%d: got %v want %v", n, got, want)
		}
	}

	// Busy disk: syncfs readings of 2 s pull the average up; with fsync at
	// 15 ms per item, groups of 8..~130 now fsync per item.
	for range 20 {
		c.observeSyncfs(2 * time.Second)
	}
	for n, want := range map[int]bool{2: false, 8: false, 64: false, 200: true} {
		if got := c.useSyncfs(n); got != want {
			t.Errorf("busy disk n=%d: got %v want %v (syncfs=%v perItem=%v)", n, got, want, c.syncfs, c.perItem)
		}
	}

	// Stale measurements fall back to the fixed rule, which re-measures.
	now = now.Add(installCostWindow + time.Second)
	if got := c.useSyncfs(8); !got {
		t.Error("stale estimate must fall back to the fixed rule (n=8 -> syncfs)")
	}
	if got := c.useSyncfs(7); got {
		t.Error("stale estimate must fall back to the fixed rule (n=7 -> fsync)")
	}
}

// The break-even log line fires on first measurement and on factor-two moves
// only, not per group.
func TestInstallSyncCostLogsOnMoveOnly(t *testing.T) {
	var lines []int
	c := &installSyncCost{logf: func(be int, _, _ time.Duration) { lines = append(lines, be) }}
	c.observeItems(10*time.Millisecond, 1)
	c.observeSyncfs(40 * time.Millisecond) // break-even 5
	for range 10 {
		c.observeSyncfs(40 * time.Millisecond)
		c.observeItems(10*time.Millisecond, 1)
	}
	if len(lines) != 1 || lines[0] != 5 {
		t.Fatalf("steady costs: logged %v, want [5]", lines)
	}
	for range 30 {
		c.observeSyncfs(400 * time.Millisecond)
	}
	if len(lines) < 2 || lines[len(lines)-1] < 10 {
		t.Fatalf("a large cost move must be logged: %v", lines)
	}
}

// Through the real copier, finishers and updater: when syncfs is measured
// slow (as on a busy shared disk), groups of 8 or more must stop paying it
// and fsync per item, where patch/67 always took syncfs at n >= 8.
func TestInstallAfterCommitAdaptsToSlowSyncfs(t *testing.T) {
	const n = 200
	var calls atomic.Int64
	f, counter, _ := runSyncfsInstallGroupPrimed(t, n, func(f *sendReceiveFolder) {
		// Prime both fences with recent measurements: syncfs 5 s, fsync
		// 1 ms per item. Break-even is ~5000 items, above any group here.
		for _, c := range []*installSyncCost{&f.installCost.pre, &f.installCost.post} {
			c.observeSyncfs(5 * time.Second)
			c.observeItems(time.Millisecond, 1)
		}
	}, func(string) error { calls.Add(1); time.Sleep(time.Millisecond); return nil })
	_ = f
	t.Logf("files=%d syncfs=%d perFileSyncs=%d", n, calls.Load(), counter.syncs.Load())
	if c := calls.Load(); c != 0 {
		t.Fatalf("syncfs measured at 5 s must not be chosen over 1 ms fsyncs, got %d syncfs calls", c)
	}
	if got := counter.syncs.Load(); got < n {
		t.Fatalf("expected per-file fsyncs for every temp (%d), got %d", n, got)
	}
}
