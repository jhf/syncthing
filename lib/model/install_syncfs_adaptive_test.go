// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"sync/atomic"
	"testing"
	"time"
)

func fillCost(c *installSyncCost, barrier, perItem time.Duration) {
	for range installCostSamples {
		c.observeBarrier(barrier)
		c.observeItems(perItem, 1)
	}
}

// Warm-up: no fixed group size. Until both rings hold five samples the
// choice alternates by the golden ratio, so both methods get measured
// within the first ~10 fences, whatever the group size. patch/71 instead
// fell back to "syncfs iff n >= 8" whenever it lacked fresh data.
func TestInstallSyncCostWarmupAlternates(t *testing.T) {
	var c installSyncCost
	var barrier, perItem int
	seq := ""
	for k := 0; k < 10; k++ {
		switch c.choose(8) {
		case fenceBarrier:
			barrier++
			seq += "B"
			c.observeBarrier(100 * time.Millisecond)
		default:
			perItem++
			seq += "P"
			c.observeItems(10*time.Millisecond, 3)
		}
	}
	t.Logf("first 10 fences of n=8: %s", seq)
	if barrier < 3 || perItem < 3 {
		t.Fatalf("warm-up must exercise both methods, got %s", seq)
	}
	if c.choose(1) != fencePerItem {
		t.Fatal("a group of one must always fsync")
	}
	// The same sequence for a small group: patch/71 never tried syncfs for
	// n < 8 until it had data, and had no data because it never tried.
	var d installSyncCost
	got := 0
	for k := 0; k < 10; k++ {
		if d.choose(2) == fenceBarrier {
			got++
			d.observeBarrier(5 * time.Millisecond)
		} else {
			d.observeItems(5*time.Millisecond, 2)
		}
	}
	if got == 0 {
		t.Fatal("warm-up never measured the barrier on small groups")
	}
}

// Steady state: the medians decide, using yarlung's measured costs
// (fsync 25.8 ms, syncfs 131.0 ms, break-even 5.08), and one outlier
// does not move the decision.
func TestInstallSyncCostMedianDecides(t *testing.T) {
	var c installSyncCost
	fillCost(&c, 131*time.Millisecond, 25800*time.Microsecond)
	c.observeBarrier(1192 * time.Millisecond) // the first-syncfs outlier
	for n, want := range map[int]fenceMethod{2: fencePerItem, 4: fencePerItem, 5: fencePerItem, 6: fenceBarrier, 8: fenceBarrier, 32: fenceBarrier} {
		c.fences = 0 // keep the refresh slot out of this table
		if got := c.choose(n); got != want {
			t.Errorf("n=%d: got %v want %v", n, got, want)
		}
	}
	// Mac-like costs (barrier no dearer than one item): barrier from n=2.
	var m installSyncCost
	fillCost(&m, 5600*time.Microsecond, 6100*time.Microsecond)
	if got := m.choose(2); got != fenceBarrier {
		t.Errorf("fast disk n=2: got %v want barrier", got)
	}
}

// Refresh: every 17th fence measures the losing method, and a per-item
// refresh on a large group fsyncs only a few items before the barrier.
// When load shifts, the decision follows within a few refreshes.
func TestInstallSyncCostRefreshFollowsLoad(t *testing.T) {
	var c installSyncCost
	fillCost(&c, 131*time.Millisecond, 25800*time.Microsecond)
	var sampled, barriers int
	for k := 1; k <= 34; k++ {
		switch c.choose(32) {
		case fenceSampleBarrier:
			sampled++
		case fenceBarrier:
			barriers++
		default:
			t.Fatalf("fence %d: per-item on a 32-item group is never right here", k)
		}
	}
	if sampled != 2 || barriers != 32 {
		t.Fatalf("over 34 fences: sampled=%d barrier=%d, want 2 refreshes on fences 17 and 34", sampled, barriers)
	}
	// Load arrives: syncfs now 2.6 s (overnight). n=8 must switch to
	// per-item once three of five barrier samples show it.
	for range 3 {
		c.observeBarrier(2600 * time.Millisecond)
	}
	c.fences = 0
	if got := c.choose(8); got != fencePerItem {
		t.Fatalf("after load: n=8 got %v, want per-item (8 x 25.8 ms < 2.6 s)", got)
	}
}

// Through the real copier, finishers and updater: with costs measured slow
// for syncfs, a 200-file stream must fsync per file and never pay syncfs,
// apart from at most the scheduled refreshes.
func TestInstallAfterCommitAdaptsToSlowSyncfs(t *testing.T) {
	const n = 200
	var calls atomic.Int64
	_, counter, _ := runSyncfsInstallGroupPrimed(t, n, func(f *sendReceiveFolder) {
		fillCost(&f.installCost.pre, 5*time.Second, time.Millisecond)
		fillCost(&f.installCost.post, 5*time.Second, time.Millisecond)
	}, func(string) error { calls.Add(1); time.Sleep(time.Millisecond); return nil })
	t.Logf("files=%d syncfs=%d perFileSyncs=%d", n, calls.Load(), counter.syncs.Load())
	if c := calls.Load(); c > 2 {
		t.Fatalf("syncfs measured at 5 s must only run as a scheduled refresh, got %d calls", c)
	}
	if got := counter.syncs.Load(); got < n {
		t.Fatalf("expected per-file fsyncs for every temp (%d), got %d", n, got)
	}
}

// The break-even log line fires once both rings are full and again only
// when the break-even moves by 2x or more.
func TestInstallSyncCostLogsOnMoveOnly(t *testing.T) {
	var lines []int
	c := installSyncCost{logf: func(be int, _, _ time.Duration) { lines = append(lines, be) }}
	fillCost(&c, 40*time.Millisecond, 10*time.Millisecond) // break-even 5
	for range 10 {
		c.observeBarrier(40 * time.Millisecond)
		c.observeItems(10*time.Millisecond, 1)
	}
	if len(lines) != 1 || lines[0] != 5 {
		t.Fatalf("steady costs: logged %v, want [5]", lines)
	}
	for range 5 {
		c.observeBarrier(400 * time.Millisecond)
	}
	if len(lines) < 2 || lines[len(lines)-1] < 10 {
		t.Fatalf("a large cost move must be logged: %v", lines)
	}
}

// A due barrier refresh must not land on a group where the barrier costs
// far more than per-item: on yarlung's trickle (2 items, 25.8 ms per item,
// syncfs 131 ms) the refresh waits, and runs on the next larger group.
func TestInstallSyncCostBarrierRefreshWaitsForCheapGroup(t *testing.T) {
	var c installSyncCost
	fillCost(&c, 131*time.Millisecond, 25800*time.Microsecond)
	for k := 1; k <= 40; k++ {
		if got := c.choose(2); got != fencePerItem {
			t.Fatalf("fence %d (n=2): got %v, want per-item; a barrier refresh here adds ~105 ms", k, got)
		}
	}
	if !c.refreshDue {
		t.Fatal("the refresh should still be pending")
	}
	if got := c.choose(4); got != fenceBarrier { // 2*4*25.8 = 206 ms >= 131 ms
		t.Fatalf("n=4: got %v, want the pending barrier refresh", got)
	}
	if c.refreshDue {
		t.Fatal("refresh should be done")
	}
}
