// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package model

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// bootDiffFakeHelper answers the boot replay sequence; its diff-baseline
// takes diffDelay (a disk-bound send) and rmsnap after an abandoned diff
// reports the kill-to-exit lag the real helper measures.
type bootDiffFakeHelper struct {
	mu        sync.Mutex
	calls     []string
	diffDelay time.Duration
	diffOpen  bool // a diff request is still being served
}

func startBootDiffFakeHelper(t *testing.T, diffDelay time.Duration) *bootDiffFakeHelper {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "helper.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	h := &bootDiffFakeHelper{diffDelay: diffDelay}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go h.serve(conn, done)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
	})
	t.Setenv("SYNCTHING_BTRFS_SOCK", sock)
	return h
}

func (h *bootDiffFakeHelper) serve(conn net.Conn, done chan struct{}) {
	defer conn.Close()
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	req := strings.TrimSpace(string(buf[:n]))
	h.mu.Lock()
	h.calls = append(h.calls, req)
	h.mu.Unlock()
	out, code := "", 0
	switch {
	case req == "status":
		out = "baseline=/x/baseline"
	case req == "subvol":
		out = "subvol=/"
	case strings.HasPrefix(req, "snap "):
		out = "snap=/x/" + strings.TrimPrefix(req, "snap ")
	case strings.HasPrefix(req, "diff-baseline "):
		h.mu.Lock()
		h.diffOpen = true
		h.mu.Unlock()
		select {
		case <-time.After(h.diffDelay):
		case <-done:
		}
		h.mu.Lock()
		h.diffOpen = false
		h.mu.Unlock()
		out = "At subvol x\n"
	case strings.HasPrefix(req, "rmsnap "):
		h.mu.Lock()
		if h.diffOpen {
			out = "cancelled-send-exit=1.500\n"
		}
		h.mu.Unlock()
	default:
		code = 2
	}
	fmt.Fprintf(conn, "%s\x00status=%d", out, code)
}

func (h *bootDiffFakeHelper) got(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

// armableBootFolder is a folder that passes every boot-replay gate up to
// the diff.
func armableBootFolder(t *testing.T, h *bootDiffFakeHelper) *folder {
	t.Helper()
	// The real diff statfs-checks that the folder is on btrfs (the test
	// dir is not); stand in the helper's diff-baseline answer, driven by
	// the same fake's delay and cancellation.
	old := btrfsScanDiffBaseline
	t.Cleanup(func() { btrfsScanDiffBaseline = old })
	btrfsScanDiffBaseline = func(ctx context.Context, snap, _ string) ([]string, error) {
		h.mu.Lock()
		h.calls = append(h.calls, "diff-baseline "+snap)
		h.diffOpen = true
		h.mu.Unlock()
		select {
		case <-time.After(h.diffDelay):
			h.mu.Lock()
			h.diffOpen = false
			h.mu.Unlock()
			return nil, nil
		case <-ctx.Done():
			// The send is still running in the helper until rmsnap kills it.
			return nil, context.Cause(ctx)
		}
	}
	_, f, _ := barrierTestFolder(t)
	f.initialScanFinished = make(chan struct{})
	f.btrfsCleanStart = true
	if err := f.db.PutKV(initialScanDoneKVPrefix+f.folderID, []byte{1}); err != nil {
		t.Fatal(err)
	}
	return f
}

func setCostRing(t *testing.T, f *folder, key string, ds ...time.Duration) {
	t.Helper()
	for _, d := range ds {
		f.recordCostSample(key, d)
	}
}

// With no full scan measured there is no evidence a scan is cheaper: the
// boot diff is waited for however long it takes. On yarlung a blanket
// 5 min helper timeout abandoned three of thirteen diffs (317 s, 566 s,
// >600 s) for full scans of 66 min to over 13 h.
//
// Teeth: give the diff a fixed deadline shorter than its 1 s here and the
// replay falls back to a full scan.
func TestBtrfsBootDiffWaitsWithoutScanMeasurement(t *testing.T) {
	helper := startBootDiffFakeHelper(t, time.Second)
	f := armableBootFolder(t, helper)
	if !f.tryArmBtrfsScanReplay(t.Context()) {
		t.Fatal("boot diff abandoned with no measured full scan to switch to")
	}
}

// Once the diff has cost as much as the measured full scan (median of the
// ring) plus the measured cancel lag, it gives up: the helper kills the
// send and deletes the snapshot, the lag of that kill is recorded, and the
// folder falls back to the full scan.
//
// Teeth: drop the give-up timer and the diff runs its full 1 min; drop the
// lag recording and the cancel-lag ring stays empty.
func TestBtrfsBootDiffGivesUpAtFullScanCost(t *testing.T) {
	helper := startBootDiffFakeHelper(t, time.Minute)
	f := armableBootFolder(t, helper)
	setCostRing(t, f, fullScanCostKVPrefix, 200*time.Millisecond, 50*time.Millisecond, 300*time.Millisecond)

	start := time.Now()
	if f.tryArmBtrfsScanReplay(t.Context()) {
		t.Fatal("boot diff applied though it ran past the full-scan cost")
	}
	if d := time.Since(start); d < 200*time.Millisecond || d > 10*time.Second {
		t.Fatalf("gave up after %v, want just past the 200ms full-scan median", d)
	}
	if !helper.got("rmsnap boot-") {
		t.Fatalf("abandoned diff's snapshot was not removed: %v", helper.calls)
	}
	lags := f.loadCostSamples(btrfsCancelLagKVPrefix)
	if len(lags) != 1 || lags[0] != 1500*time.Millisecond {
		t.Fatalf("cancel lag ring = %v, want [1.5s]", lags)
	}
}

// The give-up point includes the measured cancel lag, because switching
// costs that lag on top of the scan.
func TestBtrfsDiffGiveUpAfterAddsCancelLag(t *testing.T) {
	_, f, _ := barrierTestFolder(t)
	if _, _, _, ok := f.btrfsDiffGiveUpAfter(); ok {
		t.Fatal("bounded with an empty full-scan ring")
	}
	setCostRing(t, f, fullScanCostKVPrefix, 66*time.Minute, 4*time.Hour, 13*time.Hour)
	setCostRing(t, f, btrfsCancelLagKVPrefix, 2*time.Second, 60*time.Second, 50*time.Second)
	after, scan, lag, ok := f.btrfsDiffGiveUpAfter()
	if !ok || scan != 4*time.Hour || lag != 50*time.Second || after != 4*time.Hour+50*time.Second {
		t.Fatalf("giveUp=%v scan=%v lag=%v ok=%v, want 4h0m50s 4h 50s true", after, scan, lag, ok)
	}
}

// The ring keeps the newest btrfsCostSamples, so the decision follows the
// machine's current conditions.
func TestCostRingKeepsNewest(t *testing.T) {
	_, f, _ := barrierTestFolder(t)
	for i := 1; i <= 7; i++ {
		f.recordCostSample(fullScanCostKVPrefix, time.Duration(i)*time.Minute)
	}
	s := f.loadCostSamples(fullScanCostKVPrefix)
	want := []time.Duration{3 * time.Minute, 4 * time.Minute, 5 * time.Minute, 6 * time.Minute, 7 * time.Minute}
	if fmt.Sprint(s) != fmt.Sprint(want) {
		t.Fatalf("ring = %v, want %v", s, want)
	}
}
