// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package model

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/watchaggregator"
)

// pairingFakeHelper serves the helper protocol on a unix socket, recording
// every request. setbaseline can be told to fail. Unlike the fs package's
// sweep fake this one answers snap/setbaseline, which pairing needs.
type pairingFakeHelper struct {
	mu       sync.Mutex
	calls    []string
	failSet  bool
	failRm   bool
	sockPath string
}

func startPairingFakeHelper(t *testing.T) *pairingFakeHelper {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "helper.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	h := &pairingFakeHelper{sockPath: sock}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go h.serve(conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
	})
	t.Setenv("SYNCTHING_BTRFS_SOCK", sock)
	return h
}

func (h *pairingFakeHelper) serve(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	req := strings.TrimSpace(string(buf[:n]))
	h.mu.Lock()
	h.calls = append(h.calls, req)
	failSet, failRm := h.failSet, h.failRm
	h.mu.Unlock()
	out, code := "", 0
	switch {
	case strings.HasPrefix(req, "snap "):
		out = "snap=/home/.snapshots/syncthing/" + strings.TrimPrefix(req, "snap ")
	case strings.HasPrefix(req, "setbaseline "):
		if failSet {
			code = 1
		} else {
			out = "baseline=/home/.snapshots/syncthing/baseline"
		}
	case strings.HasPrefix(req, "rmsnap "):
		if failRm {
			code = 1
		}
	default:
		code = 2
	}
	fmt.Fprintf(conn, "%s\x00status=%d", out, code)
}

func (h *pairingFakeHelper) got(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

// drainFlush acks every watch flush request instantly.
func drainFlush(f *folder) {
	ch := make(chan chan struct{}, 16)
	f.watchMut.Lock()
	f.watchFlush = ch
	f.watchMut.Unlock()
	go func() {
		for ack := range ch {
			close(ack)
		}
	}()
}

// feedCanary watches for the barrier's canary file and delivers its watch
// batch, completing the barrier.
func feedCanary(t *testing.T, f *folder) {
	t.Helper()
	go func() {
		name := waitForCanary(t, f.Filesystem())
		f.watchChan <- watchaggregator.Batch{Paths: []string{name}}
	}()
}

// A successful pairing must NOT rmsnap the staging snapshot: setbaseline
// renamed it to the baseline, so the name no longer exists and the removal
// would be a false-failure WRN (helper exit 1 on a missing path). Observed
// live on yarlung 2026-10-21 (dev.145/146): every successful pairing logged
// "Failed to remove btrfs pairing snapshot ... rmsnap ... status 1".
//
// Teeth: revert `consumed` and the rmsnap assertion fails; the pre-fix code
// also registered the defer before the snapshot existed.
func TestPairBtrfsBaselineDoesNotRemoveConsumedSnapshot(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	drainFlush(f)
	feedCanary(t, f)

	ctx := t.Context()
	if !f.pairBtrfsBaseline(ctx, "post-replay") {
		t.Fatal("pairing failed against fake helper")
	}
	if !helper.got("snap post-replay-") {
		t.Errorf("helper never got snap request: %v", helper.calls)
	}
	if !helper.got("setbaseline post-replay-") {
		t.Errorf("helper never got setbaseline request: %v", helper.calls)
	}
	if helper.got("rmsnap post-replay-") {
		t.Errorf("consumed snapshot was removed (false-failure path): %v", helper.calls)
	}
}

// A FAILED pairing (setbaseline refused) must still rmsnap the staging
// snapshot: it was created, never consumed, and would leak otherwise.
//
// Teeth: dropping the deferred removal entirely passes the success test
// above but fails this one.
func TestPairBtrfsBaselineRemovesSnapshotOnFailure(t *testing.T) {
	helper := startPairingFakeHelper(t)
	helper.failSet = true
	_, f, _ := barrierTestFolder(t)
	drainFlush(f)
	feedCanary(t, f)

	ctx := t.Context()
	if f.pairBtrfsBaseline(ctx, "post-replay") {
		t.Fatal("pairing succeeded despite failing setbaseline")
	}
	// The deferred removal runs when pairBtrfsBaseline returns.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if helper.got("rmsnap post-replay-") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("failed pairing leaked its staging snapshot: %v", helper.calls)
}
