// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package fs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// slowDiffHelper answers diff-baseline only after delay (a disk-bound btrfs
// send) and rmsnap with a canned cancelled-send-exit line.
func startSlowDiffHelper(t *testing.T, delay time.Duration, rmsnapOut string) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "helper.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				buf := make([]byte, 4096)
				n, _ := conn.Read(buf)
				req := strings.TrimSpace(string(buf[:n]))
				switch {
				case req == "subvol":
					fmt.Fprintf(conn, "subvol=/\x00status=0")
				case strings.HasPrefix(req, "diff-baseline "):
					select {
					case <-time.After(delay):
					case <-done:
					}
					fmt.Fprintf(conn, "At subvol x\n\x00status=0")
				case strings.HasPrefix(req, "rmsnap "):
					fmt.Fprintf(conn, "%s\x00status=0", rmsnapOut)
				default:
					fmt.Fprintf(conn, "\x00status=2")
				}
			}(conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
	})
	t.Setenv("SYNCTHING_BTRFS_SOCK", sock)
}

// The diff is bounded only by its caller: a diff slower than the old
// blanket 5 min helper timeout must still be waited for. Measured on
// yarlung: three of thirteen boot diffs ran past 5 min and were abandoned
// for full scans that took 66 min to over 13 h.
//
// Teeth: route the diff through btrfsHelper (the blanket timeout, shrunk
// here) and it fails with a timeout.
func TestBtrfsScanDiffBaselineNotCappedByHelperTimeout(t *testing.T) {
	startSlowDiffHelper(t, 300*time.Millisecond, "")
	old := btrfsHelperTimeout
	t.Cleanup(func() { btrfsHelperTimeout = old })
	btrfsHelperTimeout = 100 * time.Millisecond

	if _, err := BtrfsScanDiffBaseline(context.Background(), "boot-x-1", "/"); err != nil {
		t.Fatalf("diff slower than the helper timeout was abandoned: %v", err)
	}
}

// Cancelling the caller's context ends the request at once and reports the
// context's cause, so the caller can tell its own give-up from a helper
// failure.
func TestBtrfsScanDiffBaselineCancelCarriesCause(t *testing.T) {
	startSlowDiffHelper(t, time.Minute, "")
	cause := errors.New("costlier than a full scan")
	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(100*time.Millisecond, func() { cancel(cause) })

	start := time.Now()
	_, err := BtrfsScanDiffBaseline(ctx, "boot-x-1", "/")
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want the context cause", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("cancelled diff returned after %v", d)
	}
}

func TestBtrfsScanRemoveSnapshotCancelReportsLag(t *testing.T) {
	startSlowDiffHelper(t, 0, "cancelled-send-exit=52.125\n")
	lag, err := BtrfsScanRemoveSnapshotCancel(context.Background(), "boot-x-1")
	if err != nil {
		t.Fatal(err)
	}
	if lag != 52125*time.Millisecond {
		t.Fatalf("lag = %v, want 52.125s", lag)
	}
}
