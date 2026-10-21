// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package fs

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakeBtrfsHelper serves the helper protocol on a unix socket: canned
// subvols output, rmsnap requests recorded, rmsnap of names in failRm
// answered with a nonzero status.
type fakeBtrfsHelper struct {
	sock    string
	subvols []string
	failRm  map[string]bool
	removed []string
	done    chan struct{}
}

func startFakeBtrfsHelper(t *testing.T, subvols []string) *fakeBtrfsHelper {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "helper.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	h := &fakeBtrfsHelper{sock: sock, subvols: subvols, done: make(chan struct{})}
	go func() {
		defer close(h.done)
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
		<-h.done
	})
	return h
}

func (h *fakeBtrfsHelper) serve(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	req := strings.TrimSpace(string(buf[:n]))
	var out string
	code := 0
	switch {
	case req == "subvols":
		out = strings.Join(h.subvols, "\n") + "\n"
	case strings.HasPrefix(req, "rmsnap "):
		name := strings.TrimPrefix(req, "rmsnap ")
		if h.failRm[name] {
			code = 1
			break
		}
		found := false
		for i, s := range h.subvols {
			if strings.HasSuffix(s, "/"+name) {
				h.subvols = slices.Delete(h.subvols, i, i+1)
				found = true
				break
			}
		}
		if !found {
			code = 1
		} else {
			h.removed = append(h.removed, name)
		}
	default:
		code = 2
	}
	fmt.Fprintf(conn, "%s\x00status=%d", out, code)
}

func TestBtrfsScanSweepStaleSnapshots(t *testing.T) {
	helper := startFakeBtrfsHelper(t, []string{
		"@home/.snapshots/syncthing/shutdown-home-1792137421307866309",
		"@home/.snapshots/syncthing/boot-home-1792530828360473700",
		"@home/.snapshots/syncthing/baseline",
		"@home/jhf/.config/syncthing/index-v2",
		"@home/jhf/data/boot-home-1792530828360473700", // right name, WRONG dir: user data
		"@home/jhf/projects",                           // plain user subvolume
	})
	t.Setenv("SYNCTHING_BTRFS_SOCK", helper.sock)

	removed, err := BtrfsScanSweepStaleSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"shutdown-home-1792137421307866309", "boot-home-1792530828360473700"}
	if !slices.Equal(removed, want) {
		t.Errorf("removed %v, want %v", removed, want)
	}
	if !slices.Equal(helper.removed, want) {
		t.Errorf("helper got rmsnap %v, want %v", helper.removed, want)
	}
	// The survivors must include baseline, index-v2, and the identically
	// named user subvolume outside the staging dir.
	joined := strings.Join(helper.subvols, "\n")
	for _, survivor := range []string{"baseline", "index-v2", "@home/jhf/data/boot-home-1792530828360473700"} {
		if !strings.Contains(joined, survivor) {
			t.Errorf("survivor missing from subvols: %q (have %v)", survivor, helper.subvols)
		}
	}

	// Second sweep is a no-op (self-terminating).
	removed, err = BtrfsScanSweepStaleSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Errorf("second sweep removed %v, want nothing", removed)
	}
}

func TestBtrfsScanSweepStaleSnapshotsRemovalError(t *testing.T) {
	// A rmsnap failure is reported but does not stop the sweep: the other
	// stale snapshot is still removed.
	helper := startFakeBtrfsHelper(t, []string{
		"@home/.snapshots/syncthing/boot-home-111",
		"@home/.snapshots/syncthing/shutdown-home-222",
	})
	helper.failRm = map[string]bool{"boot-home-111": true}
	t.Setenv("SYNCTHING_BTRFS_SOCK", helper.sock)

	removed, err := BtrfsScanSweepStaleSnapshots(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boot-home-111") {
		t.Errorf("expected rmsnap error naming boot-home-111, got %v", err)
	}
	want := []string{"shutdown-home-222"}
	if !slices.Equal(removed, want) {
		t.Errorf("removed %v, want %v", removed, want)
	}
}
