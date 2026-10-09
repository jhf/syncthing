// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package svcutil

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The systemd unit's stop timeout must cover syncthing's own bounded stop,
// or systemd SIGKILLs it mid-pairing (yarlung 2026-11-26 16:51:58: SIGKILL
// at 90 s, the default TimeoutStopSec). The drop-in that sets it lives in
// ops/ and is installed by ops/custom-syncthing/btrfs/install.sh.
//
// Teeth: raising ShutdownBudget without raising TimeoutStopSec in the
// drop-in (or removing it) fails here.
func TestSystemdStopTimeoutCoversShutdown(t *testing.T) {
	const dropIn = "../../ops/custom-syncthing/btrfs/syncthing-btrfs-order.conf"
	fd, err := os.Open(dropIn)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	var stop time.Duration
	sc := bufio.NewScanner(fd)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if v, ok := strings.CutPrefix(line, "TimeoutStopSec="); ok {
			secs, err := strconv.Atoi(v)
			if err != nil {
				t.Fatalf("TimeoutStopSec=%q: want whole seconds: %v", v, err)
			}
			stop = time.Duration(secs) * time.Second
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if stop == 0 {
		t.Fatalf("%s sets no TimeoutStopSec: systemd's 90 s default is below StopTimeout %v", dropIn, StopTimeout)
	}
	if stop < StopTimeout {
		t.Fatalf("TimeoutStopSec %v is below syncthing's bounded stop %v: systemd kills it mid-shutdown", stop, StopTimeout)
	}
	if StopTimeout < ShutdownServiceTimeout+DatabaseCloseTimeout+ExitFlushTimeout {
		t.Fatalf("StopTimeout %v does not cover its parts", StopTimeout)
	}
}
