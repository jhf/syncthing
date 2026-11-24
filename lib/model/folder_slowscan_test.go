// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"log/slog"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

type slowScanLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *slowScanLogBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *slowScanLogBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// slowPuller stands in for a pull that installs files on slow disks.
type slowPuller struct {
	d     time.Duration
	calls int
}

func (p *slowPuller) pull(context.Context) (bool, error) {
	p.calls++
	time.Sleep(p.d)
	return true, nil
}

// "Slow scan finished" must say how much of a scan's wall time went to
// queued work serviced at chunk boundaries. yarlung 2026-11-27 22:14:28
// logged duration=17m18.966752351s changes=0 for 11 unchanged
// .git/objects directories while 577 files were synced in the same window;
// the line could not tell walking from pulling.
func TestSlowScanReportsServicedTime(t *testing.T) {
	// Not parallel: mutates package-level budgets.
	oldBudget, oldThreshold := scanChunkBudget, slowScanLogThreshold
	t.Cleanup(func() { scanChunkBudget, slowScanLogThreshold = oldBudget, oldThreshold })
	scanChunkBudget = 0 // a boundary after every item
	slowScanLogThreshold = 0

	m, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()
	must(t, ffs.MkdirAll("a", 0o755))
	for _, n := range []string{"a/1", "a/2", "a/3"} {
		writeFile(t, ffs, n, []byte(n))
	}
	// Something must be needed or folder.pull returns before the puller.
	must(t, m.sdb.Update(f.ID, device1, []protocol.FileInfo{{
		Name:     "needed",
		Size:     5,
		Version:  protocol.Vector{}.Update(device1.Short()),
		Sequence: 1,
		Blocks:   []protocol.BlockInfo{{Hash: []byte("01234567890123456789012345678901"), Size: 5}},
	}}))
	slow := &slowPuller{d: 200 * time.Millisecond}
	old := f.puller
	f.puller = slow
	t.Cleanup(func() { f.puller = old })

	var buf slowScanLogBuffer
	f.sl = slog.New(slog.NewTextHandler(&buf, nil))
	f.SchedulePull()
	must(t, f.scanSubdirs(ctx, []string{"a"}))

	if slow.calls == 0 {
		t.Fatal("fixture broken: the queued pull was not serviced during the scan")
	}
	line := regexp.MustCompile(`msg="Slow scan finished".*`).FindString(buf.String())
	if line == "" {
		t.Fatalf("no slow-scan line:\n%s", buf.String())
	}
	m2 := regexp.MustCompile(`serviced=(\S+) servicedPulls=(\d+)`).FindStringSubmatch(line)
	if m2 == nil {
		t.Fatalf("slow-scan line does not report serviced time: %s", line)
	}
	serviced, err := time.ParseDuration(m2[1])
	if err != nil {
		t.Fatal(err)
	}
	if serviced < slow.d || m2[2] == "0" {
		t.Fatalf("serviced=%s servicedPulls=%s, want >= %s and >= 1: %s", serviced, m2[2], slow.d, line)
	}
	t.Log(line)
}
