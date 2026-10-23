// Copyright (C) 2026 The Syncthing Authors.
//
// Drip mode tests (patch/46), three layers:
//
//  1. TestDripStateMachine: scripted replay with artificial times (injected
//     clock) — the full hold/flush/cooldown state machine, deterministic.
//  2. TestDripReplayIncidentTrace: replays the REAL 2026-10-23 incident
//     timeline (11,728 pull-retry timestamps of the hot
//     .inspect/last.json file, extracted from yarlung's serve log as
//     second offsets) through the tracker with production defaults and
//     measures the coalescing it would have achieved.
//  3. TestDripRealTimeWALCollapse: real-time, real-database reproduction —
//     a synthetic hot file dripped through the actual db.Update path,
//     control vs drip-mode, measuring committed transactions and DB+WAL
//     bytes. Skipped under -short (takes ~15 s of real time).

package model

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func dripFile(name string, tick int) protocol.FileInfo {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", name, tick)))
	return protocol.FileInfo{
		Name:         name,
		Size:         128 << 10,
		ModifiedS:    1761220000 + int64(tick),
		ModifiedBy:   1,
		Version:      protocol.Vector{}.Update(42),
		Permissions:  0o644,
		RawBlockSize: 128 << 10,
		Blocks:       []protocol.BlockInfo{{Hash: h[:], Size: 128 << 10}},
	}
}

// TestDripStateMachine replays a scripted timeline through the tracker with
// production defaults (delayS=10: detectN=3, window=60s, quiet=20s,
// maxHold=60s) on an artificial clock.
func TestDripStateMachine(t *testing.T) {
	now := time.Unix(1000, 0)
	at := func(unix int64) { now = time.Unix(unix, 0) }
	d := newDripTracker(config.FolderConfiguration{}, func() time.Time { return now }, nil)

	mustCommit := func(tag string, fs []protocol.FileInfo, want int) {
		t.Helper()
		if got := len(d.filterScanUpdate(fs)); got != want {
			t.Fatalf("%s: commit count = %d, want %d", tag, got, want)
		}
	}
	mustFlush := func(tag string, want int) {
		t.Helper()
		if got := len(d.flushDue()); got != want {
			t.Fatalf("%s: flush count = %d, want %d", tag, got, want)
		}
	}

	// Below threshold: two commits pass immediately.
	at(0)
	mustCommit("first", []protocol.FileInfo{dripFile("a", 1)}, 1)
	at(5)
	mustCommit("second", []protocol.FileInfo{dripFile("a", 2)}, 1)

	// Third commit within the window: held.
	at(10)
	mustCommit("third held", []protocol.FileInfo{dripFile("a", 3)}, 0)
	if d.heldCount() != 1 {
		t.Fatalf("held = %d, want 1", d.heldCount())
	}

	// Newer version while held: last wins, no commit.
	at(12)
	mustCommit("held replace", []protocol.FileInfo{dripFile("a", 4)}, 0)
	if got := d.held["a"].fi.ModifiedS; got != 1761220004 {
		t.Fatalf("held entry not last-wins: ModifiedS = %d", got)
	}

	// Timer deadline: quiet after lastUpdate=12 -> due at 32.
	if wait, ok := d.nextIn(); !ok || wait != 20*time.Second {
		t.Fatalf("nextIn = %v, %v; want 20s, true", wait, ok)
	}
	at(31)
	mustFlush("not yet due", 0)
	at(32)
	mustFlush("quiet flush", 1)

	// Cooldown: quiet flush cleared the path's history, so the next update
	// commits immediately instead of re-entering the hold.
	at(33)
	mustCommit("cooled", []protocol.FileInfo{dripFile("a", 5)}, 1)

	// Max-hold: a continuously dripping file is committed at the staleness
	// bound even though it never goes quiet.
	at(100)
	mustCommit("c1", []protocol.FileInfo{dripFile("c", 1)}, 1)
	at(105)
	mustCommit("c2", []protocol.FileInfo{dripFile("c", 2)}, 1)
	at(110)
	mustCommit("c3 held", []protocol.FileInfo{dripFile("c", 3)}, 0)
	for tick, ts := 4, int64(115); ts <= 165; tick, ts = tick+1, ts+5 {
		at(ts)
		mustCommit("drip", []protocol.FileInfo{dripFile("c", tick)}, 0)
	}
	at(169)
	mustFlush("before max-hold", 0)
	at(170) // firstHeld=110 + 60s
	mustFlush("max-hold flush", 1)

	// Max-hold flush keeps the path hot: the next drip is held immediately.
	at(171)
	mustCommit("still hot", []protocol.FileInfo{dripFile("c", 20)}, 0)
	// Drip stops: quiet flush 20s later, then the path cools.
	at(191)
	mustFlush("final quiet flush", 1)
	at(192)
	mustCommit("cooled after quiet", []protocol.FileInfo{dripFile("c", 21)}, 1)

	// Pass-through: directories and deletes are never held, never recorded.
	at(200)
	dir := dripFile("dir", 1)
	dir.Type = protocol.FileInfoTypeDirectory
	del := dripFile("gone", 1)
	del.Deleted = true
	mustCommit("dir+del pass", []protocol.FileInfo{dir, del}, 2)
	mustCommit("dir+del pass 2", []protocol.FileInfo{dir, del}, 2)
	mustCommit("dir+del pass 3", []protocol.FileInfo{dir, del}, 2)
	if d.heldCount() != 0 {
		t.Fatalf("dirs/deletes must never be held, held = %d", d.heldCount())
	}
}

// TestDripDisabled: STDRIPMODE=0 passes everything through.
func TestDripDisabled(t *testing.T) {
	t.Setenv("STDRIPMODE", "0")
	now := time.Unix(1000, 0)
	d := newDripTracker(config.FolderConfiguration{}, func() time.Time { return now }, nil)
	for i := 0; i < 10; i++ {
		if got := len(d.filterScanUpdate([]protocol.FileInfo{dripFile("a", i)})); got != 1 {
			t.Fatalf("disabled tracker held an update (commit=%d)", got)
		}
	}
	if d.heldCount() != 0 {
		t.Fatal("disabled tracker holds nothing")
	}
}

// TestDripReplayIncidentTrace replays the real .inspect/last.json drip
// timeline (11,728 pull retries over 9.2 h on 2026-10-23, second offsets
// from yarlung's serve log) through the tracker with production defaults.
// Each trace event stands for one scan-side commit of the hot file.
func TestDripReplayIncidentTrace(t *testing.T) {
	fh, err := os.Open(filepath.Join("testdata", "drip-lastjson-trace.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	var offsets []int64
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		var v int64
		if _, err := fmt.Sscan(sc.Text(), &v); err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, v)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1000, 0)
	d := newDripTracker(config.FolderConfiguration{}, func() time.Time { return now }, nil)

	commits := 0
	for i, off := range offsets {
		now = time.Unix(1000+off, 0)
		// The folder's flush timer would have fired between events.
		commits += len(d.flushDue())
		commits += len(d.filterScanUpdate([]protocol.FileInfo{dripFile("hot/last.json", i)}))
	}
	commits += len(d.flushAll())

	ratio := float64(len(offsets)) / float64(commits)
	t.Logf("incident replay: %d events -> %d commits (%.1fx coalesced)", len(offsets), commits, ratio)
	if commits >= len(offsets)/5 {
		t.Fatalf("drip mode coalesced only %.1fx (%d -> %d commits), want >= 5x", ratio, len(offsets), commits)
	}
	if d.heldCount() != 0 {
		t.Fatalf("flushAll left %d held", d.heldCount())
	}
}

// TestDripRealTimeWALCollapse is the local reproduction with real time and
// real bytes: a synthetic hot file dripped every 300 ms through the actual
// db.Update path. Control phase commits every drip; drip-mode phase
// (compressed timing: quiet 2s, max-hold 6s) coalesces. Measures committed
// transaction count and DB+WAL bytes for both phases.
func TestDripRealTimeWALCollapse(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time drip reproduction takes ~15 s")
	}

	const drips = 20
	const interval = 300 * time.Millisecond

	run := func(withDrip bool) (commits int, bytes int64) {
		dir := t.TempDir()
		sdb, err := sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer sdb.Close()

		commit := func(fs []protocol.FileInfo) {
			if len(fs) == 0 {
				return
			}
			if err := sdb.Update("test", protocol.LocalDeviceID, fs); err != nil {
				t.Fatal(err)
			}
			commits++
		}

		// Seed the file so the drip is updates, not first inserts.
		commit([]protocol.FileInfo{dripFile("hot/state.json", 0)})

		if !withDrip {
			_, before := dripDirSizes(t, dir)
			for i := 1; i <= drips; i++ {
				commit([]protocol.FileInfo{dripFile("hot/state.json", i)})
				time.Sleep(interval)
			}
			_, after := dripDirSizes(t, dir)
			return commits - 1, after - before
		}

		t.Setenv("STDRIPQUIETS", "2")
		t.Setenv("STDRIPMAXHOLDS", "6")
		t.Setenv("STDRIPWINDOWS", "12")
		d := newDripTracker(config.FolderConfiguration{}, time.Now, func(msg string, args ...any) {
			t.Logf("  drip: %s %v", msg, args)
		})

		// Timer loop mirroring the folder Serve wiring: fire at nextIn,
		// flush due, rearm.
		done := make(chan struct{})
		go func() {
			for {
				wait, ok := d.nextIn()
				if !ok {
					select {
					case <-time.After(50 * time.Millisecond):
					case <-done:
						return
					}
					continue
				}
				select {
				case <-time.After(wait):
					commit(d.flushDue())
				case <-done:
					return
				}
			}
		}()
		defer close(done)

		_, before := dripDirSizes(t, dir)
		for i := 1; i <= drips; i++ {
			commit(d.filterScanUpdate([]protocol.FileInfo{dripFile("hot/state.json", i)}))
			time.Sleep(interval)
		}
		// Wait for the final flush (quiet 2s after the last drip).
		deadline := time.Now().Add(10 * time.Second)
		for d.heldCount() > 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		commit(d.flushDue())
		_, after := dripDirSizes(t, dir)
		return commits - 1, after - before
	}

	commitsA, bytesA := run(false)
	commitsB, bytesB := run(true)
	t.Logf("control:  %d drips -> %d commits, %d bytes", drips, commitsA, bytesA)
	t.Logf("dripmode: %d drips -> %d commits, %d bytes", drips, commitsB, bytesB)

	if commitsA != drips {
		t.Fatalf("control committed %d, want %d (every drip)", commitsA, drips)
	}
	if commitsB > 6 {
		t.Fatalf("drip mode committed %d, want <= 6", commitsB)
	}
	if bytesB >= bytesA/3 {
		t.Fatalf("drip mode wrote %d bytes vs control %d, want >= 3x reduction", bytesB, bytesA)
	}
}

// dripDirSizes sums .db and -wal file sizes under dir (checkpoint-robust
// total: a checkpoint moves bytes WAL->DB without changing the sum much).
func dripDirSizes(t *testing.T, dir string) (dbBytes, walBytes int64) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		switch {
		case strings.HasSuffix(d.Name(), "-wal"):
			walBytes += fi.Size()
		case strings.HasSuffix(d.Name(), ".db"):
			dbBytes += fi.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}
