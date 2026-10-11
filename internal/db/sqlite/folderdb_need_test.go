package sqlite

import (
	"slices"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

// TestNeedTrackingEquivalence populates a folder, builds the need tracking
// tables, then applies a series of updates. After each step the maintained
// tables must agree with the full-scan oracle.
func TestNeedTrackingEquivalence(t *testing.T) {
	t.Parallel()

	const folderID = "test"

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	other := protocol.DeviceID{2}
	third := protocol.DeviceID{3}

	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}

	// Counts as reported by the maintained tables (or the fallback).
	now := func(d protocol.DeviceID) db.Counts {
		c, err := sdb.CountNeed(folderID, d)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// Counts from the full-scan oracle, bypassing the fast paths.
	oracle := func(d protocol.DeviceID) db.Counts {
		fdb.needBuilt.Store(false)
		defer fdb.needBuilt.Store(true)
		c, err := fdb.needSizeRemote(d)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	check := func(step string, d protocol.DeviceID) {
		t.Helper()
		got, want := now(d), oracle(d)
		if got != want {
			t.Fatalf("%s: need counts for %v: maintained %v, oracle %v", step, d, got, want)
		}
		// List equality against the oracle as well.
		fdb.needBuilt.Store(false)
		it, errFn := fdb.neededGlobalFilesRemote(d, "")
		wantList := slices.Collect(it)
		if err := errFn(); err != nil {
			t.Fatal(err)
		}
		fdb.needBuilt.Store(true)
		// Read through neededGlobalFilesRemote rather than the list cache:
		// the cache deliberately serves a just-invalidated entry for up to
		// needCountCacheMinRecompute to coalesce batch bursts, which is not
		// the behaviour under test here.
		it, errFn = fdb.neededGlobalFilesRemote(d, "")
		gotList := slices.Collect(it)
		if err := errFn(); err != nil {
			t.Fatal(err)
		}
		if len(gotList) != len(wantList) {
			gotNames := make([]string, len(gotList))
			for i := range gotList {
				gotNames[i] = gotList[i].Name
			}
			wantNames := make([]string, len(wantList))
			for i := range wantList {
				wantNames[i] = wantList[i].Name
			}
			t.Fatalf("%s: need list for %v: maintained %v, oracle %v", step, d, gotNames, wantNames)
		}
		slices.SortFunc(gotList, func(a, b protocol.FileInfo) int { return strings.Compare(a.Name, b.Name) })
		slices.SortFunc(wantList, func(a, b protocol.FileInfo) int { return strings.Compare(a.Name, b.Name) })
		for i := range wantList {
			if gotList[i].Name != wantList[i].Name {
				t.Fatalf("need list for %v at %d: maintained %q, oracle %q", d, i, gotList[i].Name, wantList[i].Name)
			}
		}
	}

	// Populate before the build: local announces a, b, c; other has a.
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{
		genFile("a", 1, 1), genFile("b", 2, 2), genFile("c", 3, 3),
	}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.Update(folderID, other, []protocol.FileInfo{
		genFile("a", 1, 1), // same version as local's a
	}); err != nil {
		t.Fatal(err)
	}

	// Build the tracking tables synchronously.
	fdb.buildNeedTracking(fdb.needBuild.gen)
	if !fdb.needBuilt.Load() {
		t.Fatal("need tracking not marked built")
	}
	check("step1", other)
	check("step2", third) // unknown device: falls back to full scan

	// Local changes b.
	f4 := genFile("b", 4, 4)
	f4.Version = f4.Version.Update(1)
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{f4}); err != nil {
		t.Fatal(err)
	}
	check("step3", other)
	check("step4", third)

	// Other receives the new b.
	if err := sdb.Update(folderID, other, []protocol.FileInfo{f4}); err != nil {
		t.Fatal(err)
	}
	check("step5", other)
	check("step6", third)

	// Local deletes a; other still has the live copy -> delete-need for other.
	aDel := genFile("a", 0, 5)
	aDel.SetDeleted(42)
	aDel.Version = aDel.Version.Update(1)
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{aDel}); err != nil {
		t.Fatal(err)
	}
	check("step7", other)
	check("step8", third)

	// Other deletes a as well -> nothing needed for it anymore.
	if err := sdb.Update(folderID, other, []protocol.FileInfo{aDel}); err != nil {
		t.Fatal(err)
	}
	check("step9", other)
	check("step10", third)

	// Third finally announces its (partial) state; this exercises the
	// backfill for a device that was created after the build.
	if err := sdb.Update(folderID, third, []protocol.FileInfo{f4}); err != nil {
		t.Fatal(err)
	}
	check("step11", other)
	check("step12", third)

	// Drop all files for other: resets the tracking tables; rebuild.
	if err := sdb.DropAllFiles(folderID, other); err != nil {
		t.Fatal(err)
	}
	fdb.buildNeedTracking(fdb.needBuild.gen)
	check("step13", other)
	check("step14", third)

	// Drop the device entirely: reset and rebuild.
	if err := sdb.DropDevice(third); err != nil {
		t.Fatal(err)
	}
	fdb.buildNeedTracking(fdb.needBuild.gen)
	check("step15", other)
}

// TestNeedTrackingJournalOnLateBuild simulates updates landing while a build
// is in flight: they must be journaled and applied by the final pass.
func TestNeedTrackingJournalOnLateBuild(t *testing.T) {
	t.Parallel()

	const folderID = "test"

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	other := protocol.DeviceID{2}

	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}

	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{genFile("a", 1, 1)}); err != nil {
		t.Fatal(err)
	}

	// Pretend a build is running so the update journals the name it touches.
	fdb.needBuild.mu.Lock()
	fdb.needBuild.running = true
	fdb.needBuild.mu.Unlock()
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{genFile("b", 2, 2)}); err != nil {
		t.Fatal(err)
	}
	// Finish the (synchronous) build; it must pick up the journaled name.
	fdb.needBuild.mu.Lock()
	fdb.needBuild.running = false
	fdb.needBuild.mu.Unlock()
	fdb.buildNeedTracking(fdb.needBuild.gen)

	fdb.needBuilt.Store(false)
	want, err := fdb.needSizeRemote(other)
	if err != nil {
		t.Fatal(err)
	}
	fdb.needBuilt.Store(true)
	got, err := sdb.CountNeed(folderID, other)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("maintained %v, oracle %v", got, want)
	}
	if got.Files+got.Directories+got.Symlinks != 2 {
		t.Fatalf("expected 2 need items for other, got %v", got)
	}
}
