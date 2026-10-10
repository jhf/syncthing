// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package watchaggregator

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestMain(m *testing.M) {
	maxFiles = 32
	maxFilesPerDir = 8

	ret := m.Run()

	maxFiles = 512
	maxFilesPerDir = 128

	os.Exit(ret)
}

const (
	testNotifyDelayS   = 1
	testNotifyTimeout  = 2 * time.Second
	timeoutWithinBatch = time.Second
)

var (
	folderRoot       = filepath.Clean("/home/someuser/syncthing")
	defaultFolderCfg = config.FolderConfiguration{
		FilesystemType:  config.FilesystemTypeBasic,
		Path:            folderRoot,
		FSWatcherDelayS: testNotifyDelayS,
	}
	defaultCfg = config.Wrap("", config.Configuration{
		Folders: []config.FolderConfiguration{defaultFolderCfg},
	}, protocol.LocalDeviceID, events.NoopLogger)
)

// Represents possibly multiple (different event types) expected paths from
// aggregation, that should be received back to back.
type expectedBatch struct {
	paths    [][]string
	afterMs  int
	beforeMs int
}

// TestAggregate checks whether maxFilesPerDir+1 events in one dir are
// aggregated to parent dir
func TestAggregate(t *testing.T) {
	inProgress := make(map[string]struct{})

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = "Aggregate"
	ctx := t.Context()
	a := newAggregator(ctx, folderCfg)

	// checks whether maxFilesPerDir events in one dir are kept as is
	for i := 0; i < maxFilesPerDir; i++ {
		a.newEvent(fs.Event{
			Name: filepath.Join("parent", strconv.Itoa(i)),
			Type: fs.NonRemove,
		}, inProgress)
	}
	if l := len(getEventPaths(a.root, ".", a)); l != maxFilesPerDir {
		t.Errorf("Unexpected number of events stored, got %v, expected %v", l, maxFilesPerDir)
	}

	// checks whether maxFilesPerDir+1 events in one dir are aggregated to parent dir
	a.newEvent(fs.Event{
		Name: filepath.Join("parent", "new"),
		Type: fs.NonRemove,
	}, inProgress)
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{"parent"})

	// checks that adding an event below "parent" does not change anything
	a.newEvent(fs.Event{
		Name: filepath.Join("parent", "extra"),
		Type: fs.NonRemove,
	}, inProgress)
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{"parent"})

	// again test aggregation in "parent" but with event in subdirs
	a = newAggregator(ctx, folderCfg)
	for i := 0; i < maxFilesPerDir; i++ {
		a.newEvent(fs.Event{
			Name: filepath.Join("parent", strconv.Itoa(i)),
			Type: fs.NonRemove,
		}, inProgress)
	}
	a.newEvent(fs.Event{
		Name: filepath.Join("parent", "sub", "new"),
		Type: fs.NonRemove,
	}, inProgress)
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{"parent"})

	// test aggregation in root
	a = newAggregator(ctx, folderCfg)
	for i := 0; i < maxFiles; i++ {
		a.newEvent(fs.Event{
			Name: strconv.Itoa(i),
			Type: fs.NonRemove,
		}, inProgress)
	}
	if len(getEventPaths(a.root, ".", a)) != maxFiles {
		t.Errorf("Unexpected number of events stored in root")
	}
	a.newEvent(fs.Event{
		Name: filepath.Join("parent", "sub", "new"),
		Type: fs.NonRemove,
	}, inProgress)
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{"."})

	// checks that adding an event when "." is already stored is a noop
	a.newEvent(fs.Event{
		Name: "anythingelse",
		Type: fs.NonRemove,
	}, inProgress)
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{"."})

	a = newAggregator(ctx, folderCfg)
	filesPerDir := maxFilesPerDir / 2
	dirs := make([]string, maxFiles/filesPerDir+1)
	for i := 0; i < maxFiles/filesPerDir+1; i++ {
		dirs[i] = "dir" + strconv.Itoa(i)
	}
	for _, dir := range dirs {
		for i := range filesPerDir {
			a.newEvent(fs.Event{
				Name: filepath.Join(dir, strconv.Itoa(i)),
				Type: fs.NonRemove,
			}, inProgress)
		}
	}
	// The total overflows; at the test scale the cluster threshold is not
	// reached anywhere, so this still escalates to the root. The subtree
	// collapse is pinned by TestAggregateOverflowCollapsesToSubtrees.
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{"."})
}

// TestAggregateOverflowCollapsesToSubtrees pins the behavior that avoids a
// full-tree scan on bursty writes: when the window's distinct-path count
// overflows but the events cover only a few top-level directories, the batch
// names those directories. Nothing was lost on overflow - every event is in
// the tracked tree - so scanning the named subtrees covers every event.
func TestAggregateOverflowCollapsesToSubtrees(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = "test"
	inProgress := make(map[string]struct{})
	a := newAggregator(ctx, folderCfg)

	// The test scale (maxFiles 32, maxFilesPerDir 8) overflows before the
	// default cluster threshold is reached, so pin it low: a prefix
	// covering more than four tracked paths collapses into a subtree scan.
	oldThreshold := collapseClusterThreshold
	defer func() { collapseClusterThreshold = oldThreshold }()
	collapseClusterThreshold = 4

	// Fill the window with events confined to four directories: exactly at
	// the per-directory limit each, so they stay expanded and the total
	// reaches maxFiles.
	dirs := []string{"alpha", "beta", "gamma", "delta"}
	for _, dir := range dirs {
		for i := 0; i < maxFilesPerDir; i++ {
			a.newEvent(fs.Event{Name: filepath.Join(dir, strconv.Itoa(i)), Type: fs.NonRemove}, inProgress)
		}
	}
	// A root-level file pushes the total past the limit.
	a.newEvent(fs.Event{Name: "rootfile.txt", Type: fs.NonRemove}, inProgress)

	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), append(dirs, "rootfile.txt"))

	// Further events merge into the collapsed paths: nothing new appears,
	// and the root is never scanned for these.
	a.newEvent(fs.Event{Name: filepath.Join("alpha", "sub", "deeper"), Type: fs.NonRemove}, inProgress)
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), append(dirs, "rootfile.txt"))
}

// TestAggregateOverflowWideChurnFactors pins that churn spread over many
// directories still factors instead of flattening to the root: scanning the
// union of the touched subtrees never costs more than a full walk, so no
// path-count cap applies. The root escalation survives only when factoring
// cannot shrink the set below maxFiles (pinned by TestAggregate) and for
// explicit root events (backend drop flags).
func TestAggregateOverflowWideChurnFactors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = "test"
	inProgress := make(map[string]struct{})
	a := newAggregator(ctx, folderCfg)

	oldThreshold := collapseClusterThreshold
	defer func() { collapseClusterThreshold = oldThreshold }()
	collapseClusterThreshold = 2

	// Fill the window to exactly the limit: six directories of five files
	// plus two root-level files.
	var dirs []string
	for d := 0; d < 6; d++ {
		dirs = append(dirs, "dir"+strconv.Itoa(d))
		for i := 0; i < 5; i++ {
			a.newEvent(fs.Event{Name: filepath.Join(dirs[d], strconv.Itoa(i)), Type: fs.NonRemove}, inProgress)
		}
	}
	a.newEvent(fs.Event{Name: "rf0", Type: fs.NonRemove}, inProgress)
	a.newEvent(fs.Event{Name: "rf1", Type: fs.NonRemove}, inProgress)
	// The total is at the limit; this event overflows it.
	a.newEvent(fs.Event{Name: filepath.Join("dir6", "x"), Type: fs.NonRemove}, inProgress)

	// The six dense directories collapse to subtree scans; the rest stays
	// precise. Eight directories' worth of churn, zero of it at the root.
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a),
		append(dirs, "rf0", "rf1", filepath.Join("dir6", "x")))
}

// TestAggregateOverflowFactorsCommonPrefix pins the least-common-prefix
// factoring: bursts spread over sibling subtrees scan those subtrees, not
// their parent, and not the root.
func TestAggregateOverflowFactorsCommonPrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = "test"
	inProgress := make(map[string]struct{})
	a := newAggregator(ctx, folderCfg)

	oldThreshold := collapseClusterThreshold
	defer func() { collapseClusterThreshold = oldThreshold }()
	collapseClusterThreshold = 4

	// 31 events over four sibling subtrees, two each under "a" and "d".
	for _, dir := range []string{"a/b", "a/c", "d/e"} {
		for i := 0; i < maxFilesPerDir; i++ {
			a.newEvent(fs.Event{Name: filepath.Join(dir, strconv.Itoa(i)), Type: fs.NonRemove}, inProgress)
		}
	}
	for i := 0; i < maxFilesPerDir-1; i++ {
		a.newEvent(fs.Event{Name: filepath.Join("d", "f", strconv.Itoa(i)), Type: fs.NonRemove}, inProgress)
	}
	// The total is now at the limit; the next event overflows it. (The
	// overflow check fires when an event arrives at a full window.)
	a.newEvent(fs.Event{Name: filepath.Join("d", "f", "new"), Type: fs.NonRemove}, inProgress)
	a.newEvent(fs.Event{Name: "rootfile.txt", Type: fs.NonRemove}, inProgress)

	// Each sibling subtree is dense enough to collapse, but their parents
	// are not: the batch names the four subtrees, not "a", not "d", not "."
	compareBatchToExpectedDirect(t, getEventPaths(a.root, ".", a), []string{
		filepath.Join("a", "b"), filepath.Join("a", "c"), filepath.Join("d", "e"), filepath.Join("d", "f"), "rootfile.txt",
	})
}

// TestInProgress checks that ignoring files currently edited by Syncthing works
func TestInProgress(t *testing.T) {
	evLogger := events.NewLogger()
	ctx, cancel := context.WithCancel(context.Background())
	go evLogger.Serve(ctx)
	defer cancel()
	testCase := func(c chan<- fs.Event) {
		evLogger.Log(events.ItemStarted, map[string]string{
			"item": "inprogress",
		})
		sleepMs(100)
		c <- fs.Event{Name: "inprogress", Type: fs.NonRemove}
		sleepMs(1000)
		evLogger.Log(events.ItemFinished, map[string]any{
			"item": "inprogress",
		})
		sleepMs(100)
		c <- fs.Event{Name: "notinprogress", Type: fs.NonRemove}
		sleepMs(800)
	}

	expectedBatches := []expectedBatch{
		{[][]string{{"notinprogress"}}, 2000, 3500},
	}

	testScenario(t, "InProgress", testCase, expectedBatches, evLogger)
}

// TestDelay checks that recurring changes to the same path are delayed
// and different types separated and ordered correctly
func TestDelay(t *testing.T) {
	file := filepath.Join("parent", "file")
	delayed := "delayed"
	del := "deleted"
	delAfter := "deletedAfter"
	both := filepath.Join("parent", "sub", "both")
	testCase := func(c chan<- fs.Event) {
		sleepMs(200)
		c <- fs.Event{Name: file, Type: fs.NonRemove}
		delay := time.Duration(300) * time.Millisecond
		timer := time.NewTimer(delay)
		<-timer.C
		timer.Reset(delay)
		c <- fs.Event{Name: delayed, Type: fs.NonRemove}
		c <- fs.Event{Name: both, Type: fs.NonRemove}
		c <- fs.Event{Name: both, Type: fs.Remove}
		c <- fs.Event{Name: del, Type: fs.Remove}
		for range 9 {
			<-timer.C
			timer.Reset(delay)
			c <- fs.Event{Name: delayed, Type: fs.NonRemove}
		}
		c <- fs.Event{Name: delAfter, Type: fs.Remove}
		<-timer.C
	}

	// batches that we expect to receive with time interval in milliseconds
	expectedBatches := []expectedBatch{
		{[][]string{{file}}, 500, 2500},
		{[][]string{{delayed}, {both}, {del}}, 2500, 4500},
		{[][]string{{delayed}, {delAfter}}, 3600, 7000},
	}

	testScenario(t, "Delay", testCase, expectedBatches, nil)
}

// TestNoDelay checks that no delay occurs if there are no non-remove events
func TestNoDelay(t *testing.T) {
	mixed := "foo"
	del := "bar"
	testCase := func(c chan<- fs.Event) {
		c <- fs.Event{Name: mixed, Type: fs.NonRemove}
		c <- fs.Event{Name: mixed, Type: fs.Remove}
		c <- fs.Event{Name: del, Type: fs.Remove}
	}

	expectedBatches := []expectedBatch{
		{[][]string{{mixed}, {del}}, 500, 2000},
	}

	testScenario(t, "NoDelay", testCase, expectedBatches, nil)
}

func getEventPaths(dir *eventDir, dirPath string, a *aggregator) []string {
	var paths []string
	for childName, childDir := range dir.dirs {
		paths = append(paths, getEventPaths(childDir, filepath.Join(dirPath, childName), a)...)
	}
	for name := range dir.events {
		paths = append(paths, filepath.Join(dirPath, name))
	}
	return paths
}

func sleepMs(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func durationMs(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

func compareBatchToExpected(batch []string, expectedPaths []string) (missing []string, unexpected []string) {
	for _, expected := range expectedPaths {
		expected = filepath.Clean(expected)
		found := false
		for i, received := range batch {
			if expected == received {
				found = true
				batch = append(batch[:i], batch[i+1:]...)
				break
			}
		}
		if !found {
			missing = append(missing, expected)
		}
	}
	unexpected = append(unexpected, batch...)
	return missing, unexpected
}

func compareBatchToExpectedDirect(t *testing.T, batch []string, expectedPaths []string) {
	t.Helper()
	missing, unexpected := compareBatchToExpected(batch, expectedPaths)
	for _, p := range missing {
		t.Errorf("Did not receive event %s", p)
	}
	for _, p := range unexpected {
		t.Errorf("Received unexpected event %s", p)
	}
}

func testScenario(t *testing.T, name string, testCase func(c chan<- fs.Event), expectedBatches []expectedBatch, evLogger events.Logger) {
	t.Helper()

	if evLogger == nil {
		evLogger = events.NoopLogger
	}

	ctx, cancel := context.WithCancel(context.Background())
	eventChan := make(chan fs.Event)
	watchChan := make(chan Batch)

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = name
	a := newAggregator(ctx, folderCfg)
	a.notifyTimeout = testNotifyTimeout

	startTime := time.Now()
	go a.mainLoop(eventChan, watchChan, nil, defaultCfg, evLogger)

	sleepMs(20)

	go testCase(eventChan)

	testAggregatorOutput(t, watchChan, expectedBatches, startTime)

	cancel()
}

func testAggregatorOutput(t *testing.T, fsWatchChan <-chan Batch, expectedBatches []expectedBatch, startTime time.Time) {
	t.Helper()
	var received []string
	var elapsedTime time.Duration
	var batchIndex, innerIndex int
	timeout := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-timeout.C:
			t.Errorf("Timeout: Received only %d batches (%d expected)", batchIndex, len(expectedBatches))
			return
		case batch := <-fsWatchChan:
			received = batch.Paths
		}

		if batchIndex >= len(expectedBatches) {
			t.Errorf("Received batch %d, expected only %d", batchIndex+1, len(expectedBatches))
			continue
		}

		if !build.IsDarwin {
			now := time.Since(startTime)
			if innerIndex == 0 {
				switch {
				case now < durationMs(expectedBatches[batchIndex].afterMs):
					t.Errorf("Received batch %d after %v (too soon)", batchIndex+1, now)

				case now > durationMs(expectedBatches[batchIndex].beforeMs):
					t.Errorf("Received batch %d after %v (too late)", batchIndex+1, now)
				}
			} else if innerTime := now - elapsedTime; innerTime > timeoutWithinBatch {
				t.Errorf("Receive part %d of batch %d after %v (too late)", innerIndex+1, batchIndex+1, innerTime)
			}
			elapsedTime = now
		}

		expected := expectedBatches[batchIndex].paths[innerIndex]

		if len(received) != len(expected) {
			t.Errorf("Received %v events instead of %v for batch %v", len(received), len(expected), batchIndex+1)
		}
		missing, unexpected := compareBatchToExpected(received, expected)
		for _, p := range missing {
			t.Errorf("Did not receive event %s in batch %d (%d)", p, batchIndex+1, innerIndex+1)
		}
		for _, p := range unexpected {
			t.Errorf("Received unexpected event %s in batch %d (%d)", p, batchIndex+1, innerIndex+1)
		}

		if innerIndex == len(expectedBatches[batchIndex].paths)-1 {
			if batchIndex == len(expectedBatches)-1 {
				// received everything we expected to
				return
			}
			innerIndex = 0
			batchIndex++
		} else {
			innerIndex++
		}
	}
}

// TestAggregateMaxEventID checks that the batch watermark is the highest
// journal ID seen, and that it rides only on the last sub-batch of a flush
// (deletes last): persisting it after that batch's scan implies the earlier
// sub-batches were already scanned.
func TestAggregateMaxEventID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventChan := make(chan fs.Event)
	watchChan := make(chan Batch)

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = "maxeventid"
	a := newAggregator(ctx, folderCfg)
	a.notifyTimeout = testNotifyTimeout

	go a.mainLoop(eventChan, watchChan, nil, defaultCfg, events.NoopLogger)
	sleepMs(20)

	go func() {
		eventChan <- fs.Event{Name: "new", Type: fs.NonRemove, ID: 5}
		eventChan <- fs.Event{Name: "old", Type: fs.Remove, ID: 9}
	}()

	timeout := time.After(10 * time.Second)
	var batches []Batch
	for len(batches) < 2 {
		select {
		case b := <-watchChan:
			batches = append(batches, b)
		case <-timeout:
			t.Fatalf("received %d sub-batches, expected 2: %v", len(batches), batches)
		}
	}
	if len(batches[0].Paths) != 1 || batches[0].Paths[0] != "new" {
		t.Errorf("first sub-batch should be the non-remove: %v", batches[0])
	}
	if batches[0].MaxEventID != 0 {
		t.Errorf("watermark on non-final sub-batch: %d", batches[0].MaxEventID)
	}
	if len(batches[1].Paths) != 1 || batches[1].Paths[0] != "old" {
		t.Errorf("last sub-batch should be the remove: %v", batches[1])
	}
	if batches[1].MaxEventID != 9 {
		t.Errorf("watermark on final sub-batch = %d, expected 9 (max of 5 and 9)", batches[1].MaxEventID)
	}
}

// TestAggregateFlush verifies that the flush channel forces pending events
// out immediately, without waiting out the batching delay - the shutdown
// drain of the btrfs scan replay depends on it.
func TestAggregateFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventChan := make(chan fs.Event)
	watchChan := make(chan Batch)
	flushChan := make(chan chan struct{})

	folderCfg := defaultFolderCfg.Copy()
	folderCfg.ID = "flush"
	a := newAggregator(ctx, folderCfg)
	a.notifyTimeout = testNotifyTimeout

	go a.mainLoop(eventChan, watchChan, flushChan, defaultCfg, events.NoopLogger)
	sleepMs(20)

	// An event sits in the aggregator, younger than the batching delay.
	eventChan <- fs.Event{Name: "file.txt", Type: fs.NonRemove}
	sleepMs(20)

	// Flush; the batch must arrive well before the ~1s delay would allow.
	ack := make(chan struct{})
	select {
	case flushChan <- ack:
	case <-time.After(time.Second):
		t.Fatal("flush channel not serviced")
	}
	select {
	case b := <-watchChan:
		if len(b.Paths) != 1 || b.Paths[0] != "file.txt" {
			t.Fatalf("unexpected flushed batch: %v", b.Paths)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("flush did not emit the pending event")
	}
	select {
	case <-ack:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("flush ack not closed")
	}

	// A second flush with nothing pending must still ack and emit nothing.
	ack2 := make(chan struct{})
	flushChan <- ack2
	select {
	case <-ack2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("empty flush ack not closed")
	}
	select {
	case b := <-watchChan:
		t.Fatalf("empty flush emitted a batch: %v", b.Paths)
	case <-time.After(200 * time.Millisecond):
	}
}
