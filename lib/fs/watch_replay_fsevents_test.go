// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build darwin && !kqueue && cgo && !ios

package fs

import (
	"context"
	"testing"
	"time"

	"github.com/syncthing/notify"
)

// collectWatchEvents receives from eventChan until want returns true for the
// accumulated events or the timeout passes. Received errors fail the test
// immediately unless tolerateErrors is set.
func collectWatchEvents(t *testing.T, eventChan <-chan Event, errChan <-chan error, timeout time.Duration, want func([]Event) bool) []Event {
	t.Helper()
	deadline := time.After(timeout)
	var events []Event
	for {
		select {
		case ev := <-eventChan:
			events = append(events, ev)
			if want(events) {
				return events
			}
		case err := <-errChan:
			t.Fatalf("watch error while collecting events: %v", err)
		case <-deadline:
			return events
		}
	}
}

func hasEvent(events []Event, name string, typ EventType) bool {
	for _, ev := range events {
		if ev.Name == name && ev.Type == typ {
			return true
		}
	}
	return false
}

// TestFSEventsJournalReplay checks the core of scan-free restarts: a watcher
// seeded with a journal position from before some filesystem changes
// delivers those changes even though they happened while no watcher was
// running. The unseeded control proves the delivery is caused by the seed,
// not by FSEvents otherwise reporting old changes.
func TestFSEventsJournalReplay(t *testing.T) {
	ResetWatchReplay()
	defer ResetWatchReplay()

	name := "journalreplay"
	if err := testFs.MkdirAll(name, 0o755); err != nil {
		t.Fatal(err)
	}
	defer testFs.RemoveAll(name)

	// Watch while creating a file; record the journal position of the
	// event. This stands in for "last position the previous process
	// accounted for".
	ctx1, cancel1 := context.WithCancel(context.Background())
	eventChan1, errChan1, err := testFs.Watch(name, fakeMatcher{}, ctx1, false)
	if err != nil {
		t.Fatal(err)
	}
	createTestFile(name, "a.txt")
	events1 := collectWatchEvents(t, eventChan1, errChan1, 15*time.Second, func(evs []Event) bool {
		return hasEvent(evs, name+"/a.txt", NonRemove)
	})
	if !hasEvent(events1, name+"/a.txt", NonRemove) {
		t.Fatalf("no event for a.txt, got %v", events1)
	}
	var lastID uint64
	for _, ev := range events1 {
		if ev.ID > lastID {
			lastID = ev.ID
		}
	}
	if lastID == 0 {
		t.Fatal("events carry no journal IDs; replay cannot work")
	}
	cancel1()

	// "Downtime": no watcher anywhere while these happen.
	createTestFile(name, "b.txt")
	if err := testFs.Remove(name + "/a.txt"); err != nil {
		t.Fatal(err)
	}
	// Let the changes age firmly into journal history: events recorded
	// around stream-creation time can be delivered as live events by a
	// redelivery race regardless of seeding, which would make the test
	// prove nothing. A few seconds puts them beyond that race window.
	time.Sleep(5 * time.Second)

	// Seeded watcher: the downtime changes must be delivered.
	SetWatchReplaySince(lastID)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	eventChan2, errChan2, err := testFs.Watch(name, fakeMatcher{}, ctx2, false)
	if err != nil {
		t.Fatal(err)
	}
	events2 := collectWatchEvents(t, eventChan2, errChan2, 15*time.Second, func(evs []Event) bool {
		return hasEvent(evs, name+"/b.txt", NonRemove) && hasEvent(evs, name+"/a.txt", Remove)
	})
	if !hasEvent(events2, name+"/b.txt", NonRemove) {
		t.Errorf("seeded replay did not deliver b.txt creation, got %v", events2)
	}
	if !hasEvent(events2, name+"/a.txt", Remove) {
		t.Errorf("seeded replay did not deliver a.txt removal, got %v", events2)
	}
}

// TestFSEventsNoReplayWithoutSeed is the control: without a seed, a watcher
// started after changes happened does not report them (it starts at the
// current journal position). The change is aged past the stream-creation
// redelivery race window first (see TestFSEventsJournalReplay).
func TestFSEventsNoReplayWithoutSeed(t *testing.T) {
	ResetWatchReplay()
	defer ResetWatchReplay()

	name := "noreplay"
	if err := testFs.MkdirAll(name, 0o755); err != nil {
		t.Fatal(err)
	}
	defer testFs.RemoveAll(name)

	createTestFile(name, "before.txt")
	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventChan, errChan, err := testFs.Watch(name, fakeMatcher{}, ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	events := collectWatchEvents(t, eventChan, errChan, 3*time.Second, func(evs []Event) bool {
		return hasEvent(evs, name+"/before.txt", NonRemove)
	})
	if hasEvent(events, name+"/before.txt", NonRemove) {
		t.Fatalf("unseeded watcher reported pre-watch change: %v", events)
	}
}

// TestWatchEventClassify unit-tests the journal-failure classifier. Probing
// on macOS 26 showed that seeding a stream with a journal position the log
// no longer covers is sometimes silently clamped WITHOUT the documented
// MustScanSubDirs flag, so an OS-level gap test is not deterministic; the
// over-old-watermark case is handled by the age gate in the model
// (maxWatchReplayAge). MustScanSubDirs is an overflow, not fatal: it fires
// both for an uncovered seed and for live-delivery overload on a busy volume
// (observed on an unseeded live stream, 2026-10-08), and the stream keeps
// delivering afterwards, so a full-scan event is the right heal for all of
// it.
func TestWatchEventClassify(t *testing.T) {
	defer ResetWatchReplay()

	fatal := []notify.Event{
		notify.FSEventsEventIdsWrapped,
		notify.FSEventsRootChanged,
		notify.FSEventsCreated | notify.FSEventsRootChanged, // combined
	}
	for _, e := range fatal {
		if class := watchEventClassify(fakeNotifyEventInfo{e}); class != watchEventFatalClass {
			t.Errorf("event %v classified %v, want fatal", e, class)
		}
	}
	overflow := []notify.Event{
		notify.FSEventsMustScanSubDirs,
		notify.FSEventsUserDropped,
		notify.FSEventsKernelDropped,
		notify.FSEventsModified | notify.FSEventsMustScanSubDirs, // combined
		notify.FSEventsModified | notify.FSEventsUserDropped,    // combined
	}
	for _, e := range overflow {
		if class := watchEventClassify(fakeNotifyEventInfo{e}); class != watchEventOverflow {
			t.Errorf("event %v classified %v, want overflow", e, class)
		}
	}
	benign := []notify.Event{
		notify.Create,
		notify.Remove,
		notify.Write,
		notify.Rename,
		notify.FSEventsInodeMetaMod,
		notify.FSEventsHistoryDone,
	}
	for _, e := range benign {
		if class := watchEventClassify(fakeNotifyEventInfo{e}); class != watchEventOK {
			t.Errorf("event %v classified %v, want OK", e, class)
		}
	}
}

type fakeNotifyEventInfo struct{ e notify.Event }

func (f fakeNotifyEventInfo) Event() notify.Event { return f.e }
func (f fakeNotifyEventInfo) Path() string        { return "" }
func (f fakeNotifyEventInfo) Sys() interface{}    { return nil }
