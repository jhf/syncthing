// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

// busyThenDB fails Update with lock contention a set number of times
// before delegating to the final error (nil = success).
type busyThenDB struct {
	db.DB
	busyErr    error
	busyLeft   atomic.Int32
	finalErr   error
	updateCall atomic.Int32
}

func (b *busyThenDB) Update(string, protocol.DeviceID, []protocol.FileInfo, ...db.UpdateOption) error {
	b.updateCall.Add(1)
	if b.busyLeft.Add(-1) >= 0 {
		return b.busyErr
	}
	return b.finalErr
}

func shrinkLockBackoff(t *testing.T) {
	t.Helper()
	old := indexLockBackoff
	t.Cleanup(func() { indexLockBackoff = old })
	indexLockBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
}

// An index-update batch hitting SQLITE_BUSY must be retried, not returned
// as an error — upstream, the error kills the device connection and the
// same batch comes back after reconnect, flapping forever while a
// background writer (scan chunk commit) holds the lock. Observed live on
// yarlung 2026-10-14: ~20 s flap loop, prevSeq frozen, during a fallback
// full scan.
//
// Teeth: without the retry loop the first test fails on the first busy
// error; the second fails because exhaustion must not retry forever.
func TestIndexUpdateRetriesLockContention(t *testing.T) {
	shrinkLockBackoff(t)
	// Two contentions, three backoff entries: third attempt succeeds.
	d := &busyThenDB{
		busyErr:  fmt.Errorf("update: deviceidxlocked: database is locked (5) (SQLITE_BUSY)"),
		finalErr: nil,
	}
	d.busyLeft.Store(2)
	s := &indexHandler{folder: "home", sdb: d}

	if err := s.updateWithLockRetry(protocol.LocalDeviceID, nil); err != nil {
		t.Fatalf("batch with transient contention returned error: %v", err)
	}
	if got := d.updateCall.Load(); got != 3 {
		t.Fatalf("Update called %d times, want 3 (2 retries then success)", got)
	}
}

func TestIndexUpdateGivesUpAfterBackoffExhausted(t *testing.T) {
	shrinkLockBackoff(t)
	busyErr := fmt.Errorf("update: database is locked")
	d := &busyThenDB{busyErr: busyErr, finalErr: busyErr}
	d.busyLeft.Store(1 << 30) // always busy
	s := &indexHandler{folder: "home", sdb: d}

	if err := s.updateWithLockRetry(protocol.LocalDeviceID, nil); !errors.Is(err, busyErr) {
		t.Fatalf("exhausted retries returned %v, want the busy error", err)
	}
	if got, want := d.updateCall.Load(), int32(1+len(indexLockBackoff)); got != want {
		t.Fatalf("Update called %d times, want %d (initial + %d retries)", got, want, len(indexLockBackoff))
	}
}

// A non-contention database error must surface immediately: it is
// structural, and retrying it would only delay the (correct) connection
// teardown.
func TestIndexUpdateDoesNotRetryStructuralErrors(t *testing.T) {
	shrinkLockBackoff(t)
	perm := fmt.Errorf("update: UNIQUE constraint failed: files.name")
	d := &busyThenDB{busyErr: perm, finalErr: perm}
	d.busyLeft.Store(1 << 30) // every call returns the structural error
	s := &indexHandler{folder: "home", sdb: d}

	if err := s.updateWithLockRetry(protocol.LocalDeviceID, nil); !errors.Is(err, perm) {
		t.Fatalf("structural error returned %v, want %v", err, perm)
	}
	if got := d.updateCall.Load(); got != 1 {
		t.Fatalf("Update called %d times, want 1 (no retry on structural error)", got)
	}
}

func TestIsLockContentionMatchesObservedDriverMessages(t *testing.T) {
	busy := []error{
		// modernc (nocgo), verbatim from yarlung serve.log 2026-10-14
		fmt.Errorf("handling index-update for home: update: deviceidxlocked: database is locked (5) (SQLITE_BUSY)"),
		// mattn go-sqlite3 (cgo)
		fmt.Errorf("update: database is locked"),
		fmt.Errorf("update: database table is locked"),
	}
	for _, err := range busy {
		if !db.IsLockContention(err) {
			t.Errorf("IsLockContention(%q) = false, want true", err)
		}
	}
	other := []error{
		nil,
		fmt.Errorf("update: UNIQUE constraint failed: files.name"),
		fmt.Errorf("update: no such table: files"),
		errors.New("connection reset by peer"),
	}
	for _, err := range other {
		if db.IsLockContention(err) {
			t.Errorf("IsLockContention(%q) = true, want false", err)
		}
	}
}
