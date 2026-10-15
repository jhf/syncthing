// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"testing"
	"time"
)

// A live-but-busy aggregator must not fail the flush request: the pairing
// (and the next boot's diff-baseline replay) depends on it. Observed on
// yarlung 2026-10-15: runtime pairing aborted with "watch flush
// unavailable, watch not running" while the watcher had been up 45
// minutes, because the non-blocking send missed an aggregator blocked
// handing a batch to the scanner.
//
// Teeth: with the old default-branch send this test fails instantly (ok
// is false, no wait happens).
func TestWatchFlushWaitsForBusyAggregator(t *testing.T) {
	old := watchFlushTimeout
	t.Cleanup(func() { watchFlushTimeout = old })
	watchFlushTimeout = 2 * time.Second

	f := &folder{watchFlush: make(chan chan struct{})}
	received := make(chan struct{})
	go func() {
		time.Sleep(200 * time.Millisecond) // busy handing a batch to the scanner
		ack := <-f.watchFlush
		close(received)
		close(ack)
	}()

	start := time.Now()
	ack, ok := f.requestWatchFlush(context.Background())
	if !ok {
		t.Fatal("flush request failed against a live-but-busy aggregator")
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("flush returned after %v, before the aggregator accepted (non-blocking send?)", elapsed)
	}
	<-ack // the ack must close once the aggregator drains
	<-received
}

// A dead aggregator (watch failed or never started) must fail the flush
// request promptly; pairing then aborts and the boot does a full scan.
func TestWatchFlushTimesOutOnDeadAggregator(t *testing.T) {
	old := watchFlushTimeout
	t.Cleanup(func() { watchFlushTimeout = old })
	watchFlushTimeout = 50 * time.Millisecond

	f := &folder{watchFlush: make(chan chan struct{})} // no receiver

	start := time.Now()
	if _, ok := f.requestWatchFlush(context.Background()); ok {
		t.Fatal("flush request succeeded against a dead aggregator")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("flush request against dead aggregator took %v, want ~50 ms", elapsed)
	}
}
