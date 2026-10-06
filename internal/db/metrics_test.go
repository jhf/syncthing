// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package db

import (
	"iter"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

// These tests pin down that an iterator-returning operation is accounted for
// across the iteration, not just across the call that built the iterator. The
// bug they exist to prevent: AllNeededGlobalFiles reported 0.12 ms per call on
// a database where the same call really took 9-20 seconds, because the span
// closed before a single row was fetched. Anyone reading the metrics panel would
// have concluded the query was free.

// fakeDB only implements the method under test. Embedding the interface means
// the rest of it does not have to exist here.
type fakeDB struct {
	DB

	rows  int
	delay time.Duration
}

func (f *fakeDB) AllNeededGlobalFiles(string, protocol.DeviceID, config.PullOrder, int, int) (iter.Seq[protocol.FileInfo], func() error) {
	return func(yield func(protocol.FileInfo) bool) {
			for i := range f.rows {
				// The work happens while the caller consumes, which is exactly what
				// the old accounting missed.
				time.Sleep(f.delay)
				if !yield(protocol.FileInfo{Name: string(rune('a' + i))}) {
					return
				}
			}
		}, func() error {
			return nil
		}
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

// state reads the three metrics for one (folder, operation) pair.
func state(t *testing.T, folder string) (current, total, seconds float64) {
	t.Helper()
	current = gaugeValue(t, metricCurrentOperations.WithLabelValues(folder, "AllNeededGlobalFiles"))
	total = counterValue(t, metricTotalOperationsCount.WithLabelValues(folder, "AllNeededGlobalFiles"))
	seconds = counterValue(t, metricTotalOperationSeconds.WithLabelValues(folder, "AllNeededGlobalFiles"))
	return
}

func TestMetricsAccountCoversIteration(t *testing.T) {
	const rows, delay = 4, 25 * time.Millisecond
	folder := t.Name()
	m := MetricsWrap(&fakeDB{rows: rows, delay: delay})

	it, errFn := m.AllNeededGlobalFiles(folder, protocol.DeviceID{}, config.PullOrderAlphabetic, 0, 0)

	// The operation is on the books from the moment it is handed out, which is
	// what makes operations_current mean "ongoing" rather than "preparing".
	if current, _, _ := state(t, folder); current != 1 {
		t.Errorf("operations_current before consuming: got %v, want 1", current)
	}

	var got int
	for range it {
		got++
	}
	if got != rows {
		t.Fatalf("iterated %d rows, want %d", got, rows)
	}

	current, total, seconds := state(t, folder)
	if current != 0 {
		t.Errorf("operations_current after consuming: got %v, want 0", current)
	}
	if total != 1 {
		t.Errorf("operations_total after consuming: got %v, want 1", total)
	}
	// The assertion with teeth: the recorded span has to contain the time spent
	// fetching rows, not just the time spent preparing the statement. A generous
	// floor, because the machine running this may be busy.
	if want := float64(rows) * delay.Seconds() * 0.5; seconds < want {
		t.Errorf("operation_seconds_total: got %vs, want at least %vs - the span "+
			"closed before the iteration finished", seconds, want)
	}

	// Callers may consume and then call the error function; that must not be
	// counted, or every operation with rows would count twice.
	if err := errFn(); err != nil {
		t.Fatal(err)
	}
	if _, total, _ := state(t, folder); total != 1 {
		t.Errorf("operations_total after consuming and calling the error function: got %v, want 1", total)
	}
}

func TestMetricsAccountAbandonedIterator(t *testing.T) {
	folder := t.Name()
	m := MetricsWrap(&fakeDB{rows: 100, delay: 10 * time.Millisecond})

	it, _ := m.AllNeededGlobalFiles(folder, protocol.DeviceID{}, config.PullOrderAlphabetic, 0, 0)
	for range it {
		break
	}

	current, total, seconds := state(t, folder)
	if current != 0 {
		t.Errorf("operations_current after abandoning: got %v, want 0 - the span leaks", current)
	}
	if total != 1 {
		t.Errorf("operations_total after abandoning: got %v, want 1", total)
	}
	if seconds <= 0 {
		t.Error("operation_seconds_total after abandoning: got 0, want the time actually spent")
	}
}

func TestMetricsAccountErrorFunctionWithoutIteration(t *testing.T) {
	// A caller that gets an iterator and decides not to use it still has to be
	// able to close the span. Only calling the error function is the shape used
	// by error paths that never range.
	folder := t.Name()
	m := MetricsWrap(&fakeDB{rows: 3, delay: 5 * time.Millisecond})

	_, errFn := m.AllNeededGlobalFiles(folder, protocol.DeviceID{}, config.PullOrderAlphabetic, 0, 0)
	if err := errFn(); err != nil {
		t.Fatal(err)
	}

	current, total, _ := state(t, folder)
	if current != 0 {
		t.Errorf("operations_current: got %v, want 0", current)
	}
	if total != 1 {
		t.Errorf("operations_total: got %v, want 1", total)
	}
}
