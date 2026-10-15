// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Watch root escalations are the points where the watch pipeline stops
// naming changed paths and asks for a full-folder scan instead. On a
// multi-million-item folder each one costs a full walk plus a full
// deletion sweep (measured 2026-11-24 on the Mac: a whole-folder rescan
// loop of 2 h 38 min, ~3 full walks, with nothing in the log saying why -
// every escalation site logged at debug only). Each escalation is now
// counted and logged at INFO, so the trigger is attributable from the
// overview ring and the log file, and a log trap can arm on the line to
// capture the verbose window around the next one.
const (
	// The backend channel filled up; queued events were drained and lost.
	WatchEscalationBufferOverflow = "buffer_overflow"
	// The backend flagged dropped events (FSEvents MustScanSubDirs /
	// UserDropped / KernelDropped, including the notify fork's synthetic
	// flag for a full consumer channel).
	WatchEscalationBackendDropped = "backend_dropped"
	// The aggregator's distinct-path window overflowed and factoring
	// into common prefixes could not shrink it.
	WatchEscalationAggregatorOverflow = "aggregator_overflow"
)

// WatchEscalationMessage is the log message of every escalation line; it
// doubles as a stable log trap matcher.
const WatchEscalationMessage = "Watch escalated to a full-folder scan"

var metricWatchRootEscalations = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "syncthing",
	Subsystem: "fs",
	Name:      "watch_root_escalations_total",
	Help:      "Total number of watch escalations to a full-folder scan, per filesystem root and reason",
}, []string{"root", "reason"})

// watchEscalationLogInterval bounds the log rate per root and reason: a
// drop-flag storm can escalate many times a minute (≈22/min measured
// 2026-10-13), while the counter keeps the exact total. Variable for tests.
var watchEscalationLogInterval = time.Minute

// watchEscalationLog emits the escalation line. Variable for tests.
var watchEscalationLog = func(args ...any) {
	slog.Info(WatchEscalationMessage, args...)
}

type watchEscalationKey struct{ root, reason string }

// RegisterWatchEscalationMetrics creates the escalation series for a root
// at zero for every reason. A labeled counter that was never touched is
// absent from /metrics, which made "no escalation yet" indistinguishable
// from "metric missing" (observed on dev.186, 2026-11-24).
func RegisterWatchEscalationMetrics(root string) {
	for _, reason := range []string{WatchEscalationBufferOverflow, WatchEscalationBackendDropped, WatchEscalationAggregatorOverflow} {
		metricWatchRootEscalations.WithLabelValues(root, reason)
	}
}

var watchEscalations = struct {
	sync.Mutex
	last       map[watchEscalationKey]time.Time
	suppressed map[watchEscalationKey]int
}{
	last:       make(map[watchEscalationKey]time.Time),
	suppressed: make(map[watchEscalationKey]int),
}

// NoteWatchRootEscalation counts one escalation and logs it, at most once
// per watchEscalationLogInterval per root and reason; a logged line
// carries the number of escalations suppressed since the previous one.
func NoteWatchRootEscalation(root, reason string, attrs ...any) {
	metricWatchRootEscalations.WithLabelValues(root, reason).Inc()

	key := watchEscalationKey{root, reason}
	now := time.Now()
	watchEscalations.Lock()
	if last, ok := watchEscalations.last[key]; ok && now.Sub(last) < watchEscalationLogInterval {
		watchEscalations.suppressed[key]++
		watchEscalations.Unlock()
		return
	}
	suppressed := watchEscalations.suppressed[key]
	watchEscalations.last[key] = now
	watchEscalations.suppressed[key] = 0
	watchEscalations.Unlock()

	args := append([]any{"root", root, "reason", reason, "suppressedSinceLast", suppressed}, attrs...)
	watchEscalationLog(args...)
}
