// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The consumer side of watch root escalations (lib/fs/watch_escalation.go
// is the producer side): where a full-folder watch batch was dequeued, and
// whether it ran nested inside a running scan. 2026-11-24 on the Mac a
// root batch serviced between the chunks of a subtree scan ran a full walk
// plus a full deletion sweep nested inside it - the fullScanActive guard
// only skips the nested batch when the outer scan is itself full-tree - and
// the folder stayed in "scanning" for 2 h 38 min without a single line in
// the log saying so.

// slowScanLogThreshold is the duration from which a finished scan logs one
// INFO line. Ordinary watch-triggered subtree scans finish in well under a
// second; a minute-class scan is the event worth an evidence trail.
const slowScanLogThreshold = time.Minute

var metricFolderRootWatchBatches = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "syncthing",
	Subsystem: "model",
	Name:      "folder_root_watch_batches_total",
	Help:      "Total number of full-folder watch batches dequeued, per folder ID and where they were dequeued (serve, between-chunks, between-chunks-skipped)",
}, []string{"folder", "where"})

// isRootWatchBatch reports whether a watch batch asks for a full-folder
// scan, in either of its spellings.
func isRootWatchBatch(paths []string) bool {
	for _, p := range paths {
		if p == "." || p == "" {
			return true
		}
	}
	return false
}

// noteRootWatchBatch counts and logs a dequeued full-folder watch batch.
// skipped reports that the batch is dropped because a full-tree scan is
// already running (serviceQueuedWatch's guard).
func (f *folder) noteRootWatchBatch(ctx context.Context, where string, skipped bool) {
	if skipped {
		where += "-skipped"
	}
	metricFolderRootWatchBatches.WithLabelValues(f.folderID, where).Inc()
	f.sl.InfoContext(ctx, "Full-folder watch batch dequeued", slog.String("where", where), slog.Bool("nested", where == "between-chunks"))
}

func firstOrEmpty(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}
