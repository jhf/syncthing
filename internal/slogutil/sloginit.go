// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"
)

var (
	GlobalRecorder    = &lineRecorder{level: -1000}
	ErrorRecorder     = &lineRecorder{level: slog.LevelError}
	DefaultLineFormat = LineFormat{
		TimestampFormat: time.DateTime,
		LevelString:     true,
	}
	globalLevels = &levelTracker{
		levels: make(map[string]slog.Level),
		descrs: make(map[string]string),
	}
	globalRing      = newLogRing(logRingCapacityFromEnv())
	globalOverview  = newLogRing(overviewRingCapacityFromEnv())
	globalFormatter = &formattingOptions{
		LineFormat: DefaultLineFormat,
		recs:       []*lineRecorder{GlobalRecorder, ErrorRecorder},
		ring:       globalRing,
		overview:   globalOverview,
		out:        logWriter(),
	}
	globalDrainer *logDrainer
	slogDef       = slog.New(&formattingHandler{opts: globalFormatter})
)

func logWriter() io.Writer {
	if os.Getenv("LOGGER_DISCARD") != "" {
		// Hack to completely disable logging, for example when running
		// benchmarks.
		return io.Discard
	}

	return os.Stdout
}

func init() {
	slog.SetDefault(slogDef)
	globalDrainer = startLogDrainer(globalRing, globalFormatter.out, func() LineFormat {
		return globalFormatter.LineFormat
	})
}

// FlushLogRing waits until all log lines currently in the ring have been
// written to the output, or the timeout expires. Call before process exit.
func FlushLogRing(timeout time.Duration) {
	if globalDrainer != nil {
		globalDrainer.flush(timeout)
	}
}

// DumpLogRingToFile writes the current rings to the given file, oldest
// first within each section: the overview ring (flood-resistant, spans
// hours) first, then the full detail ring (the unfiltered recent past).
// Intended for panic handling and log-trap dumps: the rings hold the last
// N lines even when the crash destroys the regular log stream.
func DumpLogRingToFile(path string) error {
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(fd, "=== overview ring: last %d lines, bulk per-file lines excluded ===\n", len(globalOverview.lines))
	globalOverview.WriteDump(fd, DefaultLineFormat)
	_, _ = fmt.Fprintf(fd, "=== detail ring: last %d lines, unfiltered ===\n", len(globalRing.lines))
	globalRing.WriteDump(fd, DefaultLineFormat)
	return fd.Close()
}
