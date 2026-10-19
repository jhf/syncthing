// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
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
	globalFormatter = &formattingOptions{
		LineFormat: DefaultLineFormat,
		recs:       []*lineRecorder{GlobalRecorder, ErrorRecorder},
		ring:       globalRing,
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

// DumpLogRingToFile writes the entire current log ring to the given file,
// oldest first. Intended for panic handling: the ring holds the last N
// lines even when the crash destroys the regular log stream.
func DumpLogRingToFile(path string) error {
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	globalRing.WriteDump(fd, DefaultLineFormat)
	return fd.Close()
}
