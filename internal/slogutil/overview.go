// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"log/slog"
	"os"
	"strconv"
)

// The overview ring is a second, smaller ring next to the detail ring
// (ring.go). Motivation, observed live 2026-10-20 during a bulk delete
// wave: upstream logs one INF line per affected file ("Deleted file",
// "Updated file metadata", ...), thousands per minute, which wraps the
// 32k-line detail ring in under a minute and rotates the 10 MiB log file
// in ~40 s — the recent history evaporates exactly when something
// interesting is happening. The overview ring excludes those known bulk
// per-file success lines and therefore spans hours, not seconds: it is
// the "what has the system been doing" stream, while the detail ring
// remains the unfiltered forensic buffer. WARN and above always pass, so
// errors survive any flood.
//
// Classification uses the raw call-site message string (constant at each
// slog call site), matched exactly — not the formatted line, and not a
// substring/regex over rendered output. Upstream call sites are
// untouched; this is purely a sink-layer filter. Both rings are included
// in every ring dump (panic, log traps).

const defaultOverviewRingLines = 2048

// overviewExcluded lists the raw slog messages of upstream's per-file
// success lines (lib/model/folder_sendrecv.go). Failures ("Failed to
// ...") are deliberately NOT excluded: they are the diagnostic part and
// rarer.
var overviewExcluded = map[string]bool{
	"Deleted file":                 true,
	"Deleted directory":            true,
	"Deleted symlink":              true,
	"Renamed file":                 true,
	"Updated file metadata":        true,
	"Created or updated directory": true,
	"Created or updated symlink":   true,
}

// overviewWorthy reports whether a line with the given raw call-site
// message and level belongs in the overview ring.
func overviewWorthy(rawMessage string, level slog.Level) bool {
	if level >= slog.LevelWarn {
		return true
	}
	return !overviewExcluded[rawMessage]
}

func overviewRingCapacityFromEnv() int {
	if s := os.Getenv("STLOGOVERVIEWLINES"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return defaultOverviewRingLines
}
