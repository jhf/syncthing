// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOverviewWorthy(t *testing.T) {
	cases := []struct {
		msg   string
		level slog.Level
		want  bool
	}{
		// Bulk per-file success lines are excluded at INF...
		{"Deleted file", slog.LevelInfo, false},
		{"Deleted directory", slog.LevelInfo, false},
		{"Deleted symlink", slog.LevelInfo, false},
		{"Renamed file", slog.LevelInfo, false},
		{"Updated file metadata", slog.LevelInfo, false},
		{"Created or updated directory", slog.LevelInfo, false},
		{"Created or updated symlink", slog.LevelInfo, false},
		// ...but pass at WARN and above regardless.
		{"Deleted file", slog.LevelWarn, true},
		{"Updated file metadata", slog.LevelError, true},
		// Failures and anything unknown stay in the overview.
		{"Failed to delete file", slog.LevelInfo, true},
		{"Folder summary", slog.LevelInfo, true},
		{"", slog.LevelDebug, true},
	}
	for _, tc := range cases {
		if got := overviewWorthy(tc.msg, tc.level); got != tc.want {
			t.Errorf("overviewWorthy(%q, %v) = %v, want %v", tc.msg, tc.level, got, tc.want)
		}
	}
}

func TestOverviewRingSurvivesFlood(t *testing.T) {
	// A flood of excluded lines must not evict older overview lines.
	ring := newLogRing(8)
	over := newLogRing(4)
	h := &formattingHandler{opts: &formattingOptions{ring: ring, overview: over}}

	_ = h.Handle(nil, slog.NewRecord(time.Now(), slog.LevelInfo, "Folder summary", 0))     // overview-worthy
	_ = h.Handle(nil, slog.NewRecord(time.Now(), slog.LevelInfo, "Folder summary two", 0)) // overview-worthy
	for i := 0; i < 100; i++ {
		_ = h.Handle(nil, slog.NewRecord(time.Now(), slog.LevelInfo, "Deleted file", 0)) // flood, excluded
	}

	lines, _, _ := over.drainFrom(1)
	if len(lines) != 2 {
		t.Fatalf("overview holds %d lines, want 2 (flood must not enter)", len(lines))
	}
	if !strings.HasPrefix(lines[0].Message, "Folder summary") {
		t.Errorf("oldest overview line = %q, want the pre-flood summary", lines[0].Message)
	}
	// The detail ring kept the flood (unfiltered forensic buffer).
	dLines, _, dropped := ring.drainFrom(1)
	if len(dLines)+int(dropped) != 102 {
		t.Errorf("detail ring saw %d+%d lines, want 102", len(dLines), dropped)
	}
}

func TestDumpLogRingToFileHasBothSections(t *testing.T) {
	globalRing.append(Line{When: time.Now(), Level: slog.LevelInfo, Message: "detail marker line"})
	globalOverview.append(Line{When: time.Now(), Level: slog.LevelInfo, Message: "overview marker line"})

	path := filepath.Join(t.TempDir(), "dump.log")
	if err := DumpLogRingToFile(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	ovIdx := strings.Index(s, "=== overview ring:")
	detIdx := strings.Index(s, "=== detail ring:")
	if ovIdx < 0 || detIdx < 0 || ovIdx > detIdx {
		t.Fatalf("dump sections missing or out of order (overview %d, detail %d)", ovIdx, detIdx)
	}
	if !strings.Contains(s[:detIdx], "overview marker line") {
		t.Error("overview marker missing from overview section")
	}
	if !strings.Contains(s[detIdx:], "detail marker line") {
		t.Error("detail marker missing from detail section")
	}
}
