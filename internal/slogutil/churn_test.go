// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"bytes"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestChurnSchemaRingsWrapAndNoAutomaticDumps(t *testing.T) {
	before := LogTrapStatus()
	detail, overview := newLogRing(5), newLogRing(3)
	h := &formattingHandler{opts: &formattingOptions{ring: detail, overview: overview}}
	samples := `[{"path":"dir/a \"quoted\".txt","outcome":"not_checked"}]`
	emit := func(msg string) {
		t.Helper()
		rec := slog.NewRecord(time.Now(), slog.LevelInfo, msg, 0)
		rec.Add("churn.schema", 1, "bucket_kind", "overflow", "parent", "other_dirs", "failures", 1000, "bucket_overflow", 992, "samples_omitted", 996, "samples", samples)
		if err := h.Handle(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
	}
	emit("Pull churn summary")
	var pre bytes.Buffer
	overview.WriteDump(&pre, DefaultLineFormat)
	if !strings.Contains(pre.String(), "Pull churn summary") {
		t.Fatal("INFO prehistory unavailable without DEBUG")
	}
	for range 8 {
		emit("Pull churn totals")
	}
	emit("Pull churn detected")
	for _, tc := range []struct {
		name string
		r    *logRing
		want uint64
	}{{"detail", detail, 5}, {"overview", overview, 7}} {
		lines, _, dropped := tc.r.drainFrom(1)
		if dropped != tc.want {
			t.Fatalf("%s dropped=%d want %d", tc.name, dropped, tc.want)
		}
		last := lines[len(lines)-1].Message
		for _, s := range []string{"churn.schema=1", "bucket_overflow=992", "samples_omitted=996", "samples=" + strconv.Quote(samples)} {
			if !strings.Contains(last, s) {
				t.Fatalf("%s missing schema/quoting %q: %s", tc.name, s, last)
			}
		}
		var post bytes.Buffer
		tc.r.WriteDump(&post, DefaultLineFormat)
		if !strings.Contains(post.String(), "log lines dropped") || !strings.Contains(post.String(), "Pull churn detected") {
			t.Fatalf("%s wrap diagnostics missing: %s", tc.name, post.String())
		}
	}
	// These are explicit in-memory test dumps, not producer-triggered capture.
	// No trap registration, firing, or disk dump is introduced by the markers.
	if !reflect.DeepEqual(before, LogTrapStatus()) {
		t.Fatal("churn markers installed or fired an automatic dump trap")
	}
}
