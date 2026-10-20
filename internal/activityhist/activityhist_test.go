// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package activityhist

import (
	"path/filepath"
	"testing"
	"time"
)

func day(s string) time.Time {
	t, err := time.ParseInLocation("2006-01-02 15:04", s, time.Local)
	if err != nil {
		panic(err)
	}
	return t
}

func TestAddBuckets(t *testing.T) {
	h := &Hist{}
	ts := day("2026-10-20 14:37") // a Tuesday
	h.Add(ts)
	minute := 14*60 + 37
	if h.MinuteOfDay[minute] != 1 {
		t.Errorf("MinuteOfDay[%d] = %d, want 1", minute, h.MinuteOfDay[minute])
	}
	weekHour := int(ts.Weekday())*24 + 14
	if h.HourOfWeek[weekHour] != 1 {
		t.Errorf("HourOfWeek[%d] = %d, want 1", weekHour, h.HourOfWeek[weekHour])
	}
	if !h.Days["2026-10-20"] {
		t.Error("day not recorded")
	}
}

func TestDecayHalvesWeekly(t *testing.T) {
	h := &Hist{Days: map[string]bool{}}
	first := day("2026-10-01 03:00")
	h.Add(first)
	for i := 0; i < 99; i++ {
		h.Add(first)
	}
	// Eight days later: decay fires once on the next Add (101 halved).
	later := first.Add(8 * 24 * time.Hour)
	h.Add(later)
	if got := h.MinuteOfDay[180]; got != 50 {
		t.Errorf("after decay MinuteOfDay[180] = %d, want 50", got)
	}
	if h.LastDecay.IsZero() {
		t.Error("LastDecay not set by decay")
	}
}

func TestPersistRoundTrip(t *testing.T) {
	h := &Hist{Days: map[string]bool{}, DurationsNs: map[string]int64{}}
	h.Add(day("2026-10-20 22:10"))
	h.RecordDuration("db-defrag", 7*time.Minute)
	path := filepath.Join(t.TempDir(), "hist.json")
	if err := h.Save(path); err != nil {
		t.Fatal(err)
	}
	back, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if back.MinuteOfDay[22*60+10] != 1 || !back.Days["2026-10-20"] {
		t.Error("counters or days lost in round trip")
	}
	if back.ExpectedDuration("db-defrag") != 7*time.Minute {
		t.Errorf("duration = %v, want 7m", back.ExpectedDuration("db-defrag"))
	}
}

func TestMergeFrom(t *testing.T) {
	a := &Hist{Days: map[string]bool{}, DurationsNs: map[string]int64{}}
	a.Add(day("2026-10-19 03:00"))
	b := &Hist{Days: map[string]bool{}, DurationsNs: map[string]int64{"db-defrag": int64(5 * time.Minute)}}
	b.Add(day("2026-10-20 03:00"))
	b.Add(day("2026-10-20 03:00"))
	a.MergeFrom(b)
	if a.MinuteOfDay[180] != 3 {
		t.Errorf("merged minute counter = %d, want 3", a.MinuteOfDay[180])
	}
	if len(a.Days) != 2 {
		t.Errorf("merged days = %d, want 2", len(a.Days))
	}
	// A fresh measurement in the receiver beats a stale one in the merge.
	fresh := &Hist{DurationsNs: map[string]int64{"db-defrag": int64(9 * time.Minute)}}
	c := &Hist{DurationsNs: map[string]int64{"db-defrag": int64(time.Minute)}}
	fresh.MergeFrom(c)
	if got := fresh.ExpectedDuration("db-defrag"); got != 9*time.Minute {
		t.Errorf("fresh duration overwritten by stale merge: %v", got)
	}
	// A job only the source knows about carries over.
	d := &Hist{}
	d.MergeFrom(c)
	if got := d.ExpectedDuration("db-defrag"); got != time.Minute {
		t.Errorf("carried duration = %v, want 1m", got)
	}
}

func TestDurationEMA(t *testing.T) {
	h := &Hist{}
	h.RecordDuration("db-defrag", 10*time.Minute)
	h.RecordDuration("db-defrag", 6*time.Minute)
	if got := h.ExpectedDuration("db-defrag"); got != 8*time.Minute {
		t.Errorf("EMA = %v, want 8m", got)
	}
	if got := h.ExpectedDuration("never-run"); got != 0 {
		t.Errorf("unknown job = %v, want 0 (admissible)", got)
	}
}

func TestPickDefaultsToSleepWindow(t *testing.T) {
	h := &Hist{Days: map[string]bool{}} // no data
	pick, runLen := h.Pick(day("2026-10-20 12:00"))
	hour := pick.Hour()
	if hour < 0 || hour > 6 { // middle of 01:00-06:00 = 03:30, ±15m jitter
		t.Errorf("pick = %v, want ~03:30 (window 00:45-06:15 with jitter)", pick)
	}
	if runLen != 300 {
		t.Errorf("runLen = %d, want 300 (default window)", runLen)
	}
}

func TestPickFindsIdleRun(t *testing.T) {
	h := &Hist{Days: map[string]bool{}}
	// Ten days of identical pattern: busy 08:00-23:00, quiet 00:00-06:00.
	for d := 0; d < 10; d++ {
		date := day("2026-10-05 00:00").Add(time.Duration(d) * 24 * time.Hour)
		h.Days[date.Format("2006-01-02")] = true
		for m := 8 * 60; m < 23*60; m++ {
			h.MinuteOfDay[m] += 100
		}
		for m := 23*60 + 30; m < 24*60; m++ { // evening wind-down, half busy
			h.MinuteOfDay[m] += 50
		}
	}
	pick, runLen := h.Pick(day("2026-10-20 12:00"))
	if pick.Hour() < 1 || pick.Hour() > 5 {
		t.Errorf("pick = %v, want middle of the 00:00-06:00 quiet run (~03:00)", pick)
	}
	if runLen < 5*60 {
		t.Errorf("runLen = %d, want at least the 5 quiet hours", runLen)
	}
}

func TestPickJitterDeterministic(t *testing.T) {
	h := &Hist{Days: map[string]bool{}}
	d := day("2026-10-20 12:00")
	p1, _ := h.Pick(d)
	p2, _ := h.Pick(d)
	if !p1.Equal(p2) {
		t.Errorf("same-day picks differ: %v vs %v", p1, p2)
	}
	// Across a week the jitter should visibly move the pick.
	seen := map[int]bool{}
	for i := 0; i < 7; i++ {
		p, _ := h.Pick(d.Add(time.Duration(i) * 24 * time.Hour))
		seen[p.Hour()*60+p.Minute()] = true
	}
	if len(seen) < 2 {
		t.Errorf("jitter never moved the pick across 7 days: %v", seen)
	}
}
