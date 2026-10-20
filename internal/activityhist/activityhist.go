// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// Package activityhist records when this installation is busy, cyclically:
// a minute-of-day histogram (1440 buckets, minute precision) and an
// hour-of-week histogram (168 buckets, weekday/weekend patterns). The
// maintenance scheduler (lib/syncthing/maintenance.go) uses it to place
// expensive work — currently the database defrag restart — in the middle
// of the deepest predicted idle run instead of at an arbitrary restart.
//
// The data source is the log sink: on this system the log rate tracks
// sync churn almost perfectly (tens of thousands of per-file lines during
// a pull burst, a handful of lines while idle), so one increment per log
// record is a faithful, zero-upstream-footprint activity signal. A
// constant background (health polling etc.) is harmless: the picker works
// on relative minima.
//
// Counters are halved weekly (exponential decay), so a changed life
// pattern — new job, night batches — is reflected within weeks rather
// than fossilized by old history.
package activityhist

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sync"
	"time"
)

const (
	minutesPerDay = 24 * 60
	hoursPerWeek  = 7 * 24

	// minDaysForPrediction: below this many distinct observed days the
	// histograms are noise; the picker falls back to the default window.
	minDaysForPrediction = 7

	// decayInterval: counters are halved once per this period.
	decayInterval = 7 * 24 * time.Hour

	// Default assumed idle window, local time: humans sleep. The picker
	// lands in the middle of it, plus jitter.
	defaultIdleStartMin = 1 * 60 // 01:00
	defaultIdleEndMin   = 6 * 60 // 06:00

	// smoothRadiusMin: minute scores are smoothed by a moving sum of this
	// radius, so a two-minute cron blip does not split an idle run.
	smoothRadiusMin = 30

	// jitterMinutes: the picked time wobbles deterministically within ±
	// this many minutes (hash of the date), so maintenance does not land
	// on the same wall-clock minute every day.
	jitterMinutes = 15
)

// Hist is the activity history. Counters only ever grow between decays;
// the picker cares about shape, not magnitude.
type Hist struct {
	mu          sync.Mutex
	MinuteOfDay [minutesPerDay]uint64 `json:"minuteOfDay"`
	HourOfWeek  [hoursPerWeek]uint64  `json:"hourOfWeek"`
	Days        map[string]bool       `json:"days"`
	LastDecay   time.Time             `json:"lastDecay"`

	// DurationsNs records how long each expensive maintenance job took
	// last time, as an exponential moving average: every fresh
	// observation halves the weight of the previous estimate (decay of
	// staleness — an un-observed estimate stays; it is still our best
	// guess). The scheduler refuses to start a job whose expected
	// duration does not fit comfortably inside the predicted idle run.
	DurationsNs map[string]int64 `json:"durationsNs,omitempty"`
}

// Global is the process-wide histogram, fed by the log sink.
var Global = &Hist{Days: map[string]bool{}, DurationsNs: map[string]int64{}}

// Add records one activity event at time t (local time is what matters:
// humans sleep in local time).
func (h *Hist) Add(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	minute := t.Hour()*60 + t.Minute()
	h.MinuteOfDay[minute]++
	weekHour := int(t.Weekday())*24 + t.Hour()
	h.HourOfWeek[weekHour]++
	if h.Days == nil {
		h.Days = map[string]bool{}
	}
	h.Days[t.Format("2006-01-02")] = true
	if h.LastDecay.IsZero() {
		h.LastDecay = t
	} else if t.Sub(h.LastDecay) > decayInterval {
		h.decayLocked(t)
	}
}

func (h *Hist) decayLocked(now time.Time) {
	for i := range h.MinuteOfDay {
		h.MinuteOfDay[i] /= 2
	}
	for i := range h.HourOfWeek {
		h.HourOfWeek[i] /= 2
	}
	h.LastDecay = now
}

// MergeFrom folds a loaded histogram into this one: counters add (they
// cover disjoint time spans — previous runs versus this process), days
// union, and durations carry over only for jobs this process has not
// freshly measured (a fresh measurement beats a stale estimate).
func (h *Hist) MergeFrom(other *Hist) {
	if other == nil {
		return
	}
	other.mu.Lock()
	defer other.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.MinuteOfDay {
		h.MinuteOfDay[i] += other.MinuteOfDay[i]
	}
	for i := range h.HourOfWeek {
		h.HourOfWeek[i] += other.HourOfWeek[i]
	}
	if h.Days == nil {
		h.Days = map[string]bool{}
	}
	for d := range other.Days {
		h.Days[d] = true
	}
	if h.DurationsNs == nil {
		h.DurationsNs = map[string]int64{}
	}
	for k, v := range other.DurationsNs {
		if _, ok := h.DurationsNs[k]; !ok {
			h.DurationsNs[k] = v
		}
	}
	if other.LastDecay.Before(h.LastDecay) {
		h.LastDecay = other.LastDecay
	}
}

// Load reads a persisted histogram from path; a missing file is a fresh
// histogram, not an error.
func Load(path string) (*Hist, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Hist{Days: map[string]bool{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var h Hist
	if err := json.Unmarshal(data, &h); err != nil {
		// Corrupt history is worthless but harmless; start over.
		return &Hist{Days: map[string]bool{}}, nil
	}
	if h.Days == nil {
		h.Days = map[string]bool{}
	}
	return &h, nil
}

// Save persists the histogram atomically (temp + rename).
func (h *Hist) Save(path string) error {
	h.mu.Lock()
	data, err := json.Marshal(h)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// RecordDuration folds a fresh duration observation for a maintenance
// job into the EMA (half the old estimate, half the new).
func (h *Hist) RecordDuration(kind string, d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.DurationsNs == nil {
		h.DurationsNs = map[string]int64{}
	}
	if old, ok := h.DurationsNs[kind]; ok && old > 0 {
		h.DurationsNs[kind] = old/2 + int64(d)/2
	} else {
		h.DurationsNs[kind] = int64(d)
	}
}

// ExpectedDuration returns the current duration estimate for a job; 0
// means never observed, and the scheduler treats unknown as admissible
// (the first run establishes the measurement).
func (h *Hist) ExpectedDuration(kind string) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Duration(h.DurationsNs[kind])
}

// Pick returns the maintenance time for the local calendar day of `day`
// and the length in minutes of the predicted idle run containing it: the
// middle of the deepest predicted idle run, plus a deterministic per-day
// jitter. It is a pure function of the histogram contents and the date —
// same inputs, same minute.
//
// With too little history the answer is the middle of the default sleep
// window and its length; with history, the answer is the middle of the
// longest run whose smoothed activity is within 25% (+2) of the day's
// minimum. The weekly histogram adds that weekday's per-hour weight, so
// e.g. a Monday-night import shifts Monday's pick but not Wednesday's.
func (h *Hist) Pick(day time.Time) (time.Time, int) {
	h.mu.Lock()
	minute, runLen := h.pickMinuteLocked(day)
	h.mu.Unlock()
	minute += jitterFor(day)
	y, m, d := day.Date()
	return time.Date(y, m, d, 0, minute, 0, 0, day.Location()), runLen
}

func (h *Hist) pickMinuteLocked(day time.Time) (int, int) {
	defaultMid := (defaultIdleStartMin + defaultIdleEndMin) / 2
	defaultLen := defaultIdleEndMin - defaultIdleStartMin
	if len(h.Days) < minDaysForPrediction {
		return defaultMid, defaultLen
	}

	// Smoothed minute-of-day score plus this weekday's hourly weight.
	var score [minutesPerDay]uint64
	weekday := int(day.Weekday())
	for m := 0; m < minutesPerDay; m++ {
		var sum uint64
		for k := -smoothRadiusMin; k <= smoothRadiusMin; k++ {
			sum += h.MinuteOfDay[(m+k+minutesPerDay)%minutesPerDay]
		}
		score[m] = sum + h.HourOfWeek[weekday*24+m/60]
	}

	var total, minScore uint64
	minScore = ^uint64(0)
	for _, s := range score {
		total += s
		if s < minScore {
			minScore = s
		}
	}
	if total == 0 {
		return defaultMid, defaultLen
	}

	// Longest run at or near the minimum, circular via array doubling.
	threshold := minScore + max(minScore/4, 2)
	bestStart, bestLen, runStart, runLen := -1, -1, -1, 0
	for i := 0; i < 2*minutesPerDay; i++ {
		if score[i%minutesPerDay] <= threshold {
			if runLen == 0 {
				runStart = i
			}
			runLen++
			if runLen > bestLen && runLen <= minutesPerDay {
				bestStart, bestLen = runStart, runLen
			}
		} else {
			runLen = 0
		}
	}
	if bestStart < 0 {
		return defaultMid, defaultLen
	}
	return (bestStart + bestLen/2) % minutesPerDay, bestLen
}

// jitterFor returns a deterministic per-day offset in ±jitterMinutes.
func jitterFor(day time.Time) int {
	h := fnv.New32a()
	h.Write([]byte(day.Format("2006-01-02")))
	return int(h.Sum32()%uint32(2*jitterMinutes+1)) - jitterMinutes
}
