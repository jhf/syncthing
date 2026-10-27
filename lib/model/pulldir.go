// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	pullDirBuckets        = 256
	pullDirBytes          = 2 << 20
	pullDirProcessBuckets = 2048
	pullDirProcessBytes   = 16 << 20
	pullDirEpoch          = time.Minute
	pullDirProbe          = 10 * time.Second
	pullDirFolderProbes   = 8
)

var pullDirKeys = [...]string{
	"dir.qualified", "dir.promoted", "dir.defer", "dir.probe_reserved", "dir.probe_admit", "dir.probe_stale", "dir.max_hold_latched", "dir.max_hold_admit", "dir.epoch_expired", "dir.evidence_overflow", "dir.state_capacity_bypass",
}

type pullDirBudget struct{ buckets, bytes atomic.Int64 }

var globalPullDirBudget pullDirBudget

type pullDirBucket struct {
	start, holdUntil, nextProbe time.Time
	children                    [8]string
	childN                      int
	qualifiedChildren           [2]string
	qualifiedN                  int
	directQualified             bool
	bytes                       int
	probeName                   string // Refers to a name already retained in the bounded path map.
	probeWindow                 time.Time
}

type pullDirState struct {
	buckets     map[string]*pullDirBucket
	bytes       int
	budget      *pullDirBudget
	sequence    uint64
	probeWindow time.Time
	probeUsed   int
	counts      [len(pullDirKeys)]uint64
	samples     [churnSamples]churnSample
	sampleN     int
	omitted     uint64
}

func (d *pullDirState) sample(name, ancestor, reason string, depth int, b *pullDirBucket, now time.Time) {
	if d.sampleN == len(d.samples) {
		d.omitted++
		return
	}
	s := churnSample{Path: churnBound(name), Ancestor: churnBound(ancestor), Event: "queue_deferral", Outcome: reason, Depth: depth}
	if b != nil {
		s.Distinct = b.childN
		if b.qualifiedN >= 2 {
			s.Distinct = max(s.Distinct, b.qualifiedN*3)
		}
		s.EpochAge = max(0, now.Sub(b.holdUntil.Add(-pullDirEpoch)).Seconds())
	}
	d.samples[d.sampleN] = s
	d.sampleN++
}

func newPullDirState() *pullDirState {
	return &pullDirState{buckets: make(map[string]*pullDirBucket), budget: &globalPullDirBudget}
}
func (d *pullDirState) count(key string) {
	for i, k := range pullDirKeys {
		if key == k {
			d.counts[i]++
			return
		}
	}
	panic("unknown directory retry counter")
}

// All methods below are called under the owning retry tracker's mutex. There
// are no DB calls, logging, waits, or additional mutexes in this state machine.
func dirGateable(name string) bool {
	normalized := filepath.ToSlash(name)
	return name != "." && strings.Count(normalized, "/") >= 1 && retryValidPath(name)
}
func (d *pullDirState) reserveBytes(n int) bool {
	if d.bytes+n > pullDirBytes || !retryReserve(&d.budget.bytes, int64(n), pullDirProcessBytes) {
		d.count("dir.evidence_overflow")
		return false
	}
	d.bytes += n
	return true
}
func (d *pullDirState) bucket(name string, now time.Time) *pullDirBucket {
	if b := d.buckets[name]; b != nil {
		return b
	}
	if !dirGateable(name) {
		return nil
	}
	if len(d.buckets) >= pullDirBuckets || !retryReserve(&d.budget.buckets, 1, pullDirProcessBuckets) {
		d.count("dir.evidence_overflow")
		return nil
	}
	if !d.reserveBytes(len(name)) {
		d.budget.buckets.Add(-1)
		return nil
	}
	b := &pullDirBucket{start: now, bytes: len(name)}
	d.buckets[strings.Clone(name)] = b
	return b
}
func (d *pullDirState) remove(name string, b *pullDirBucket) {
	delete(d.buckets, name)
	d.bytes -= b.bytes
	d.budget.bytes.Add(-int64(b.bytes))
	d.budget.buckets.Add(-1)
}
func (d *pullDirState) resetEvidence(name string, b *pullDirBucket, now time.Time) {
	if now.Before(b.start.Add(pullDirEpoch)) {
		return
	}
	released := b.bytes - len(name)
	b.children = [8]string{}
	b.childN = 0
	b.qualifiedChildren = [2]string{}
	b.qualifiedN = 0
	b.directQualified = false
	b.start = now
	b.bytes = len(name)
	d.bytes -= released
	d.budget.bytes.Add(-int64(released))
}
func (d *pullDirState) addName(b *pullDirBucket, name string, qualified bool) bool {
	if qualified {
		for i := range b.qualifiedN {
			if b.qualifiedChildren[i] == name {
				return true
			}
		}
		if b.qualifiedN == len(b.qualifiedChildren) {
			return true
		}
	} else {
		for i := range b.childN {
			if b.children[i] == name {
				return true
			}
		}
		if b.childN == len(b.children) {
			d.count("dir.evidence_overflow")
			return false
		}
	}
	if !d.reserveBytes(len(name)) {
		return false
	}
	b.bytes += len(name)
	if qualified {
		b.qualifiedChildren[b.qualifiedN] = strings.Clone(name)
		b.qualifiedN++
	} else {
		b.children[b.childN] = strings.Clone(name)
		b.childN++
	}
	return true
}
func (d *pullDirState) qualify(b *pullDirBucket, now time.Time, promoted bool) {
	// A running epoch never slides with announcements or new failures.
	if now.Before(b.holdUntil) {
		return
	}
	b.holdUntil = now.Add(pullDirEpoch)
	b.nextProbe = now.Add(pullDirProbe)
	if promoted {
		d.count("dir.promoted")
	} else {
		d.count("dir.qualified")
	}
}
func (d *pullDirState) failure(name string, now time.Time) {
	parent := filepath.Dir(name)
	b := d.bucket(parent, now)
	if b == nil {
		return
	}
	d.resetEvidence(parent, b, now)
	if !d.addName(b, filepath.Base(name), false) || b.childN < 3 || b.directQualified {
		return
	}
	b.directQualified = true
	d.qualify(b, now, false)
	d.sample(name, parent, "qualified", 1, b, now)
	// Only independently qualified immediate child buckets promote. A promoted
	// grandparent is never recursively fed into another promotion.
	grand := filepath.Dir(parent)
	p := d.bucket(grand, now)
	if p == nil {
		return
	}
	d.resetEvidence(grand, p, now)
	if d.addName(p, filepath.Base(parent), true) && p.qualifiedN >= 2 {
		d.qualify(p, now, true)
		d.sample(name, grand, "promoted", 2, p, now)
	}
}
func (d *pullDirState) match(name string, now time.Time) (string, *pullDirBucket, int) {
	dir := name
	for depth := 1; depth <= 2; depth++ {
		dir = filepath.Dir(dir)
		if !dirGateable(dir) {
			break
		}
		if b := d.buckets[dir]; b != nil && now.Before(b.holdUntil) {
			return dir, b, depth
		}
	}
	return "", nil, 0
}
func (d *pullDirState) releaseReservation(name string) {
	for _, b := range d.buckets {
		if b.probeName != name {
			continue
		}
		b.probeName = ""
		if b.probeWindow.Equal(d.probeWindow) && d.probeUsed > 0 {
			d.probeUsed--
		}
		b.nextProbe = time.Time{}
		d.count("dir.probe_stale")
	}
}
func (d *pullDirState) consume(name string, must bool, now time.Time) {
	for ancestor, b := range d.buckets {
		if b.probeName == name {
			b.probeName = ""
			b.nextProbe = now.Add(pullDirProbe)
			d.count("dir.probe_admit")
			d.sample(name, ancestor, "probe_admit", strings.Count(filepath.ToSlash(name), "/")-strings.Count(filepath.ToSlash(ancestor), "/"), b, now)
		}
	}
	if must {
		d.count("dir.max_hold_admit")
		d.sample(name, "", "max_hold_admit", 0, nil, now)
	}
}
func (r *pullRetryTracker) prepareDirLocked(now time.Time) {
	d := r.dir
	if d == nil {
		return
	}
	if d.probeWindow.IsZero() || !now.Before(d.probeWindow.Add(pullDirProbe)) {
		for _, b := range d.buckets {
			if b.probeName != "" {
				b.probeName = ""
				d.count("dir.probe_stale")
			}
		}
		d.probeWindow = now
		d.probeUsed = 0
	}
	for name, b := range d.buckets {
		if !now.Before(b.start.Add(pullDirEpoch)) && !now.Before(b.holdUntil) {
			if !b.holdUntil.IsZero() {
				d.count("dir.epoch_expired")
			}
			d.remove(name, b)
		}
	}
	for _, e := range r.entries {
		if !e.firstDirDeferred.IsZero() && !now.Before(e.firstDirDeferred.Add(pullDirEpoch)) && !e.mustAdmit {
			e.mustAdmit = true
			d.count("dir.max_hold_latched")
		}
	}
	type candidate struct {
		name string
		b    *pullDirBucket
	}
	dirs := make([]candidate, 0, len(d.buckets))
	for name, b := range d.buckets {
		if b.probeName == "" && now.Before(b.holdUntil) && !now.Before(b.nextProbe) {
			dirs = append(dirs, candidate{name, b})
		}
	}
	sort.Slice(dirs, func(i, j int) bool {
		if dirs[i].b.nextProbe.Equal(dirs[j].b.nextProbe) {
			return dirs[i].name < dirs[j].name
		}
		return dirs[i].b.nextProbe.Before(dirs[j].b.nextProbe)
	})
	for _, c := range dirs {
		if d.probeUsed >= pullDirFolderProbes {
			break
		}
		var oldest string
		var sequence uint64
		for name, e := range r.entries {
			if e.firstDirDeferred.IsZero() || e.mustAdmit || now.Before(e.nextEligible) {
				continue
			}
			matched, _, _ := d.match(name, now)
			if matched != c.name {
				continue
			}
			if oldest == "" || e.waitSequence < sequence {
				oldest = name
				sequence = e.waitSequence
			}
		}
		if oldest != "" {
			c.b.probeName = oldest
			c.b.probeWindow = d.probeWindow
			c.b.nextProbe = now.Add(pullDirProbe)
			d.probeUsed++
			d.count("dir.probe_reserved")
		}
	}
}
func (r *pullRetryTracker) dirGateLocked(name string, now time.Time) bool {
	d := r.dir
	if d == nil {
		return false
	}
	e := r.entries[name]
	if e != nil && !e.firstDirDeferred.IsZero() && !now.Before(e.firstDirDeferred.Add(pullDirEpoch)) && !e.mustAdmit {
		e.mustAdmit = true
		d.count("dir.max_hold_latched")
	}
	ancestor, b, depth := d.match(name, now)
	// A permit cannot survive its window, even if a long need walk has not
	// reached the next reservation pass yet.
	if b != nil && b.probeName == name && !now.Before(b.probeWindow.Add(pullDirProbe)) {
		d.releaseReservation(name)
	}
	if b == nil || e != nil && e.mustAdmit || b.probeName == name {
		return false
	}
	if e == nil {
		if !retryValidPath(name) || len(r.entries) >= pullRetryPaths || r.bytes+len(name) > pullRetryBytes || !r.budget.take(len(name)) {
			d.count("dir.state_capacity_bypass")
			return false
		}
		name = strings.Clone(name)
		e = new(pullRetryEntry)
		r.entries[name] = e
		r.bytes += len(name)
	}
	e.seen = r.round
	if e.firstDirDeferred.IsZero() {
		d.sequence++
		e.waitSequence = d.sequence
		e.firstDirDeferred = now
	}
	d.count("dir.defer")
	d.sample(name, ancestor, "defer", depth, b, now)
	return true
}
func (r *pullRetryTracker) dirWakeLocked(name string, e *pullRetryEntry, now time.Time) time.Time {
	if now.Before(e.nextEligible) || e.firstDirDeferred.IsZero() {
		return e.nextEligible
	}
	if e.mustAdmit || !now.Before(e.firstDirDeferred.Add(pullDirEpoch)) {
		return now
	}
	_, b, _ := r.dir.match(name, now)
	if b == nil || b.probeName == name {
		return now
	}
	probe := b.nextProbe
	if r.dir.probeUsed >= pullDirFolderProbes {
		probe = maxTime(probe, r.dir.probeWindow.Add(pullDirProbe))
	}
	return minTime(minTime(probe, b.holdUntil), e.firstDirDeferred.Add(pullDirEpoch))
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func (r *pullRetryTracker) flushDirCounts() {
	if r == nil {
		return
	}
	r.mu.Lock()
	var counts [len(pullDirKeys)]uint64
	var samples [churnSamples]churnSample
	var sampleN int
	var omitted uint64
	if r.dir != nil {
		counts = r.dir.counts
		r.dir.counts = [len(pullDirKeys)]uint64{}
		samples, sampleN, omitted = r.dir.samples, r.dir.sampleN, r.dir.omitted
		r.dir.samples, r.dir.sampleN, r.dir.omitted = [churnSamples]churnSample{}, 0, 0
	}
	r.mu.Unlock()
	for i, n := range counts {
		if n > 0 {
			r.churn.count("", pullDirKeys[i], n)
		}
	}
	for _, s := range samples[:sampleN] {
		r.churn.observe(s, "dir.sampled")
	}
	if omitted > 0 {
		r.churn.count("", "dir.samples_omitted", omitted)
	}
}
