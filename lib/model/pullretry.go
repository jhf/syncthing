// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
)

const (
	pullRetryPaths        = 4096
	pullRetryBytes        = 4 << 20
	pullRetryProcessPaths = 32768
	pullRetryProcessBytes = 32 << 20
)

type pullRetryBudget struct{ paths, bytes atomic.Int64 }

var globalPullRetryBudget pullRetryBudget

func retryReserve(v *atomic.Int64, n, limit int64) bool {
	for {
		old := v.Load()
		if old+n > limit {
			return false
		}
		if v.CompareAndSwap(old, old+n) {
			return true
		}
	}
}
func (b *pullRetryBudget) take(n int) bool {
	if !retryReserve(&b.paths, 1, pullRetryProcessPaths) {
		return false
	}
	if !retryReserve(&b.bytes, int64(n), pullRetryProcessBytes) {
		b.paths.Add(-1)
		return false
	}
	return true
}
func (b *pullRetryBudget) release(n int) { b.paths.Add(-1); b.bytes.Add(-int64(n)) }

type pullRetryEntry struct {
	nextEligible, firstFailure, lastFailure time.Time
	seen                                    uint64
	attempt                                 uint64
	message, class, stage                   string
	expired                                 bool
	firstDirDeferred                        time.Time
	waitSequence                            uint64
	mustAdmit                               bool
}
type pullRetryStatus struct {
	nextEligible                              time.Time
	deferredNeeded, attempted, actualFailures int
	completeNeedWalk                          bool
	oldest                                    time.Time
	dirDeferred                               int
}

func (s pullRetryStatus) pureDeferral() bool {
	return s.completeNeedWalk && s.attempted == 0 && s.actualFailures == 0 && s.deferredNeeded > 0
}

type pullRetryTracker struct {
	mu             sync.Mutex
	entries        map[string]*pullRetryEntry
	bytes          int
	round, attempt uint64
	status         pullRetryStatus
	now            func() time.Time
	pause          func() time.Duration
	budget         *pullRetryBudget
	churn          *churnTracker
	dir            *pullDirState
}

func newPullRetryTracker(pause func() time.Duration, churn *churnTracker) *pullRetryTracker {
	return &pullRetryTracker{entries: make(map[string]*pullRetryEntry), now: time.Now, pause: pause, budget: &globalPullRetryBudget, churn: churn}
}

// The wire validates canonical slash paths. Validate again before retaining
// scheduler state, including native-path conversion on Windows.
func retryValidPath(name string) bool {
	canonical, err := fs.Canonicalize(name)
	if err != nil || canonical != name {
		return false
	}
	name = filepath.ToSlash(name)
	return name != "" && name != "." && name != ".." && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && path.Clean(name) == name
}
func (r *pullRetryTracker) begin() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.status = pullRetryStatus{}
	r.mu.Unlock()
}
func (r *pullRetryTracker) beginWalk() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.prepareDirLocked(r.now())
	r.round++
	r.status.completeNeedWalk = false
	r.mu.Unlock()
}
func (r *pullRetryTracker) removeLocked(name string) bool {
	e, ok := r.entries[name]
	if !ok {
		return false
	}
	n := len(name) + len(e.message)
	if r.dir != nil {
		r.dir.releaseReservation(name)
	}
	delete(r.entries, name)
	r.bytes -= n
	r.budget.release(n)
	return true
}
func (r *pullRetryTracker) clear(name string, success bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	removed := r.removeLocked(name)
	r.mu.Unlock()
	if removed {
		key := "queue.retry_clear_obsolete"
		if success {
			key = "queue.retry_clear_success"
		}
		r.churn.count(name, key, 1)
	}
}
func (r *pullRetryTracker) clearAll() {
	if r == nil {
		return
	}
	r.mu.Lock()
	for name := range r.entries {
		r.removeLocked(name)
	}
	if r.dir != nil {
		for name, b := range r.dir.buckets {
			r.dir.remove(name, b)
		}
	}
	r.status = pullRetryStatus{}
	r.mu.Unlock()
	r.churn.retryPending(0, 0)
	r.churn.dirPending(0)
	r.flushDirCounts()
}
func (r *pullRetryTracker) finishWalk(complete bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	cleared := 0
	if complete {
		for name, e := range r.entries {
			if e.seen != r.round {
				r.removeLocked(name)
				cleared++
			}
		}
	}
	r.status.completeNeedWalk = complete
	r.mu.Unlock()
	if cleared > 0 {
		r.churn.count("", "queue.stale_clear", uint64(cleared))
	}
}
func (r *pullRetryTracker) gate(name string) bool {
	if r == nil {
		return false
	}
	now := r.now()
	r.mu.Lock()
	e := r.entries[name]
	held, expired := false, false
	if e != nil {
		e.seen = r.round
		held = now.Before(e.nextEligible)
		if !held && !e.expired && !e.nextEligible.IsZero() {
			e.expired = true
			expired = true
		}
	}
	if !held {
		held = r.dirGateLocked(name, now)
	}
	r.mu.Unlock()
	if held {
		r.churn.count(name, "queue.defer", 1)
	}
	if expired {
		r.churn.count(name, "queue.expire", 1)
	}
	return held
}
func (r *pullRetryTracker) admit(name string) uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	r.attempt++
	id := r.attempt
	r.status.attempted++
	if e := r.entries[name]; e != nil {
		e.attempt = id
		e.seen = r.round
		if r.dir != nil {
			r.dir.consume(name, e.mustAdmit, r.now())
		}
		e.firstDirDeferred = time.Time{}
		e.mustAdmit = false
		if e.message == "" {
			r.removeLocked(name)
		}
	}
	r.mu.Unlock()
	return id
}

func (r *pullRetryTracker) otherFailure() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.status.actualFailures++
	r.mu.Unlock()
}
func retryEligible(class, stage string) bool {
	return class == "remote_no_such_file" && stage == "block_request" || class == "local_modified" && (stage == "shortcut_check" || stage == "finalize")
}
func (r *pullRetryTracker) failure(name, class, stage, message string, attempt ...uint64) {
	if r == nil {
		return
	}
	now := r.now()
	r.mu.Lock()
	r.status.actualFailures++
	if !retryEligible(class, stage) || !retryValidPath(name) {
		r.removeLocked(name)
		r.mu.Unlock()
		return
	}
	// Bound the retained message, not the public current-error string.
	message = churnBound(message)
	e := r.entries[name]
	bypass := false
	if e == nil {
		n := len(name) + len(message)
		if len(r.entries) >= pullRetryPaths || r.bytes+n > pullRetryBytes || !r.budget.take(n) {
			bypass = true
		} else {
			name = strings.Clone(name)
			e = &pullRetryEntry{firstFailure: now}
			r.entries[name] = e
			r.bytes += n
		}
	} else {
		// Reserve any growth before replacing a bounded diagnostic sample.
		delta := len(message) - len(e.message)
		if delta > 0 && (r.bytes+delta > pullRetryBytes || !retryReserve(&r.budget.bytes, int64(delta), pullRetryProcessBytes)) {
			message = e.message
		} else {
			r.bytes += delta
			if delta < 0 {
				r.budget.bytes.Add(int64(delta))
			}
		}
	}
	if e != nil {
		if e.firstFailure.IsZero() {
			e.firstFailure = now
		}
		e.nextEligible = now.Add(r.pause())
		if len(attempt) > 0 {
			e.attempt = attempt[0]
		}
		e.lastFailure = now
		e.seen = r.round
		e.message = message
		e.class = class
		e.stage = stage
		e.expired = false
	}
	if r.dir != nil && class == "remote_no_such_file" && stage == "block_request" {
		r.dir.failure(name, now)
	}
	r.mu.Unlock()
	if bypass {
		r.churn.count(name, "queue.state_capacity_bypass", 1)
	}
}
func (r *pullRetryTracker) publish() (pullRetryStatus, map[string]churnPullError) {
	if r == nil {
		return pullRetryStatus{}, nil
	}
	now := r.now()
	r.mu.Lock()
	s := r.status
	s.deferredNeeded = 0
	s.dirDeferred = 0
	s.nextEligible = time.Time{}
	s.oldest = time.Time{}
	messages := make(map[string]churnPullError)
	if s.completeNeedWalk {
		for name, e := range r.entries {
			if e.seen != r.round {
				continue
			}
			s.deferredNeeded++
			wake := r.dirWakeLocked(name, e, now)
			if s.nextEligible.IsZero() || wake.Before(s.nextEligible) {
				s.nextEligible = wake
			}
			first := e.firstFailure
			if !e.firstDirDeferred.IsZero() {
				s.dirDeferred++
				if first.IsZero() || e.firstDirDeferred.Before(first) {
					first = e.firstDirDeferred
				}
			}
			if s.oldest.IsZero() || first.Before(s.oldest) {
				s.oldest = first
			}
			if e.message != "" {
				messages[name] = churnPullError{e.message, e.class}
			}
		}
	}
	r.status = s
	r.mu.Unlock()
	age := 0.0
	if !s.oldest.IsZero() {
		age = max(0, now.Sub(s.oldest).Seconds())
	}
	r.churn.retryPending(s.deferredNeeded, age)
	r.churn.dirPending(s.dirDeferred)
	r.flushDirCounts()
	return s, messages
}
func (r *pullRetryTracker) snapshot() pullRetryStatus {
	if r == nil {
		return pullRetryStatus{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}
func (f *sendReceiveFolder) RetryStatus() pullRetryStatus { return f.retry.snapshot() }
func (f *sendReceiveFolder) ClearRetryState()             { f.retry.clearAll() }

type pullAttemptError struct {
	error
	id uint64
}

func (e pullAttemptError) Unwrap() error { return e.error }
func retryAttemptError(err error, id uint64) error {
	if id == 0 || err == nil {
		return err
	}
	return pullAttemptError{err, id}
}
func retryAttemptID(err error) uint64 {
	var e pullAttemptError
	if errors.As(err, &e) {
		return e.id
	}
	return 0
}
