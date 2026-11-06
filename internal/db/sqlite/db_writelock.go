// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// writeLock is a baseDB's updateLock: the mutex every writer holds (see
// baseDB). It reports, at INFO and so into the log rings, every hold that
// kept another writer waiting longer than gcLiveHoldTarget, the bounded-step
// budget of the single-writer design, naming the holder and each waiter by
// call site. Without this a long hold (tidy's 12.7 s WAL checkpoint on
// yarlung, 2026-11-28 07:35:47) shows only its own duration, never whether
// a pull or scan stood behind it.
//
// The uncontended path costs one TryLock and one time.Now; call sites are
// resolved only for a hold or wait that is reported.
type writeLock struct {
	mu      sync.Mutex
	db      string
	since   time.Time    // guarded by mu
	waiting atomic.Int32 // goroutines blocked in Lock
}

func (l *writeLock) Lock() {
	if !l.mu.TryLock() {
		l.waiting.Add(1)
		t0 := time.Now()
		l.mu.Lock()
		l.waiting.Add(-1)
		if waited := time.Since(t0); waited > gcLiveHoldTarget {
			slog.Info("Database write lock waited", slog.String("db", l.db), slog.Duration("waited", waited), slog.String("waiter", callSite(2)))
		}
	}
	l.since = time.Now()
}

func (l *writeLock) TryLock() bool {
	if !l.mu.TryLock() {
		return false
	}
	l.since = time.Now()
	return true
}

func (l *writeLock) Unlock() {
	held := time.Since(l.since)
	waiters := l.waiting.Load()
	l.mu.Unlock()
	if waiters > 0 && held > gcLiveHoldTarget {
		slog.Info("Database write lock held while writers waited", slog.String("db", l.db), slog.Duration("held", held), slog.Int("waiters", int(waiters)), slog.String("holder", callSite(2)))
	}
}

// callSite names the function skip frames above callSite, as
// "file.go:line func".
func callSite(skip int) string {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}
	name := "?"
	if fn := runtime.FuncForPC(pc); fn != nil {
		name = filepath.Base(fn.Name())
	}
	return fmt.Sprintf("%s:%d %s", filepath.Base(file), line, name)
}
