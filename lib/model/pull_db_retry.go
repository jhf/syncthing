// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"log/slog"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

type pullDBRetryPolicy struct {
	initial, maximum, warningInterval time.Duration
	now                               func() time.Time
	wait                              func(context.Context, time.Duration) error // nil uses one reusable timer
}

func (f *sendReceiveFolder) pullDBPolicy() pullDBRetryPolicy {
	p := pullDBRetryPolicy{}
	if f.pullDBRetryPolicy != nil {
		p = *f.pullDBRetryPolicy
	}
	if p.initial <= 0 {
		p.initial = 250 * time.Millisecond
	}
	if p.maximum <= 0 {
		p.maximum = 30 * time.Second
	}
	if p.initial > p.maximum {
		p.initial = p.maximum
	}
	if p.warningInterval <= 0 {
		p.warningInterval = 5 * time.Minute
	}
	if p.now == nil {
		p.now = time.Now
	}
	return p
}

// Constant-size retry state. Neither a growing schedule nor an attempt history
// is retained. Even the diagnostic attempt counter saturates instead of wrapping.
type pullDBRetryState struct {
	delay, maximum       time.Duration
	started, nextWarning time.Time
	attempts             uint64
	version              uint64
	versionKnown         bool
}

func (s *pullDBRetryState) nextDelay() time.Duration {
	delay := s.delay
	if s.delay >= s.maximum/2 {
		s.delay = s.maximum
	} else {
		s.delay *= 2
	}
	return delay
}

func (s *pullDBRetryState) attempted() {
	if s.attempts != ^uint64(0) {
		s.attempts++
	}
}

func (s *pullDBRetryState) heartbeat(version uint64, err error) string {
	moved := "unknown"
	if err == nil && s.versionKnown {
		moved = "false"
		if version != s.version {
			moved = "true"
		}
	}
	s.version, s.versionKnown = version, err == nil
	return moved
}

// One observer at most per updater/folder, reused across every held batch.
// Acquisition failures are diagnostic only and are not retried per attempt.
type pullDBObserver struct {
	conn      db.DataVersionObserver
	attempted bool
}

func (o *pullDBObserver) open(ctx context.Context, f *sendReceiveFolder) {
	if o.attempted {
		return
	}
	o.attempted = true
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	o.conn, _ = f.db.OpenDataVersionObserver(bounded, f.ID)
}

func (o *pullDBObserver) close() {
	if o.conn != nil {
		o.conn.Close()
	}
}

func (o *pullDBObserver) sample(ctx context.Context, s *pullDBRetryState) string {
	if o.conn == nil {
		s.versionKnown = false
		return "unknown"
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	version, err := o.conn.DataVersion(bounded)
	return s.heartbeat(version, err)
}

func (f *sendReceiveFolder) updatePullDBWithRetry(ctx context.Context, files []protocol.FileInfo, observer *pullDBObserver, durable bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := f.pullDBPolicy()
	started := p.now()
	state := pullDBRetryState{delay: p.initial, maximum: p.maximum, started: started, nextWarning: started.Add(p.warningInterval), attempts: 1}
	var opts []db.UpdateOption
	if durable {
		opts = append(opts, db.WithDurable())
	}
	err := f.updateLocals(files, opts...)
	if err == nil || !db.IsLockContention(err) {
		return err
	}
	f.sl.Info("Pull database update hit lock contention, retrying", slog.Int("files", len(files)))
	observer.open(ctx, f)
	observer.sample(ctx, &state) // baseline, always on the same physical connection
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := p.now()
		if !now.Before(state.nextWarning) {
			moved := observer.sample(ctx, &state)
			f.sl.Warn("Pull database update still waiting", slog.Int("files", len(files)), slog.Duration("elapsed", now.Sub(state.started)), slog.Uint64("attempts", state.attempts), slog.String("data_version_moved", moved))
			f.churn.count("", "db_update.lock_wait_warning", 1)
			state.nextWarning = now.Add(p.warningInterval)
		}
		delay := state.nextDelay()
		if untilWarning := state.nextWarning.Sub(p.now()); untilWarning > 0 && delay > untilWarning {
			delay = untilWarning
		}
		if p.wait != nil {
			if err := p.wait(ctx, delay); err != nil {
				return err
			}
		} else {
			timer.Reset(delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		state.attempted()
		err = f.updateLocals(files, opts...)
		if err == nil || !db.IsLockContention(err) {
			return err
		}
	}
}
