// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
)

// Mtime bookkeeping (the mtimeFS virtual-time store) is advisory and very
// chatty: directory mtime restoration during pulls and walks calls save()
// per directory, and each call used to land here as its own synchronous
// write transaction. On slow storage those micro-transactions serialize
// behind the index batch commits and stall the walk (observed on yarlung
// 2026-10-20: the post-restart need-walk blocked in DeleteMtime behind
// batch commits, progressing at ~6 items/min on a 7200rpm RAID1).
//
// Mtime writes are therefore batched: Put/Delete record the operation in a
// pending map (last operation per name wins) and one transaction applies
// the map when it reaches a size threshold or a short timer fires.
// GetMtime consults the pending map first (read-your-writes), so callers
// observe their own unflushed state. A crash before a flush loses a few
// seconds of virtual-time bookkeeping; the worst outcome is a directory
// whose real mtime is seen at the next scan, which is harmless.
//
// Reads stay continuous through a flush. A swapped-out batch stays on the
// in-flight list, visible to GetMtime, until its transaction commits:
// otherwise a mapping vanished for as long as the flush waited for the
// folder write lock, and a scan in that window saw the physical timestamp
// of a whole-second filesystem and minted a receiver version of unchanged
// sender content. Whoever holds the write lock applies every in-flight
// batch, oldest first, so overlapping flushes commit in swap order and an
// older batch never overwrites a newer one. A failed flush keeps its
// batches (merged, so memory stays bounded by distinct names) for the next
// attempt instead of discarding them.
const (
	mtimeBatchMaxOps     = 512
	mtimeBatchMaxLatency = 2 * time.Second
)

type mtimeOp struct {
	ondisk, virtual int64
	delete          bool
}

type mtimeBatcher struct {
	s       *folderDB
	mut     sync.Mutex
	pending map[string]mtimeOp
	// inflight holds swapped-out batches not yet committed, oldest first.
	// Appended under mut; only the holder of s.updateLock removes them.
	inflight []map[string]mtimeOp
	timer    *time.Timer
	closed   bool // no retry after close: the pool is gone
}

func newMtimeBatcher(s *folderDB) *mtimeBatcher {
	return &mtimeBatcher{s: s, pending: make(map[string]mtimeOp)}
}

func (b *mtimeBatcher) add(name string, op mtimeOp) {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.pending[name] = op
	if len(b.pending) >= mtimeBatchMaxOps {
		b.flushLocked()
		return
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(mtimeBatchMaxLatency, b.flush)
	}
}

func (b *mtimeBatcher) lookup(name string) (mtimeOp, bool) {
	b.mut.Lock()
	defer b.mut.Unlock()
	if op, ok := b.pending[name]; ok {
		return op, true
	}
	for i := len(b.inflight) - 1; i >= 0; i-- {
		if op, ok := b.inflight[i][name]; ok {
			return op, true
		}
	}
	return mtimeOp{}, false
}

// flush applies all pending operations in a single transaction.
func (b *mtimeBatcher) flush() {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.flushLocked()
}

// flushLocked moves the pending map to the in-flight list and commits every
// in-flight batch. Caller holds b.mut; it is released while waiting for the
// folder write lock and reacquired before return.
func (b *mtimeBatcher) flushLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.pending) > 0 {
		b.inflight = append(b.inflight, b.pending)
		b.pending = make(map[string]mtimeOp, len(b.pending))
	}
	if len(b.inflight) == 0 {
		return
	}
	b.mut.Unlock()
	defer b.mut.Lock()

	b.s.updateLock.Lock()
	defer b.s.updateLock.Unlock()

	// Everything on the list now is older than anything appended later, and
	// no other flush can apply batches while we hold updateLock.
	b.mut.Lock()
	batches := b.inflight[:len(b.inflight):len(b.inflight)]
	b.mut.Unlock()
	if len(batches) == 0 {
		return // an earlier flush committed ours while we waited
	}

	err := b.apply(batches)

	b.mut.Lock()
	defer b.mut.Unlock()
	rest := b.inflight[len(batches):]
	if err == nil {
		b.inflight = append([]map[string]mtimeOp(nil), rest...)
		return
	}
	// Keep the failed batches, merged oldest first, ahead of newer ones,
	// and retry on the timer.
	merged := make(map[string]mtimeOp)
	for _, ops := range batches {
		for name, op := range ops {
			merged[name] = op
		}
	}
	b.inflight = append([]map[string]mtimeOp{merged}, rest...)
	if b.timer == nil && !b.closed {
		b.timer = time.AfterFunc(mtimeBatchMaxLatency, b.flush)
	}
}

// apply commits the batches, oldest first, in one transaction. Caller holds
// s.updateLock and not b.mut.
func (b *mtimeBatcher) apply(batches []map[string]mtimeOp) error {
	tx, err := b.s.sql.BeginTxx(context.Background(), nil)
	if err != nil {
		slog.Warn("Mtime batch flush failed to begin", slogutil.Error(err))
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	delStmt, err := tx.Preparex(`DELETE FROM mtimes WHERE name = ?`)
	if err != nil {
		slog.Warn("Mtime batch flush failed to prepare", slogutil.Error(err))
		return err
	}
	defer delStmt.Close() //nolint:errcheck
	putStmt, err := tx.Preparex(`INSERT OR REPLACE INTO mtimes (name, ondisk, virtual) VALUES (?, ?, ?)`)
	if err != nil {
		slog.Warn("Mtime batch flush failed to prepare", slogutil.Error(err))
		return err
	}
	defer putStmt.Close() //nolint:errcheck

	for _, ops := range batches {
		for name, op := range ops {
			var err error
			if op.delete {
				_, err = delStmt.Exec(name)
			} else {
				_, err = putStmt.Exec(name, op.ondisk, op.virtual)
			}
			if err != nil {
				slog.Warn("Mtime batch operation failed", slogutil.FilePath(name), slogutil.Error(err))
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		slog.Warn("Mtime batch flush failed to commit", slogutil.Error(err))
		return err
	}
	return nil
}

// close flushes any remaining operations and disarms the timer.
func (b *mtimeBatcher) close() {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.closed = true
	b.flushLocked()
}

func (s *folderDB) GetMtime(name string) (ondisk, virtual time.Time) {
	if op, ok := s.mtimes.lookup(name); ok {
		if op.delete {
			return time.Time{}, time.Time{}
		}
		return time.Unix(0, op.ondisk), time.Unix(0, op.virtual)
	}
	var res struct {
		Ondisk  int64
		Virtual int64
	}
	if err := s.stmt(`
		SELECT m.ondisk, m.virtual FROM mtimes m
		WHERE m.name = ?
	`).Get(&res, name); err != nil {
		return time.Time{}, time.Time{}
	}
	return time.Unix(0, res.Ondisk), time.Unix(0, res.Virtual)
}

func (s *folderDB) PutMtime(name string, ondisk, virtual time.Time) error {
	s.mtimes.add(name, mtimeOp{ondisk: ondisk.UnixNano(), virtual: virtual.UnixNano()})
	return nil
}

func (s *folderDB) DeleteMtime(name string) error {
	s.mtimes.add(name, mtimeOp{delete: true})
	return nil
}
