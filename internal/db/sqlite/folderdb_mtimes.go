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
	timer   *time.Timer
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
	op, ok := b.pending[name]
	return op, ok
}

// flush applies all pending operations in a single transaction.
func (b *mtimeBatcher) flush() {
	b.mut.Lock()
	defer b.mut.Unlock()
	b.flushLocked()
}

// flushLocked swaps out the pending map and applies it. Caller holds b.mut.
func (b *mtimeBatcher) flushLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.pending) == 0 {
		return
	}
	ops := b.pending
	b.pending = make(map[string]mtimeOp, len(ops))
	b.mut.Unlock()
	defer b.mut.Lock()

	b.s.updateLock.Lock()
	defer b.s.updateLock.Unlock()

	tx, err := b.s.sql.BeginTxx(context.Background(), nil)
	if err != nil {
		slog.Warn("Mtime batch flush failed to begin", slogutil.Error(err))
		return
	}
	defer tx.Rollback() //nolint:errcheck

	delStmt, err := tx.Preparex(`DELETE FROM mtimes WHERE name = ?`)
	if err != nil {
		slog.Warn("Mtime batch flush failed to prepare", slogutil.Error(err))
		return
	}
	defer delStmt.Close() //nolint:errcheck
	putStmt, err := tx.Preparex(`INSERT OR REPLACE INTO mtimes (name, ondisk, virtual) VALUES (?, ?, ?)`)
	if err != nil {
		slog.Warn("Mtime batch flush failed to prepare", slogutil.Error(err))
		return
	}
	defer putStmt.Close() //nolint:errcheck

	for name, op := range ops {
		var err error
		if op.delete {
			_, err = delStmt.Exec(name)
		} else {
			_, err = putStmt.Exec(name, op.ondisk, op.virtual)
		}
		if err != nil {
			slog.Warn("Mtime batch operation failed", slogutil.FilePath(name), slogutil.Error(err))
			return
		}
	}
	if err := tx.Commit(); err != nil {
		slog.Warn("Mtime batch flush failed to commit", slogutil.Error(err))
	}
}

// close flushes any remaining operations and disarms the timer.
func (b *mtimeBatcher) close() {
	b.mut.Lock()
	defer b.mut.Unlock()
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
