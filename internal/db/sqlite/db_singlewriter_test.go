// Copyright (C) 2026 The Syncthing Authors.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

// The probe wraps the actual SQLite driver, not a mock. It observes all
// connection/statement entry points, including implicit writes via Query and
// RETURNING. Tests install it only after exclusive schema initialization.
type writerProbe struct {
	driver.Driver
	db         *baseDB
	mu         sync.Mutex
	violations []string
	busyErrors []string
	before     func(string)
	after      func(string, time.Duration)
}

func (p *writerProbe) observe(q string) {
	if p.before != nil {
		p.before(q)
	}
	if !writingSQL(q) {
		return
	}
	if p.db.updateLock.TryLock() {
		p.db.updateLock.Unlock()
		p.mu.Lock()
		p.violations = append(p.violations, q)
		p.mu.Unlock()
	}
}

func (p *writerProbe) record(q string, err error) {
	if !db.IsLockContention(err) {
		return
	}
	p.mu.Lock()
	p.busyErrors = append(p.busyErrors, strings.Join(strings.Fields(q), " ")+": "+err.Error())
	p.mu.Unlock()
}

func writingSQL(q string) bool {
	q = strings.ToUpper(strings.TrimSpace(q))
	if strings.HasPrefix(q, "SELECT") || strings.HasPrefix(q, "EXPLAIN") {
		return false
	}
	if strings.HasPrefix(q, "PRAGMA") {
		// Read-only or connection-local. Everything else is conservatively a write.
		for _, s := range []string{"FREELIST_COUNT", "PAGE_COUNT", "PAGE_SIZE", "BUSY_TIMEOUT", "FOREIGN_KEYS", "WAL_AUTOCHECKPOINT"} {
			if strings.Contains(q, s) {
				return false
			}
		}
	}
	return true
}

func (p *writerProbe) Open(dsn string) (driver.Conn, error) {
	c, err := p.Driver.Open(dsn)
	p.record("OPEN", err)
	if err != nil {
		return nil, err
	}
	return &probeConn{Conn: c, p: p}, nil
}

type probeConn struct {
	driver.Conn
	p  *writerProbe
	tx *probeTx
}

func (c *probeConn) observe(q string) {
	if c.tx != nil && strings.Contains(q, "CROSS JOIN devices d") {
		c.tx.rangeChunk = true
	}
	c.p.observe(q)
}

func (c *probeConn) Prepare(q string) (driver.Stmt, error) {
	s, err := c.Conn.Prepare(q)
	c.p.record("PREPARE "+q, err)
	if err != nil {
		return nil, err
	}
	return &probeStmt{Stmt: s, p: c.p, c: c, q: q}, nil
}
func (c *probeConn) Begin() (driver.Tx, error) {
	c.p.observe("BEGIN IMMEDIATE")
	tx, err := c.Conn.Begin()
	return c.wrapTx(tx, err)
}
func (c *probeConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	c.p.observe("BEGIN IMMEDIATE")
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err := b.BeginTx(ctx, o)
		return c.wrapTx(tx, err)
	}
	tx, err := c.Conn.Begin()
	return c.wrapTx(tx, err)
}
func (c *probeConn) wrapTx(tx driver.Tx, err error) (driver.Tx, error) {
	c.p.record("BEGIN IMMEDIATE", err)
	if err != nil {
		return nil, err
	}
	c.tx = &probeTx{Tx: tx, p: c.p, start: time.Now()}
	return c.tx, nil
}

type probeTx struct {
	driver.Tx
	p          *writerProbe
	start      time.Time
	rangeChunk bool
}

func (tx *probeTx) Commit() error {
	err := tx.Tx.Commit()
	tx.p.record("COMMIT", err)
	if tx.p.after != nil {
		kind := "COMMIT"
		if tx.rangeChunk {
			kind = "COMMIT RANGE"
		}
		tx.p.after(kind, time.Since(tx.start))
	}
	return err
}

func (tx *probeTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.p.record("ROLLBACK", err)
	return err
}

func (c *probeConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	e, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.observe(q)
	start := time.Now()
	res, err := e.ExecContext(ctx, q, args)
	c.p.record(q, err)
	if c.p.after != nil {
		c.p.after(q, time.Since(start))
	}
	return res, err
}
func (c *probeConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	e, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.observe(q)
	start := time.Now()
	rows, err := e.QueryContext(ctx, q, args)
	c.p.record(q, err)
	if err != nil {
		return nil, err
	}
	return &probeRows{Rows: rows, p: c.p, q: q, start: start}, nil
}

type probeStmt struct {
	driver.Stmt
	p *writerProbe
	c *probeConn
	q string
}

func (s *probeStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.c.observe(s.q)
	start := time.Now()
	r, e := s.Stmt.Exec(args)
	s.p.record(s.q, e)
	if s.p.after != nil {
		s.p.after(s.q, time.Since(start))
	}
	return r, e
}
func (s *probeStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.c.observe(s.q)
	start := time.Now()
	r, e := s.Stmt.Query(args)
	s.p.record(s.q, e)
	if e != nil {
		return nil, e
	}
	return &probeRows{Rows: r, p: s.p, q: s.q, start: start}, nil
}

type probeRows struct {
	driver.Rows
	p     *writerProbe
	q     string
	start time.Time
	once  sync.Once
}

func (r *probeRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	r.p.record(r.q, err)
	return err
}

func (r *probeRows) Close() error {
	err := r.Rows.Close()
	r.p.record(r.q, err)
	r.once.Do(func() {
		if r.p.after != nil {
			r.p.after(r.q, time.Since(r.start))
		}
	})
	return err
}

var probeSerial atomic.Uint64

func attachWriterProbe(t *testing.T, b *baseDB) *writerProbe {
	t.Helper()
	p := &writerProbe{Driver: b.sql.Driver(), db: b}
	for q, s := range b.statements {
		_ = s.Close()
		delete(b.statements, q)
	}
	if err := b.sql.Close(); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("writer_probe_%d", probeSerial.Add(1))
	sql.Register(name, p)
	u := url.URL{Scheme: "file", Path: fileToUriPath(b.path), RawQuery: commonOptions}
	pool, err := sqlx.Open(name, u.String())
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(8)
	pool.SetMaxIdleConns(8)
	b.sql = pool
	// Set the busy timeout on every connection, not merely one pool member.
	conns := make([]*sql.Conn, 8)
	for i := range conns {
		conns[i], err = pool.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conns[i].ExecContext(context.Background(), "PRAGMA busy_timeout=50"); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range conns {
		_ = c.Close()
	}
	return p
}

// This adapter deliberately also compiles on 371de9964, so the exact same
// regression can be run against the original tidy, not a reimplementation.
func runProbedTidy(ctx context.Context, b *baseDB) error {
	switch f := any(tidy).(type) {
	case func(context.Context, *baseDB) error:
		return f(ctx, b)
	case func(context.Context, *sqlx.DB) error:
		return f(ctx, b.sql)
	default:
		panic("unknown tidy signature")
	}
}

func TestTidySingleWriter(t *testing.T) {
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	fdb, err := sdb.getFolderDB("test", true)
	if err != nil {
		t.Fatal(err)
	}
	// A sizeable, indexed synthetic table and a multi-batch freelist. The
	// setup is exclusive, before attaching the production-write observer.
	for _, q := range []string{
		`PRAGMA auto_vacuum = INCREMENTAL`,
		`VACUUM`,
		`CREATE TABLE tidy_fixture (k INTEGER PRIMARY KEY, v TEXT)`,
		`CREATE INDEX tidy_fixture_v ON tidy_fixture(v)`,
		`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO tidy_fixture SELECT x, printf('%08d',x%100)||zeroblob(100) FROM n`,
		`CREATE TABLE tidy_garbage (v BLOB)`,
		`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<512) INSERT INTO tidy_garbage SELECT zeroblob(4096) FROM n`,
		`DROP TABLE tidy_garbage`,
	} {
		if _, err := fdb.sql.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	p := attachWriterProbe(t, fdb.baseDB)
	var once sync.Once
	done := make(chan error, 1)
	var updated atomic.Bool
	var interleaved atomic.Bool
	var maxStep time.Duration
	var steps, vacuums int
	p.before = func(q string) {
		if strings.HasPrefix(q, "PRAGMA") || strings.HasPrefix(q, "ANALYZE") {
			if updated.Load() {
				interleaved.Store(true)
			}
		}
	}
	p.after = func(q string, d time.Duration) {
		if !strings.HasPrefix(q, "PRAGMA") && !strings.HasPrefix(q, "ANALYZE") {
			return
		}
		if d > maxStep {
			maxStep = d
		}
		steps++
		if strings.Contains(q, "incremental_vacuum") {
			vacuums++
		}
		once.Do(func() {
			go func() {
				err := fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{genFile("between-tidy-steps", 1, 1)}, db.UpdateOptions{})
				updated.Store(err == nil)
				done <- err
			}()
			// Park an Update on the mutex for long enough to request fair handoff.
			time.Sleep(5 * time.Millisecond)
		})
	}
	if err := runProbedTidy(context.Background(), fdb.baseDB); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Update stalled behind tidy")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.violations) > 0 {
		t.Errorf("unlocked production writes: %v", p.violations)
	}
	if !interleaved.Load() {
		t.Error("Update did not interleave between tidy statements")
	}
	if vacuums < 2 {
		t.Errorf("vacuum was not chunked: %d statements", vacuums)
	}
	if maxStep > 2*time.Second {
		t.Errorf("bounded tidy step took %s", maxStep)
	}
	t.Logf("tidy fixture=100000 indexed rows steps=%d vacuumSteps=%d maxStep=%s updateInterleaved=%v", steps, vacuums, maxStep, interleaved.Load())
}
