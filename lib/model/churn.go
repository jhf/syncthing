// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syncthing/syncthing/internal/activityhist"
	"github.com/syncthing/syncthing/lib/protocol"
)

type churnPullError struct{ text, class string }

func (e churnPullError) String() string { return e.text }

const (
	churnWindow         = time.Minute
	churnParents        = 8
	churnSamples        = 4
	churnStringBytes    = 256
	churnProcessLines   = 120
	churnDetectFailures = 30 // 5x patch/46's 3-6/min post-drip WARN proxy baseline
)

// All dimensions are finite constants, never paths, peers or error strings.
var churnKeys = [...]string{
	"attempts", "completed", "cancelled", "failures", "other_item_failures", "block_requests", "block_failures", "warn_emitted", "warn_suppressed", "metadata_emitted", "metadata_suppressed",
	"class.remote_no_such_file", "class.local_modified", "class.no_source", "class.local_io", "class.other",
	"stage.shortcut_check", "stage.local_copy", "stage.block_request", "stage.finalize",
	"need.queue_pop.still_needed", "need.queue_pop.superseded", "need.queue_pop.tombstoned", "need.queue_pop.global_missing", "need.queue_pop.already_satisfied", "need.queue_pop.invalid_or_type_changed", "need.queue_pop.db_error", "need.queue_pop.not_checked",
	"need.failure_sample.still_needed", "need.failure_sample.superseded", "need.failure_sample.tombstoned", "need.failure_sample.global_missing", "need.failure_sample.already_satisfied", "need.failure_sample.invalid_or_type_changed", "need.failure_sample.db_error", "need.failure_sample.not_checked",
	"source.real_missing", "source.nonregular", "source.stat_other", "source.traversal", "source.temp_missing_or_nonregular", "source.hash_mismatch", "source.read_other",
	"queue.admit", "queue.defer", "queue.expire", "queue.clear_success", "queue.clear_obsolete",
	"queue.retry_clear_success", "queue.retry_clear_obsolete", "queue.stale_clear", "queue.state_capacity_bypass",
	"queue.scope_receive_encrypted", "queue.disabled", "queue.wake", "queue.wake_late_ms",
	"drip.held_updates", "drip.released_entries", "drip.coalesced_away", "drip.quiet", "drip.max_hold", "drip.shutdown",
	"drip.discarded_entries", "drip.discarded_updates", "drip.superseded_immediate",
	"backlog.zero", "backlog.nonzero", "backlog.db_error",
	"trigger.index", "trigger.scan", "trigger.retry", "trigger.initial", "trigger.other", "trigger.mixed", "trigger.unknown",
}

// The fixed upper bound is intentionally independent of input cardinality.
type churnCounts struct {
	values                                [len(churnKeys)]uint64
	first, last                           time.Time
	samples                               [churnSamples]churnSample
	sampleN                               int
	omitted, overflow, truncated, evicted uint64
	heldMax                               float64
}

type churnSample struct {
	Path       string `json:"path"`
	Event      string `json:"event"`
	Phase      string `json:"phase,omitempty"`
	LocalState string `json:"local_state,omitempty"`
	At         string `json:"at"`
	Run        uint64 `json:"run,omitempty"`
	Attempt    uint64 `json:"attempt,omitempty"`
	Iteration  uint64 `json:"iteration,omitempty"`
	Trigger    string `json:"trigger,omitempty"`
	Class      string `json:"class,omitempty"`
	Stage      string `json:"stage,omitempty"`
	Outcome    string `json:"outcome,omitempty"`
	Captured   string `json:"captured,omitempty"`
	Current    string `json:"current,omitempty"`
	Local      string `json:"local,omitempty"`
	View       string `json:"view,omitempty"`
	Peer       string `json:"peer,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	Size       int    `json:"size,omitempty"`
	Hash       string `json:"hash,omitempty"`
	Temporary  bool   `json:"temporary,omitempty"`
}

func churnIndex(key string) int {
	for i, k := range churnKeys {
		if key == k {
			return i
		}
	}
	panic("unregistered churn counter: " + key)
}
func (c *churnCounts) add(now time.Time, key string, n uint64) {
	c.values[churnIndex(key)] += n
	if c.first.IsZero() {
		c.first = now
	}
	c.last = now
}
func churnBound(s string) string {
	if len(s) > churnStringBytes {
		return strings.Clone(strings.ToValidUTF8(s[:churnStringBytes], "?"))
	}
	return strings.Clone(s)
}
func (c *churnCounts) sample(s churnSample) bool {
	if c.sampleN == churnSamples {
		c.omitted++
		return false
	}
	if len(s.Path) > churnStringBytes {
		c.truncated++
	}
	s.Path = churnBound(s.Path)
	c.samples[c.sampleN] = s
	c.sampleN++
	return true
}

// No logging, database calls, or user callbacks under mu. The first four
// representative observations form a bounded reservoir (not a random sample).
type churnTracker struct {
	activity        *activityhist.Hist
	budget          *churnBudget
	mu              sync.Mutex
	start           time.Time
	parents         map[string]*churnCounts
	other, total    churnCounts
	examples        uint8
	failureSamples  int
	backlogRead     bool
	settled         bool
	active          bool
	emissionDenied  uint64
	deferredPending int
	deferredOldest  float64
	run, iteration  atomic.Uint64
	trigger         atomic.Uint32
}

func newChurnTracker(now time.Time) *churnTracker {
	return &churnTracker{start: now, parents: make(map[string]*churnCounts), budget: &globalChurnBudget, activity: activityhist.Global}
}

func (c *churnTracker) retryPending(pending int, oldest float64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.deferredPending, c.deferredOldest = pending, oldest
	c.mu.Unlock()
}
func (c *churnTracker) bucket(path string) *churnCounts {
	parent := filepath.ToSlash(filepath.Dir(path))
	// Overlong parent names cannot alias a real named bucket by truncation.
	if len(parent) <= churnStringBytes {
		if b := c.parents[parent]; b != nil {
			return b
		}
		if len(c.parents) < churnParents {
			b := new(churnCounts)
			c.parents[strings.Clone(parent)] = b
			return b
		}
	}
	c.other.overflow++
	c.total.overflow++
	return &c.other
}
func (c *churnTracker) count(path, key string, n uint64) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bucket(path).add(now, key, n)
	c.total.add(now, key, n)
}
func (c *churnTracker) admit(path string) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(path)
	for _, key := range []string{"attempts", "queue.admit", "trigger." + churnTrigger(c.trigger.Load())} {
		b.add(now, key, 1)
		c.total.add(now, key, 1)
	}
}

func (c *churnTracker) dripFlush(path string, held int, age float64, reason string) {
	if c != nil {
		defer c.activity.Add(time.Now())
	} // replace the old per-path INFO activity
	if reason == "max-hold" {
		reason = "max_hold"
	}
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(path)
	for _, dst := range []*churnCounts{b, &c.total} {
		dst.add(now, "drip.held_updates", uint64(held))
		dst.add(now, "drip.released_entries", 1)
		dst.add(now, "drip.coalesced_away", uint64(max(held-1, 0)))
		dst.add(now, "drip."+reason, 1)
		dst.heldMax = max(dst.heldMax, age)
		dst.sample(churnSample{Path: path, Event: "drip_coalescing", Outcome: reason, At: now.UTC().Format(time.RFC3339Nano)})
	}
}

// Discarded cohorts never reach the DB and are not released/coalesced-away
// flush cohorts. Keep their update count separate from dripFlush's counters.
func (c *churnTracker) dripDiscard(path string, held int, age float64) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(path)
	for _, dst := range []*churnCounts{b, &c.total} {
		dst.add(now, "drip.discarded_entries", 1)
		dst.add(now, "drip.discarded_updates", uint64(held))
		dst.add(now, "drip.superseded_immediate", 1)
		dst.heldMax = max(dst.heldMax, age)
		dst.sample(churnSample{Path: path, Event: "drip_coalescing", Outcome: "superseded_immediate", At: now.UTC().Format(time.RFC3339Nano)})
	}
}

func (c *churnTracker) observe(s churnSample, key string) bool {
	if c == nil {
		return false
	}
	now := time.Now()
	s.At = now.UTC().Format(time.RFC3339Nano)
	s.Run = c.run.Load()
	s.Iteration = c.iteration.Load()
	s.Trigger = churnTrigger(c.trigger.Load())
	if s.Event == "source_reject" {
		s.Run = 0
		s.Iteration = 0
		s.Trigger = ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(s.Path)
	b.add(now, key, 1)
	c.total.add(now, key, 1)
	sampled := b.sample(s)
	c.total.sample(s)
	return sampled
}

// One shared token budget covers summary, transition AND example records.
// Denied summaries remain accumulated with their original start and diagnostics.
type churnBudget struct {
	mu                sync.Mutex
	start             time.Time
	used              int
	denied            atomic.Uint64
	shutdownUnemitted atomic.Uint64
	shutdownCounts    [len(churnKeys)]atomic.Uint64
}

var globalChurnBudget churnBudget

func (b *churnBudget) take(now time.Time, n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.start.IsZero() || now.Sub(b.start) >= churnWindow {
		b.start = now
		b.used = 0
	}
	if b.used+n > churnProcessLines {
		b.denied.Add(uint64(n))
		return false
	}
	b.used += n
	return true
}
func (c *churnTracker) example(path, class, kind string) bool {
	if c == nil {
		return true
	}
	if class != "remote_no_such_file" && class != "local_modified" {
		c.count(path, kind+"_emitted", 1)
		return true
	}
	bit := uint8(1)
	if class == "local_modified" {
		bit <<= 1
	}
	if kind == "metadata" {
		bit <<= 2
	}
	c.mu.Lock()
	allow := c.examples&bit == 0
	if allow {
		c.examples |= bit
	}
	c.mu.Unlock()
	allow = allow && c.budget.take(time.Now(), 1)
	if allow {
		c.count(path, kind+"_emitted", 1)
	} else {
		c.count(path, kind+"_suppressed", 1)
		// Preserve the old maintenance activity proxy at the original event time,
		// without producing another log record or waiting until the summary.
		c.activity.Add(time.Now())
	}
	return allow
}
func churnAttrs(b churnCounts, start, end time.Time, parent, kind string, denied, processDenied uint64) []any {
	args := []any{"windows_merged", 1 + denied, "bucket_kind", kind, "process_emission_denied", processDenied, "churn.schema", 1, "window.start", start.UTC().Format(time.RFC3339Nano), "window.end", end.UTC().Format(time.RFC3339Nano), "parent", parent, "first", b.first.UTC().Format(time.RFC3339Nano), "last", b.last.UTC().Format(time.RFC3339Nano), "samples_evicted", b.evicted, "samples_omitted", b.omitted, "bucket_overflow", b.overflow, "bucket_evictions", 0, "samples_truncated", b.truncated, "input_lost", 0, "emission_denied", denied, "deferred_pending", 0, "deferred_oldest_s", 0, "drip.held_max_s", b.heldMax}
	for i, k := range churnKeys {
		args = append(args, k, b.values[i])
	}
	samples, _ := json.Marshal(b.samples[:b.sampleN])
	return append(args, "samples", string(samples))
}
func (c *churnTracker) flush(now time.Time, sl *slog.Logger, budget *churnBudget) {
	if c == nil {
		return
	}
	type record struct {
		msg          string
		level        slog.Level
		args         []any
		counts       churnCounts
		parent, kind string
	}
	c.mu.Lock()
	failures := c.total.values[churnIndex("failures")]
	rate := float64(failures) * float64(churnWindow) / float64(max(now.Sub(c.start), churnWindow))
	detected := !c.active && rate >= churnDetectFailures
	// Recovery means no failures AND a successful zero-need pull observed in
	// this window. An idle but still failed folder is not falsely recovered.
	recovered := c.active && failures == 0 && c.settled && c.deferredPending == 0
	if c.total.first.IsZero() && !recovered {
		c.examples = 0
		c.failureSamples = 0
		c.backlogRead = false
		c.start = now
		c.mu.Unlock()
		return
	}
	n := len(c.parents) + 1
	if !c.other.first.IsZero() {
		n++
	}
	if detected || recovered {
		n++
	}
	if !budget.take(now, n) {
		c.emissionDenied++
		c.examples = 0
		c.failureSamples = 0
		c.backlogRead = false
		c.mu.Unlock()
		return
	}
	records := make([]record, 0, n)
	names := make([]string, 0, len(c.parents))
	for p := range c.parents {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		records = append(records, record{msg: "Pull churn summary", level: slog.LevelInfo, counts: *c.parents[p], parent: p, kind: "named"})
	}
	if !c.other.first.IsZero() {
		records = append(records, record{msg: "Pull churn summary", level: slog.LevelInfo, counts: c.other, parent: "other_dirs", kind: "overflow"})
	}
	records = append(records, record{msg: "Pull churn totals", level: slog.LevelInfo, counts: c.total, kind: "total"})
	if detected || recovered {
		msg, level := "Pull churn recovered", slog.LevelInfo
		if detected {
			msg, level = "Pull churn detected", slog.LevelWarn
		}
		records = append(records, record{msg: msg, level: level, args: []any{"window.start", c.start.UTC().Format(time.RFC3339Nano), "window.end", now.UTC().Format(time.RFC3339Nano), "churn.schema", 1, "failure_rate_per_minute", rate, "threshold", churnDetectFailures}})
		c.active = detected
	}
	start, denied := c.start, c.emissionDenied
	pending, oldest := c.deferredPending, c.deferredOldest
	c.start = now
	c.parents = make(map[string]*churnCounts)
	c.other = churnCounts{}
	c.total = churnCounts{}
	c.examples = 0
	c.failureSamples = 0
	c.backlogRead = false
	c.emissionDenied = 0
	c.mu.Unlock()
	for _, r := range records {
		if r.kind != "" {
			r.args = churnAttrs(r.counts, start, now, r.parent, r.kind, denied, c.budget.denied.Load())
			// These are folder gauges, not parent operation counts.
			for i := 0; i < len(r.args); i += 2 {
				if r.args[i] == "deferred_pending" {
					r.args[i+1] = pending
				}
				if r.args[i] == "deferred_oldest_s" {
					r.args[i+1] = oldest
				}
			}
			r.args = append(r.args, "process_shutdown_unemitted_windows", c.budget.shutdownUnemitted.Load())
			if r.kind == "total" {
				for i, key := range churnKeys {
					if n := c.budget.shutdownCounts[i].Load(); n > 0 {
						r.args = append(r.args, "process_shutdown_unemitted."+key, n)
					}
				}
			}
		}
		sl.Log(context.Background(), r.level, r.msg, r.args...)
	}
}
func (c *churnTracker) complete(path string) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(path)
	for _, dst := range []*churnCounts{b, &c.total} {
		dst.add(now, "completed", 1)
		dst.add(now, "queue.clear_success", 1)
	}
}
func (c *churnTracker) clearSuccess() { c.noteBacklog(0, nil) }
func (c *churnTracker) noteBacklog(items int, err error) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	key := "backlog.nonzero"
	c.settled = err == nil && items == 0
	if err != nil {
		key = "backlog.db_error"
	} else if items == 0 {
		key = "backlog.zero"
	}
	b := c.bucket(".")
	b.add(now, key, 1)
	c.total.add(now, key, 1)
}
func (c *churnTracker) wantsBacklogRead() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active || c.backlogRead {
		return false
	}
	c.backlogRead = true
	return true
}

func (c *churnTracker) serve(ctx context.Context, sl *slog.Logger) {
	ticker := time.NewTicker(churnWindow)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			c.flush(now, sl, c.budget)
		case <-ctx.Done():
			c.flush(time.Now(), sl, c.budget)
			// Preserve a final refused batch in bounded process diagnostics.
			c.mu.Lock()
			if !c.total.first.IsZero() {
				c.budget.shutdownUnemitted.Add(1)
				for i, n := range c.total.values {
					c.budget.shutdownCounts[i].Add(n)
				}
			}
			c.mu.Unlock()
			return
		}
	}
}

const (
	churnIndexTrigger uint32 = 1 << iota
	churnScanTrigger
	churnRetryTrigger
	churnInitialTrigger
	churnOtherTrigger
)

func churnTrigger(bits uint32) string {
	if bits == 0 {
		return "unknown"
	}
	if bits&(bits-1) != 0 {
		return "mixed"
	}
	switch bits {
	case churnIndexTrigger:
		return "index"
	case churnScanTrigger:
		return "scan"
	case churnRetryTrigger:
		return "retry"
	case churnInitialTrigger:
		return "initial"
	default:
		return "other"
	}
}

type churnStageError struct {
	error
	stage string
}

func (e churnStageError) Unwrap() error { return e.error }
func churnAt(stage string, err error) error {
	if err == nil {
		return nil
	}
	return churnStageError{err, stage}
}
func churnClass(err error) string {
	switch {
	case errors.Is(err, protocol.ErrNoSuchFile):
		return "remote_no_such_file"
	case errors.Is(err, errModified):
		return "local_modified"
	case errors.Is(err, errNoDevice), errors.Is(err, errNotAvailable):
		return "no_source"
	}
	var pe *os.PathError
	var le *os.LinkError
	var se *os.SyscallError
	if errors.As(err, &pe) || errors.As(err, &le) || errors.As(err, &se) || errors.Is(err, io.ErrShortWrite) {
		return "local_io"
	}
	return "other"
}
func churnStage(err error, fallback string) string {
	var e churnStageError
	if errors.As(err, &e) {
		return e.stage
	}
	return fallback
}
func churnToken(fi protocol.FileInfo) string {
	return churnHash([]byte(fi.Version.String() + fmt.Sprintf("/%d/%t/%d/", fi.Type, fi.Deleted, fi.Size) + hex.EncodeToString(fi.BlocksHash)))
}
func churnHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:8]) }

// Separate reads are explicitly labelled raced, never advertised as a snapshot.
// This is at most four post-failure observations per folder emission interval,
// not a per-block query. A supersession is only evidence at observation time.
func (f *sendReceiveFolder) churnFailure(path string, err error, captured *protocol.FileInfo) {
	c := f.churn
	if c == nil {
		return
	}
	class, stage := churnClass(err), churnStage(err, "finalize")
	c.mu.Lock()
	check := captured != nil && c.failureSamples < churnSamples
	if check {
		c.failureSamples++
	}
	c.mu.Unlock()
	s := churnSample{Path: path, Event: "pull_failure", Phase: "failure_sample", Class: class, Stage: stage, Attempt: retryAttemptID(err), Outcome: "not_checked", View: "not_checked"}
	if check {
		s.Captured = churnToken(*captured)
		global, ok, e := f.db.GetGlobalFile(f.folderID, path)
		local, hasLocal, le := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, path)
		s.View = "raced_separate_reads"
		if ok {
			s.Current = churnToken(global)
		}
		if hasLocal {
			s.Local = churnToken(local)
			s.LocalState = "present"
		} else {
			s.LocalState = "missing"
		}
		if le != nil {
			s.LocalState = "unknown"
		}
		s.Outcome = churnNeed(*captured, global, ok, local, hasLocal, errors.Join(e, le))
	}
	now := time.Now()
	s.At = now.UTC().Format(time.RFC3339Nano)
	s.Run = c.run.Load()
	s.Iteration = c.iteration.Load()
	s.Trigger = churnTrigger(c.trigger.Load())
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.bucket(path)
	c.settled = false
	failureKey := "failures"
	if captured == nil {
		failureKey = "other_item_failures"
		s.Event = "item_failure"
	}
	for _, dst := range []*churnCounts{b, &c.total} {
		for _, key := range []string{failureKey, "class." + class, "stage." + stage, "need.failure_sample." + s.Outcome} {
			dst.add(now, key, 1)
		}
		// Checked failures take priority over earlier queue/request detail.
		if check && dst.sampleN == churnSamples {
			replace := churnSamples - 1
			for i, sample := range dst.samples {
				if sample.Event != "pull_failure" || sample.Outcome == "not_checked" {
					replace = i
					break
				}
			}
			dst.samples[replace] = dst.samples[churnSamples-1]
			dst.sampleN--
			dst.evicted++
			dst.omitted++
		}
		dst.sample(s)
	}
}
func churnNeed(captured, global protocol.FileInfo, exists bool, local protocol.FileInfo, hasLocal bool, err error) string {
	if err != nil {
		return "db_error"
	}
	if !exists {
		return "global_missing"
	}
	if global.IsDeleted() {
		return "tombstoned"
	}
	if global.IsInvalid() || global.Type != protocol.FileInfoTypeFile {
		return "invalid_or_type_changed"
	}
	if !captured.Version.Equal(global.Version) || !captured.BlocksEqual(global) {
		return "superseded"
	}
	// This is a real local/global version comparison, matching the DB's equal
	// or better-version rule, not a global-existence proxy.
	if hasLocal && (local.Version.Equal(global.Version) || local.Version.Compare(global.Version) == protocol.Greater) {
		return "already_satisfied"
	}
	return "still_needed"
}
func (f *folder) schedulePullChurn(trigger uint32) { f.churnPending.Or(trigger); f.notifyPull() }
func scheduleIndexPull(runner service) {
	if r, ok := runner.(interface{ schedulePullChurn(uint32) }); ok {
		r.schedulePullChurn(churnIndexTrigger)
	} else {
		runner.SchedulePull()
	}
}
func (m *model) noteChurnSourceReject(req *protocol.Request, reason string) {
	if runner, ok := m.folderRunners.Get(req.Folder); ok {
		if r, ok := runner.(interface {
			noteSourceReject(*protocol.Request, string)
		}); ok {
			r.noteSourceReject(req, reason)
		}
	}
}

func (f *folder) noteSourceReject(req *protocol.Request, reason string) {
	f.churn.observe(churnSample{Path: req.Name, Event: "source_reject", Outcome: reason, Offset: req.Offset, Size: int(req.Size), Hash: churnHash(req.Hash), Temporary: req.FromTemporary}, "source."+reason)
}
