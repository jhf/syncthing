// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/activityhist"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

type churnTestHandler struct {
	mu      sync.Mutex
	records []slog.Record
	check   func()
}

func (*churnTestHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *churnTestHandler) Handle(_ context.Context, r slog.Record) error {
	if h.check != nil {
		h.check()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *churnTestHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *churnTestHandler) WithGroup(string) slog.Handler      { return h }
func churnAttr(r slog.Record, key string) slog.Value {
	var v slog.Value
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value
		}
		return true
	})
	return v
}
func churnTestTracker() *churnTracker {
	c := newChurnTracker(time.Now())
	c.budget = new(churnBudget)
	return c
}

type churnReadDB struct {
	db.DB
	global, local           protocol.FileInfo
	exists, hasLocal        bool
	err                     error
	globalReads, localReads int
	check                   func()
}

func (d *churnReadDB) GetGlobalFile(string, string) (protocol.FileInfo, bool, error) {
	if d.check != nil {
		d.check()
	}
	d.globalReads++
	return d.global, d.exists, d.err
}
func (d *churnReadDB) GetDeviceFile(string, protocol.DeviceID, string) (protocol.FileInfo, bool, error) {
	if d.check != nil {
		d.check()
	}
	d.localReads++
	return d.local, d.hasLocal, d.err
}

func TestChurnTotalsReconcileUnderSuppression(t *testing.T) {
	c := churnTestTracker()
	fi := protocol.FileInfo{Name: "hot/file", Version: protocol.Vector{}.Update(1)}
	d := &churnReadDB{global: fi, exists: true}
	f := &sendReceiveFolder{folder: &folder{db: d, churn: c, sl: slog.New(slog.NewTextHandler(io.Discard, nil))}, tempPullErrors: make(map[string]churnPullError)}
	d.check = func() {
		if !f.errorsMut.TryLock() {
			t.Fatal("database read under error lock")
		}
		f.errorsMut.Unlock()
		if !c.mu.TryLock() {
			t.Fatal("database read under churn lock")
		}
		c.mu.Unlock()
	}
	emitted := 0
	for i := range 1000 {
		name := fmt.Sprintf("parent-%d/file", i)
		fi.Name = name
		err := churnAt("shortcut_check", errModified)
		if i%2 == 0 {
			err = churnAt("block_request", protocol.ErrNoSuchFile)
		}
		c.admit(name)
		f.newPullError(name, fmt.Errorf("outer wrapper: %w", err), fi)
		// Exactly the producer decision made by the last-iteration warning loop.
		if c.example(name, churnClass(err), "warn") {
			emitted++
		}
	}
	want := map[string]uint64{"attempts": 1000, "failures": 1000, "class.remote_no_such_file": 500, "class.local_modified": 500, "warn_emitted": 2, "warn_suppressed": 998, "need.failure_sample.still_needed": 4, "need.failure_sample.not_checked": 996, "queue.defer": 0}
	for k, n := range want {
		if got := c.total.values[churnIndex(k)]; got != n {
			t.Errorf("%s=%d want %d", k, got, n)
		}
	}
	if emitted != 2 || len(f.tempPullErrors) != 1000 {
		t.Fatalf("emitted=%d current errors=%d", emitted, len(f.tempPullErrors))
	}
	if d.globalReads != 4 || d.localReads != 4 {
		t.Fatalf("read pairs=%d/%d want 4/4", d.globalReads, d.localReads)
	}
	if len(c.parents) != 8 || c.other.values[churnIndex("failures")] != 992 {
		t.Fatalf("parents=%d overflow failures=%d", len(c.parents), c.other.values[churnIndex("failures")])
	}
	if c.total.sampleN > churnSamples || c.other.sampleN > churnSamples || c.total.omitted == 0 {
		t.Fatal("sample bound/diagnostic missing")
	}
	var sum uint64
	for _, b := range c.parents {
		sum += b.values[churnIndex("failures")]
		if b.sampleN > churnSamples {
			t.Fatal("parent sample overflow")
		}
	}
	sum += c.other.values[churnIndex("failures")]
	if sum != c.total.values[churnIndex("failures")] {
		t.Fatal("parent counts do not reconcile")
	}
	h := new(churnTestHandler)
	h.check = func() {
		if !c.mu.TryLock() {
			t.Fatal("logging under churn lock")
		}
		c.mu.Unlock()
	}
	c.flush(c.start.Add(time.Minute), slog.New(h), c.budget)
	if len(h.records) != 11 {
		t.Fatalf("records=%d want 10 summaries plus 1 transition", len(h.records))
	}
	totals := h.records[len(h.records)-2]
	if totals.Message != "Pull churn totals" || churnAttr(totals, "failures").Uint64() != 1000 || churnAttr(totals, "samples_omitted").Uint64() == 0 {
		t.Fatal("totals missing exact counts/overflow diagnostics")
	}
}

func TestChurnNeedOutcomes(t *testing.T) {
	fi := protocol.FileInfo{Name: "file", Version: protocol.Vector{}.Update(1)}
	cases := []struct {
		name     string
		mutate   func(*churnReadDB)
		captured protocol.FileInfo
	}{
		{"still_needed", func(*churnReadDB) {}, fi},
		{"already_satisfied", func(d *churnReadDB) { d.local = fi; d.hasLocal = true }, fi},
		{"superseded", func(d *churnReadDB) { d.global.Version = d.global.Version.Copy().Update(1) }, fi},
		{"tombstoned", func(d *churnReadDB) { d.global.Deleted = true }, fi},
		{"global_missing", func(d *churnReadDB) { d.exists = false }, fi},
		{"invalid_or_type_changed", func(d *churnReadDB) { d.global.Type = protocol.FileInfoTypeDirectory }, fi},
		{"db_error", func(d *churnReadDB) { d.err = errors.New("database unavailable") }, fi},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := churnTestTracker()
			d := &churnReadDB{global: fi, exists: true}
			tc.mutate(d)
			f := &sendReceiveFolder{folder: &folder{db: d, churn: c}}
			f.churnFailure(fi.Name, errModified, &tc.captured)
			if got := c.total.values[churnIndex("need.failure_sample."+tc.name)]; got != 1 {
				t.Fatalf("outcome count=%d samples=%+v", got, c.total.samples)
			}
			sample := c.total.samples[0]
			if sample.View != "raced_separate_reads" || sample.At == "" || sample.Captured == "" {
				t.Fatalf("observation missing honesty/tokens: %+v", sample)
			}
		})
	}
	t.Run("not_checked", func(t *testing.T) {
		c := churnTestTracker()
		c.failureSamples = churnSamples
		f := &sendReceiveFolder{folder: &folder{churn: c}} // no DB, a query would panic
		f.churnFailure(fi.Name, errModified, &fi)
		if c.total.values[churnIndex("need.failure_sample.not_checked")] != 1 {
			t.Fatal("unsampled failure not counted")
		}
	})
	t.Run("local_newer", func(t *testing.T) {
		local := fi
		local.Version = local.Version.Copy().Update(1)
		if got := churnNeed(fi, fi, true, local, true, nil); got != "already_satisfied" {
			t.Fatal(got)
		}
	})
}

func TestChurnQueuePopExistingChecks(t *testing.T) {
	cases := []struct {
		outcome string
		global  protocol.FileInfo
		exists  bool
		err     error
	}{
		{"global_missing", protocol.FileInfo{}, false, nil},
		{"tombstoned", protocol.FileInfo{Deleted: true}, true, nil},
		{"invalid_or_type_changed", protocol.FileInfo{Type: protocol.FileInfoTypeDirectory}, true, nil},
		{"db_error", protocol.FileInfo{}, false, errors.New("read failed")},
	}
	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			c := churnTestTracker()
			d := &churnReadDB{global: tc.global, exists: tc.exists, err: tc.err}
			f := &sendReceiveFolder{folder: &folder{churn: c, model: &model{sdb: d}}, queue: newJobQueue()}
			f.queue.Push("file", 0, time.Now())
			err := f.processQueue(t.Context(), nil, nil, nil, nil, nil)
			if (err != nil) != (tc.err != nil) {
				t.Fatal(err)
			}
			if d.globalReads != 1 || d.localReads != 0 {
				t.Fatalf("extra reads: %d/%d", d.globalReads, d.localReads)
			}
			if c.total.values[churnIndex("need.queue_pop."+tc.outcome)] != 1 {
				t.Fatalf("wrong outcome %+v", c.total)
			}
		})
	}
}

func TestChurnProcessBudgetRetainsDeniedTotals(t *testing.T) {
	now := time.Now()
	budget := new(churnBudget)
	h := new(churnTestHandler)
	sl := slog.New(h)
	var denied []*churnTracker
	for range 1000 {
		c := newChurnTracker(now)
		c.budget = budget
		for i := range 1000 {
			c.count(fmt.Sprintf("parent%d/file", i), "attempts", 1)
		}
		c.flush(now.Add(time.Minute), sl, budget)
		if c.emissionDenied > 0 {
			denied = append(denied, c)
		}
	}
	if len(h.records) > churnProcessLines || len(denied) == 0 || budget.denied.Load() == 0 {
		t.Fatalf("global bound: records=%d denied=%d", len(h.records), len(denied))
	}
	c := denied[0]
	if len(c.parents) != 8 || c.total.values[churnIndex("attempts")] != 1000 {
		t.Fatal("denied totals lost or cardinality grew")
	}
	n := len(h.records)
	c.flush(now.Add(2*time.Minute), sl, budget)
	if len(h.records)-n != 10 {
		t.Fatal("denied folder did not resume within budget")
	}
	total := h.records[len(h.records)-1]
	if churnAttr(total, "emission_denied").Uint64() != 1 || churnAttr(total, "attempts").Uint64() != 1000 {
		t.Fatal("denial diagnostics or exact totals missing")
	}
}

func TestChurnDeferralAndMixedDetectionCaptureRecovery(t *testing.T) {
	for _, tc := range []struct {
		reason   string
		failures uint64
	}{{"deferrals", 2}, {"both", churnDetectFailures}} {
		t.Run(tc.reason, func(t *testing.T) {
			c := churnTestTracker()
			h := new(churnTestHandler)
			sl := slog.New(h)
			now := c.start
			requests := 0
			c.capture = func() string { requests++; return "accepted" }
			r := newPullRetryTracker(func() time.Duration { return time.Minute }, c)
			r.budget = new(pullRetryBudget)
			r.now = func() time.Time { return now }
			defer r.clearAll()
			r.failure("hot/file", "remote_no_such_file", "block_request", "missing")
			for range churnDetectDeferrals {
				if !r.gate("hot/file") {
					t.Fatal("real deferral not held")
				}
			}
			c.count("hot/file", "failures", tc.failures)
			c.count("hot/file", "class.remote_no_such_file", tc.failures)
			c.flush(now.Add(time.Minute), sl, c.budget)
			detected := h.records[len(h.records)-1]
			if detected.Message != "Pull churn detected" || churnAttr(detected, "detection.reason").String() != tc.reason || churnAttr(detected, "failure_count").Uint64() != tc.failures || churnAttr(detected, "deferral_count").Uint64() != churnDetectDeferrals || requests != 1 {
				t.Fatalf("wrong trigger or capture: %+v requests=%d", detected, requests)
			}
			r.clearAll()
			c.clearSuccess()
			c.flush(now.Add(2*time.Minute), sl, c.budget)
			if !c.active || c.quietWindows != 1 {
				t.Fatal("one quiet window recovered")
			}
			for _, rec := range h.records {
				if rec.Message == "Pull churn recovered" {
					t.Fatal("early recovery")
				}
			}
			c.flush(now.Add(3*time.Minute), sl, c.budget)
			if c.active || h.records[len(h.records)-1].Message != "Pull churn recovered" || requests != 1 {
				t.Fatal("two complete quiet windows failed recovery or recaptured")
			}
		})
	}
}

func TestChurnQuietRecoveryRequiresCompleteConsecutiveWindows(t *testing.T) {
	c := churnTestTracker()
	h := new(churnTestHandler)
	sl := slog.New(h)
	now := c.start
	c.active = true
	c.clearSuccess()
	c.flush(now.Add(30*time.Second), sl, c.budget)
	if c.quietWindows != 0 {
		t.Fatal("partial window counted")
	}
	c.flush(now.Add(90*time.Second), sl, c.budget)
	if c.quietWindows != 1 || !c.active {
		t.Fatal("first complete window wrong")
	}
	c.retryPending(1, 60)
	c.flush(now.Add(150*time.Second), sl, c.budget)
	if c.quietWindows != 0 || !c.active {
		t.Fatal("pending hold did not reset recovery")
	}
	c.retryPending(0, 0)
	c.flush(now.Add(210*time.Second), sl, c.budget)
	if c.quietWindows != 0 || !c.active {
		t.Fatal("window that began with a pending hold counted as quiet")
	}
	c.flush(now.Add(270*time.Second), sl, c.budget)
	if c.quietWindows != 1 || !c.active {
		t.Fatal("first fully quiet window missing")
	}
	// A transient hold that clears before the end still makes this window busy.
	c.retryPending(1, 1)
	c.retryPending(0, 0)
	c.flush(now.Add(330*time.Second), sl, c.budget)
	if c.quietWindows != 0 || !c.active {
		t.Fatal("end-state gauge hid a hold earlier in the window")
	}
	c.count("hot/file", "class.local_modified", 1)
	c.flush(now.Add(390*time.Second), sl, c.budget)
	if c.quietWindows != 0 || !c.active {
		t.Fatal("eligible failure did not reset recovery")
	}
}

func TestChurnCaptureDetectionAdmissionAndBudget(t *testing.T) {
	stop := slogutil.StartChurnCapture(t.Context(), filepath.Join(t.TempDir(), "capture"))
	defer stop(time.Second)
	before := slogutil.ChurnCaptureStats()
	for range 2 {
		c := churnTestTracker()
		c.count("hot/file", "failures", churnDetectFailures)
		c.flush(c.start.Add(time.Minute), slog.New(new(churnTestHandler)), c.budget)
	}
	after := slogutil.ChurnCaptureStats()
	if after.Accepted-before.Accepted != 1 || after.RejectedBusy-before.RejectedBusy != 1 {
		t.Fatalf("detector bypassed singleton: before=%+v after=%+v", before, after)
	}
	c := churnTestTracker()
	requests := 0
	c.capture = func() string { requests++; return "accepted" }
	c.count("hot/file", "failures", churnDetectFailures)
	c.budget.used = churnProcessLines
	c.budget.start = c.start.Add(time.Minute)
	c.flush(c.start.Add(time.Minute), slog.New(new(churnTestHandler)), c.budget)
	if requests != 0 || c.active {
		t.Fatal("denied transition silently triggered capture")
	}
}

func TestChurnTransitionsNoAutomaticDumps(t *testing.T) {
	before := slogutil.LogTrapStatus()
	c := churnTestTracker()
	h := new(churnTestHandler)
	sl := slog.New(h)
	now := c.start
	requests := 0
	c.capture = func() string {
		if !c.mu.TryLock() {
			t.Fatal("capture enqueue under tracker lock")
		}
		c.mu.Unlock()
		if len(h.records) == 0 || h.records[len(h.records)-1].Message != "Pull churn detected" {
			t.Fatal("capture requested before detection entered logger")
		}
		requests++
		return "accepted"
	}
	c.count("hot/file", "failures", churnDetectFailures)
	c.flush(now.Add(time.Minute), sl, c.budget)
	c.count("hot/file", "failures", churnDetectFailures)
	c.flush(now.Add(2*time.Minute), sl, c.budget)
	c.flush(now.Add(3*time.Minute), sl, c.budget) // no success: cannot claim recovery
	c.clearSuccess()
	c.flush(now.Add(4*time.Minute), sl, c.budget)
	c.flush(now.Add(5*time.Minute), sl, c.budget)
	counts := map[string]int{}
	for _, r := range h.records {
		counts[r.Message]++
	}
	if counts["Pull churn detected"] != 1 || counts["Pull churn recovered"] != 1 {
		t.Fatalf("transitions=%v", counts)
	}
	if requests != 1 {
		t.Fatalf("capture requests=%d", requests)
	}
	if !reflect.DeepEqual(before, slogutil.LogTrapStatus()) {
		t.Fatal("churn installed or fired a trap")
	}
	// Guard the producer boundary: only enqueue, never a synchronous dump or
	// automatic trap registration. Explicit externally armed traps are unchanged.
	src, err := os.ReadFile("churn.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"DumpLogRingToFile(", "AddLogTrap("} {
		if strings.Contains(string(src), forbidden) {
			t.Fatalf("automatic capture integration: %s", forbidden)
		}
	}
}

func TestChurnTypedClassesAndStage(t *testing.T) {
	cases := []struct {
		err   error
		class string
	}{
		{protocol.ErrNoSuchFile, "remote_no_such_file"}, {errModified, "local_modified"}, {errNoDevice, "no_source"}, {errNotAvailable, "no_source"},
		{&os.PathError{Op: "open", Path: "file", Err: os.ErrPermission}, "local_io"}, {errors.New("unexpected"), "other"},
	}
	for _, tc := range cases {
		err := fmt.Errorf("finishing: %w", churnAt("block_request", tc.err))
		if churnClass(err) != tc.class || churnStage(err, "finalize") != "block_request" {
			t.Fatalf("lost classification: %v", err)
		}
		s := &sharedPullerState{}
		s.fail(err)
		if churnStage(s.failed(), "finalize") != "block_request" || !errors.Is(s.failed(), tc.err) {
			t.Fatal("first state failure lost typed cause")
		}
	}
}

func TestChurnTriggerCoalescing(t *testing.T) {
	f := &folder{pullScheduled: make(chan struct{}, 1)}
	f.schedulePullChurn(churnIndexTrigger)
	f.schedulePullChurn(churnScanTrigger)
	if len(f.pullScheduled) != 1 || churnTrigger(f.churnPending.Swap(0)) != "mixed" {
		t.Fatal("coalesced scheduling cause was overwritten")
	}
	f.SchedulePull()
	if churnTrigger(f.churnPending.Swap(0)) != "other" || churnTrigger(0) != "unknown" {
		t.Fatal("generic scheduling invented a precise cause")
	}
}

func TestChurnDripSeparateCounters(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newDripTracker(config.FolderConfiguration{}, func() time.Time { return now }, func(string, ...any) { t.Fatal("per-path drip INFO emitted") })
	c := churnTestTracker()
	d.churn = c
	d.held["a/file"] = &dripHeld{fi: protocol.FileInfo{Name: "a/file"}, firstHeld: now.Add(-time.Minute), lastUpdate: now, coalesced: 25}
	d.flushDue()
	d.held["b/file"] = &dripHeld{fi: protocol.FileInfo{Name: "b/file"}, firstHeld: now, lastUpdate: now, coalesced: 2}
	d.flushAll()
	for k, n := range map[string]uint64{"drip.held_updates": 27, "drip.released_entries": 2, "drip.coalesced_away": 25, "drip.max_hold": 1, "drip.shutdown": 1} {
		if c.total.values[churnIndex(k)] != n {
			t.Errorf("%s=%d want %d", k, c.total.values[churnIndex(k)], n)
		}
	}
	if c.total.heldMax != 60 {
		t.Fatal(c.total.heldMax)
	}
}

func TestChurnSuppressionPreservesActivityProxy(t *testing.T) {
	c := churnTestTracker()
	c.activity = &activityhist.Hist{}
	for range 10 {
		c.example("hot/file", "local_modified", "warn")
	}
	var sum uint64
	for _, n := range c.activity.MinuteOfDay {
		sum += n
	}
	if sum != 9 {
		t.Fatalf("suppressed activity=%d want 9 (emitted example is counted by handler)", sum)
	}
	// Unexpected classes must remain visible, independently of the budget.
	for range 200 {
		if !c.example("hot/file", "other", "warn") {
			t.Fatal("unexpected error suppressed")
		}
	}
}
