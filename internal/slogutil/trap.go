// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Log traps are the debugging answer to "I can't be connected at the right
// moment": a trap is a regular expression watched against every line that
// enters the log ring. On a match the trap fires: it dumps the current ring
// (the recent past at the levels that were active), elevates the given
// packages to the trap's level for the trap's duration (the verbose future),
// and dumps the ring again when the window closes. A cooldown keeps a
// continuously-matching pattern from dumping in a loop.
//
// Traps are armed and disarmed at runtime via the REST API; nothing here
// survives a restart.

// TrapSpec describes a log trap. The matcher is either a plain substring
// (Contains - the fast default; since our line format is flat and greppable,
// an operation like "Deleted file" or a path like "file.name=Projects/foo/"
// are both just substrings) or a regular expression (Regex - Go's RE2,
// guaranteed linear-time, no backtracking blowups). Exactly one must be set.
// DumpPath, when non-nil, maps a tag ("pre"/"post") to the file the ring
// should be dumped to on fire/expiry.
type TrapSpec struct {
	Contains string
	Regex    string
	Packages []string
	Level    slog.Level
	Duration time.Duration
	Cooldown time.Duration
	DumpPath func(tag string) string
}

// TrapStatus is the externally visible state of an armed trap.
type TrapStatus struct {
	Contains   string     `json:"contains,omitempty"`
	Regex      string     `json:"regex,omitempty"`
	Packages   []string   `json:"packages"`
	Level      string     `json:"level"`
	Duration   string     `json:"duration"`
	Cooldown   string     `json:"cooldown"`
	Fires      int        `json:"fires"`
	LastFired  *time.Time `json:"lastFired,omitempty"`
	ActiveTill *time.Time `json:"activeUntil,omitempty"`
}

const (
	defaultTrapDuration = 10 * time.Minute
	minTrapCooldown     = time.Minute
)

type logTrap struct {
	spec    TrapSpec
	matcher func(string) bool

	mut      sync.Mutex
	fires    int
	lastFire time.Time
	until    time.Time
	prev     map[string]slog.Level
}

// identify returns the trap's match expression, its registry identity.
func (t *logTrap) identify() string {
	if t.spec.Contains != "" {
		return t.spec.Contains
	}
	return t.spec.Regex
}

var trapRegistry = struct {
	sync.Mutex
	traps []*logTrap
}{}

// AddLogTrap arms a trap. Zero duration and cooldown get defaults (10
// minutes, and the duration with a one-minute floor).
func AddLogTrap(spec TrapSpec) error {
	var matcher func(string) bool
	switch {
	case spec.Contains != "" && spec.Regex != "":
		return fmt.Errorf("set contains or regex, not both")
	case spec.Contains != "":
		needle := spec.Contains
		matcher = func(msg string) bool { return strings.Contains(msg, needle) }
	case spec.Regex != "":
		re, err := regexp.Compile(spec.Regex)
		if err != nil {
			return fmt.Errorf("trap regex: %w", err)
		}
		matcher = re.MatchString
	default:
		return fmt.Errorf("trap needs a contains string or a regex")
	}
	if spec.Duration <= 0 {
		spec.Duration = defaultTrapDuration
	}
	if spec.Cooldown < minTrapCooldown {
		spec.Cooldown = max(spec.Duration, minTrapCooldown)
	}
	t := &logTrap{spec: spec, matcher: matcher}
	trapRegistry.Lock()
	trapRegistry.traps = append(trapRegistry.traps, t)
	trapRegistry.Unlock()
	slog.Info("Log trap armed", slog.String("match", t.identify()), slog.Any("packages", spec.Packages), slog.String("level", spec.Level.String()), slog.Duration("duration", spec.Duration))
	return nil
}

// RemoveLogTrap disarms the trap with the given match expression, restoring
// levels first if it is currently active. Returns whether a trap was removed.
func RemoveLogTrap(match string) bool {
	trapRegistry.Lock()
	var found *logTrap
	for i, t := range trapRegistry.traps {
		if t.identify() == match {
			found = t
			trapRegistry.traps = append(trapRegistry.traps[:i], trapRegistry.traps[i+1:]...)
			break
		}
	}
	trapRegistry.Unlock()
	if found == nil {
		return false
	}
	// Outside the registry lock: restoring levels logs, and logging
	// re-enters the trap machinery.
	found.restore(time.Now())
	slog.Info("Log trap disarmed", slog.String("match", match))
	return true
}

// LogTrapStatus reports on all armed traps.
func LogTrapStatus() []TrapStatus {
	trapRegistry.Lock()
	defer trapRegistry.Unlock()
	out := make([]TrapStatus, 0, len(trapRegistry.traps))
	for _, t := range trapRegistry.traps {
		t.mut.Lock()
		st := TrapStatus{
			Contains: t.spec.Contains, Regex: t.spec.Regex,
			Packages: t.spec.Packages,
			Level:    t.spec.Level.String(),
			Duration: t.spec.Duration.String(),
			Cooldown: t.spec.Cooldown.String(),
			Fires:    t.fires,
		}
		if !t.lastFire.IsZero() {
			lf := t.lastFire
			st.LastFired = &lf
		}
		if time.Now().Before(t.until) {
			u := t.until
			st.ActiveTill = &u
		}
		t.mut.Unlock()
		out = append(out, st)
	}
	return out
}

// checkTraps is called by the ring for every appended line. Matching cost is
// one matcher evaluation per armed trap that is neither active nor cooling
// down; traps are few and lines are short. Lines from this package itself
// ("Log trap armed/fired/...") are exempt: they quote the match expression
// and would otherwise self-trigger every trap.
func checkTraps(l Line) {
	if l.pkg == "slogutil" {
		return
	}
	trapRegistry.Lock()
	traps := trapRegistry.traps
	trapRegistry.Unlock()
	for _, t := range traps {
		t.maybeFire(l)
	}
}

func (t *logTrap) maybeFire(l Line) {
	t.mut.Lock()
	now := time.Now()
	if now.Before(t.until) || (!t.lastFire.IsZero() && now.Sub(t.lastFire) < t.spec.Cooldown) {
		t.mut.Unlock()
		return
	}
	if !t.matcher(l.Message) {
		t.mut.Unlock()
		return
	}
	t.fires++
	t.lastFire = now
	t.until = now.Add(t.spec.Duration)
	t.prev = make(map[string]slog.Level, len(t.spec.Packages))
	for _, pkg := range t.spec.Packages {
		t.prev[pkg] = globalLevels.Get(pkg)
	}
	spec := t.spec
	t.mut.Unlock()

	slog.Info("Log trap fired", slog.String("match", t.identify()), slog.Any("packages", spec.Packages), slog.Duration("duration", spec.Duration))
	if spec.DumpPath != nil {
		if path := spec.DumpPath("pre"); path != "" {
			if err := DumpLogRingToFile(path); err != nil {
				slog.Warn("Log trap pre-fire dump failed", Error(err))
			} else {
				slog.Info("Log trap pre-fire ring dump", slog.String("path", path))
			}
		}
	}
	for _, pkg := range spec.Packages {
		SetPackageLevel(pkg, spec.Level)
	}
	time.AfterFunc(spec.Duration, func() { t.expire() })
}

func (t *logTrap) expire() {
	t.mut.Lock()
	spec := t.spec
	t.mut.Unlock()
	t.restore(time.Now())
	slog.Info("Log trap window closed, levels restored", slog.String("match", t.identify()))
	if spec.DumpPath != nil {
		if path := spec.DumpPath("post"); path != "" {
			if err := DumpLogRingToFile(path); err != nil {
				slog.Warn("Log trap post-window dump failed", Error(err))
			} else {
				slog.Info("Log trap post-window ring dump", slog.String("path", path))
			}
		}
	}
}

func (t *logTrap) restore(now time.Time) {
	t.mut.Lock()
	prev := t.prev
	t.prev = nil
	t.until = now // not active any more, but the cooldown runs from lastFire
	t.mut.Unlock()
	// Outside the lock: SetPackageLevel logs the change, and logging
	// re-enters checkTraps for this trap.
	for pkg, lvl := range prev {
		SetPackageLevel(pkg, lvl)
	}
}
