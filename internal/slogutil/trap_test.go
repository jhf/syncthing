// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"log/slog"
	"testing"
	"time"
)

// Trap tests are not parallel: they share the global level tracker and trap
// registry.

func TestLogTrapFiresElevatesAndRestores(t *testing.T) {
	const pkg = "traptest-fire"
	SetPackageLevel(pkg, slog.LevelWarn)
	t.Cleanup(func() {
		RemoveLogTrap("needle-in-haystack")
		SetPackageLevel(pkg, slog.LevelInfo)
	})

	err := AddLogTrap(TrapSpec{
		Contains: "needle-in-haystack",
		Packages: []string{pkg},
		Level:    slog.LevelDebug,
		Duration: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A non-matching line must not fire.
	checkTraps(Line{When: time.Now(), Message: "hay", Level: slog.LevelInfo})
	if got := globalLevels.Get(pkg); got != slog.LevelWarn {
		t.Fatalf("level changed on non-matching line: %v", got)
	}

	checkTraps(Line{When: time.Now(), Message: "found the needle-in-haystack here", Level: slog.LevelInfo})
	if got := globalLevels.Get(pkg); got != slog.LevelDebug {
		t.Fatalf("level not elevated after match: %v", got)
	}

	// During the active window another match must not re-fire.
	checkTraps(Line{When: time.Now(), Message: "needle-in-haystack again", Level: slog.LevelInfo})
	st := LogTrapStatus()
	if len(st) != 1 || st[0].Fires != 1 {
		t.Fatalf("expected exactly one fire, got %+v", st)
	}
	if st[0].ActiveTill == nil {
		t.Error("trap should report an active window")
	}

	// After the duration the level is restored.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if globalLevels.Get(pkg) == slog.LevelWarn {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("level not restored after trap expiry, still %v", globalLevels.Get(pkg))
}

func TestLogTrapRemoveRestoresMidWindow(t *testing.T) {
	const pkg = "traptest-remove"
	SetPackageLevel(pkg, slog.LevelInfo)
	t.Cleanup(func() {
		RemoveLogTrap("remove-me")
		SetPackageLevel(pkg, slog.LevelInfo)
	})

	if err := AddLogTrap(TrapSpec{
		Contains: "remove-me",
		Packages: []string{pkg},
		Level:    slog.LevelDebug,
		Duration: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	checkTraps(Line{When: time.Now(), Message: "please remove-me", Level: slog.LevelInfo})
	if got := globalLevels.Get(pkg); got != slog.LevelDebug {
		t.Fatalf("not elevated: %v", got)
	}
	if !RemoveLogTrap("remove-me") {
		t.Fatal("RemoveLogTrap reported no trap")
	}
	if got := globalLevels.Get(pkg); got != slog.LevelInfo {
		t.Fatalf("level not restored on disarm: %v", got)
	}
	if RemoveLogTrap("remove-me") {
		t.Error("second remove should report absence")
	}
}

func TestLogTrapBadPattern(t *testing.T) {
	if err := AddLogTrap(TrapSpec{Regex: "([unclosed"}); err == nil {
		t.Error("expected a compile error for a bad pattern")
	}
}
