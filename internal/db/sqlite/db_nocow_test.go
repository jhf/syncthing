// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"path/filepath"
	"testing"
)

func TestIsCoWFilesystem(t *testing.T) {
	cases := []struct {
		magic int64
		name  string
		want  bool
	}{
		{0x9123683e, "btrfs", true},
		{0xca451a4e, "bcachefs", true},
		{0xEF53, "ext4", false},
		{0x58465342, "xfs", false},
		{0x01021994, "tmpfs", false},
		{0x6969, "nfs", false},
		{0, "unset", false},
	}
	for _, tc := range cases {
		if got := isCoWFilesystem(tc.magic); got != tc.want {
			t.Errorf("%s: isCoWFilesystem(%#x) = %v, want %v", tc.name, tc.magic, got, tc.want)
		}
	}
}

func TestApplyNoCOWBestEffort(t *testing.T) {
	// On any platform and filesystem this must never fail or panic: at
	// worst it is a no-op (non-Linux, or non-CoW filesystem on Linux).
	dir := t.TempDir()
	applyNoCOW(filepath.Join(dir, "test.db"))

	// The kill switch must also be accepted.
	t.Setenv("STDBNOCOW", "0")
	applyNoCOW(filepath.Join(dir, "test.db"))

	// A nonexistent path must not cause trouble either.
	t.Setenv("STDBNOCOW", "")
	applyNoCOW(filepath.Join(dir, "nonexistent", "test.db"))
}
