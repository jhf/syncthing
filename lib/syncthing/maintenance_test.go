// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package syncthing

import (
	"testing"
	"time"
)

func TestInMaintenanceWindow(t *testing.T) {
	pick := time.Date(2026, 10, 21, 3, 17, 0, 0, time.Local)
	cases := []struct {
		name   string
		now    time.Time
		runLen int
		want   bool
	}{
		{"before pick", pick.Add(-time.Hour), 300, false},
		{"at pick", pick, 300, true},
		{"inside", pick.Add(90 * time.Minute), 300, true},
		{"at capped end (3h)", pick.Add(3 * time.Hour), 300, false},
		{"inside short run", pick.Add(45 * time.Minute), 60, true},
		{"past short run", pick.Add(61 * time.Minute), 60, false},
		// The dev.137 bug: first boot at 21:15 with a 03:30 pick must not fire.
		{"evening of the same day", time.Date(2026, 10, 21, 21, 15, 0, 0, time.Local), 300, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inMaintenanceWindow(tc.now, pick, tc.runLen); got != tc.want {
				t.Errorf("inMaintenanceWindow(%v, pick, %d) = %v, want %v", tc.now, tc.runLen, got, tc.want)
			}
		})
	}
}
