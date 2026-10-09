// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package fs

import (
	"slices"
	"testing"
)

func TestPairingSnapBasename(t *testing.T) {
	t.Parallel()
	match := []string{
		"boot-home-1792530828360473700",
		"shutdown-home-1792137421307866309",
		"post-replay-home-1792530828360473700",
		"runtime-my.folder-1",
	}
	reject := []string{
		"baseline",
		"index-v2",
		"index-v2.subvol-tmp",
		"boot-home",                      // no timestamp
		"boot-home-",                     // empty timestamp
		"pairing-home-17925308283604737", // unknown trigger
		"BOOT-home-1792530828360473700",  // case
		"boot-home-1792530828360473700x", // trailing garbage
		"",                               // empty
	}
	for _, m := range match {
		if !pairingSnapBasename.MatchString(m) {
			t.Errorf("should match: %q", m)
		}
	}
	for _, r := range reject {
		if pairingSnapBasename.MatchString(r) {
			t.Errorf("should reject: %q", r)
		}
	}
}

func TestStalePairingSnapNames(t *testing.T) {
	t.Parallel()
	listing := `@home/.snapshots/syncthing/shutdown-home-1792137421307866309
@home/.snapshots/syncthing/boot-home-1792530828360473700
@home/.snapshots/syncthing/baseline
@home/jhf/.config/syncthing/index-v2
@home/jhf/data/boot-home-1792530828360473700
@home/jhf/projects
`
	want := []string{"shutdown-home-1792137421307866309", "boot-home-1792530828360473700"}
	got := stalePairingSnapNames(listing)
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := stalePairingSnapNames(""); len(got) != 0 {
		t.Errorf("empty listing: got %v", got)
	}
}
