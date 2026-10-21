// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"path/filepath"
	"testing"
)

func TestOwnStateFindings(t *testing.T) {
	t.Parallel()
	home := filepath.Join(string(filepath.Separator), "home", "jhf")
	db := filepath.Join(home, ".config", "syncthing", "index-v2")
	cfg := filepath.Join(home, ".config", "syncthing")
	elsewhere := filepath.Join(string(filepath.Separator), "var", "lib", "syncthing")

	own := [][2]string{
		{"database", db},
		{"configuration", cfg},
	}

	cases := []struct {
		name       string
		folderRoot string
		own        [][2]string
		ignored    map[string]bool
		want       []ownStateFinding
	}{
		{
			name:       "inside and ignored: silent",
			folderRoot: home,
			own:        own,
			ignored:    map[string]bool{".config/syncthing/index-v2": true, ".config/syncthing": true},
			want:       nil,
		},
		{
			name:       "inside, not ignored: one finding, deduped suggestion",
			folderRoot: home,
			own:        own,
			ignored:    map[string]bool{},
			// Both paths share the /.config suggestion: one finding.
			want: []ownStateFinding{{Kind: "database", Rel: ".config/syncthing/index-v2", Suggestion: "/.config"}},
		},
		{
			name:       "outside folder: silent",
			folderRoot: home,
			own:        [][2]string{{"database", elsewhere}},
			ignored:    map[string]bool{},
			want:       nil,
		},
		{
			name:       "folder rooted at the state dir: database subdir flagged, root itself skipped",
			folderRoot: cfg,
			own:        own,
			ignored:    map[string]bool{},
			want:       []ownStateFinding{{Kind: "database", Rel: "index-v2", Suggestion: "/index-v2"}},
		},
		{
			name:       "distinct suggestions both reported",
			folderRoot: home,
			own: [][2]string{
				{"database", db},
				{"configuration", filepath.Join(home, "Library", "syncthing")},
			},
			ignored: map[string]bool{},
			want: []ownStateFinding{
				{Kind: "database", Rel: ".config/syncthing/index-v2", Suggestion: "/.config"},
				{Kind: "configuration", Rel: "Library/syncthing", Suggestion: "/Library"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ownStateFindings(tc.folderRoot, tc.own, func(rel string) bool {
				return tc.ignored[rel]
			})
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("finding %d: got %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
