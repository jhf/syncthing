// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/locations"
)

// patch/45: syncing one's whole home directory is a legitimate, supported
// shape — but then syncthing's own state (the database, the configuration)
// can end up INSIDE a synced folder. If such a path is not ignored, its
// churn syncs to every device: index files race the local index on the
// peers, config rewrites fan out, and on btrfs the database sits in the
// snapshot's reach (patch/44). The machinery to live safely inside a
// synced tree exists (ignores, the nested database subvolume) — this audit
// makes its precondition OBSERVABLE: every boot verifies coverage and
// warns with the exact ignore pattern when it is missing, instead of the
// user discovering the leak from a multi-gigabyte index appearing on a
// peer.

// ownStateFinding describes one syncthing-owned path found inside a synced
// folder without ignore coverage.
type ownStateFinding struct {
	Kind       string // "database", "configuration"
	Rel        string // folder-relative path, slash-separated
	Suggestion string // ignore pattern covering it, e.g. "/.config/syncthing"
}

// ownStateFindings reports which of the given syncthing-owned paths lie
// inside folderRoot without ignore coverage, at most one finding per
// suggested ignore pattern. Pure; the folder method supplies live values.
func ownStateFindings(folderRoot string, own [][2]string, ignored func(rel string) bool) []ownStateFinding {
	var out []ownStateFinding
	seen := make(map[string]bool)
	for _, kv := range own {
		kind, path := kv[0], kv[1]
		rel, err := filepath.Rel(folderRoot, path)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			continue // not inside this folder
		}
		relSlash := filepath.ToSlash(rel)
		if ignored(relSlash) {
			continue
		}
		suggestion := "/" + strings.SplitN(relSlash, "/", 2)[0]
		if seen[suggestion] {
			continue
		}
		seen[suggestion] = true
		out = append(out, ownStateFinding{Kind: kind, Rel: relSlash, Suggestion: suggestion})
	}
	return out
}

// auditOwnStatePaths warns once per boot for every syncthing-owned path
// that lies inside this folder without ignore coverage. Must run where the
// ignores are known-good (after a successful ignore load).
func (f *folder) auditOwnStatePaths(ctx context.Context) {
	own := [][2]string{
		{"database", locations.Get(locations.Database)},
		{"configuration", filepath.Dir(locations.Get(locations.ConfigFile))},
	}
	for _, fnd := range ownStateFindings(f.Filesystem().URI(), own, func(rel string) bool {
		return f.ignores.Match(rel).IsIgnored()
	}) {
		f.sl.WarnContext(ctx, "Syncthing's own "+fnd.Kind+" path lies inside the synced folder and is not ignored; its churn would sync to every device and race the local index",
			"path", fnd.Rel, "ignoreSuggestion", fnd.Suggestion, slogutil.FilePath(filepath.Join(f.Filesystem().URI(), fnd.Rel)))
	}
}
