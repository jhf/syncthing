// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestBoundedPathSetExactBelowLimit(t *testing.T) {
	s := NewBoundedPathSet(8)
	for _, p := range []string{"a/b", "a/c", "a/b/x", "d"} {
		s.Add(filepath.FromSlash(p))
	}
	want := []string{filepath.FromSlash("a/b"), filepath.FromSlash("a/c"), "d"}
	if got := s.Paths(); !slices.Equal(got, want) {
		t.Fatalf("Paths() = %q, want %q (a/b/x covered by a/b)", got, want)
	}
	if s.Root() || s.DepthCap() != 0 {
		t.Fatalf("root=%v depthCap=%d, want exact set", s.Root(), s.DepthCap())
	}
}

// TestBoundedPathSetCoarsensNeverDrops pins the overflow policy: past the
// limit, paths widen to ancestors, every added path stays covered, and the
// set does not collapse to the root while the top level still fits.
func TestBoundedPathSetCoarsensNeverDrops(t *testing.T) {
	s := NewBoundedPathSet(16)
	var added []string
	for top := 0; top < 4; top++ {
		for i := 0; i < 50; i++ {
			p := filepath.FromSlash("top" + string(rune('a'+top)) + "/sub/" + string(rune('a'+i%26)) + "/f" + string(rune('a'+i/26)))
			added = append(added, p)
			s.Add(p)
		}
	}
	if s.Root() {
		t.Fatal("collapsed to the root although the 4 top-level directories fit the limit")
	}
	if s.Len() > 16 {
		t.Fatalf("Len() = %d, over the limit 16", s.Len())
	}
	paths := s.Paths()
	for _, p := range added {
		covered := false
		for _, q := range paths {
			if p == q || IsParent(p, q) {
				covered = true
				break
			}
		}
		if !covered {
			t.Fatalf("added path %q not covered by %q", p, paths)
		}
	}
	// Later additions below a widened ancestor stay covered without growth.
	before := s.Len()
	s.Add(filepath.FromSlash("topa/sub/zz/deep/file"))
	if s.Len() != before {
		t.Fatalf("Len() %d -> %d: a path under a widened ancestor grew the set", before, s.Len())
	}
}

func TestBoundedPathSetRootOnlyWhenTopLevelOverflows(t *testing.T) {
	s := NewBoundedPathSet(4)
	for i := 0; i < 5; i++ {
		s.Add("t" + strings.Repeat("x", i))
	}
	if !s.Root() {
		t.Fatalf("5 top-level paths in a 4-path set must collapse to the root, got %q", s.Paths())
	}
	if got := s.Paths(); !slices.Equal(got, []string{"."}) {
		t.Fatalf("Paths() = %q, want [.]", got)
	}
}
