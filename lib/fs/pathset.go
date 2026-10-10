// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"slices"
	"strings"
)

// BoundedPathSet collects relative paths for a later scan in bounded memory.
// Past its limit it coarsens instead of dropping: every path deeper than a
// depth cap is replaced by its ancestor at the cap, the cap dropping one
// level per round until the set holds at most half the limit (so rounds
// amortize). A path is never lost, only widened to an ancestor; the folder
// root is the last resort, reached only when even the top-level directories
// exceed the limit. Scanning the result covers every added path.
//
// Not safe for concurrent use.
type BoundedPathSet struct {
	limit    int
	depthCap int // 0 = uncapped
	paths    map[string]struct{}
	root     bool
}

// NewBoundedPathSet returns an empty set holding at most limit paths
// before coarsening. A limit below 2 is raised to 2.
func NewBoundedPathSet(limit int) *BoundedPathSet {
	if limit < 2 {
		limit = 2
	}
	return &BoundedPathSet{limit: limit, paths: make(map[string]struct{})}
}

func isRootPath(p string) bool {
	return p == "" || p == "." || p == string(PathSeparator)
}

// truncateDepth returns p cut to its first n components (n > 0).
func truncateDepth(p string, n int) string {
	idx := 0
	for i := 0; i < n; i++ {
		j := strings.IndexByte(p[idx:], PathSeparator)
		if j < 0 {
			return p
		}
		idx += j + 1
	}
	return p[:idx-1]
}

func pathDepth(p string) int {
	return strings.Count(p, string(PathSeparator)) + 1
}

// hasAncestorOrSelf reports whether p or one of its ancestors is in the set.
func (s *BoundedPathSet) hasAncestorOrSelf(p string) bool {
	for {
		if _, ok := s.paths[p]; ok {
			return true
		}
		i := strings.LastIndexByte(p, PathSeparator)
		if i < 0 {
			return false
		}
		p = p[:i]
	}
}

// Add records p. It reports whether the set coarsened (widened paths to
// ancestors, or collapsed to the root) as a consequence.
func (s *BoundedPathSet) Add(p string) bool {
	if s.root {
		return false
	}
	if isRootPath(p) {
		s.collapseToRoot()
		return true
	}
	p = strings.TrimRight(p, string(PathSeparator))
	if s.depthCap > 0 {
		p = truncateDepth(p, s.depthCap)
	}
	if s.hasAncestorOrSelf(p) {
		return false
	}
	s.paths[p] = struct{}{}
	if len(s.paths) <= s.limit {
		return false
	}
	s.coarsen()
	return true
}

func (s *BoundedPathSet) collapseToRoot() {
	s.root = true
	s.paths = make(map[string]struct{})
}

func (s *BoundedPathSet) coarsen() {
	maxDepth := 0
	for p := range s.paths {
		if d := pathDepth(p); d > maxDepth {
			maxDepth = d
		}
	}
	for len(s.paths) > s.limit/2 {
		maxDepth--
		if maxDepth == 0 {
			s.collapseToRoot()
			return
		}
		s.depthCap = maxDepth
		// Shallow paths first, so a descendant arriving after its widened
		// ancestor is recognized as covered.
		old := make([]string, 0, len(s.paths))
		for p := range s.paths {
			old = append(old, p)
		}
		slices.SortFunc(old, func(a, b string) int { return pathDepth(a) - pathDepth(b) })
		s.paths = make(map[string]struct{}, len(old))
		for _, p := range old {
			p = truncateDepth(p, maxDepth)
			if !s.hasAncestorOrSelf(p) {
				s.paths[p] = struct{}{}
			}
		}
	}
}

// Len returns the number of distinct paths held (1 once collapsed to the
// root, 0 when empty).
func (s *BoundedPathSet) Len() int {
	if s.root {
		return 1
	}
	return len(s.paths)
}

// Root reports whether the set collapsed to the folder root.
func (s *BoundedPathSet) Root() bool { return s.root }

// DepthCap returns the current depth cap (0 = paths held exactly).
func (s *BoundedPathSet) DepthCap() int { return s.depthCap }

// Paths returns the held paths sorted, without paths covered by another
// held path; ["."] once collapsed to the root, nil when empty.
func (s *BoundedPathSet) Paths() []string {
	if s.root {
		return []string{"."}
	}
	if len(s.paths) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.paths))
	for p := range s.paths {
		covered := false
		for q := p; ; {
			i := strings.LastIndexByte(q, PathSeparator)
			if i < 0 {
				break
			}
			q = q[:i]
			if _, ok := s.paths[q]; ok {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

// Reset empties the set, keeping its limit.
func (s *BoundedPathSet) Reset() {
	s.paths = make(map[string]struct{})
	s.root = false
	s.depthCap = 0
}
