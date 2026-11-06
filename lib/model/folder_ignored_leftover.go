// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// A directory deleted on a remote device stays on disk when it holds locally
// ignored content that is not marked (?d): the user chose to keep it. Upstream
// reports that with a guessed message, re-walks the directory and re-fails it
// in every pull, and logs a warning each time. patch/69 makes it one clear
// warning: it names the blocking ignored paths and the patterns that keep
// them, the Failed Items entry stays visible, and the directory is not walked
// again until the ignore patterns, the blockers on disk, or the directory's
// deletion change. The content is never auto-deleted.

const (
	// ignoredLeftoverPaths caps the blocking paths named per directory.
	ignoredLeftoverPaths = 5
	// ignoredLeftoverDirs bounds the remembered directories per folder. Past
	// it, directories are simply retried each round as before.
	ignoredLeftoverDirs = 4096
)

// ignoredLeftover is one remembered kept directory. Its error, which the
// Failed Items list shows, still matches errDirHasIgnored.
type ignoredLeftover struct {
	ignoreHash string
	version    protocol.Vector
	paths      []string // blocking ignored entries, at most ignoredLeftoverPaths
	patterns   []string // the deciding pattern for each path, same index
	more       int      // blocking entries beyond paths
	text       string   // the Failed Items error text
}

func (l *ignoredLeftover) err() error { return ignoredLeftoverError{l.text} }

type ignoredLeftoverError struct{ text string }

func (e ignoredLeftoverError) Error() string { return e.text }
func (ignoredLeftoverError) Is(target error) bool {
	return target == errDirHasIgnored
}

type ignoredLeftovers struct {
	mu   sync.Mutex
	dirs map[string]*ignoredLeftover
}

// lookup returns the remembered leftover for dir when it still applies: the
// same deletion, the same ignore patterns, and every named blocker still on
// disk. Anything else forgets it, so the directory is walked again.
func (s *ignoredLeftovers) lookup(dir string, version protocol.Vector, ignoreHash string, lstat func(string) error) *ignoredLeftover {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.dirs[dir]
	if l == nil {
		return nil
	}
	if l.ignoreHash != ignoreHash || !l.version.Equal(version) {
		delete(s.dirs, dir)
		return nil
	}
	for _, p := range l.paths {
		if lstat(p) != nil {
			delete(s.dirs, dir)
			return nil
		}
	}
	return l
}

// remember stores l for dir and reports whether it is new (warn) or a
// refresh of an equal entry (stay quiet).
func (s *ignoredLeftovers) remember(dir string, l *ignoredLeftover) (isNew bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dirs == nil {
		s.dirs = make(map[string]*ignoredLeftover)
	}
	if old := s.dirs[dir]; old != nil && old.text == l.text && old.ignoreHash == l.ignoreHash {
		return false
	}
	if _, ok := s.dirs[dir]; !ok && len(s.dirs) >= ignoredLeftoverDirs {
		return true // warn, but do not remember: bounded memory
	}
	s.dirs[dir] = l
	return true
}

func (s *ignoredLeftovers) forget(dir string) {
	s.mu.Lock()
	delete(s.dirs, dir)
	s.mu.Unlock()
}

// collectIgnoredLeftover lists, in walk order, the ignored non-deletable
// entries that keep dir: the topmost ignored entry of each ignored subtree
// (the walk does not descend into it). Returns nil when there are none.
func (f *sendReceiveFolder) collectIgnoredLeftover(dir string, version protocol.Vector) *ignoredLeftover {
	l := &ignoredLeftover{ignoreHash: f.ignores.Hash(), version: version}
	_ = f.mtimefs.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil || path == dir {
			return nil
		}
		res, pattern := f.ignores.MatchPattern(path)
		if !res.IsIgnored() || res.IsDeletable() || fs.IsTemporary(path) || fs.IsInternal(path) {
			return nil
		}
		if len(l.paths) < ignoredLeftoverPaths {
			l.paths = append(l.paths, path)
			l.patterns = append(l.patterns, pattern)
		} else {
			l.more++
		}
		if info.IsDir() {
			return fs.SkipDir
		}
		return nil
	})
	if len(l.paths) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(errDirPrefix)
	b.WriteString("is kept because it contains locally ignored files: ")
	for i, p := range l.paths {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s (pattern %q)", p, l.patterns[i])
	}
	if l.more > 0 {
		fmt.Fprintf(&b, " and %d more", l.more)
	}
	b.WriteString(". They are not deleted because the pattern lacks the (?d) prefix; remove them or add (?d) to let the directory go")
	l.text = b.String()
	return l
}

// ignoredLeftoverFor is deleteDir's entry point. When the directory is a
// remembered leftover that still applies, it returns that leftover and the
// directory is not walked again. Otherwise it returns nil.
func (f *sendReceiveFolder) ignoredLeftoverFor(file protocol.FileInfo) *ignoredLeftover {
	return f.leftovers.lookup(file.Name, file.Version, f.ignores.Hash(), func(p string) error {
		_, err := f.mtimefs.Lstat(p)
		return err
	})
}

// recordIgnoredLeftover is called when deleteDirOnDisk reported ignored
// contents. It builds the precise report, remembers it, and warns once.
func (f *sendReceiveFolder) recordIgnoredLeftover(file protocol.FileInfo) error {
	l := f.collectIgnoredLeftover(file.Name, file.Version)
	if l == nil {
		// The ignored entry vanished between the two walks; the next pull
		// sees the directory afresh.
		return errDirHasIgnored
	}
	if f.leftovers.remember(file.Name, l) {
		f.sl.Warn("Directory deleted on a remote device is kept because of locally ignored files",
			slogutil.FilePath(file.Name),
			slog.Any("ignored", l.paths),
			slog.Any("patterns", l.patterns),
			slog.Int("more", l.more))
	}
	return l.err()
}
