// Copyright (C) 2019 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"errors"
	"fmt"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/syncthing/syncthing/lib/build"
)

// reverseDirNamesFilesystem returns directory names in reverse lexical order.
// DirNames does not promise any order, and this is the deterministic stand-in
// for that: readdir order on a real filesystem is arbitrary, and the fake
// filesystem's map iteration order is nondeterministic, so neither can pin
// Walk's documented lexical order in a test.
type reverseDirNamesFilesystem struct {
	Filesystem
}

func (f reverseDirNamesFilesystem) DirNames(path string) ([]string, error) {
	names, err := f.Filesystem.DirNames(path)
	if err != nil {
		return nil, err
	}
	slices.Sort(names)
	slices.Reverse(names)
	return names, nil
}

// TestWalkVisitsLexically pins the order Walk documents: lexical, depth
// first. The tree deliberately puts a directory ("documents") beside a
// sibling file with the same prefix ("documents.txt"), so lexical DFS order
// visits "documents/z" before "documents.txt" - the shape a string comparison
// of full paths gets wrong.
func TestWalkVisitsLexically(t *testing.T) {
	base := NewFilesystem(FilesystemTypeBasic, t.TempDir())
	for _, name := range []string{"a", filepath.Join("b", "x"), filepath.Join("documents", "z"), "documents.txt"} {
		if dir := filepath.Dir(name); dir != "." {
			if err := base.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		fd, err := base.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := fd.Close(); err != nil {
			t.Fatal(err)
		}
	}

	wfs := NewWalkFilesystem(reverseDirNamesFilesystem{base})
	var visited []string
	if err := wfs.Walk(".", func(path string, info FileInfo, err error) error {
		if err != nil {
			return err
		}
		visited = append(visited, path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	want := []string{".", "a", "b", filepath.Join("b", "x"), "documents", filepath.Join("documents", "z"), "documents.txt"}
	if !slices.Equal(visited, want) {
		t.Errorf("Walk visited %v, want lexical depth-first order %v", visited, want)
	}
}

func testWalkSkipSymlink(t *testing.T, fsType FilesystemType, uri string) {
	if build.IsWindows {
		t.Skip("Symlinks skipping is not tested on windows")
	}

	fs := NewFilesystem(fsType, uri)

	if err := fs.MkdirAll("target/foo", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mkdir("towalk", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fs.CreateSymlink("target", "towalk/symlink"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Walk("towalk", func(path string, info FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() != "symlink" && info.Name() != "towalk" {
			t.Fatal("Walk unexpected file", info.Name())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func createDirJunct(target string, name string) error {
	output, err := osexec.Command("cmd", "/c", "mklink", "/J", name, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("Failed to run mklink %v %v: %v %q", name, target, err, output)
	}
	return nil
}

func testWalkTraverseDirJunct(t *testing.T, fsType FilesystemType, uri string) {
	if !build.IsWindows {
		t.Skip("Directory junctions are available and tested on windows only")
	}

	fs := NewFilesystem(fsType, uri, new(OptionJunctionsAsDirs))

	if err := fs.MkdirAll("target/foo", 0); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mkdir("towalk", 0); err != nil {
		t.Fatal(err)
	}
	if err := createDirJunct(filepath.Join(uri, "target"), filepath.Join(uri, "towalk/dirjunct")); err != nil {
		t.Fatal(err)
	}
	traversed := false
	if err := fs.Walk("towalk", func(path string, info FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == "foo" {
			traversed = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !traversed {
		t.Fatal("Directory junction was not traversed")
	}
}

func testWalkInfiniteRecursion(t *testing.T, fsType FilesystemType, uri string) {
	if !build.IsWindows {
		t.Skip("Infinite recursion detection is tested on windows only")
	}

	fs := NewFilesystem(fsType, uri, new(OptionJunctionsAsDirs))

	if err := fs.MkdirAll("target/foo", 0); err != nil {
		t.Fatal(err)
	}
	if err := fs.Mkdir("towalk", 0); err != nil {
		t.Fatal(err)
	}
	if err := createDirJunct(filepath.Join(uri, "target"), filepath.Join(uri, "towalk/dirjunct")); err != nil {
		t.Fatal(err)
	}
	if err := createDirJunct(filepath.Join(uri, "towalk"), filepath.Join(uri, "target/foo/recurse")); err != nil {
		t.Fatal(err)
	}
	dirjunctCnt := 0
	fooCnt := 0
	found := false
	if err := fs.Walk("towalk", func(path string, info FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, ErrInfiniteRecursion) {
				if found {
					t.Fatal("second infinite recursion detected at", path)
				}
				found = true
				return nil
			}
			t.Fatal(err)
		}
		if info.Name() == "dirjunct" {
			dirjunctCnt++
		} else if info.Name() == "foo" {
			fooCnt++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if dirjunctCnt != 2 || fooCnt != 1 || !found {
		t.Fatal("Infinite recursion not detected correctly")
	}
}
