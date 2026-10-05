// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package osutil_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/rand"
)

func TestIsDeleted(t *testing.T) {
	type tc struct {
		path  string
		isDel bool
	}
	cases := []tc{
		{"del", true},
		{"del.file", false},
		{filepath.Join("del", "del"), true},
		{"file", false},
		{"linkToFile", false},
		{"linkToDel", false},
		{"linkToDir", false},
		{filepath.Join("linkToDir", "file"), true},
		{filepath.Join("file", "behindFile"), true},
		{"dir", false},
		{"dir.file", false},
		{filepath.Join("dir", "file"), false},
		{filepath.Join("dir", "del"), true},
		{filepath.Join("dir", "del", "del"), true},
		{filepath.Join("del", "del", "del"), true},
	}

	testFs := fs.NewFilesystem(fs.FilesystemTypeFake, "testdata")

	testFs.MkdirAll("dir", 0o777)
	for _, f := range []string{"file", "del.file", "dir.file", filepath.Join("dir", "file")} {
		fd, err := testFs.Create(f)
		if err != nil {
			t.Fatal(err)
		}
		fd.Close()
	}

	for _, n := range []string{"Dir", "File", "Del"} {
		if err := testFs.CreateSymlink(strings.ToLower(n), "linkTo"+n); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range cases {
		if osutil.IsDeleted(testFs, c.path) != c.isDel {
			t.Errorf("IsDeleted(%v) != %v", c.path, c.isDel)
		}
	}
}

type countingMtimeStore struct {
	gets int
}

func (s *countingMtimeStore) GetMtime(_, _ string) (ondisk, virtual time.Time) {
	s.gets++
	return time.Time{}, time.Time{}
}

func (s *countingMtimeStore) PutMtime(_, _ string, _, _ time.Time) error { return nil }

func (s *countingMtimeStore) DeleteMtime(_, _ string) error { return nil }

func TestIsDeletedSkipsMtimeDatabase(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	db := &countingMtimeStore{}
	ffs := fs.NewFilesystem(fs.FilesystemTypeBasic, dir, new(fs.OptionDetectCaseConflicts), fs.NewMtimeOption(db, "test"))
	if osutil.IsDeleted(ffs, "file") {
		t.Fatal("existing file should not be deleted")
	}
	if db.gets != 0 {
		t.Fatalf("IsDeleted should skip the mtime database, got %d lookups", db.gets)
	}
	if !osutil.IsDeleted(ffs, "missing") {
		t.Fatal("missing file should be deleted")
	}
	if db.gets != 0 {
		t.Fatalf("IsDeleted missing path should skip the mtime database, got %d lookups", db.gets)
	}

	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", "b", "c"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("b", filepath.Join(dir, "a", "l")); err != nil {
		t.Fatal(err)
	}
	if osutil.IsDeleted(ffs, filepath.Join("a", "b", "c")) {
		t.Fatal("nested existing file should not be deleted")
	}
	if db.gets != 0 {
		t.Fatalf("nested IsDeleted should skip the mtime database, got %d lookups", db.gets)
	}
	if err := osutil.TraversesSymlink(ffs, filepath.Join("a", "b")); err != nil {
		t.Fatalf("TraversesSymlink on real dirs: %v", err)
	}
	if db.gets != 0 {
		t.Fatalf("TraversesSymlink should skip the mtime database, got %d lookups", db.gets)
	}
	if err := osutil.TraversesSymlink(ffs, filepath.Join("a", "l", "c")); err == nil {
		t.Fatal("expected symlink traversal error")
	}
	if db.gets != 0 {
		t.Fatalf("symlink TraversesSymlink should skip the mtime database, got %d lookups", db.gets)
	}
	if !osutil.IsDeleted(ffs, filepath.Join("a", "l", "c")) {
		t.Fatal("path behind a symlink should count as deleted")
	}
	if db.gets != 0 {
		t.Fatalf("IsDeleted behind symlink should skip the mtime database, got %d lookups", db.gets)
	}

	if build.IsDarwin || build.IsWindows {
		if !osutil.IsDeleted(ffs, "FILE") {
			t.Fatal("wrong-case existing file should count as deleted on a case-conflict wrapping")
		}
		if db.gets != 0 {
			t.Fatalf("case-conflict IsDeleted should skip the mtime database, got %d lookups", db.gets)
		}
	}
}

func TestRenameOrCopy(t *testing.T) {
	sameFs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true")
	tests := []struct {
		src  fs.Filesystem
		dst  fs.Filesystem
		file string
	}{
		{
			src:  sameFs,
			dst:  sameFs,
			file: "file",
		},
		{
			src:  fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			dst:  fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			file: "file",
		},
		{
			src:  fs.NewFilesystem(fs.FilesystemTypeFake, `fake://fake/?files=1&seed=42`),
			dst:  fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true"),
			file: osutil.NativeFilename(`05/7a/4d52f284145b9fe8`),
		},
	}

	for _, test := range tests {
		content := test.src.URI()
		if _, err := test.src.Lstat(test.file); err != nil {
			if !fs.IsNotExist(err) {
				t.Fatal(err)
			}
			if fd, err := test.src.Create(test.file); err != nil {
				t.Fatal(err)
			} else {
				if _, err := fd.Write([]byte(test.src.URI())); err != nil {
					t.Fatal(err)
				}
				_ = fd.Close()
			}
		} else {
			fd, err := test.src.Open(test.file)
			if err != nil {
				t.Fatal(err)
			}
			buf, err := io.ReadAll(fd)
			if err != nil {
				t.Fatal(err)
			}
			_ = fd.Close()
			content = string(buf)
		}

		err := osutil.RenameOrCopy(fs.CopyRangeMethodStandard, test.src, test.dst, test.file, "new")
		if err != nil {
			t.Fatal(err)
		}

		if fd, err := test.dst.Open("new"); err != nil {
			t.Fatal(err)
		} else {
			t.Cleanup(func() {
				_ = fd.Close()
			})

			if buf, err := io.ReadAll(fd); err != nil {
				t.Fatal(err)
			} else if string(buf) != content {
				t.Fatalf("expected %s got %s", content, string(buf))
			}
		}
	}
}

func TestIPFromString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in  string
		out string
	}{
		{"192.168.178.1", "192.168.178.1"},
		{"192.168.178.1:8384", "192.168.178.1"},
		{"fe80::20c:29ff:fe9a:46d2", "fe80::20c:29ff:fe9a:46d2"},
		{"[fe80::20c:29ff:fe9a:46d2]:8384", "fe80::20c:29ff:fe9a:46d2"},
		{"[fe80::20c:29ff:fe9a:46d2%eno1]:8384", "fe80::20c:29ff:fe9a:46d2"},
		{"google.com", ""},
		{"1.1.1.1.1", ""},
		{"", ""},
	}

	for _, c := range cases {
		ip := osutil.IPFromString(c.in)
		var address string
		if ip != nil {
			address = ip.String()
		} else {
			address = ""
		}

		if c.out != address {
			t.Fatalf("result should be %s != %s", c.out, address)
		}
	}
}
