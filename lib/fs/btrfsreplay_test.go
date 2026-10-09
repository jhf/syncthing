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

// TestParseBtrfsDump feeds the parser real `btrfs receive --dump` output
// captured on yarlung (btrfs-progs 6.17.1, 2026-10-09): a create with a
// backslash-escaped spaced name, a symlink via temp-name + rename, a
// hardlink (dest= without ./ is the unchanged source, not a tree path), an
// unlink, an rmdir, and chatter outside the folder prefix that must be
// dropped.
func TestParseBtrfsDump(t *testing.T) {
	dump := []byte(`# newsnap=/home/.snapshots/syncthing/now
At subvol /home/.snapshots/syncthing/now
snapshot        ./now                           uuid=a1c8de05-c882-4f4a-b5e4-085a96b1d342 transid=3765342 parent_uuid=9a714e68-ee00-3841-98b7-ec34e277b43f parent_transid=3765341
mkfile          ./now/o26769070-3765330-0
rename          ./now/o26769070-3765330-0    dest=./now/jhf/space\ name\ test\ -\ kopi.txt
update_extent   ./now/jhf/space\ name\ test\ -\ kopi.txt offset=0 len=2
chown           ./now/jhf/space\ name\ test\ -\ kopi.txt gid=0 uid=0
chmod           ./now/jhf/space\ name\ test\ -\ kopi.txt mode=644
utimes          ./now/jhf/space\ name\ test\ -\ kopi.txt atime=2026-10-09T15:15:07+0000 mtime=2026-10-09T15:15:07+0000 ctime=2026-10-09T15:15:07+0000
link            ./now/jhf/fmt-test/renamed-a.txt dest=jhf/fmt-test/file-a.txt
unlink          ./now/jhf/fmt-test/file-a.txt
symlink         ./now/o26769084-3765342-0       dest=file-a.txt
rename          ./now/o26769084-3765342-0       dest=./now/jhf/fmt-test/link\ to\ a.txt
rmdir           ./now/jhf/fmt-test/sub
utimes          ./now/otheruser/secret.txt atime=2026-10-09T15:15:07+0000 mtime=2026-10-09T15:15:07+0000 ctime=2026-10-09T15:15:07+0000
utimes          ./now/jhf atime=2026-10-09T15:15:07+0000 mtime=2026-10-09T15:15:07+0000 ctime=2026-10-09T15:15:07+0000
`)

	got, err := parseBtrfsDump(dump, "jhf")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"space name test - kopi.txt",
		"fmt-test/renamed-a.txt",
		"fmt-test/file-a.txt",
		"fmt-test/link to a.txt",
		"fmt-test/sub",
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestSplitDumpFields verifies escape-aware tokenization.
func TestSplitDumpFields(t *testing.T) {
	fields := splitDumpFields(`rename          ./now/o26769084-3765342-0       dest=./now/jhf/fmt-test/link\ to\ a.txt`)
	want := []string{"rename", "./now/o26769084-3765342-0", "dest=./now/jhf/fmt-test/link to a.txt"}
	if !slices.Equal(fields, want) {
		t.Errorf("got %q, want %q", fields, want)
	}
}

// TestParseBtrfsDumpRootFolder checks a folder mounted at the subvolume root
// (prefix ".").
func TestParseBtrfsDumpRootFolder(t *testing.T) {
	dump := []byte("unlink          ./snap/a/b.txt\nutimes          ./snap atime=x mtime=y ctime=z\n")
	got, err := parseBtrfsDump(dump, ".")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"a/b.txt"}) {
		t.Errorf("got %q, want %q", got, []string{"a/b.txt"})
	}
}
