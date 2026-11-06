// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// leftoverFixture: tree/ with a tracked file and a locally ignored (not (?d))
// file, both on disk; device1 deletes the tree. Returns the log buffer the
// folder logger writes to.
func leftoverFixture(t *testing.T) (*testModel, *sendReceiveFolder, *fakeConnection, *bytes.Buffer) {
	t.Helper()
	m, f := setupSendReceiveFolder(t)
	conn := addFakeConn(m, device1, f.ID)
	var logs bytes.Buffer
	f.sl = slog.New(slog.NewTextHandler(&logs, nil))

	writeFile(t, f.mtimefs, ".stignore", []byte("*.local\n"))
	must(t, f.ignores.Load(".stignore"))

	must(t, f.mtimefs.MkdirAll("tree", 0o755))
	writeFile(t, f.mtimefs, "tree/tracked", []byte("x"))
	writeFile(t, f.mtimefs, "tree/notes.local", []byte("mine"))
	must(t, f.scanSubdirs(t.Context(), nil))

	var tombs []protocol.FileInfo
	for _, name := range []string{"tree", "tree/tracked"} {
		cur, ok := m.testCurrentFolderFile(f.ID, name)
		if !ok {
			t.Fatalf("%s missing from local index", name)
		}
		tomb := cur
		tomb.SetDeleted(device1.Short())
		tomb.Version = cur.Version.Update(device1.Short())
		tombs = append(tombs, tomb)
	}
	must(t, m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: f.ID, Files: tombs}))
	return m, f, conn, &logs
}

func countLines(logs *bytes.Buffer, substr string) int {
	n := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func leftoverError(f *sendReceiveFolder) string {
	for _, fe := range f.Errors() {
		if fe.Path == "tree" {
			return fe.Err
		}
	}
	return ""
}

// A remotely deleted directory kept by a locally ignored file: one warning
// that names the file and the pattern, a Failed Items entry that does the
// same and survives every pull, and no warning in later pulls. Before
// patch/69 every pull logged "Failed to sync ... probably ignored" again.
func TestIgnoredLeftoverWarnsOnce(t *testing.T) {
	_, f, _, logs := leftoverFixture(t)

	for range 3 {
		_, err := f.pull(t.Context())
		must(t, err)
		fe := leftoverError(f)
		if fe == "" {
			t.Fatal("kept directory must stay in Failed Items")
		}
		if !strings.Contains(fe, "tree/notes.local") || !strings.Contains(fe, `"*.local"`) {
			t.Fatalf("Failed Items entry must name the ignored path and its pattern, got %q", fe)
		}
	}
	if n := countLines(logs, "tree"); n != 1 {
		t.Fatalf("expected exactly one warning for the kept directory over 3 pulls, got %d:\n%s", n, logs.String())
	}
	if _, err := f.mtimefs.Lstat("tree/notes.local"); err != nil {
		t.Fatalf("ignored content must never be deleted: %v", err)
	}
}

// The memory lets go when the blocker goes: the directory is deleted.
func TestIgnoredLeftoverReleasedWhenBlockerRemoved(t *testing.T) {
	_, f, _, _ := leftoverFixture(t)
	_, err := f.pull(t.Context())
	must(t, err)
	if leftoverError(f) == "" {
		t.Fatal("precondition: directory kept")
	}
	must(t, f.mtimefs.Remove("tree/notes.local"))
	_, err = f.pull(t.Context())
	must(t, err)
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Fatalf("tree should be gone once its ignored content is removed, stat err = %v", err)
	}
	if fe := leftoverError(f); fe != "" {
		t.Fatalf("no error expected after deletion, got %q", fe)
	}
}

// The memory lets go when the ignore patterns change: with (?d) the
// directory and its ignored content go.
func TestIgnoredLeftoverReleasedWhenIgnoresChange(t *testing.T) {
	_, f, _, logs := leftoverFixture(t)
	_, err := f.pull(t.Context())
	must(t, err)
	writeFile(t, f.mtimefs, ".stignore", []byte("(?d)*.local\n"))
	must(t, f.ignores.Load(".stignore"))
	_, err = f.pull(t.Context())
	must(t, err)
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Fatalf("tree should be gone with (?d), stat err = %v", err)
	}
	if n := countLines(logs, "tree"); n != 1 {
		t.Fatalf("expected the single first warning only, got %d", n)
	}
}
