// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

// TestDeleteConflictLongNameConverges asserts the behaviour a fix must give
// and fails on upstream: a remote deletion of a 248-byte `.md` file that needs
// a conflict copy must converge like the short control file does. Upstream
// builds a 286-byte conflict name (conflictName adds 38 bytes, no length
// check), the Lstat of that destination fails with ENAMETOOLONG, the original
// stays and the deletion stays needed on every retry. Real puller iterations,
// real SQLite need list, real basic filesystem; skips unless the host enforces
// a 255-byte component limit.
func TestDeleteConflictLongNameConverges(t *testing.T) {
	dir := t.TempDir()
	// fakeFS has no component-length enforcement. Probe the real host rather
	// than assuming its filesystem matches the production receiver's 255 bytes.
	probe := filepath.Join(dir, strings.Repeat("p", 255))
	if err := os.WriteFile(probe, nil, 0o600); err != nil {
		t.Skipf("requires a 255-byte component limit: cannot create 255 bytes: %v", err)
	}
	must(t, os.Remove(probe))
	probe = filepath.Join(dir, strings.Repeat("p", 256))
	if err := os.WriteFile(probe, nil, 0o600); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Skipf("requires a 255-byte component limit: creating 256 bytes returned %v", err)
	}

	w, fcfg := newDefaultCfgWrapper(t)
	fcfg.FilesystemType = config.FilesystemTypeBasic
	fcfg.Path = dir
	fcfg.CaseSensitiveFS = false // production default, checks destination before rename
	fcfg.MaxConflicts = -1
	_, err := w.Modify(func(cfg *config.Configuration) { cfg.SetFolder(fcfg) })
	must(t, err)
	m := setupModel(t, w) // real SQLite, see newModel
	m.cancel()
	<-m.stopped
	r, ok := m.folderRunners.Get(fcfg.ID)
	if !ok {
		t.Fatal("missing folder runner")
	}
	f := r.(*sendReceiveFolder)
	ffs := f.Filesystem()

	longName := strings.Repeat("a", 245) + ".md" // exactly 248 ASCII bytes
	const shortName = "short.md"
	content := []byte("receiver content to preserve in a conflict copy\n")
	stamp := time.Date(2026, time.October, 29, 21, 50, 46, 0, time.UTC)
	for _, name := range []string{longName, shortName} {
		writeFile(t, ffs, name, content)
		must(t, ffs.Chtimes(name, stamp, stamp))
	}
	must(t, f.scanSubdirs(t.Context(), []string{longName, shortName}))

	var remote []protocol.FileInfo
	for _, name := range []string{longName, shortName} {
		cur, ok, err := m.sdb.GetDeviceFile(f.ID, protocol.LocalDeviceID, name)
		must(t, err)
		if !ok {
			t.Fatalf("missing local file %q", name)
		}
		deleted := cur
		deleted.Deleted = true
		deleted.Size = 0
		deleted.Blocks = nil
		deleted.BlocksHash = nil
		deleted.PreviousBlocksHash = nil
		deleted.ModifiedBy = device1.Short()
		deleted.ModifiedS = stamp.Add(time.Hour).Unix() // remote wins global conflict
		deleted.Version = protocol.Vector{Counters: []protocol.Counter{{ID: device1.Short(), Value: 2}}}
		if !deleted.InConflictWith(cur) {
			t.Fatal("fixture must require conflict preservation")
		}
		remote = append(remote, deleted)
	}
	must(t, m.sdb.Update(f.ID, device1, remote))

	assertNeeded := func(want ...string) {
		t.Helper()
		it, errFn := m.sdb.AllNeededGlobalFiles(f.ID, protocol.LocalDeviceID, f.Order, 0, 0)
		got := make(map[string]bool)
		for fi := range it {
			if !fi.IsDeleted() {
				t.Fatalf("needed item is not remote deletion: %+v", fi)
			}
			got[fi.Name] = true
		}
		must(t, errFn())
		if len(got) != len(want) {
			t.Fatalf("needed = %v, want %v", got, want)
		}
		for _, name := range want {
			if !got[name] {
				t.Fatalf("%q not needed", name)
			}
		}
	}
	assertNeeded(longName, shortName)

	// Re-enter through the need walk to verify completion is durable and no
	// extra conflict copies or scan requests are produced on later attempts.
	for attempt := 1; attempt <= 3; attempt++ {
		scanChan := make(chan string, 8)
		changed, err := f.pullerIteration(t.Context(), scanChan)
		must(t, err) // per-file failure is recorded, not returned as a round error
		wantChanged := 0
		if attempt == 1 {
			wantChanged = 2
		}
		if len(f.tempPullErrors) != 0 {
			t.Fatalf("attempt %d: the deletion did not converge: %v", attempt, f.tempPullErrors)
		}
		if changed != wantChanged {
			t.Fatalf("attempt %d: changed = %d, want %d", attempt, changed, wantChanged)
		}
		assertNeeded()
		for i, name := range []string{longName, shortName} {
			if _, err := ffs.Lstat(name); !os.IsNotExist(err) {
				t.Fatalf("original %q must be gone, got %v", name, err)
			}
			cur, ok, err := m.sdb.GetDeviceFile(f.ID, protocol.LocalDeviceID, name)
			must(t, err)
			if !ok || !cur.IsDeleted() || !cur.Version.Equal(remote[i].Version) {
				t.Fatalf("deletion %q was not committed to SQLite", name)
			}
			conflicts := existingConflicts(name, ffs)
			if len(conflicts) != 1 {
				t.Fatalf("%q conflict copies = %v", name, conflicts)
			}
			if len(filepath.Base(conflicts[0])) > 255 || !isConflict(conflicts[0]) {
				t.Fatalf("invalid conflict name %q", conflicts[0])
			}
			data, err := os.ReadFile(filepath.Join(dir, conflicts[0]))
			must(t, err)
			if !bytes.Equal(data, content) {
				t.Fatal("conflict did not preserve contents")
			}
		}
		if attempt == 1 {
			if len(scanChan) != 2 {
				t.Fatalf("scan requests = %d, want 2", len(scanChan))
			}
			seen := make(map[string]bool)
			for range 2 {
				name := <-scanChan
				if seen[name] || !isConflict(name) {
					t.Fatalf("invalid or duplicate scan request %q", name)
				}
				seen[name] = true
			}
		} else if len(scanChan) != 0 {
			t.Fatal("completed deletion scheduled another scan")
		}
	}

}
