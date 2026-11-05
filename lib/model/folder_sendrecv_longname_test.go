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

// TestDeleteConflictLongNameCurrentlyStaysNeeded pins the BUG, not desired
// behavior. Flip the long-name expectations to successful conflict preservation
// and deletion when a fix lands. No sleeps, peer transport, or background folder
// services: each real puller iteration consumes the SQLite remote need list.
func TestDeleteConflictLongNameCurrentlyStaysNeeded(t *testing.T) {
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

	local := make(map[string]protocol.FileInfo)
	var remote []protocol.FileInfo
	for _, name := range []string{longName, shortName} {
		cur, ok, err := m.sdb.GetDeviceFile(f.ID, protocol.LocalDeviceID, name)
		must(t, err)
		if !ok {
			t.Fatalf("missing local file %q", name)
		}
		local[name] = cur
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

	// Re-enter through the need walk three times. A permanently overlong
	// destination cannot heal on retry; no local DB completion is emitted.
	for attempt := 1; attempt <= 3; attempt++ {
		scanChan := make(chan string, 8)
		changed, err := f.pullerIteration(t.Context(), scanChan)
		must(t, err) // per-file failure is recorded, not returned as a round error
		wantChanged := 0
		if attempt == 1 {
			wantChanged = 1 // only the short control completes
		}
		if changed != wantChanged {
			t.Fatalf("attempt %d: changed = %d, want %d", attempt, changed, wantChanged)
		}
		if len(f.tempPullErrors) != 1 {
			t.Fatalf("attempt %d: errors = %v", attempt, f.tempPullErrors)
		}
		text := f.tempPullErrors[longName].text
		// This distinguishes the destination's checkCase Lstat from either
		// the source Lstat or an os.Rename ENAMETOOLONG failure.
		prefix := "syncing: delete file: lstat " + filepath.Join(dir, strings.TrimSuffix(longName, ".md")) + ".sync-conflict-"
		if !strings.HasPrefix(text, prefix) || !strings.HasSuffix(text, "-"+device1.Short().String()+".md: file name too long") {
			t.Fatalf("attempt %d: expected overlong conflict destination lstat, got %q", attempt, text)
		}
		data, err := os.ReadFile(filepath.Join(dir, longName))
		must(t, err)
		if !bytes.Equal(data, content) {
			t.Fatal("failed conflict move changed original contents")
		}
		cur, ok, err := m.sdb.GetDeviceFile(f.ID, protocol.LocalDeviceID, longName)
		must(t, err)
		if !ok || cur.IsDeleted() || !cur.Version.Equal(local[longName].Version) {
			t.Fatal("failed deletion changed local index")
		}
		assertNeeded(longName)

		if attempt == 1 {
			if _, err := ffs.Lstat(shortName); !os.IsNotExist(err) {
				t.Fatalf("short original must be gone, got %v", err)
			}
			cur, ok, err := m.sdb.GetDeviceFile(f.ID, protocol.LocalDeviceID, shortName)
			must(t, err)
			if !ok || !cur.IsDeleted() || !cur.Version.Equal(remote[1].Version) {
				t.Fatal("short deletion was not committed to SQLite")
			}
			conflicts := existingConflicts(shortName, ffs)
			if len(conflicts) != 1 || len(scanChan) != 1 {
				t.Fatalf("short conflict copies = %v, scan requests = %d", conflicts, len(scanChan))
			}
			if path := <-scanChan; path != conflicts[0] {
				t.Fatalf("scan path %q, want %q", path, conflicts[0])
			}
			data, err := os.ReadFile(filepath.Join(dir, conflicts[0]))
			must(t, err)
			if !bytes.Equal(data, content) {
				t.Fatal("short conflict did not preserve contents")
			}
		} else if len(scanChan) != 0 {
			t.Fatal("failed long conflict move scheduled a scan")
		}
	}
	names, err := ffs.DirNames(".")
	must(t, err)
	for _, name := range names {
		if strings.HasPrefix(name, strings.TrimSuffix(longName, ".md")+".sync-conflict-") {
			t.Fatalf("failed move left conflict copy %q", name)
		}
	}
}
