// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// TestDeleteConflictLongNameCurrentlyStaysNeeded guards the former bug: both
// long and short originals must converge to the remote deletion while retaining
// their bytes in one conflict copy. Real iterations consume SQLite's need list.
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
		if changed != wantChanged {
			t.Fatalf("attempt %d: changed = %d, want %d", attempt, changed, wantChanged)
		}
		if len(f.tempPullErrors) != 0 {
			t.Fatalf("attempt %d: errors = %v", attempt, f.tempPullErrors)
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

func TestConflictNameByteLimit(t *testing.T) {
	const id = "ABCDEFG"
	const suffixLen = len(".sync-conflict-20060102-150405-") + len(id)
	for _, tc := range []struct {
		name, input, stem, ext string
	}{
		{"boundary", strings.Repeat("a", 255-suffixLen-3) + ".md", strings.Repeat("a", 255-suffixLen-3), ".md"},
		{"over_boundary", strings.Repeat("a", 256-suffixLen-3) + ".md", strings.Repeat("a", 255-suffixLen-3-9), ".md"},
		{"utf8_stem", strings.Repeat("a", 255-suffixLen-4) + "€z.md", strings.Repeat("a", 255-suffixLen-3-9), ".md"},
		{"utf8_at_hashed_cut", strings.Repeat("a", 255-suffixLen-3-10) + "€" + strings.Repeat("z", 30) + ".md", strings.Repeat("a", 255-suffixLen-3-10), ".md"},
		{"no_extension", strings.Repeat("a", 248), strings.Repeat("a", 255-suffixLen-9), ""},
		{"long_extension", "a." + strings.Repeat("x", 229), "", "." + strings.Repeat("x", 254-suffixLen-9)},
		{"utf8_extension", "a." + strings.Repeat("€", 80), "", "." + strings.Repeat("€", (254-suffixLen-9)/3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.input)+suffixLen > 255 {
				tc.stem += fmt.Sprintf("~%x", sha256.Sum256([]byte(tc.input)))[:9]
			}
			input := filepath.Join("parent.with.extension", tc.input)
			got := conflictName(input, id)
			base := filepath.Base(got)
			if len(base) > 255 || !utf8.ValidString(got) || !isConflict(got) || filepath.Dir(got) != filepath.Dir(input) {
				t.Fatalf("invalid conflict name (%d bytes): %q", len(base), got)
			}
			if !strings.HasPrefix(base, tc.stem+".sync-conflict-") || !strings.HasSuffix(base, "-"+id+tc.ext) || len(base) != len(tc.stem)+suffixLen+len(tc.ext) {
				t.Fatalf("stem, suffix or extension changed unexpectedly: %q", base)
			}
			suffix := base[len(tc.stem) : len(tc.stem)+suffixLen]
			if _, err := time.Parse(".sync-conflict-20060102-150405-"+id, suffix); err != nil {
				t.Fatalf("incomplete conflict suffix %q: %v", suffix, err)
			}
		})
	}
}

func TestConflictLongNamePruning(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	f.MaxConflicts = 1
	name := strings.Repeat("a", 245) + ".md"
	// Seed an older bounded copy, then go through real moveForConflict pruning.
	old := conflictName(name, device1.Short().String())
	start := strings.Index(old, ".sync-conflict-") + len(".sync-conflict-")
	old = old[:start] + "20000101-000000" + old[start+len("20000101-000000"):]
	writeFile(t, f.mtimefs, old, []byte("old conflict"))
	writeFile(t, f.mtimefs, name, []byte("new conflict"))
	scans := make(chan string, 1)
	must(t, f.moveForConflict(name, device1.Short().String(), scans))
	conflicts := existingConflicts(name, f.mtimefs)
	if len(conflicts) != 1 || conflicts[0] == old || len(scans) != 1 || <-scans != conflicts[0] {
		t.Fatalf("pruning failed: conflicts=%v scans=%d", conflicts, len(scans))
	}
	if _, err := f.mtimefs.Lstat(old); !os.IsNotExist(err) {
		t.Fatalf("old conflict survived pruning: %v", err)
	}
}

func TestConflictShortenedNamesDoNotCrossPrune(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	f.MaxConflicts = 1
	a := strings.Repeat("a", 240) + "first.md"
	b := strings.Repeat("a", 240) + "other.md"
	other := conflictName(b, device1.Short().String())
	start := strings.Index(other, ".sync-conflict-") + len(".sync-conflict-")
	other = other[:start] + "20000101-000000" + other[start+15:]
	writeFile(t, f.mtimefs, other, []byte("other bytes"))
	writeFile(t, f.mtimefs, a, []byte("a bytes"))
	must(t, f.moveForConflict(a, device1.Short().String(), make(chan string, 1)))
	if _, err := f.mtimefs.Lstat(other); err != nil {
		t.Fatalf("pruning first original deleted other original's conflict: %v", err)
	}
	if got := existingConflicts(a, f.mtimefs); len(got) != 1 || got[0] == other {
		t.Fatalf("first original matched other copies: %v", got)
	}
}

type conflictLengthFS struct {
	fs.Filesystem
	limit, attempts int
	err             error
}

func (f *conflictLengthFS) Rename(old, new string) error {
	f.attempts++
	if len(filepath.Base(new)) > f.limit {
		// Mimic caseFilesystem's destination Lstat, not just os.Rename.
		return &os.PathError{Op: "lstat", Path: new, Err: f.err}
	}
	return f.Filesystem.Rename(old, new)
}

func TestConflictNameTooLongFallback(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	limited := &conflictLengthFS{Filesystem: f.mtimefs, limit: 143, err: syscall.ENAMETOOLONG}
	f.mtimefs = limited
	name := strings.Repeat("a", 120) + ".md"
	writeFile(t, f.mtimefs, name, []byte("preserved"))
	scans := make(chan string, 1)
	if err := f.moveForConflict(name, device1.Short().String(), scans); err != nil {
		t.Fatalf("conflict rename did not recover from destination lstat ENAMETOOLONG: %v", err)
	}
	if limited.attempts != 2 || len(scans) != 1 {
		t.Fatalf("rename attempts=%d scans=%d, want 2 and 1", limited.attempts, len(scans))
	}
	copy := <-scans
	identity := fmt.Sprintf("~%x", sha256.Sum256([]byte(name)))[:17]
	if len(filepath.Base(copy)) > 143 || !strings.HasPrefix(copy, identity+".sync-conflict-") {
		t.Fatalf("invalid fallback name: %q", copy)
	}
	if got := existingConflicts(name, f.mtimefs); len(got) != 1 || got[0] != copy {
		t.Fatalf("fallback not discoverable: %v", got)
	}
	fd, err := f.mtimefs.Open(copy)
	must(t, err)
	defer fd.Close()
	buf := make([]byte, 9)
	_, err = fd.Read(buf)
	must(t, err)
	if string(buf) != "preserved" {
		t.Fatalf("fallback changed contents: %q", buf)
	}
}

func TestConflictNameDiscoveryFormats(t *testing.T) {
	for _, base := range []string{"short[*]?.md", strings.Repeat("a", 245) + ".md", "a." + strings.Repeat("x", 229), "a." + strings.Repeat("€", 80)} {
		for _, id := range []string{"ABCDEFG", ""} {
			_, f := setupSendReceiveFolder(t)
			must(t, f.mtimefs.MkdirAll("parent", 0o755))
			name := filepath.Join("parent", base)
			want := make(map[string]bool)
			for i, mode := range []int{conflictHashed, conflictHashOnly} {
				suffix := fmt.Sprintf(".sync-conflict-2000010%d-000000-%s", i+1, id)
				copy := conflictNameWithSuffix(name, suffix, mode)
				writeFile(t, f.mtimefs, copy, []byte("copy"))
				want[copy] = true
			}
			// Also discover the original upstream spelling, where no byte budget applied.
			ext := filepath.Ext(name)
			old := name[:len(name)-len(ext)] + ".sync-conflict-20000104-000000-" + id + ext
			writeFile(t, f.mtimefs, old, []byte("upstream"))
			want[old] = true
			got := existingConflicts(name, f.mtimefs)
			if len(got) != len(want) {
				t.Fatalf("%q id=%q: got %v, want %v", name, id, got, want)
			}
			for _, copy := range got {
				if !want[copy] {
					t.Fatalf("matched unrelated copy %q", copy)
				}
			}
		}
	}
}

func TestConflictNameFallbackErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit    int
		err      error
		attempts int
	}{
		{"retry_also_too_long", 10, syscall.ENAMETOOLONG, 2},
		{"permission_not_length", 10, syscall.EACCES, 1},
		{"invalid_not_length", 10, syscall.EINVAL, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, f := setupSendReceiveFolder(t)
			limited := &conflictLengthFS{Filesystem: f.mtimefs, limit: tc.limit, err: tc.err}
			f.mtimefs = limited
			writeFile(t, f.mtimefs, "original.md", []byte("preserved"))
			scans := make(chan string, 1)
			err := f.moveForConflict("original.md", device1.Short().String(), scans)
			if !errors.Is(err, tc.err) || limited.attempts != tc.attempts || len(scans) != 0 {
				t.Fatalf("err=%v attempts=%d scans=%d", err, limited.attempts, len(scans))
			}
			if _, err := f.mtimefs.Lstat("original.md"); err != nil {
				t.Fatalf("failed move lost original: %v", err)
			}
		})
	}
	for _, err := range []error{
		syscall.ENAMETOOLONG,
		&os.PathError{Op: "lstat", Path: "destination", Err: syscall.ENAMETOOLONG},
		&os.LinkError{Op: "rename", Old: "source", New: "destination", Err: syscall.ENAMETOOLONG},
	} {
		if !conflictNameTooLong(fmt.Errorf("wrapper: %w", err)) {
			t.Fatalf("did not classify wrapped length error: %v", err)
		}
	}
	if conflictNameTooLong(errors.New("file name too long")) {
		t.Fatal("classified untyped error text as ENAMETOOLONG")
	}
}

func TestConflictMixedFormatPruning(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	f.MaxConflicts = 1
	name := strings.Repeat("a", 245) + ".md"
	for i, mode := range []int{conflictHashed, conflictHashOnly} {
		suffix := fmt.Sprintf(".sync-conflict-2000010%d-000000-%s", i+1, device1.Short().String())
		writeFile(t, f.mtimefs, conflictNameWithSuffix(name, suffix, mode), []byte("old"))
	}
	writeFile(t, f.mtimefs, name, []byte("new"))
	scans := make(chan string, 1)
	must(t, f.moveForConflict(name, device1.Short().String(), scans))
	newName := <-scans
	if got := existingConflicts(name, f.mtimefs); len(got) != 1 || got[0] != newName {
		t.Fatalf("retention kept an older format instead of newest copy: %v", got)
	}
}

func TestConflictNameFallbackExtension(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	limited := &conflictLengthFS{Filesystem: f.mtimefs, limit: 143, err: syscall.ENAMETOOLONG}
	f.mtimefs = limited
	name := "a." + strings.Repeat("€", 36)
	writeFile(t, f.mtimefs, name, []byte("preserved"))
	scans := make(chan string, 1)
	must(t, f.moveForConflict(name, device1.Short().String(), scans))
	copy := <-scans
	wantExt := "." + strings.Repeat("€", 29) // 88 bytes after 17-byte hash and 38-byte suffix
	if limited.attempts != 2 || len(copy) != 143 || !utf8.ValidString(copy) || !strings.HasSuffix(copy, wantExt) {
		t.Fatalf("invalid fallback extension: %q (%d bytes), attempts=%d", copy, len(copy), limited.attempts)
	}
	if got := existingConflicts(name, f.mtimefs); len(got) != 1 || got[0] != copy {
		t.Fatalf("cut fallback extension not found: %v", got)
	}
}
