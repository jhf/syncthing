// Copyright (C) 2026 The Syncthing Authors.

package model

import (
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func metaBatchTestConfig() config.FolderConfiguration {
	return config.FolderConfiguration{FSWatcherDelayS: 10}
}

func TestMetaBatchHoldLastWins(t *testing.T) {
	now := time.Now()
	clock := now
	m := newMetaBatchTracker(metaBatchTestConfig(), func() time.Time { return clock }, nil)

	m.hold(protocol.FileInfo{Name: "a", ModifiedS: 1})
	m.hold(protocol.FileInfo{Name: "a", ModifiedS: 2})
	m.hold(protocol.FileInfo{Name: "b", ModifiedS: 3})

	if m.len() != 2 {
		t.Fatalf("expected 2 pending, got %d", m.len())
	}
	if h := m.pending["a"]; h.fi.ModifiedS != 2 || h.coalesced != 2 {
		t.Fatalf("last-wins broken: ModifiedS=%d coalesced=%d", h.fi.ModifiedS, h.coalesced)
	}
	if !m.oldest.Equal(now) {
		t.Fatal("oldest should be the first hold time")
	}
}

func TestMetaBatchWindowFlush(t *testing.T) {
	now := time.Now()
	clock := now
	m := newMetaBatchTracker(metaBatchTestConfig(), func() time.Time { return clock }, nil)
	window := m.params.window

	m.hold(protocol.FileInfo{Name: "a", ModifiedS: 1})
	clock = clock.Add(window / 2)
	m.hold(protocol.FileInfo{Name: "b", ModifiedS: 2})

	if d, ok := m.nextIn(); !ok || d != window/2 {
		t.Fatalf("nextIn = %v, %v; want %v, true", d, ok, window/2)
	}
	if due := m.flushDue(); len(due) != 0 {
		t.Fatalf("flushed %d before window", len(due))
	}

	clock = clock.Add(window/2 + time.Second)
	due := m.flushDue()
	if len(due) != 2 {
		t.Fatalf("expected 2 flushed at window, got %d", len(due))
	}
	if m.len() != 0 {
		t.Fatal("pending not empty after flush")
	}
	if _, ok := m.nextIn(); ok {
		t.Fatal("nextIn should report nothing pending")
	}
}

func TestMetaBatchCapFlush(t *testing.T) {
	now := time.Now()
	m := newMetaBatchTracker(metaBatchTestConfig(), func() time.Time { return now }, nil)
	m.params.cap = 3
	m.hold(protocol.FileInfo{Name: "a"})
	m.hold(protocol.FileInfo{Name: "b"})
	if m.due() {
		t.Fatal("due before cap")
	}
	m.hold(protocol.FileInfo{Name: "c"})
	if !m.due() {
		t.Fatal("not due at cap")
	}
	if due := m.flushDue(); len(due) != 3 {
		t.Fatalf("expected 3 flushed at cap, got %d", len(due))
	}
}

func TestMetaBatchDrainExceptSuperseded(t *testing.T) {
	now := time.Now()
	m := newMetaBatchTracker(metaBatchTestConfig(), func() time.Time { return now }, nil)
	m.hold(protocol.FileInfo{Name: "a", ModifiedS: 1})
	m.hold(protocol.FileInfo{Name: "b", ModifiedS: 2})

	drained := m.drainExcept(map[string]struct{}{"a": {}}, "piggyback")
	if len(drained) != 1 || drained[0].Name != "b" {
		t.Fatalf("expected only b drained, got %v", drained)
	}
	if m.len() != 0 {
		t.Fatal("superseded entry must be dropped, not re-held")
	}
}

func metaBatchTestFile(name string, modifiedS int64) protocol.FileInfo {
	return protocol.FileInfo{
		Name:       name,
		Type:       protocol.FileInfoTypeFile,
		Size:       100,
		ModifiedS:  modifiedS,
		Blocks:     []protocol.BlockInfo{{Offset: 0, Size: 100, Hash: []byte("hash")}},
		BlocksHash: []byte("blockshash"),
	}
}

func TestMetadataOnlyUpdate(t *testing.T) {
	cur := metaBatchTestFile("a", 1000)
	comp := protocol.FileInfoComparison{}

	// mtime-only bump: metadata-only
	fi := metaBatchTestFile("a", 2000)
	if !metadataOnlyUpdate(cur, fi, comp) {
		t.Error("mtime-only bump should classify as metadata-only")
	}

	// size change: real
	fi = metaBatchTestFile("a", 2000)
	fi.Size = 200
	if metadataOnlyUpdate(cur, fi, comp) {
		t.Error("size change must not classify as metadata-only")
	}

	// content change (same size, different blocks): real
	fi = metaBatchTestFile("a", 2000)
	fi.Blocks = []protocol.BlockInfo{{Offset: 0, Size: 100, Hash: []byte("other")}}
	fi.BlocksHash = []byte("otherhash")
	if metadataOnlyUpdate(cur, fi, comp) {
		t.Error("content change must not classify as metadata-only")
	}

	// permission change: real
	fi = metaBatchTestFile("a", 2000)
	fi.Permissions = 0o600
	if metadataOnlyUpdate(cur, fi, comp) {
		t.Error("permission change must not classify as metadata-only")
	}

	// delete: real
	fi = metaBatchTestFile("a", 2000)
	fi.Deleted = true
	if metadataOnlyUpdate(cur, fi, comp) {
		t.Error("delete must not classify as metadata-only")
	}

	// directory: real
	fi = metaBatchTestFile("a", 2000)
	fi.Type = protocol.FileInfoTypeDirectory
	if metadataOnlyUpdate(cur, fi, comp) {
		t.Error("directory must not classify as metadata-only")
	}

	// invalid: real
	fi = metaBatchTestFile("a", 2000)
	fi.LocalFlags = protocol.FlagLocalRemoteInvalid
	if metadataOnlyUpdate(cur, fi, comp) {
		t.Error("invalid entry must not classify as metadata-only")
	}

	// symlink with same target, newer mtime: metadata-only
	curSym := protocol.FileInfo{Name: "s", Type: protocol.FileInfoTypeSymlink, SymlinkTarget: []byte("/tmp/x"), ModifiedS: 1000}
	fiSym := protocol.FileInfo{Name: "s", Type: protocol.FileInfoTypeSymlink, SymlinkTarget: []byte("/tmp/x"), ModifiedS: 2000}
	if !metadataOnlyUpdate(curSym, fiSym, comp) {
		t.Error("mtime-only symlink bump should classify as metadata-only")
	}
	fiSym.SymlinkTarget = []byte("/tmp/y")
	if metadataOnlyUpdate(curSym, fiSym, comp) {
		t.Error("symlink retarget must not classify as metadata-only")
	}
}
