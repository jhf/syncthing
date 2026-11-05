// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// TestPullConflictFromReceiverLocalVersionDivergence reproduces the hypothesized
// conflict mechanism in the 2026-11-03 lib.rs incident: the conflict contained an
// earlier sender version, not unique receiver work. This pins current behavior,
// not a claim that producing this conflict is desirable.
//
// A stopped model gives us the real DB, scanner, and performFinish (including
// InConflictWith and moveForConflict), without peer or watcher scheduling. We
// inject an mtime-only mismatch, not a content edit. This proves what a rescan of
// that mismatch does, but does NOT prove that pull writes/watch events caused
// the production mismatch, or exercise transport of the conflict back to sender.
func TestPullConflictFromReceiverLocalVersionDivergence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		basedOnCurrent   bool
		includesReceiver bool
		wantConflict     bool
	}{
		{name: "missed_intermediate_sender_versions", wantConflict: true},
		{name: "previous_hash_matches_current", basedOnCurrent: true},
		{name: "incoming_version_dominates", includesReceiver: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, f := setupSendReceiveFolder(t)
			ffs := f.Filesystem()
			f.MaxConflicts = -1
			const name = "lib.rs"
			oldContent := []byte("// sender v1, earlier Mac content\n")
			newContent := []byte("// sender v4, later Mac content\n")
			stamp := time.Date(2026, time.November, 2, 23, 2, 26, 0, time.UTC)
			writeFile(t, ffs, name, oldContent)
			must(t, ffs.Chtimes(name, stamp, stamp))
			must(t, f.scanSubdirs(t.Context(), []string{name}))

			current := func() protocol.FileInfo {
				t.Helper()
				fi, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, name)
				must(t, err)
				if !ok {
					t.Fatal("missing local index entry")
				}
				return fi
			}
			read := func(path string) []byte {
				t.Helper()
				fd, err := ffs.Open(path)
				must(t, err)
				defer fd.Close()
				data, err := io.ReadAll(fd)
				must(t, err)
				return data
			}

			// Model a completed pull of sender v1, preserving its remote vector.
			v1 := current()
			sender := device1.Short()
			receiver := f.shortID
			v1.Version = protocol.Vector{Counters: []protocol.Counter{{ID: sender, Value: 1}}}
			v1.ModifiedBy = sender
			must(t, f.updateLocalsFromPulling([]protocol.FileInfo{v1}))
			v1 = current()
			if !v1.Version.Equal(protocol.Vector{Counters: []protocol.Counter{{ID: sender, Value: 1}}}) {
				t.Fatal("pull did not preserve sender-only v1 vector")
			}

			// Only disk metadata diverges. The first finish must refuse to
			// overwrite it and request a scan, just as in the production log.
			changedStamp := stamp.Add(time.Minute)
			must(t, ffs.Chtimes(name, changedStamp, changedStamp))
			incoming := v1
			incoming.Version = protocol.Vector{Counters: []protocol.Counter{{ID: sender, Value: 4}}}
			incoming.ModifiedS = stamp.Add(31*time.Minute + 23*time.Second).Unix()
			incoming.Size = int64(len(newContent))
			var err error
			incoming.Blocks, err = scanner.Blocks(t.Context(), bytes.NewReader(newContent), protocol.MinBlockSize, incoming.Size, nil)
			must(t, err)
			incoming.BlocksHash = protocol.BlocksHash(incoming.Blocks)
			v3Blocks, err := scanner.Blocks(t.Context(), strings.NewReader("// sender v3, never received\n"), protocol.MinBlockSize, -1, nil)
			must(t, err)
			incoming.PreviousBlocksHash = protocol.BlocksHash(v3Blocks)
			temp := fs.TempName(name)
			writeFile(t, ffs, temp, newContent)
			scanChan := make(chan string, 2)
			dbChan := make(chan dbUpdateJob, 1)
			if err := f.performFinish(incoming, v1, true, temp, dbChan, scanChan); !errors.Is(err, errModified) {
				t.Fatalf("expected unrescanned modification, got %v", err)
			}
			select {
			case path := <-scanChan:
				if path != name {
					t.Fatalf("scan requested for %q, want %q", path, name)
				}
			default:
				t.Fatal("finish did not schedule rescan")
			}
			if len(dbChan) != 0 || len(existingConflicts(name, ffs)) != 0 || !bytes.Equal(read(name), oldContent) {
				t.Fatal("rejected finish changed index, created conflict, or changed content")
			}

			must(t, f.scanSubdirs(t.Context(), []string{name}))
			cur := current()
			if cur.Version.Counter(sender) != 1 || cur.Version.Counter(receiver) == 0 || len(cur.Version.Counters) != 2 {
				t.Fatalf("rescan must preserve sender v1 and add only receiver counter: %v", cur.Version)
			}
			if cur.ModifiedBy != receiver || !bytes.Equal(cur.BlocksHash, v1.BlocksHash) || !bytes.Equal(cur.PreviousBlocksHash, v1.BlocksHash) || !bytes.Equal(read(name), oldContent) {
				t.Fatal("rescan must attribute unchanged content to receiver with v1 previous hash")
			}
			if incoming.Version.GreaterEqual(cur.Version) || cur.Version.GreaterEqual(incoming.Version) {
				t.Fatalf("expected concurrent sender/receiver vectors: %v and %v", incoming.Version, cur.Version)
			}
			if bytes.Equal(incoming.PreviousBlocksHash, cur.BlocksHash) {
				t.Fatal("v3 hash unexpectedly equals receiver v1 hash")
			}
			if tc.basedOnCurrent {
				incoming.PreviousBlocksHash = cur.BlocksHash
			}
			if tc.includesReceiver {
				incoming.Version = incoming.Version.Merge(cur.Version)
			}
			if got := incoming.InConflictWith(cur); got != tc.wantConflict {
				t.Fatalf("InConflictWith = %v, want %v", got, tc.wantConflict)
			}

			must(t, f.performFinish(incoming, cur, true, temp, dbChan, scanChan))
			if !bytes.Equal(read(name), newContent) {
				t.Fatal("finish did not install sender v4")
			}
			select {
			case job := <-dbChan:
				if job.jobType != dbUpdateHandleFile || job.file.Name != name || !job.file.Version.Equal(incoming.Version) {
					t.Fatalf("unexpected completion job: %+v", job)
				}
				must(t, f.updateLocalsFromPulling([]protocol.FileInfo{job.file}))
			default:
				t.Fatal("finish did not queue index update")
			}
			if !current().Version.Equal(incoming.Version) {
				t.Fatal("pull completion changed incoming vector")
			}
			conflicts := existingConflicts(name, ffs)
			if !tc.wantConflict {
				if len(conflicts) != 0 || len(scanChan) != 0 {
					t.Fatalf("unexpected conflicts or scan requests: %v, %d", conflicts, len(scanChan))
				}
				return
			}
			if len(conflicts) != 1 {
				t.Fatalf("expected one conflict, got %v", conflicts)
			}
			if !bytes.Equal(read(conflicts[0]), oldContent) {
				t.Fatal("conflict does not contain the earlier sender content")
			}
			if !strings.HasSuffix(conflicts[0], "-"+sender.String()+".rs") {
				t.Fatalf("conflict name does not identify incoming sender: %s", conflicts[0])
			}
			select {
			case path := <-scanChan:
				if path != conflicts[0] {
					t.Fatalf("expected conflict scan, got %q", path)
				}
				// A real scan indexes the new filename for subsequent syncing.
				must(t, f.scanSubdirs(t.Context(), []string{path}))
				fi, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, path)
				must(t, err)
				if !ok || !bytes.Equal(fi.BlocksHash, v1.BlocksHash) {
					t.Fatal("conflict was not indexed with earlier content")
				}
			default:
				t.Fatal("conflict was not scheduled for scanning")
			}
		})
	}
}
