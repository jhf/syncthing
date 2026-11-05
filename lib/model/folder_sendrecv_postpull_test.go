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
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// TestPostPullScanTimestampPrecision installs the content only through the real
// receiver finish path, commits its completion job, then runs the real scanner
// and DB update path. There is no receiver-side content or metadata edit.
//
// scanSubdirs is the body called by Scan, which pullScannerRoutine schedules
// after deduplicating paths and finishing the pull. The model is stopped, so
// calling Scan would wait for its stopped event loop. Two synchronous scans
// also cover a redundant watcher notification without introducing scheduling.
func TestPostPullScanTimestampPrecision(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		nanos                      int
		coarse, loseMapping, basic bool
		window                     time.Duration
		ignorePerms, noPerms       bool
		wantLocal                  bool
	}{
		{name: "basic_fs_nanoseconds", nanos: 123456789, basic: true},
		{name: "basic_fs_microseconds", nanos: 123456000, basic: true},
		{name: "basic_fs_whole_seconds", basic: true},
		{name: "ignore_permissions", nanos: 123456789, ignorePerms: true},
		{name: "remote_no_permissions", nanos: 123456789, noPerms: true},
		{name: "nanoseconds", nanos: 123456789},
		{name: "microseconds", nanos: 123456000},
		{name: "whole_seconds"},
		{name: "seconds_fs_nanoseconds", nanos: 123456789, coarse: true},
		{name: "seconds_fs_microseconds", nanos: 123456000, coarse: true},
		{name: "seconds_fs_whole_seconds", coarse: true},
		{name: "lost_mapping", nanos: 123456789, coarse: true, loseMapping: true, wantLocal: true},
		{name: "lost_mapping_inside_window", nanos: 123456789, coarse: true, loseMapping: true, window: time.Second},
		{name: "lost_mapping_at_window_boundary", nanos: 123456789, coarse: true, loseMapping: true, window: 123456789 * time.Nanosecond, wantLocal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, batching := range []string{"0", "1"} {
				t.Run("metabatch_"+batching, func(t *testing.T) {
					t.Setenv("STMETABATCH", batching)
					w, cfg := newDefaultCfgWrapper(t)
					if tc.coarse || tc.basic {
						if tc.basic {
							cfg.Path = t.TempDir()
							cfg.FilesystemType = config.FilesystemTypeBasic
						} else {
							cfg.Path += "&timeprecisionsecond=true"
						}
						_, err := w.Modify(func(c *config.Configuration) { c.SetFolder(cfg) })
						must(t, err)
					}
					m := setupModel(t, w)
					m.cancel()
					<-m.stopped
					runner, _ := m.folderRunners.Get(cfg.ID)
					f := runner.(*sendReceiveFolder)
					f.modTimeWindow = tc.window
					f.IgnorePerms = tc.ignorePerms
					const name = "lib.rs"
					content := []byte("// downloaded Mac content, never edited on receiver\n")
					// A non-UTC location encodes the same instant, not a timezone offset
					// error. Wire timestamps are Unix seconds plus nanoseconds.
					stamp := time.Date(2026, time.November, 2, 23, 2, 26, tc.nanos, time.UTC).In(time.FixedZone("sender", 3600))
					blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(content), protocol.MinBlockSize, int64(len(content)), nil)
					must(t, err)
					remote := protocol.FileInfo{
						Name: name, Type: protocol.FileInfoTypeFile, Size: int64(len(content)),
						Permissions: 0o640, NoPermissions: tc.noPerms, ModifiedS: stamp.Unix(), ModifiedNs: int32(stamp.Nanosecond()),
						ModifiedBy: device1.Short(), Version: protocol.Vector{Counters: []protocol.Counter{{ID: device1.Short(), Value: 4}}},
						Blocks: blocks, BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize,
					}
					temp := fs.TempName(name)
					writeFile(t, f.mtimefs, temp, content)
					jobs := make(chan dbUpdateJob, 1)
					scans := make(chan string, 2)
					must(t, f.performFinish(remote, protocol.FileInfo{}, false, temp, jobs, scans))
					job := <-jobs
					if job.jobType != dbUpdateHandleFile || !job.file.Version.Equal(remote.Version) || len(scans) != 0 {
						t.Fatal("unexpected finish completion or scan request")
					}
					commitQueuedFinishForTest(t, f, job)
					current := func() protocol.FileInfo {
						t.Helper()
						fi, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, name)
						must(t, err)
						if !ok {
							t.Fatal("missing pulled index entry")
						}
						return fi
					}
					if !current().Version.Equal(remote.Version) {
						t.Fatal("pull changed remote vector")
					}
					raw, err := cfg.Filesystem().Lstat(name)
					must(t, err)
					wantRaw := stamp
					if tc.coarse {
						wantRaw = stamp.Truncate(time.Second)
					}
					if !tc.basic && !raw.ModTime().Equal(wantRaw) {
						t.Fatalf("raw mtime %s, want %s", raw.ModTime(), wantRaw)
					}
					resolved, err := f.mtimefs.Lstat(name)
					must(t, err)
					if !resolved.ModTime().Equal(stamp) || (!tc.ignorePerms && !tc.noPerms && !protocol.PermsEqual(uint32(resolved.Mode()), remote.Permissions)) {
						t.Fatalf("finish metadata not restored: %s %o", resolved.ModTime(), resolved.Mode())
					}
					if !raw.ModTime().Equal(stamp) {
						disk, virtual := m.sdb.GetMtime(cfg.ID, name)
						if !disk.Equal(raw.ModTime()) || !virtual.Equal(stamp) {
							t.Fatalf("wrong mapping: %s -> %s", disk, virtual)
						}
					}
					if tc.loseMapping {
						// Explicit fault injection, NOT an observed production event. The
						// only changed state is Syncthing's advisory DB mapping, not the file.
						must(t, m.sdb.DeleteMtime(cfg.ID, name))
					}
					stat, err := f.mtimefs.Lstat(name)
					must(t, err)
					err = f.scanIfItemChanged(name, stat, current(), true, false, scans)
					if tc.wantLocal {
						if !errors.Is(err, errModified) || len(scans) != 1 {
							t.Fatalf("expected preflight rescan, got %v (%d requests)", err, len(scans))
						}
					} else if err != nil || len(scans) != 0 {
						t.Fatalf("unchanged pull rejected: %v", err)
					}
					var afterFirstScan protocol.Vector
					for scan := range 2 {
						must(t, f.scanSubdirs(t.Context(), []string{name}))
						// A metadata-only scan may be held. Flush via the production commit
						// path without sleeping 60 seconds, preserving the actual scan vector.
						held := f.metaBatch.flushAll()
						if !tc.wantLocal && len(held) != 0 {
							t.Fatal("unchanged pull generated held metadata update")
						}
						if len(held) > 0 {
							must(t, f.commitScanBatch(held))
						}
						got := current()
						if scan == 0 {
							afterFirstScan = got.Version
						} else if !got.Version.Equal(afterFirstScan) {
							t.Fatalf("redundant scan changed version: %v -> %v", afterFirstScan, got.Version)
						}
						if tc.wantLocal {
							if got.Version.Counter(f.shortID) == 0 || got.Version.Counter(device1.Short()) != 4 || len(got.Version.Counters) != 2 || got.ModifiedBy != f.shortID || !bytes.Equal(got.PreviousBlocksHash, remote.BlocksHash) {
								t.Fatalf("expected receiver-local metadata version: %+v", got)
							}
						} else if !got.Version.Equal(remote.Version) || got.ModifiedBy != remote.ModifiedBy || !bytes.Equal(got.PreviousBlocksHash, remote.PreviousBlocksHash) {
							t.Fatalf("scan replaced remote version: %+v", got)
						}
						if !bytes.Equal(got.BlocksHash, remote.BlocksHash) {
							t.Fatal("scan changed content hash")
						}
					}
					fd, err := f.mtimefs.Open(name)
					must(t, err)
					data, err := io.ReadAll(fd)
					must(t, err)
					must(t, fd.Close())
					if !bytes.Equal(data, content) || len(existingConflicts(name, f.mtimefs)) != 0 {
						t.Fatal("scan changed content or created conflict")
					}
					t.Logf("raw=%s resolved-after-finish=%s receiver-counter=%d", raw.ModTime().Format(time.RFC3339Nano), resolved.ModTime().Format(time.RFC3339Nano), current().Version.Counter(f.shortID))
				})
			}
		})
	}
}
