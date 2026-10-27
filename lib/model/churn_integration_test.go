// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestChurnSourceRejectRequestBranches(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	c := churnTestTracker()
	f.churn = c
	must(t, f.mtimefs.MkdirAll("directory", 0o755))
	fd, err := f.mtimefs.Create("file")
	must(t, err)
	_, err = fd.Write([]byte("payload"))
	must(t, err)
	must(t, fd.Close())
	cases := []struct {
		name, reason string
		temp         bool
	}{
		{"missing", "real_missing", false},
		{"directory", "nonregular", false},
		{"file", "hash_mismatch", false},
		{"missing-temp", "temp_missing_or_nonregular", true},
	}
	if !build.IsWindows {
		must(t, f.mtimefs.CreateSymlink("directory", "link"))
		cases = append(cases, struct {
			name, reason string
			temp         bool
		}{"link/file", "traversal", false})
	}
	for _, tc := range cases {
		req := &protocol.Request{Folder: f.folderID, Name: tc.name, Size: 7, Hash: []byte("wrong hash"), FromTemporary: tc.temp}
		response, err := m.Request(device1Conn, req)
		if response != nil {
			response.Close()
			t.Fatal("rejected request returned a response")
		}
		if !errors.Is(err, protocol.ErrNoSuchFile) {
			t.Fatalf("%s wire error changed: %v", tc.reason, err)
		}
		if c.total.values[churnIndex("source."+tc.reason)] != 1 {
			t.Fatalf("%s reason not recorded", tc.reason)
		}
	}
	// Receiver and source use exactly the same bounded request hash token.
	s := c.total.samples[0]
	if s.Hash != churnHash([]byte("wrong hash")) || s.Size != 7 || s.Path != "missing" {
		t.Fatalf("uncorrelatable request sample: %+v", s)
	}
}

type churnEventLogger struct {
	events.Logger
	finished []map[string]any
}

func (l *churnEventLogger) Log(kind events.EventType, data any) {
	if kind == events.ItemFinished {
		l.finished = append(l.finished, data.(map[string]any))
	}
}

func TestChurnMetadataProducerKeepsItemFinished(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	c := churnTestTracker()
	f.churn = c
	logger := new(churnEventLogger)
	f.evLogger = logger
	fi := protocol.FileInfo{Name: "missing", Version: protocol.Vector{}.Update(1)}
	for range 20 {
		f.tempPullErrors = make(map[string]churnPullError)
		f.shortcutFile(fi, fi, make(chan dbUpdateJob, 1), make(chan string, 1))
		if err := f.tempPullErrors[fi.Name]; !strings.Contains(err.text, "file modified but not rescanned") || err.class != "local_modified" {
			t.Fatalf("current error state changed: %+v", err)
		}
	}
	for k, n := range map[string]uint64{"attempts": 20, "failures": 20, "metadata_emitted": 1, "metadata_suppressed": 19, "stage.shortcut_check": 20} {
		if c.total.values[churnIndex(k)] != n {
			t.Errorf("%s=%d want %d", k, c.total.values[churnIndex(k)], n)
		}
	}
	if len(logger.finished) != 20 {
		t.Fatalf("ItemFinished count=%d want 20", len(logger.finished))
	}
	for _, event := range logger.finished {
		if event["item"] != "missing" || event["error"] == nil || event["action"] != "metadata" {
			t.Fatalf("ItemFinished changed: %+v", event)
		}
	}
}
