// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestPullDirConfigurationMatrix(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	for _, tc := range []struct {
		name, a, b   string
		wantA, wantB bool
	}{
		{"default", "", "", true, true}, {"A-only", "", "0", true, false}, {"ungated", "0", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("STPULLRETRYCOOLDOWN", tc.a)
			t.Setenv("STPULLDIRCOOLDOWN", tc.b)
			for _, kind := range []config.FolderType{config.FolderTypeSendReceive, config.FolderTypeReceiveOnly, config.FolderTypeReceiveEncrypted} {
				cfg := f.FolderConfiguration
				cfg.Type = kind
				runner := newSendReceiveFolder(m.model, f.ignores, cfg, nil, f.evLogger, f.ioLimiter).(*sendReceiveFolder)
				if (runner.retry != nil) != (tc.wantA && kind != config.FolderTypeReceiveEncrypted) {
					t.Fatalf("A scope %v", kind)
				}
				if (runner.retry != nil && runner.retry.dir != nil) != (tc.wantB && kind != config.FolderTypeReceiveEncrypted) {
					t.Fatalf("B scope %v", kind)
				}
				if runner.churn == nil {
					t.Fatal("disabled scheduler disabled observability")
				}
				runner.ClearRetryState()
			}
		})
	}
}

func TestPullDirBothGatesFreshPendingWithoutEvents(t *testing.T) {
	for _, shortcut := range []bool{false, true} {
		t.Run(fmt.Sprint(shortcut), func(t *testing.T) {
			m, f, conn, _ := retryPeerFixture(t)
			f.retry.dir = newPullDirState()
			f.retry.dir.budget = new(pullDirBudget)
			logger := new(churnEventLogger)
			f.evLogger = logger
			name := "top/branch/fresh"
			conn.addFile(name, 0o644, protocol.FileInfoTypeFile, []byte("payload"))
			retryPeerIndex(t, m, f, conn)
			if shortcut {
				local := conn.files[0]
				local.Version = protocol.Vector{}
				must(t, m.sdb.Update(f.folderID, protocol.LocalDeviceID, []protocol.FileInfo{local}))
			}
			qualifyDir(f.retry, "top/branch")
			_, _, _, _, err := f.processNeeded(t.Context(), make(chan dbUpdateJob, 10), make(chan copyBlocksState, 10), make(chan string, 10))
			must(t, err)
			if !f.queue.ContainsPathUnder("top/branch") || len(f.tempPullErrors) != 0 {
				t.Fatal("B need-walk gate lost pending or fabricated error")
			}
			s, messages := f.retry.publish()
			if s.dirDeferred != 1 || len(messages) != 0 {
				t.Fatalf("fresh B pending fabricated errors %+v %v", s, messages)
			}
			f.queue.Reset()
			f.queue.Push(name, 7, time.Time{})
			must(t, f.processQueue(t.Context(), nil, nil, make(chan copyBlocksState, 10), make(chan dbUpdateJob, 10), make(chan string, 10)))
			if len(f.queue.held) != 1 || len(f.queue.progress) != 0 || len(logger.finished) != 0 {
				t.Fatal("B pop gate lost membership or emitted ItemFinished")
			}
		})
	}
}

func TestPullDirPeerSiblingProbeAndMaxHold(t *testing.T) {
	m, f, conn, now := retryPeerFixture(t)
	f.retry.dir = newPullDirState()
	f.retry.dir.budget = new(pullDirBudget)
	// Seed actual terminal peer failures before announcing fresh siblings.
	for i := range 3 {
		conn.addFile(fmt.Sprintf("top/branch/f%d", i), 0o644, protocol.FileInfoTypeFile, []byte(fmt.Sprint(i)))
	}
	retryPeerIndex(t, m, f, conn)
	conn.RequestReturns(nil, protocol.ErrNoSuchFile)
	success, err := f.pull(t.Context())
	must(t, err)
	if success || f.retry.dir.buckets["top/branch"] == nil || f.retry.dir.buckets["top/branch"].holdUntil.IsZero() {
		t.Fatal("real terminal failures did not qualify B")
	}
	conn.addFile("top/branch/stable-inside", 0o644, protocol.FileInfoTypeFile, []byte("inside"))
	conn.addFile("top/outside/stable", 0o644, protocol.FileInfoTypeFile, []byte("outside"))
	retryPeerIndex(t, m, f, conn)
	var inside, outside atomic.Int64
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		switch req.Name {
		case "top/branch/stable-inside":
			inside.Add(1)
		case "top/outside/stable":
			outside.Add(1)
		default:
			return nil, protocol.ErrNoSuchFile
		}
		return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
	})
	success, err = f.pull(t.Context())
	must(t, err)
	if success || inside.Load() != 0 || outside.Load() != 1 || f.RetryStatus().dirDeferred != 1 {
		t.Fatalf("wrong collateral delay inside=%d outside=%d status=%+v", inside.Load(), outside.Load(), f.RetryStatus())
	}
	*now = now.Add(10 * time.Second)
	_, err = f.pull(t.Context())
	must(t, err)
	if inside.Load() != 1 {
		t.Fatal("stable inside sibling starved instead of receiving oldest probe")
	}
	// A second fresh stable sibling converges at max hold even if it never
	// receives a probe. Expiry is an independent nonextending escape boundary.
	conn.addFile("top/branch/maxhold", 0o644, protocol.FileInfoTypeFile, []byte("final"))
	retryPeerIndex(t, m, f, conn)
	_, err = f.pull(t.Context())
	must(t, err)
	if e := f.retry.entries["top/branch/maxhold"]; e == nil || e.firstDirDeferred.IsZero() {
		t.Fatal("fresh sibling not pending")
	}
	*now = now.Add(time.Minute)
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
	})
	success, err = f.pull(t.Context())
	must(t, err)
	if !success || len(f.retry.entries) != 0 {
		t.Fatal("settled peer did not converge at nonextending epoch/max-hold boundary")
	}
}

func TestPullDirUniqueSiblingAdmissionMeasurement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			m, f, conn, _ := retryPeerFixture(t)
			if enabled {
				f.retry.dir = newPullDirState()
				f.retry.dir.budget = new(pullDirBudget)
			}
			var requests atomic.Int64
			conn.RequestCalls(func(context.Context, *protocol.Request) ([]byte, error) {
				requests.Add(1)
				return nil, protocol.ErrNoSuchFile
			})
			for i := range 8 {
				conn.addFile(fmt.Sprintf("top/branch/unique%d", i), 0o644, protocol.FileInfoTypeFile, []byte(fmt.Sprint(i)))
				retryPeerIndex(t, m, f, conn)
				_, err := f.pull(t.Context())
				must(t, err)
			}
			want := int64(8)
			if enabled {
				want = 3
			}
			if requests.Load() != want {
				t.Fatalf("requests=%d want %d", requests.Load(), want)
			}
			t.Logf("B=%v unique_siblings=8 requests=%d attempts=%d pending=%d dir_pending=%d", enabled, requests.Load(), f.churn.total.values[churnIndex("attempts")], f.RetryStatus().deferredNeeded, f.RetryStatus().dirDeferred)
		})
	}
}
