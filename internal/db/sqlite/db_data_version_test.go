// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDataVersionObserverDedicatedConnection(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, err := s.getFolderDB("observer", true)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := s.OpenDataVersionObserver(t.Context(), "observer")
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	before, err := observer.DataVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The reserved observer cannot be borrowed by this write, so this commit
	// really is on a different physical connection to the same folder file.
	if err := f.PutKV("observer-heartbeat", []byte("commit")); err != nil {
		t.Fatal(err)
	}
	after, err := observer.DataVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("dedicated observer missed another connection's commit")
	}
	stable, err := observer.DataVersion(t.Context())
	if err != nil || stable != after {
		t.Fatalf("unchanged read: value=%d err=%v", stable, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := observer.DataVersion(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("read error not surfaced: %v", err)
	}
	if _, err := observer.DataVersion(t.Context()); err != nil {
		t.Fatalf("failed read leaked transaction/connection: %v", err)
	}
}

func TestDataVersionObserverBoundedAcquisition(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, err := s.getFolderDB("observer", true)
	if err != nil {
		t.Fatal(err)
	}
	f.sql.SetMaxOpenConns(1)
	observer, err := s.OpenDataVersionObserver(t.Context(), "observer")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	other, err := s.OpenDataVersionObserver(ctx, "observer")
	if other != nil {
		other.Close()
		t.Fatal("reserved physical connection was reused")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exhausted acquisition ignored context: %v", err)
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.OpenDataVersionObserver(t.Context(), "observer")
	if err != nil {
		t.Fatalf("observer close leaked pool slot: %v", err)
	}
	replacement.Close()
}

func TestDataVersionObserverBusyRegistry(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.getFolderDB("observer", true); err != nil {
		t.Fatal(err)
	}
	s.folderDBsMut.Lock()
	done := make(chan error, 1)
	go func() {
		o, err := s.OpenDataVersionObserver(t.Context(), "observer")
		if o != nil {
			o.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		s.folderDBsMut.Unlock()
		if err == nil {
			t.Fatal("busy registry unexpectedly acquired an observer")
		}
	case <-time.After(time.Second):
		s.folderDBsMut.Unlock()
		<-done
		t.Fatal("observer blocked behind folder opener instead of reporting unknown")
	}
}
