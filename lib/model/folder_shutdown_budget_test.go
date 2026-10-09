// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/thejerf/suture/v4"
)

// slowStopService stands in for a folder whose shutdown work (drain,
// btrfs pairing) outlasts suture's default 10 s stop timeout.
type slowStopService struct {
	stopFor time.Duration
	done    atomic.Bool
}

func (s *slowStopService) Serve(ctx context.Context) error {
	<-ctx.Done()
	time.Sleep(s.stopFor)
	s.done.Store(true)
	return nil
}

// The supervisors on the folder path (main -> model -> folder runners) must
// wait for a folder's shutdown work instead of abandoning it after
// svcutil.ServiceTimeout. yarlung 2026-11-24, three restarts: the folder
// runners' supervisor gave up after 10 s mid-pairing (silently, at debug),
// the app closed the database under the still-running pairing, logged
// "Database failed to stop within 10s" and exited.
//
// Teeth: with SpecWithDebugLogger back in newServiceMap (or on main/model
// here) main returns at ~10 s while the folder is still stopping.
func TestFolderPathWaitsForShutdownWork(t *testing.T) {
	t.Parallel()
	if svcutil.ShutdownServiceTimeout < svcutil.ShutdownBudget+svcutil.ShutdownCleanupTimeout {
		t.Fatalf("stop timeout %v does not cover budget %v + cleanup %v", svcutil.ShutdownServiceTimeout, svcutil.ShutdownBudget, svcutil.ShutdownCleanupTimeout)
	}

	main := suture.New("main", svcutil.SpecWithShutdownBudget())
	mdl := suture.New("model", svcutil.SpecWithShutdownBudget())
	runners := newServiceMap[string, suture.Service](events.NoopLogger)
	main.Add(mdl)
	mdl.Add(runners)
	svc := &slowStopService{stopFor: svcutil.ServiceTimeout + time.Second}
	runners.Add("folder", svc)

	ctx, cancel := context.WithCancel(context.Background())
	errC := main.ServeBackground(ctx)
	time.Sleep(100 * time.Millisecond) // let the tree start the service
	cancel()
	<-errC
	if !svc.done.Load() {
		t.Fatalf("supervisor tree returned before the folder finished its %v shutdown work (abandoned at the stop timeout)", svc.stopFor)
	}
}
