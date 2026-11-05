// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestInstallAfterCommitPerFileResult(t *testing.T) {
	f, fi, data, firstJobs := intentFixture(t, "result-failed")
	first := <-firstJobs
	second := fi
	second.Name = "result-success"
	temp := fs.TempName(second.Name)
	writeFile(t, f.mtimefs, temp, data)
	jobs := make(chan dbUpdateJob, 2)
	jobs <- first
	pending, err := f.queueInstall(second, protocol.FileInfo{}, false, temp, jobs, make(chan string, 1))
	must(t, err)
	// Simulate a per-file publication failure after both jobs were prepared.
	must(t, f.mtimefs.Remove(first.install.temp))
	close(jobs)
	_, err = f.dbUpdaterRoutine(t.Context(), jobs)
	if err == nil {
		t.Fatal("expected first file's publication failure")
	}
	if err := <-first.install.done; err == nil {
		t.Error("failed file acknowledged success")
	}
	if err := <-pending.done; err != nil {
		t.Errorf("sibling failure poisoned successful publication: %v", err)
	}
	if string(readInstallBytes(t, f, second.Name)) != string(data) {
		t.Fatal("successful sibling was not published")
	}
	for _, tc := range []struct {
		name    string
		pending bool
	}{{fi.Name, true}, {second.Name, false}} {
		indexed, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, tc.name)
		must(t, err)
		if !ok || (indexed.LocalFlags&protocol.FlagLocalInstallPending != 0) != tc.pending {
			t.Errorf("incorrect pending state for %s", tc.name)
		}
	}
}
