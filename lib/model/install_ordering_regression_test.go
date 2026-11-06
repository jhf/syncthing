// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"errors"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"testing"
)

type installOrderingRegressionDB struct {
	db.DB
	check func()
	fail  error
}

func (d *installOrderingRegressionDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	if d.check != nil {
		d.check()
	}
	if d.fail != nil {
		return d.fail
	}
	return d.DB.Update(folder, device, files, opts...)
}
func TestInstallOrderingRegressionBarrier(t *testing.T) {
	f, fi, _, jobs := intentFixture(t, "ordering-barrier")
	checks := 0
	f.db = &installOrderingRegressionDB{DB: f.db, check: func() {
		checks++
		if checks == 1 {
			if _, err := f.mtimefs.Lstat(fi.Name); !fs.IsNotExist(err) {
				t.Error("rename happened before durable commit returned")
			}
		}
	}}
	close(jobs)
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	if err != nil {
		t.Fatal(err)
	}
	if checks == 0 {
		t.Fatal("no commit boundary checked")
	}
}
func TestInstallOrderingRegressionNonLockError(t *testing.T) {
	f, fi, _, jobs := intentFixture(t, "ordering-nonlock")
	terminal := errors.New("structural database failure")
	f.db = &installOrderingRegressionDB{DB: f.db, fail: terminal}
	close(jobs)
	_, err := f.dbUpdaterRoutine(t.Context(), jobs)
	if !errors.Is(err, terminal) {
		t.Fatal(err)
	}
	if _, err := f.mtimefs.Lstat(fi.Name); !fs.IsNotExist(err) {
		t.Fatal("failed commit published file")
	}
	if _, err := f.mtimefs.Lstat(fs.TempName(fi.Name)); err != nil {
		t.Fatal("failed commit lost reusable temp")
	}
}
