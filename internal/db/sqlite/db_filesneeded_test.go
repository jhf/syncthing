// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

// The needed-files iteration (local pull queue) must use the files_needed
// partial index instead of scanning every row in the folder. Without it the
// query is O(folder size) per pull-queue build — upstream syncthing#10902
// measured 40.5 s median on a large tree. Adopted from the unmerged fix by
// MahdiBM.
//
// Teeth: without the index in 20-files.sql the plan detail reads "SCAN g"
// over the full files table and the test fails.
func TestNeededFilesQueryUsesIndex(t *testing.T) {
	t.Parallel()

	const folderID = "test"

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Populate enough rows that the plan choice is meaningful, and make a
	// few of them genuinely needed (remote has a newer version).
	local := make([]protocol.FileInfo, 0, 300)
	for i := range 300 {
		local = append(local, genFile(fmt.Sprintf("f%04d", i), 1, i+1))
	}
	if err := sdb.Update(folderID, protocol.LocalDeviceID, local); err != nil {
		t.Fatal(err)
	}
	other := protocol.DeviceID{2}
	remote := make([]protocol.FileInfo, 0, 3)
	for i := range 3 {
		f := genFile(fmt.Sprintf("f%04d", i), 1, i+1)
		f.Version = f.Version.Update(2)
		remote = append(remote, f)
	}
	if err := sdb.Update(folderID, other, remote); err != nil {
		t.Fatal(err)
	}

	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}

	// The exact predicate shape of neededGlobalFilesLocal.
	q := fmt.Sprintf(`
		EXPLAIN QUERY PLAN
		SELECT g.sequence FROM files g
		WHERE g.local_flags & %d = 0 AND g.local_flags & %d != 0
	`, protocol.FlagLocalIgnored, protocol.FlagLocalNeeded)

	var plans []string
	rows, err := fdb.sql.Queryx(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plans = append(plans, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(plans, "\n")
	if !strings.Contains(joined, "files_needed") {
		t.Fatalf("needed-files query does not use files_needed index; plan:\n%s", joined)
	}
}
