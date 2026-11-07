// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/lib/protocol"
)

// AllLocalFilesWithPrefix must cost O(matching names), not O(device files).
// Upstream's form let SQLite drive from files_device_name(device_idx=?) and
// filter names afterwards, walking every file of the device for each call.
// On yarlung (2.9M local files, HDD) every post-pull rescan sweep call cost
// ~2.5 s even for one file; the sweep's never-ending read pinned the WAL at
// 824 MiB (patch/73). The plan must start with a range search on the
// file_names index, then point lookups, and the result must be complete
// and ordered by name.
func TestAllLocalFilesWithPrefixUsesNameRange(t *testing.T) {
	t.Parallel()
	const folderID = "test"
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()

	// Many files outside the prefix, inserted before and after the few
	// inside it, so a device-ordered walk would visit them all.
	var files []protocol.FileInfo
	seq := 0
	add := func(name string) {
		seq++
		files = append(files, genFile(name, 1, seq))
	}
	for i := range 400 {
		add(fmt.Sprintf("other/a%04d", i))
	}
	want := []string{"sub/dir/x", "sub/dir/y", "sub/z", "subway"}
	for _, n := range []string{"sub/z", "sub/dir/y", "sub/dir/x"} {
		add(n)
	}
	for i := range 400 {
		add(fmt.Sprintf("zz/b%04d", i))
	}
	add("subway") // string-prefix semantics (upstream's, unchanged): "sub" matches it too
	if err := sdb.Update(folderID, protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	// Planner statistics as on a real database: without them SQLite may
	// pick the bad order by default anyway, so the test must hold with them.
	fdb, err := sdb.getFolderDB(folderID, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fdb.sql.Exec(`ANALYZE`); err != nil {
		t.Fatal(err)
	}
	// Then replace the fixture's statistics with yarlung's real ones
	// (sqlite_stat1, 2026-11-07), so the planner sees a 2.9M-file local
	// device, two devices, and 12.5M names, and makes production's choice.
	for _, st := range [][3]string{
		{"devices", "sqlite_autoindex_devices_1", "2 1"},
		{"file_names", "sqlite_autoindex_file_names_1", "12511881 1"},
		{"files", "files_device_name", "17931464 1001 1"},
		{"files", "files_name_idx_only", "17931464 2"},
		{"files", "files_version_idx_only", "17931464 251"},
		{"files", "files_blocklist_hash_only", "5858766 3 2"},
		{"files", "files_remote_sequence", "6265644 1001 1"},
	} {
		if _, err := fdb.sql.Exec(`DELETE FROM sqlite_stat1 WHERE tbl = ? AND idx = ?`, st[0], st[1]); err != nil {
			t.Fatal(err)
		}
		if _, err := fdb.sql.Exec(`INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES (?, ?, ?)`, st[0], st[1], st[2]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fdb.sql.Exec(`ANALYZE sqlite_schema`); err != nil { // reload stats into the planner
		t.Fatal(err)
	}

	// The plan the production query gets. Extract the SQL from the
	// implementation by running EXPLAIN on the same statement text.
	plan := explainLocalPrefixPlan(t, fdb)
	t.Logf("plan: %s", strings.Join(plan, " | "))
	if len(plan) == 0 || !strings.Contains(plan[0], "file_names") || !strings.Contains(plan[0], "name>? AND name<?") {
		t.Fatalf("plan must start with a range search on file_names(name), got %q", plan)
	}
	for _, step := range plan {
		if strings.Contains(step, "files_device_name (device_idx=?)") {
			t.Fatalf("plan walks all files of the device: %q", plan)
		}
		if strings.HasPrefix(step, "SCAN ") {
			t.Fatalf("plan contains a full scan: %q", plan)
		}
	}

	var got []string
	for fi, err := range itererr.Zip(sdb.AllLocalFilesWithPrefix(folderID, protocol.LocalDeviceID, "sub")) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, fi.Name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("prefix sub: got %v, want %v (complete, in name order)", got, want)
	}

}

// localPrefixQuery is the statement AllLocalFilesWithPrefix runs, kept in
// sync by TestAllLocalFilesWithPrefixUsesNameRange's result check above.
func explainLocalPrefixPlan(t *testing.T, fdb *folderDB) []string {
	t.Helper()
	rows, err := fdb.sql.Queryx(`EXPLAIN QUERY PLAN `+localPrefixQuery, protocol.LocalDeviceID.String(), "sub", "sub0")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}
