// Copyright (C) 2026 The Syncthing Authors.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// This is intentionally a reviewed allowlist, not a heuristic which treats
// any Get/SELECT-looking entry point as safe. A RETURNING query writes too.
// Function-body fingerprints freeze lock placement, SQL and helper calls,
// not line numbers. Any changed/new SQL boundary or locked-helper caller
// requires review and an updated, reasoned entry in testdata/write_inventory.json.
type writerInventoryEntry struct {
	SHA256 string `json:"sha256"`
	Reason string `json:"reason"`
}

var sqlBoundaryNames = map[string]bool{
	"Begin": true, "BeginTx": true, "Beginx": true, "BeginTxx": true, "Commit": true, "Rollback": true,
	"Exec": true, "ExecContext": true, "NamedExec": true, "NamedExecContext": true,
	"Query": true, "QueryContext": true, "QueryRow": true, "QueryRowContext": true,
	"Queryx": true, "QueryxContext": true, "QueryRowx": true, "QueryRowxContext": true,
	"Get": true, "GetContext": true, "Select": true, "SelectContext": true,
	"Prepare": true, "PrepareContext": true, "Preparex": true, "PreparexContext": true,
	"PrepareNamed": true, "PrepareNamedContext": true, "MustExec": true,
	"openBase": true, "openFolderDB": true, "openFolderDBForMigration": true, "folderDBOpener": true,
	"stmt": true, "runScripts": true, "vacuumAndOptimize": true, "expandTemplateVars": true,
	"recalcNeedForFile": true, "periodicCheckpointLocked": true,
	"deviceIdxLocked": true, "folderIdxLocked": true,
	"pullCommitConnectionLocked": true,
	"garbageCollectNamesAndVersions": true, "garbageCollectOldDeletedLocked": true,
	"recalcGlobalForFile": true, "recalcGlobalForFolder": true,
	"insertBlocksLocked": true, "resetNeedTracking": true, "backfillNeedForDevice": true,
	"putNeedDetail": true, "delNeedDetail": true, "bumpNeedTotals": true,
	"clearNeedForName": true, "journalNeedBuildName": true,
	"enqueueRetiredBlocklist": true, "retireReplacedBlocklist": true,
	"retireBlocklistsForDevice": true, "retireBlocklistsForNamedFiles": true,
	"execRowsAffected": true, "setAppliedSchemaVersion": true,
}
var sqlLiteralStart = regexp.MustCompile(`(?i)^\s*(SELECT|INSERT|UPDATE|DELETE|REPLACE|ANALYZE|VACUUM|PRAGMA|CREATE|DROP|ALTER|WITH|BEGIN|ATTACH)\b`)

func inventoriedSQLNode(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		if found {
			return false
		}
		switch n := n.(type) {
		case *ast.SelectorExpr:
			found = sqlBoundaryNames[n.Sel.Name]
		case *ast.Ident:
			found = sqlBoundaryNames[n.Name]
		case *ast.BasicLit:
			if n.Kind == token.STRING {
				s, err := strconv.Unquote(n.Value)
				found = found || err == nil && sqlLiteralStart.MatchString(s)
			}
		}
		return !found
	})
	return found
}

func scanWriterInventory(t *testing.T, dir string) map[string]writerInventoryEntry {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]writerInventoryEntry)
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if !inventoriedSQLNode(decl) {
				continue
			}
			var key string
			switch fn := decl.(type) {
			case *ast.FuncDecl:
				var receiver bytes.Buffer
				if fn.Recv != nil {
					_ = printer.Fprint(&receiver, fset, fn.Recv.List[0].Type)
				}
				key = filepath.Base(path) + ":" + receiver.String() + "." + fn.Name.Name
			case *ast.GenDecl:
				if fn.Tok != token.VAR && fn.Tok != token.CONST {
					continue
				}
				var names []string
				for _, spec := range fn.Specs {
					if v, ok := spec.(*ast.ValueSpec); ok {
						for _, name := range v.Names {
							names = append(names, name.Name)
						}
					}
				}
				key = filepath.Base(path) + ":declaration:" + strings.Join(names, ",")
			default:
				continue
			}
			var body bytes.Buffer
			if err := printer.Fprint(&body, fset, decl); err != nil {
				t.Fatal(err)
			}
			got[key] = writerInventoryEntry{SHA256: fmt.Sprintf("%x", sha256.Sum256(body.Bytes()))}
		}
	}
	return got
}

func TestSQLWriteInventory(t *testing.T) {
	got := scanWriterInventory(t, ".")
	path := filepath.Join("testdata", "write_inventory.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]writerInventoryEntry
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	for key, entry := range got {
		approved, ok := want[key]
		if !ok {
			t.Errorf("new SQL boundary/helper caller needs lock review: %s", key)
			continue
		}
		if entry.SHA256 != approved.SHA256 {
			t.Errorf("SQL/lock body changed, needs lock review: %s (%s)", key, approved.Reason)
		}
		if strings.TrimSpace(approved.Reason) == "" {
			t.Errorf("missing lock reason: %s", key)
		}
	}
	for key := range want {
		if _, ok := got[key]; !ok {
			t.Errorf("stale reviewed entry: %s", key)
		}
	}
	t.Logf("reviewed SQL boundary/helper functions=%d", len(got))
}

// Exercise the lint on a new Query-based writer and an unlocked helper
// caller. This makes accidental scanner weakening visible without touching
// production files or trusting a manually refreshed allowlist.
func TestSQLWriteInventoryDetectsNewPaths(t *testing.T) {
	dir := t.TempDir()
	src := `package sqlite
 func bypass(db DB) { db.QueryContext(nil,"INSERT INTO kv VALUES ('x','y') RETURNING key") }
 func hidden(s folderDB) { s.deviceIdxLocked(device) }
 func read(db DB) { db.Get(nil,"SELECT 1") }
 var initWrite = func(db DB) { db.Exec(query) }
 func alias(db DB) { run := db.Exec; run(query) }
 `
	if err := os.WriteFile(filepath.Join(dir, "new.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	got := scanWriterInventory(t, dir)
	var names []string
	for key := range got {
		names = append(names, key)
	}
	slices.Sort(names)
	if len(got) != 5 {
		t.Fatalf("missed SQL boundary: %v", names)
	}
	t.Logf("detected new SQL boundaries=%v", names)
}
