// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package fs

import (
	"bufio"
	"bytes"
	"strings"
)

// parseBtrfsDump parses `btrfs receive --dump` output into the set of
// folder-relative paths touched by the diff. A dump line is a command
// followed by space-separated fields where spaces inside paths are
// backslash-escaped:
//
//	mkfile          ./now/o26769075-3765333-0
//	rename          ./now/o26769075-3765333-0       dest=./now/jhf/a file.txt
//	unlink          ./now/jhf/gone.txt
//
// Tree paths always start with "./"; symlink payloads and hardlink sources do
// not. Every "./" token (positional or key=value) is collected, the snapshot
// component stripped, and paths outside the folder prefix dropped.
// Over-collection is safe (scanning an unchanged path is a no-op), so unknown
// commands default to collecting.
func parseBtrfsDump(dump []byte, prefix string) ([]string, error) {
	seen := make(map[string]struct{})
	var paths []string
	add := func(p string) {
		// Strip "./<snapshot>/" and the folder prefix.
		rest, ok := strings.CutPrefix(p, "./")
		if !ok {
			return
		}
		_, rest, ok = strings.Cut(rest, "/")
		if !ok {
			return
		}
		if prefix != "." {
			rest, ok = strings.CutPrefix(rest, prefix+"/")
			if !ok {
				return
			}
		}
		if rest == "" {
			return
		}
		if _, dup := seen[rest]; !dup {
			seen[rest] = struct{}{}
			paths = append(paths, rest)
		}
	}

	sc := bufio.NewScanner(bytes.NewReader(dump))
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := splitDumpFields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "snapshot", "subvol", "At":
			continue
		}
		for _, f := range fields[1:] {
			add(f)
			if k, v, ok := strings.Cut(f, "="); ok && k != "" {
				add(v)
			}
		}
	}
	return paths, sc.Err()
}

// splitDumpFields splits a dump line on unescaped spaces and removes the
// backslash escapes (the dump escapes at least space and dash).
func splitDumpFields(line string) []string {
	var fields []string
	var cur strings.Builder
	inToken := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '\\' && i+1 < len(line):
			cur.WriteByte(line[i+1])
			i++
			inToken = true
		case c == ' ' || c == '\t':
			if inToken {
				fields = append(fields, cur.String())
				cur.Reset()
				inToken = false
			}
		default:
			cur.WriteByte(c)
			inToken = true
		}
	}
	if inToken {
		fields = append(fields, cur.String())
	}
	return fields
}
