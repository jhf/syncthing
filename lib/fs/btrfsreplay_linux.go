// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// btrfsScanReplay skips the initial full scan on btrfs volumes: a root-owned
// helper (snapshot + send --no-data | receive --dump) produces the exact list
// of paths that changed between a baseline snapshot taken while the index was
// complete and a snapshot taken now. This is the Linux counterpart of the
// FSEvents journal replay on darwin (watch_replay_fsevents.go), but stronger:
// the diff is exact, so there is no coverage uncertainty, only operational
// failure modes (helper missing, baseline deleted, nested subvolumes), all of
// which fall back to the regular full scan.
//
// The helper is a fixed path with fixed subcommands so that a sudoers entry
// can grant it without granting anything else:
//
//	syncthing-btrfs-helper refresh|diff|promote|status|subvol|subvols
const btrfsHelperPath = "/usr/local/sbin/syncthing-btrfs-helper"

const btrfsSuperMagic = 0x9123683e

// errBtrfsReplayUnsupported is wrapped by every "cannot replay" condition so
// callers can distinguish it from real execution errors if they care; both
// mean: do a full scan.
var errBtrfsReplayUnsupported = errors.New("btrfs scan replay not available")

var btrfsHelperOnce = sync.OnceValues(checkBtrfsHelper)

func checkBtrfsHelper() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "sudo", "-n", btrfsHelperPath, "subvol").Output()
	if err != nil {
		return false, fmt.Errorf("%w: helper: %w", errBtrfsReplayUnsupported, err)
	}
	subvol, ok := strings.CutPrefix(strings.TrimSpace(string(out)), "subvol=")
	if !ok || !filepath.IsAbs(subvol) {
		return false, fmt.Errorf("%w: helper returned %q", errBtrfsReplayUnsupported, out)
	}
	return true, nil
}

// BtrfsScanReplaySupported reports whether the helper is installed and
// answers. Cheap after the first call.
func BtrfsScanReplaySupported() bool {
	ok, _ := btrfsHelperOnce()
	return ok
}

// btrfsHelper runs the helper with the given subcommand.
func btrfsHelper(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sudo", append([]string{"-n", btrfsHelperPath}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			err = fmt.Errorf("%w: %s", err, bytes.TrimSpace(exitErr.Stderr))
		}
	}
	return out, err
}

// BtrfsScanBaselineOK reports whether the helper has a baseline snapshot to
// diff against.
func BtrfsScanBaselineOK(ctx context.Context) (bool, error) {
	out, err := btrfsHelper(ctx, "status")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) != "baseline=none", nil
}

// BtrfsScanRefresh replaces the baseline snapshot with one of the current
// state. Call only when the folder index is known complete for the folder;
// diffing later over-reports rather than under-reports when in doubt.
func BtrfsScanRefresh(ctx context.Context) error {
	_, err := btrfsHelper(ctx, "refresh")
	return err
}

// BtrfsScanPromote makes the snapshot taken by the last BtrfsScanDiff the new
// baseline. Call only after the diff's paths have been scanned into the
// index; a crash before promotion simply replays a superset next time.
func BtrfsScanPromote(ctx context.Context) error {
	_, err := btrfsHelper(ctx, "promote")
	return err
}

// btrfsFolderPrefix returns the folder's path relative to the helper's
// subvolume ("jhf" for folder /home/jhf on subvol /home), or an error when
// the folder is not on that btrfs subvolume.
func btrfsFolderPrefix(ctx context.Context, folderRoot string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(folderRoot, &st); err != nil {
		return "", err
	}
	if st.Type != btrfsSuperMagic {
		return "", fmt.Errorf("%w: %s is not on btrfs", errBtrfsReplayUnsupported, folderRoot)
	}
	out, err := btrfsHelper(ctx, "subvol")
	if err != nil {
		return "", err
	}
	subvol := strings.TrimPrefix(strings.TrimSpace(string(out)), "subvol=")
	rel, err := filepath.Rel(subvol, folderRoot)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("%w: %s is not under helper subvolume %s", errBtrfsReplayUnsupported, folderRoot, subvol)
	}
	return rel, nil
}

// BtrfsScanNestedSubvols lists btrfs subvolumes nested under folderRoot. A
// snapshot diff does not cross subvolume boundaries, so replay must refuse to
// arm when the folder contains one.
func BtrfsScanNestedSubvols(ctx context.Context, folderRoot string) ([]string, error) {
	prefix, err := btrfsFolderPrefix(ctx, folderRoot)
	if err != nil {
		return nil, err
	}
	out, err := btrfsHelper(ctx, "subvols")
	if err != nil {
		return nil, err
	}
	var nested []string
	for line := range strings.Lines(string(out)) {
		// Helper prints subvolume paths by name, e.g.
		// "@home/.snapshots/syncthing/baseline"; the first component is the
		// subvolume name, the rest is relative to the subvolume root.
		line = strings.TrimSpace(line)
		if i := strings.IndexByte(line, '/'); i >= 0 {
			line = line[i+1:]
		} else {
			continue
		}
		if line == prefix || strings.HasPrefix(line, prefix+"/") {
			nested = append(nested, line)
		}
	}
	return nested, nil
}

// BtrfsScanDiff returns the folder-relative paths that changed between the
// baseline snapshot and now. The snapshot taken for the diff stays around for
// BtrfsScanPromote.
func BtrfsScanDiff(ctx context.Context, folderRoot string) ([]string, error) {
	prefix, err := btrfsFolderPrefix(ctx, folderRoot)
	if err != nil {
		return nil, err
	}
	out, err := btrfsHelper(ctx, "diff")
	if err != nil {
		return nil, err
	}
	return parseBtrfsDump(out, prefix)
}
