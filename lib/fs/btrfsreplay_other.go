// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !linux

package fs

import (
	"context"
	"errors"
)

var errBtrfsReplayUnsupported = errors.New("btrfs scan replay not available")

func BtrfsScanReplaySupported() bool { return false }

func BtrfsScanReplaySupportedErr() (bool, error) { return false, errBtrfsReplayUnsupported }

func BtrfsScanBaselineOK(context.Context) (bool, error) { return false, errBtrfsReplayUnsupported }

func BtrfsScanSnapshot(context.Context, string) error { return errBtrfsReplayUnsupported }

func BtrfsScanSetBaseline(context.Context, string) error { return errBtrfsReplayUnsupported }

func BtrfsScanRemoveSnapshot(context.Context, string) error { return errBtrfsReplayUnsupported }

// BtrfsScanSweepStaleSnapshots is a no-op off Linux: no helper, no
// snapshots, no residue.
func BtrfsScanSweepStaleSnapshots(context.Context) ([]string, error) { return nil, nil }

func BtrfsScanNestedSubvols(context.Context, string) ([]string, error) {
	return nil, errBtrfsReplayUnsupported
}

func BtrfsScanDiffBaseline(context.Context, string, string) ([]string, error) {
	return nil, errBtrfsReplayUnsupported
}
