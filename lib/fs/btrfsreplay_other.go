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

func BtrfsScanBaselineOK(context.Context) (bool, error) { return false, errBtrfsReplayUnsupported }

func BtrfsScanRefresh(context.Context) error { return errBtrfsReplayUnsupported }

func BtrfsScanPromote(context.Context) error { return errBtrfsReplayUnsupported }

func BtrfsScanNestedSubvols(context.Context, string) ([]string, error) {
	return nil, errBtrfsReplayUnsupported
}

func BtrfsScanDiff(context.Context, string) ([]string, error) { return nil, errBtrfsReplayUnsupported }
