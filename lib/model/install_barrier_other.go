// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !linux && !darwin

package model

import "errors"

// No group barrier on other platforms: every item is fsynced on its own.
const installBarrierSupported = false

const installBarrierName = "none"

var installBarrierFn = func(string, []string) error {
	return errors.New("no install group barrier on this platform")
}
