// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"errors"

	"golang.org/x/sys/windows"
)

func conflictWindowsNameTooLong(err error) bool {
	return errors.Is(err, windows.ERROR_FILENAME_EXCED_RANGE)
}
