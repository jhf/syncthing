// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"fmt"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestConflictWindowsNameTooLong(t *testing.T) {
	for _, err := range []error{
		windows.ERROR_FILENAME_EXCED_RANGE,
		&os.PathError{Op: "lstat", Path: "destination", Err: windows.ERROR_FILENAME_EXCED_RANGE},
		&os.LinkError{Op: "rename", Old: "source", New: "destination", Err: windows.ERROR_FILENAME_EXCED_RANGE},
	} {
		if !conflictNameTooLong(fmt.Errorf("wrapped: %w", err)) {
			t.Fatalf("did not recognize native length error: %v", err)
		}
	}
	if conflictNameTooLong(windows.ERROR_INVALID_NAME) {
		t.Fatal("invalid name is not specifically a name-too-long error")
	}
}
