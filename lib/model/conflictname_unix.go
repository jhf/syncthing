// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package model

func conflictWindowsNameTooLong(error) bool { return false }
