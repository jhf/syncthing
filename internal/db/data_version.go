// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package db

import "context"

// DataVersionObserver samples one physical connection over its lifetime.
// Values from separate observers must never be compared.
type DataVersionObserver interface {
	DataVersion(context.Context) (uint64, error)
	Close() error
}
