// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"encoding/binary"
	"errors"
	"slices"
	"time"
)

// The boot snapshot diff races its own fallback, a full scan. Neither cost
// is predictable up front: on yarlung (2026-11-14..25) diffs took 0.1 s to
// over 600 s with no relation to diff size or baseline age (763 paths took
// 20.8 s, 159,990 took 34.7 s), and full scans took 66 min to over 13 h. A
// fixed 5 min cap gave up on three of thirteen diffs, each time for a full
// scan that cost far more than waiting would have.
//
// So the diff gives up only once it has cost as much as switching would:
// the median of the last measured full scans plus the median measured lag
// of cancelling a send (SIGKILL to exit: 1.8 s cached, 50-60 s when the
// send is waiting on metadata reads, because the kernel only notices the
// signal at its next stream write). This is the break-even rule for an
// unknown remaining duration: never worse than twice the better choice in
// hindsight. Cancelling voids the work (send only reads; the helper deletes
// the snapshot once send has exited), so switching costs nothing beyond
// the lag. Without a measured full scan there is no evidence a scan is
// cheaper, and the diff runs until shutdown.
const (
	btrfsCostSamples = 5 // ring size; decisions use the median of what the ring holds

	fullScanCostKVPrefix   = "model/fullScanCost/"
	btrfsCancelLagKVPrefix = "model/btrfsDiffCancelLag/"
)

// errBtrfsDiffCostlierThanScan is the cause attached to the boot diff's
// context when it gives up.
var errBtrfsDiffCostlierThanScan = errors.New("btrfs snapshot diff has cost as much as a full scan")

// btrfsDiffProgressInterval paces the "still running" line while the boot
// diff runs.
var btrfsDiffProgressInterval = 30 * time.Second

// loadCostSamples returns the persisted samples under key, oldest first.
// Missing or malformed data is an empty ring.
func (f *folder) loadCostSamples(key string) []time.Duration {
	b, err := f.db.GetKV(key + f.folderID)
	if err != nil || len(b)%8 != 0 {
		return nil
	}
	s := make([]time.Duration, 0, len(b)/8)
	for i := 0; i+8 <= len(b); i += 8 {
		s = append(s, time.Duration(binary.BigEndian.Uint64(b[i:])))
	}
	return s
}

// recordCostSample appends d to the ring under key, keeping the newest
// btrfsCostSamples, and returns the ring.
func (f *folder) recordCostSample(key string, d time.Duration) []time.Duration {
	s := append(f.loadCostSamples(key), d)
	if len(s) > btrfsCostSamples {
		s = s[len(s)-btrfsCostSamples:]
	}
	b := make([]byte, 8*len(s))
	for i, v := range s {
		binary.BigEndian.PutUint64(b[8*i:], uint64(v))
	}
	if err := f.db.PutKV(key+f.folderID, b); err != nil {
		f.sl.Warn("Failed to record cost sample", "key", key, "error", err)
	}
	return s
}

// medianDuration is the upper median (the same choice as costRing), 0 for
// an empty ring.
func medianDuration(s []time.Duration) time.Duration {
	if len(s) == 0 {
		return 0
	}
	c := slices.Clone(s)
	slices.Sort(c)
	return c[len(c)/2]
}

// btrfsDiffGiveUpAfter returns how long the boot diff may run before
// switching to a full scan is the cheaper bet, and false when no full scan
// has been measured (wait for the diff).
func (f *folder) btrfsDiffGiveUpAfter() (after, scan, lag time.Duration, ok bool) {
	scans := f.loadCostSamples(fullScanCostKVPrefix)
	if len(scans) == 0 {
		return 0, 0, 0, false
	}
	scan = medianDuration(scans)
	lag = medianDuration(f.loadCostSamples(btrfsCancelLagKVPrefix))
	return scan + lag, scan, lag, true
}
