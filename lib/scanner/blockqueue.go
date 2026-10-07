// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"context"
	"errors"
	"sync"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// HashFile hashes the files and returns a list of blocks representing the file.
func HashFile(ctx context.Context, folderID string, fs fs.Filesystem, path string, blockSize int, counter Counter) ([]protocol.BlockInfo, error) {
	fd, err := fs.Open(path)
	if err != nil {
		l.Debugln("open:", err)
		return nil, err
	}
	defer fd.Close()

	// Get the size and modtime of the file before we start hashing it.

	fi, err := fd.Stat()
	if err != nil {
		l.Debugln("stat before:", err)
		return nil, err
	}
	size := fi.Size()
	modTime := fi.ModTime()

	// Hash the file. This may take a while for large files.

	blocks, err := Blocks(ctx, fd, blockSize, size, counter)
	if err != nil {
		l.Debugln("blocks:", err)
		return nil, err
	}

	metricHashedBytes.WithLabelValues(folderID).Add(float64(size))

	// Recheck the size and modtime again. If they differ, the file changed
	// while we were reading it and our hash results are invalid.

	fi, err = fd.Stat()
	if err != nil {
		l.Debugln("stat after:", err)
		return nil, err
	}
	if size != fi.Size() || !modTime.Equal(fi.ModTime()) {
		return nil, errors.New("file changed during hashing")
	}

	return blocks, nil
}

// The parallel hasher reads FileInfo structures from the inbox, hashes the
// file to populate the Blocks element and sends it to the outbox. A number of
// workers are used in parallel. The outbox will become closed when the inbox
// is closed and all items handled.
type parallelHasher struct {
	folderID string
	fs       fs.Filesystem
	outbox   chan<- ScanResult
	inbox    <-chan protocol.FileInfo
	counter  Counter
	done     chan<- struct{}
	wg       sync.WaitGroup
}

func newParallelHasher(ctx context.Context, folderID string, fs fs.Filesystem, workers int, outbox chan<- ScanResult, inbox <-chan protocol.FileInfo, counter Counter, done chan<- struct{}) {
	ph := &parallelHasher{
		folderID: folderID,
		fs:       fs,
		outbox:   outbox,
		inbox:    inbox,
		counter:  counter,
		done:     done,
	}

	ph.wg.Add(workers)
	for range workers {
		go ph.hashFiles(ctx)
	}

	go ph.closeWhenDone(ctx)
}

func (ph *parallelHasher) hashFiles(ctx context.Context) {
	defer ph.wg.Done()

	for {
		// Do not start on another item once the scan is cancelled. This is checked
		// explicitly, not just in the select below, so that a pending inbox send does
		// not win the race against cancellation.
		if ctx.Err() != nil {
			return
		}

		select {
		case f, ok := <-ph.inbox:
			if !ok {
				return
			}

			l.Debugln("started hashing:", f)

			if f.IsDirectory() || f.IsDeleted() {
				panic("Bug. Asked to hash a directory or a deleted file.")
			}

			blocks, err := HashFile(ctx, ph.folderID, ph.fs, f.Name, f.BlockSize(), ph.counter)
			if err != nil {
				// The hash may have been cut short by cancellation. Either way the
				// file must be reported, not dropped: the walker counted the handover,
				// and a caller resuming the scan from a marker relies on every handed
				// over file being either applied or reported.
				handleError(ctx, "hashing", f.Name, err, ph.outbox)
				continue
			}

			f.Blocks = blocks
			f.BlocksHash = protocol.BlocksHash(blocks)

			// The size we saw when initially deciding to hash the file
			// might not have been the size it actually had when we hashed
			// it. Update the size from the block list.

			f.Size = 0
			for _, b := range blocks {
				f.Size += int64(b.Size)
			}

			l.Debugln("completed hashing:", f)
			select {
			case ph.outbox <- ScanResult{File: f}:
			case <-ctx.Done():
				// The scan was cancelled with this file fully hashed but
				// undelivered. Report it as aborted instead of dropping it, so
				// that a caller resuming the scan knows it was not applied.
				handleError(ctx, "hashing", f.Name, ctx.Err(), ph.outbox)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (ph *parallelHasher) closeWhenDone(ctx context.Context) {
	ph.wg.Wait()
	// In case the hasher aborted on context, wait for filesystem
	// walking/progress routine to finish. Whatever the walk still hands over
	// is not going to be hashed; report each such file as aborted instead of
	// throwing it away, so that a caller resuming the scan knows it was not
	// handled.
	err := ctx.Err()
	if err == nil {
		err = context.Canceled
	}
	for f := range ph.inbox {
		handleError(ctx, "hashing", f.Name, err, ph.outbox)
	}
	if ph.done != nil {
		close(ph.done)
	}
	close(ph.outbox)
}
