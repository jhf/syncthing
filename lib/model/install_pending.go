// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
	"google.golang.org/protobuf/proto"
)

func (f *sendReceiveFolder) installContentMatches(ctx context.Context, name string, file protocol.FileInfo) (bool, error) {
	stat, err := f.mtimefs.Lstat(name)
	if fs.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !stat.IsRegular() || stat.Size() != file.Size {
		return false, nil
	}
	fd, err := f.mtimefs.Open(name)
	if err != nil {
		return false, err
	}
	defer fd.Close()
	if f.Type == config.FolderTypeReceiveEncrypted {
		// Encrypted block hashes are opaque tokens, not hashes of ciphertext.
		// As with the existing encrypted download and scanner, an untrusted
		// receiver cannot authenticate content. Require the exact existing
		// trailer identity instead, including the encrypted FileInfo and blocks.
		// This rejects predecessor versions without inventing a new format.
		n := file.EncryptionTrailerSize
		if n < 4 || int64(n) > file.Size {
			return false, nil
		}
		trailer := make([]byte, n)
		if _, err := fd.ReadAt(trailer, file.Size-int64(n)); err != nil {
			return false, err
		}
		if binary.BigEndian.Uint32(trailer[n-4:]) != uint32(n-4) {
			return false, nil
		}
		var wire bep.FileInfo
		if err := proto.Unmarshal(trailer[:n-4], &wire); err != nil {
			return false, nil
		}
		file.Size -= int64(n)
		want := file.ToWire(false)
		// Sequence belongs to the sending/receiving index, not content identity.
		// BlocksHash may be populated by the DB, while the trailer has blocks.
		want.Sequence, wire.Sequence = 0, 0
		want.BlocksHash, wire.BlocksHash = nil, nil
		return proto.Equal(want, &wire), nil
	}
	blocks, err := scanner.Blocks(ctx, fd, file.BlockSize(), file.Size, nil)
	if err != nil {
		return false, err
	}
	return bytes.Equal(protocol.BlocksHash(blocks), file.BlocksHash), nil
}

// RecoverPendingInstalls runs before scans and pulls, with no live pull
// producers. The same durable FileInfo supplies the exact sender vector and
// expected blocks after reopening the DB. Never infer a local version from
// predecessor bytes while a durable install is still pending.
func (f *sendReceiveFolder) RecoverPendingInstalls(ctx context.Context) error {
	pending, err := f.db.PendingInstalls(f.ID)
	if err != nil {
		return err
	}
	var complete []protocol.FileInfo
	dirs := make(map[string]struct{})
	for _, file := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := osutil.TraversesSymlink(f.mtimefs, filepath.Dir(file.Name)); err != nil {
			return err
		}
		matches, err := f.installContentMatches(ctx, file.Name, file)
		if err != nil {
			return err
		}
		if !matches {
			temp := fs.TempName(file.Name)
			matches, err = f.installContentMatches(ctx, temp, file)
			if err != nil {
				return err
			}
			if !matches {
				return fmt.Errorf("pending install %q has neither installed content nor matching temp, refusing to scan predecessor", file.Name)
			}
			// A failed postcommit read may have returned an error to the
			// producer and refreshed the download mtime. Reapply the durable
			// entry's metadata before making recovered bytes visible.
			if !f.IgnorePerms && !file.NoPermissions {
				if err := f.mtimefs.Chmod(temp, fs.FileMode(file.Permissions&0o777)); err != nil {
					return err
				}
			}
			if err := f.setPlatformData(&file, temp); err != nil {
				return err
			}
			if err := f.mtimefs.Chtimes(temp, file.ModTime(), file.ModTime()); err != nil {
				return err
			}
			if !f.DisableFsync {
				fd, err := f.mtimefs.Open(temp)
				if err != nil {
					return err
				}
				err = fd.Sync()
				closeErr := fd.Close()
				if err != nil {
					return err
				}
				if closeErr != nil {
					return closeErr
				}
			}
			if _, err := f.mtimefs.Lstat(file.Name); err == nil {
				// The old FileInfo is not part of the new DB entry. We cannot prove
				// whether these are predecessor bytes or a genuine concurrent edit.
				// Preserve them, including complete directories and symlinks.
				conflict := conflictName(file.Name, file.ModifiedBy.String())
				if _, err := f.mtimefs.Lstat(conflict); !fs.IsNotExist(err) {
					return fmt.Errorf("pending install conflict destination is occupied: %s", conflict)
				}
				if err := f.inWritableDir(func(name string) error { return f.mtimefs.Rename(name, conflict) }, file.Name); err != nil {
					return err
				}
				f.ScheduleScan()
			} else if !fs.IsNotExist(err) {
				return err
			}
			// Read-only parents are common (Go's module cache is 0555): make
			// the directory writable for the rename, as every other install
			// and delete path does. Without it a single such file kept the
			// whole folder in error, since recovery precedes every pull.
			if err := f.inWritableDir(func(name string) error { return f.mtimefs.Rename(temp, name) }, file.Name); err != nil {
				return err
			}
			if err := f.mtimefs.Chtimes(file.Name, file.ModTime(), file.ModTime()); err != nil {
				return err
			}
		}
		dirs[filepath.Dir(file.Name)] = struct{}{}
		file.LocalFlags &^= protocol.FlagLocalInstallPending
		complete = append(complete, file)
	}
	if len(complete) == 0 {
		return nil
	}
	if err := f.syncInstallDirs(dirs); err != nil {
		return err
	}
	return f.updateLocals(complete, db.WithSkipBlockIndex())
}
