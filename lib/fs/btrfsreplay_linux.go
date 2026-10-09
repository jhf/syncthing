// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// btrfsScanReplay skips the initial full scan on btrfs volumes: a root-owned
// helper (snapshot + send --no-data | receive --dump) produces the exact list
// of paths that changed between a baseline snapshot taken while the index was
// complete and a snapshot taken now. This is the Linux counterpart of the
// FSEvents journal replay on darwin (watch_replay_fsevents.go), but stronger:
// the diff is exact, so there is no coverage uncertainty, only operational
// failure modes (helper missing, baseline deleted, nested subvolumes), all of
// which fall back to the regular full scan.
//
// The privileged helper is reached over a unix socket served by a
// socket-activated systemd service (Accept=true, one helper instance per
// connection), so the syncthing process itself needs no capabilities and the
// stock hardened unit stays intact:
//
//	syncthing-btrfs.socket: ListenStream=/run/syncthing-btrfs.sock,
//	                        SocketMode=0660, SocketGroup=<syncthing user>
//	syncthing-btrfs@.service: root, runs an adapter that reads one request
//	                        line ("<verb> [name...]") and execs
//	                        /usr/local/sbin/syncthing-btrfs-helper, which owns
//	                        all validation (fixed verb list, name charset).
//
// Protocol: the client writes the request line and half-closes; the service
// streams the helper's stdout back and appends a status trailer
// "\x00status=<exitcode>\n" so failures can never pass as silent success.
const btrfsSocketPath = "/run/syncthing-btrfs.sock"

const btrfsSuperMagic = 0x9123683e

// errBtrfsReplayUnsupported is wrapped by every "cannot replay" condition so
// callers can distinguish it from real execution errors if they care; both
// mean: do a full scan.
var errBtrfsReplayUnsupported = errors.New("btrfs scan replay not available")

// btrfsStatusMarker introduces the status trailer appended by the socket
// adapter. NUL cannot appear in btrfs paths or helper output, so the last
// NUL in the response unambiguously starts the trailer.
var btrfsStatusMarker = []byte("\x00status=")

var btrfsHelperOnce = sync.OnceValues(checkBtrfsHelper)

// btrfsSock returns the helper socket path, overridable for tests and
// non-standard installs.
func btrfsSock() string {
	if p := os.Getenv("SYNCTHING_BTRFS_SOCK"); p != "" {
		return p
	}
	return btrfsSocketPath
}

func checkBtrfsHelper() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := btrfsHelper(ctx, "subvol")
	if err != nil {
		return false, err
	}
	subvol, ok := strings.CutPrefix(strings.TrimSpace(string(out)), "subvol=")
	if !ok || !filepath.IsAbs(subvol) {
		return false, fmt.Errorf("%w: helper returned %q", errBtrfsReplayUnsupported, out)
	}
	return true, nil
}

// BtrfsScanReplaySupported reports whether the helper socket answers. Cheap
// after the first call.
func BtrfsScanReplaySupported() bool {
	ok, _ := btrfsHelperOnce()
	return ok
}

// BtrfsScanReplaySupportedErr is BtrfsScanReplaySupported with the reason,
// for one-time diagnostics at folder start (a missing socket/service
// otherwise disables the feature silently).
func BtrfsScanReplaySupportedErr() (bool, error) {
	return btrfsHelperOnce()
}

// btrfsHelperTimeout bounds a helper request whose caller set no deadline
// of its own. It is not a cost budget: callers with one (the shutdown
// pairing, the boot diff) pass it in ctx, and the diff is exempt (see
// BtrfsScanDiffBaseline).
var btrfsHelperTimeout = 5 * time.Minute

// btrfsHelper sends one request to the helper over the socket and returns
// the helper's stdout. A nonzero helper exit status is an error; the status
// trailer guarantees that a failed helper cannot masquerade as success with
// empty output.
func btrfsHelper(ctx context.Context, args ...string) ([]byte, error) {
	return btrfsHelperBounded(ctx, btrfsHelperTimeout, args...)
}

// btrfsHelperBounded is btrfsHelper with an explicit timeout; 0 means only
// ctx bounds the request. Cancelling ctx aborts the read at once (the
// connection deadline alone would only fire at ctx's deadline, not on a
// plain cancel).
func btrfsHelperBounded(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", btrfsSock())
	if err != nil {
		return nil, fmt.Errorf("%w: helper socket %s: %w", errBtrfsReplayUnsupported, btrfsSock(), err)
	}
	defer conn.Close()
	// ctx alone ends the request, deadline or cancel: the read then fails
	// only after ctx is done, so the error carries ctx's cause (the boot
	// diff's give-up reason) rather than a bare i/o timeout.
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if _, err := conn.Write([]byte(strings.Join(args, " ") + "\n")); err != nil {
		return nil, fmt.Errorf("helper request: %w", err)
	}
	if uc, ok := conn.(*net.UnixConn); ok {
		uc.CloseWrite()
	}
	out, err := io.ReadAll(conn)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("helper response: %w", context.Cause(ctx))
		}
		return nil, fmt.Errorf("helper response: %w", err)
	}
	i := bytes.LastIndex(out, btrfsStatusMarker)
	if i < 0 {
		return nil, fmt.Errorf("helper response missing status trailer (%d bytes)", len(out))
	}
	var code int
	if _, err := fmt.Sscanf(string(out[i+len(btrfsStatusMarker):]), "%d", &code); err != nil {
		return nil, fmt.Errorf("helper status trailer malformed: %q", out[i:])
	}
	out = out[:i]
	if code != 0 {
		return nil, fmt.Errorf("helper %q failed with status %d (details in the syncthing-btrfs service journal)", strings.Join(args, " "), code)
	}
	return out, nil
}

// BtrfsScanBaselineOK reports whether the helper has a baseline snapshot to
// diff against.
func BtrfsScanBaselineOK(ctx context.Context) (bool, error) {
	out, err := btrfsHelper(ctx, "status")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) != "baseline=none", nil
}

// BtrfsScanSnapshot takes a read-only snapshot of the helper's subvolume
// under the given name (restricted to [a-z0-9.-] by the helper).
func BtrfsScanSnapshot(ctx context.Context, name string) error {
	_, err := btrfsHelper(ctx, "snap", name)
	return err
}

// BtrfsScanSetBaseline makes the named snapshot the baseline, deleting the
// previous one.
func BtrfsScanSetBaseline(ctx context.Context, name string) error {
	_, err := btrfsHelper(ctx, "setbaseline", name)
	return err
}

// BtrfsScanRemoveSnapshot deletes the named snapshot (never the baseline).
func BtrfsScanRemoveSnapshot(ctx context.Context, name string) error {
	_, err := BtrfsScanRemoveSnapshotCancel(ctx, name)
	return err
}

// BtrfsScanRemoveSnapshotCancel deletes the named snapshot like
// BtrfsScanRemoveSnapshot. A diff still reading it (an abandoned diff
// whose btrfs send runs on in the kernel) is killed first and waited for;
// cancelled reports how long that send took to exit after the kill, or -1
// when no send was running.
func BtrfsScanRemoveSnapshotCancel(ctx context.Context, name string) (cancelled time.Duration, err error) {
	out, err := btrfsHelper(ctx, "rmsnap", name)
	if err != nil {
		return -1, err
	}
	return parseCancelledSendExit(string(out)), nil
}

// BtrfsScanSweepStaleSnapshots deletes pairing-snapshot residue, returning
// the removed names. Conservative by construction: only basenames matching
// pairingSnapBasename under the helper's staging dir are ever sent to the
// helper's rmsnap, and the helper itself refuses "baseline" and anything
// outside [a-z0-9.-]. User subvolumes elsewhere in the tree are never
// candidates.
func BtrfsScanSweepStaleSnapshots(ctx context.Context) ([]string, error) {
	out, err := btrfsHelper(ctx, "subvols")
	if err != nil {
		return nil, err
	}
	var removed []string
	var errs []error
	for _, base := range stalePairingSnapNames(string(out)) {
		if _, err := btrfsHelper(ctx, "rmsnap", base); err != nil {
			errs = append(errs, fmt.Errorf("rmsnap %s: %w", base, err))
			continue
		}
		removed = append(removed, base)
	}
	return removed, errors.Join(errs...)
}

// btrfsFolderPrefix returns the folder's path relative to the helper's
// subvolume ("jhf" for folder /home/jhf on subvol /home), or an error when
// the folder is not on that btrfs subvolume.
func btrfsFolderPrefix(ctx context.Context, folderRoot string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(folderRoot, &st); err != nil {
		return "", err
	}
	// Statfs Type is int32 on 32-bit platforms, where the untyped magic
	// constant overflows; truncate through uint32 for a portable compare.
	if uint32(st.Type) != btrfsSuperMagic {
		return "", fmt.Errorf("%w: %s is not on btrfs", errBtrfsReplayUnsupported, folderRoot)
	}
	out, err := btrfsHelper(ctx, "subvol")
	if err != nil {
		return "", err
	}
	subvol := strings.TrimPrefix(strings.TrimSpace(string(out)), "subvol=")
	rel, err := filepath.Rel(subvol, folderRoot)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("%w: %s is not under helper subvolume %s", errBtrfsReplayUnsupported, folderRoot, subvol)
	}
	return rel, nil
}

// BtrfsScanNestedSubvols lists btrfs subvolumes nested under folderRoot. A
// snapshot diff does not cross subvolume boundaries, so replay must refuse to
// arm when the folder contains one.
func BtrfsScanNestedSubvols(ctx context.Context, folderRoot string) ([]string, error) {
	prefix, err := btrfsFolderPrefix(ctx, folderRoot)
	if err != nil {
		return nil, err
	}
	out, err := btrfsHelper(ctx, "subvols")
	if err != nil {
		return nil, err
	}
	var nested []string
	for line := range strings.Lines(string(out)) {
		// Helper prints subvolume paths by name, e.g.
		// "@home/.snapshots/syncthing/baseline"; the first component is the
		// subvolume name, the rest is relative to the subvolume root.
		line = strings.TrimSpace(line)
		if i := strings.IndexByte(line, '/'); i >= 0 {
			line = line[i+1:]
		} else {
			continue
		}
		if line == prefix || strings.HasPrefix(line, prefix+"/") {
			nested = append(nested, line)
		}
	}
	return nested, nil
}

// BtrfsScanDiffBaseline returns the folder-relative paths that changed
// between the baseline snapshot and the named snapshot. Only ctx bounds the
// diff: measured on yarlung, diffs took 0.1 s to over 600 s, and a fixed
// 5 min cap abandoned three of thirteen in favour of full scans costing
// 66 min to 13 h. The caller decides when to give up (folder_btrfs.go) and
// must then rmsnap the snapshot, which also kills the send.
func BtrfsScanDiffBaseline(ctx context.Context, snapName, folderRoot string) ([]string, error) {
	prefix, err := btrfsFolderPrefix(ctx, folderRoot)
	if err != nil {
		return nil, err
	}
	out, err := btrfsHelperBounded(ctx, 0, "diff-baseline", snapName)
	if err != nil {
		return nil, err
	}
	return parseBtrfsDump(out, prefix)
}
