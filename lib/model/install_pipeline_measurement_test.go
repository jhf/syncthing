// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

type installBenchFS struct {
	fs.Filesystem
	mu                sync.Mutex
	tempSync, dirSync int
	flushed           map[string]time.Time
	latency           []time.Duration
	visible           chan string
}
type installBenchFile struct {
	fs.File
	owner *installBenchFS
	name  string
}

func (f installBenchFile) Sync() error {
	err := f.File.Sync()
	if err == nil {
		f.owner.mu.Lock()
		if fs.IsTemporary(f.name) {
			f.owner.tempSync++
			f.owner.flushed[f.name] = time.Now()
		} else {
			f.owner.dirSync++
		}
		f.owner.mu.Unlock()
	}
	return err
}
func (f *installBenchFS) Open(name string) (fs.File, error) {
	fd, err := f.Filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	return installBenchFile{fd, f, name}, nil
}
func (f *installBenchFS) OpenFile(name string, flags int, mode fs.FileMode) (fs.File, error) {
	fd, err := f.Filesystem.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	return installBenchFile{fd, f, name}, nil
}
func (f *installBenchFS) Create(name string) (fs.File, error) {
	fd, err := f.Filesystem.Create(name)
	if err != nil {
		return nil, err
	}
	return installBenchFile{fd, f, name}, nil
}
func (f *installBenchFS) Rename(from, to string) error {
	err := f.Filesystem.Rename(from, to)
	if err == nil && fs.IsTemporary(from) && !strings.Contains(filepath.Base(to), "sync-conflict") {
		visible := time.Now()
		f.mu.Lock()
		start, ok := f.flushed[from]
		if ok {
			f.latency = append(f.latency, visible.Sub(start))
		}
		f.mu.Unlock()
		f.visible <- to
	}
	return err
}

// This opt-in acceptance measurement is identical on rebased-66 and candidate.
// It runs the real handleFile -> copier -> eight finishers -> dbUpdater path.
// Complete temporary payloads model downloaded/reusable blocks, not a mocked
// finisher or database. All DB and folder paths belong to this test's TempDir.
func TestInstallPipelineMeasurement(t *testing.T) {
	if os.Getenv("STINSTALLBENCH") != "1" {
		t.Skip("isolated opt-in measurement")
	}
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(oldLogger)
	t.Setenv("STDRIPMODE", "0")
	t.Setenv("STMETABATCH", "0")
	defaultSync := os.Getenv("STINSTALLBENCH_SYNC") == "default"
	if defaultSync {
		t.Setenv("STSYNCMODE", "")
	} else {
		t.Setenv("STSYNCMODE", "fsync")
	}
	for _, mode := range []string{"trickle", "burst"} {
		t.Run(mode, func(t *testing.T) {
			n := 2000
			gap := time.Duration(0)
			if mode == "trickle" {
				n = 50
				gap = 50 * time.Millisecond
			}
			m, f := setupSendReceiveFolder(t)
			root := t.TempDir()
			f.FilesystemType = config.FilesystemTypeBasic
			f.Path = filepath.Join(root, "files")
			f.DisableFsync = false
			f.Copiers = 8
			must(t, os.MkdirAll(f.Path, 0o755))
			measured := &installBenchFS{Filesystem: f.Filesystem(), flushed: make(map[string]time.Time), visible: make(chan string, n)}
			f.mtimefs = measured
			dbdir := filepath.Join(root, "db")
			realDB, err := sqlite.Open(dbdir)
			must(t, err)
			defer realDB.Close()
			oldDB := m.sdb
			m.sdb = realDB
			defer func() { m.sdb = oldDB }()
			f.db = realDB
			// Open the folder and finish exclusive schema initialization before the
			// measured interval. Keep it empty, so no warmup file alters grouping.
			must(t, realDB.Update(f.ID, protocol.LocalDeviceID, nil))
			payload := bytes.Repeat([]byte("small sender file\n"), 32)
			blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(payload), protocol.MinBlockSize, int64(len(payload)), nil)
			must(t, err)
			files := make([]protocol.FileInfo, n)
			for i := range n {
				files[i] = protocol.FileInfo{Name: fmt.Sprintf("file-%05d", i), Type: protocol.FileInfoTypeFile, Size: int64(len(payload)), Permissions: 0o644,
					ModifiedS: 1793916000, ModifiedBy: device1.Short(), Version: protocol.Vector{}.Update(device1.Short()), Blocks: append([]protocol.BlockInfo(nil), blocks...), BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize}
				writeFile(t, measured, fs.TempName(files[i].Name), payload)
			}
			// Start all pipeline goroutines before timing. Only the producer cadence
			// differs between trickle and burst. No ticker or artificial delay is
			// inserted into the production DB writer.
			jobs := make(chan dbUpdateJob, 64)
			finish := make(chan *sharedPullerState, 64)
			pulls := make(chan pullBlockState, n)
			copies, copyWG := startCopier(t.Context(), f, pulls, finish)
			var finishWG sync.WaitGroup
			for range 8 {
				finishWG.Go(func() { f.finisherRoutine(t.Context(), finish, jobs, make(chan string, n)) })
			}
			updater := make(chan error, 1)
			go func() { _, err := f.dbUpdaterRoutine(t.Context(), jobs); updater <- err }()
			start := time.Now()
			for _, file := range files {
				must(t, f.handleFile(t.Context(), file, copies))
				if gap > 0 {
					<-measured.visible
					time.Sleep(gap)
				}
			}
			close(copies)
			copyWG.Wait()
			if len(pulls) != 0 {
				t.Fatalf("unexpected download requests: %d", len(pulls))
			}
			close(finish)
			finishWG.Wait()
			close(jobs)
			must(t, <-updater)
			end := time.Now()
			measured.mu.Lock()
			defer measured.mu.Unlock()
			if len(measured.latency) != n && !(defaultSync && len(measured.latency) == 0) {
				t.Fatalf("visible files with observed temp flush=%d want=%d", len(measured.latency), n)
			}
			sort.Slice(measured.latency, func(i, j int) bool { return measured.latency[i] < measured.latency[j] })
			p50, p99 := -time.Millisecond, -time.Millisecond // undefined for baseline postrename syncfs
			if len(measured.latency) == n {
				p50 = measured.latency[(n-1)*50/100]
				p99 = measured.latency[(n-1)*99/100]
			}
			fmt.Printf("INSTALLBENCH mode=%s files=%d finishers=8 gap_ms=%d elapsed_s=%.9f files_s=%.6f temp_sync=%d dir_sync=%d temp_per_file=%.6f dir_per_file=%.6f p50_ms=%.6f p99_ms=%.6f start_ns=%d end_ns=%d dbdir=%s\n", mode, n, gap.Milliseconds(), end.Sub(start).Seconds(), float64(n)/end.Sub(start).Seconds(), measured.tempSync, measured.dirSync, float64(measured.tempSync)/float64(n), float64(measured.dirSync)/float64(n), float64(p50)/float64(time.Millisecond), float64(p99)/float64(time.Millisecond), start.UnixNano(), end.UnixNano(), dbdir)
		})
	}
}
