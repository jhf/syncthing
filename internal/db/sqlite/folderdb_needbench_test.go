package sqlite

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

// TestNeedTrackingRealDB benchmarks the need tracking build and the
// maintained-table reads against a real folder database snapshot. It only
// runs when SYNCTHING_NEEDBENCH_DB points at a folder.*.db file; the file is
// copied into a temporary database directory so the source is never mutated.
func TestNeedTrackingRealDB(t *testing.T) {
	src := os.Getenv("SYNCTHING_NEEDBENCH_DB")
	if src == "" {
		t.Skip("SYNCTHING_NEEDBENCH_DB not set")
	}

	dir := t.TempDir()
	sdb, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB("bench", true)
	if err != nil {
		t.Fatal(err)
	}
	name := fdb.baseName
	if err := sdb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}

	t0 := time.Now()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	in.Close()
	out.Close()
	t.Logf("staged snapshot in %v", time.Since(t0))

	sdb, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	fdb, err = sdb.getFolderDB("bench", false)
	if err != nil {
		t.Fatal(err)
	}

	// Inventory.
	var devices []struct {
		DeviceID string `db:"device_id"`
		Idx      int64  `db:"idx"`
	}
	if err := fdb.stmt(`SELECT device_id, idx FROM devices`).Select(&devices); err != nil {
		t.Fatal(err)
	}
	for _, d := range devices {
		t.Logf("device idx=%d id=%s", d.Idx, d.DeviceID)
	}

	// Build the need tracking tables.
	t0 = time.Now()
	fdb.buildNeedTracking(fdb.needBuild.gen)
	t.Logf("need tracking build: %v (built=%v)", time.Since(t0), fdb.needBuilt.Load())

	var detailRows int64
	if err := fdb.stmt(`SELECT count(*) FROM need_detail`).Get(&detailRows); err != nil {
		t.Fatal(err)
	}
	t.Logf("need_detail rows: %d", detailRows)

	// Compare maintained vs oracle, with timings, per remote device.
	for _, d := range devices {
		var dev protocol.DeviceID
		if err := dev.UnmarshalText([]byte(d.DeviceID)); err != nil {
			t.Fatal(err)
		}
		if dev == protocol.LocalDeviceID {
			continue
		}

		t0 = time.Now()
		maintained, err := sdb.CountNeed("bench", dev)
		if err != nil {
			t.Fatal(err)
		}
		maintainedDur := time.Since(t0)

		fdb.needBuilt.Store(false)
		t0 = time.Now()
		oracle, err := fdb.needSizeRemote(dev)
		if err != nil {
			t.Fatal(err)
		}
		oracleDur := time.Since(t0)
		fdb.needBuilt.Store(true)

		status := "MATCH"
		if maintained != oracle {
			status = "MISMATCH"
		}
		t.Logf("%s: device=%s maintained=%v (%v) oracle=%v (%v)", status, d.DeviceID, maintained, maintainedDur, oracle, oracleDur)
		if maintained != oracle {
			t.Errorf("counts mismatch for %s", d.DeviceID)
		}
	}

	// Simulate a substantially out-of-sync device: drop the first 100k of the
	// remote device's rows, rebuild the tracking tables, and compare again.
	// This exercises the build's insert paths and the maintained reads at a
	// realistic need size.
	if len(devices) >= 2 {
		remote := devices[len(devices)-1]
		t0 = time.Now()
		res, err := fdb.stmt(`
			DELETE FROM files WHERE device_idx = ? AND name_idx IN (
				SELECT name_idx FROM files WHERE device_idx = ? ORDER BY name_idx LIMIT 100000
			)
		`).Exec(remote.Idx, remote.Idx)
		if err != nil {
			t.Fatal(err)
		}
		dropped, _ := res.RowsAffected()
		t.Logf("dropped %d rows for device idx=%d in %v", dropped, remote.Idx, time.Since(t0))

		if err := func() error {
			tx, err := fdb.sql.BeginTxx(context.Background(), nil)
			if err != nil {
				return err
			}
			defer tx.Rollback() //nolint:errcheck
			if err := fdb.resetNeedTracking(&txPreparedStmts{Tx: tx}); err != nil {
				return err
			}
			return tx.Commit()
		}(); err != nil {
			t.Fatal(err)
		}
		t0 = time.Now()
		fdb.buildNeedTracking(fdb.needBuild.gen)
		t.Logf("need tracking rebuild with non-empty need: %v", time.Since(t0))
		var detailRows2 int64
		if err := fdb.stmt(`SELECT count(*) FROM need_detail`).Get(&detailRows2); err != nil {
			t.Fatal(err)
		}
		t.Logf("need_detail rows: %d", detailRows2)

		var dev protocol.DeviceID
		if err := dev.UnmarshalText([]byte(remote.DeviceID)); err != nil {
			t.Fatal(err)
		}
		t0 = time.Now()
		maintained, err := sdb.CountNeed("bench", dev)
		if err != nil {
			t.Fatal(err)
		}
		maintainedDur := time.Since(t0)

		fdb.needBuilt.Store(false)
		t0 = time.Now()
		oracle, err := fdb.needSizeRemote(dev)
		if err != nil {
			t.Fatal(err)
		}
		oracleDur := time.Since(t0)
		fdb.needBuilt.Store(true)

		t.Logf("device=%s maintained=%v (%v) oracle=%v (%v)", remote.DeviceID, maintained, maintainedDur, oracle, oracleDur)
		if maintained != oracle {
			t.Errorf("counts mismatch after simulated divergence: maintained %v, oracle %v", maintained, oracle)
		}
		if maintained.Files+maintained.Directories+maintained.Symlinks == 0 {
			t.Error("expected non-empty need after simulated divergence")
		}
	}
}
