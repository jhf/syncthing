# Custom Syncthing serve (live home)

Started: 2026-10-08 21:43 CEST (pid 89061, via tmp/run-custom-syncthing.sh, no STTRACE)
Binary:  /Users/jhf/Projects/syncthing/custom-syncthing-darwin-arm64
Version: v2.1.7-dev.32.gb6db71bd-fsevents-replay (run 3 acceptance passed 2026-10-08 23:51)

> Corrected 2026-10-06. This header previously read `Started: 2026-10-05 20:11:39` and
> `Version: v2.1.6-rc.4.dev.10.g8fe69934-scan-and-gc`, which described a serve that had
> already exited. The binary path had not changed. Re-check these three lines with
> `ps -p $(cat tmp/custom-serve.pid) -o lstart=` and the GUI's `/rest/system/version`
> whenever the serve is restarted; a stale header here is harmless, but a stale
> `tmp/CURRENT-BINARY` is not, because the launch script reads that one.
Pointer: /Users/jhf/Projects/syncthing/tmp/CURRENT-BINARY
Launch:  /Users/jhf/Projects/syncthing/tmp/run-custom-syncthing.sh
Log:     /Users/jhf/Projects/syncthing/tmp/custom-serve.log
Watch:   /Users/jhf/Projects/syncthing/tmp/dev-watch.log
GUI:     http://127.0.0.1:4080/
Home:    ~/Library/Application Support/Syncthing

Do not start Syncthing.app while this is running. The menu-bar app
relaunches the stock v2.1.5 binary.

When a release with both fixes is merged:

1. Quit this serve (`kill $(cat tmp/custom-serve.pid)`).
2. Edit `tmp/CURRENT-BINARY` so KIND=release and BINARY= the app/release path.
3. Either relaunch Syncthing.app, or run `tmp/run-custom-syncthing.sh` again.

## 2026-10-08: FSEvents replay build

Current binary is built from `feature/fsevents-scan-replay`, which REQUIRES
the notify fork (`/Users/jhf/Projects/notify`, branch `fsevents-replay`) via
the git-excluded `go.work` replace. Building without that replace fails:
upstream notify has no `SetSinceEventID`. Design notes and the live
acceptance log are in `tmp/fsevents-journal-replay.md`. On restart this build
logs either `Skipping initial scan, resuming filesystem watching from the
journal` (replay armed) or nothing special (normal initial scan); overflow
flags heal via a full scan without killing the watch.

## 2026-10-09: yarlung peer (linux/amd64)

Binary:  /usr/bin/syncthing (dpkg-diverted; distro binary at /usr/bin/syncthing.distrib)
Version: v2.1.7-dev.43.g46c1226b (sweep state-label fix; both Mac and yarlung run dev.43)
Install: ops/custom-syncthing/build-remote.sh HEAD install
         (builds natively on yarlung as devops, installs over /usr/bin/syncthing
         with a timestamped .bak and sidecars, restarts syncthing@jhf)
Feature: btrfs snapshot-diff scan replay - skips the 12-14 min initial scan;
         baseline paired with the index at every clean shutdown, replayed at
         boot. Docs: tmp/btrfs-scan-replay.md.
Requires: /usr/local/sbin/syncthing-btrfs-helper + the socket-activated
         service (syncthing-btrfs.socket / syncthing-btrfs@.service +
         /usr/local/sbin/syncthing-btrfs-socket-adapter, socket
         /run/syncthing-btrfs.sock mode 0660 group jhf). The syncthing@jhf
         unit is STOCK (no override, no sudoers entry); without the socket
         the feature logs the reason once and falls back to full scans.
