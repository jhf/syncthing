> **This is a maintained soft fork of [syncthing/syncthing](https://github.com/syncthing/syncthing).**
>
> **North Star:** Syncthing, fast on one real workload — a Mac whose home
> tree (measured: 2.38M files, 394k directories, 262 GiB) mirrors to a
> remote Linux machine on BTRFS over WAN. Boot to ready in seconds instead
> of a ~40-minute full scan; a change on one side visible on the other in
> ~22 s; and the watcher must not melt the machine it watches. Everything
> this fork carries serves that pair of machines; that is the scope we
> maintain and test against. (Windows, other platforms, and general-purpose
> hardening are upstream's domain — maybe one day, not now.)
>
> It tracks upstream and carries the patches listed in [PATCHES.md](PATCHES.md)
> (FSEvents journal replay, poll-mode watching, BTRFS snapshot-diff replay,
> chunked scanning, GC/need-list fixes; background in
> [syncthing#10921](https://github.com/syncthing/syncthing/issues/10921)).
> Maintenance model: [FORK.md](FORK.md). For everything else, refer to the
> upstream project below.

## The approach

We don't hammer on one slow function. For each slowdown we step back and
ask: *for this workload to be fast, what must never happen at all?* Then we
find where it happens and remove the pattern, not the symptom:

- **Turn O(tree) into O(changes).** A 2.38M-file tree must never be
  re-walked to learn what changed. The FSEvents journal and BTRFS snapshots
  already know; replay them (patches 05–07, 12–14).
- **Poll perfect queues instead of holding live streams.** The kernel
  journals every event for every open stream; on a busy tree that burns a
  core per subscriber. Poll the journal every 5 s instead, and size the
  pipeline for storms, not averages (patches 08, 14).
- **No lock held longer than a chunk.** Scans and GC commit per chunk and
  service pulls and watch events between chunks, so a 45-minute operation
  never starves the rest of the system (patches 02, 04, 11).
- **A transient resource error must never kill a long-lived connection.**
  SQLITE_BUSY means "wait", not "reconnect" (patches 11, 15).
- **Precompute the questions the UI and the puller ask every few seconds.**
  Incrementally maintained need tables and indexes instead of
  per-query full-table scans (patches 03, 10, 17).
- **Measure, then claim.** Every line below is from this repo's logs, not
  estimates.

## Measured results (Mac Studio ↔ Linux/BTRFS, 2.38M files / 262 GiB)

- Boot after 5.8 h offline: **~40 min full scan → 6 s** journal replay
  (4.17M events).
- Watcher cost: fseventsd **104–110 % CPU / 9–19 GB RSS → 0.2–3 % /
  14–16 MB**; 11,877+ poll cycles, zero drop flags.
- Change visible on the other side: **10–23 s** end to end, deletes included.
- A database that previously killed the device connection every ~20 s under
  scan load (172 SQLITE_BUSY errors, index frozen): **zero** lock errors
  since the fix.

Full table with per-patch numbers: [PATCHES.md](PATCHES.md#measured-results).

---

[![Syncthing][14]][15]

---

[![MPLv2 License](https://img.shields.io/badge/license-MPLv2-blue.svg?style=flat-square)](https://www.mozilla.org/MPL/2.0/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/88/badge)](https://bestpractices.coreinfrastructure.org/projects/88)
[![Go Report Card](https://goreportcard.com/badge/github.com/syncthing/syncthing)](https://goreportcard.com/report/github.com/syncthing/syncthing)

## Goals

Syncthing is a **continuous file synchronization program**. It synchronizes
files between two or more computers. We strive to fulfill the goals below.
The goals are listed in order of importance, the most important ones first.
This is the summary version of the goal list - for more
commentary, see the full [Goals document][13].

Syncthing should be:

1. **Safe From Data Loss**

   Protecting the user's data is paramount. We take every reasonable
   precaution to avoid corrupting the user's files.

2. **Secure Against Attackers**

   Again, protecting the user's data is paramount. Regardless of our other
   goals, we must never allow the user's data to be susceptible to
   eavesdropping or modification by unauthorized parties.

3. **Easy to Use**

   Syncthing should be approachable, understandable, and inclusive.

4. **Automatic**

   User interaction should be required only when absolutely necessary.

5. **Universally Available**

   Syncthing should run on every common computer. We are mindful that the
   latest technology is not always available to every individual.

6. **For Individuals**

   Syncthing is primarily about empowering the individual user with safe,
   secure, and easy to use file synchronization.

7. **Everything Else**

   There are many things we care about that don't make it on to the list. It
   is fine to optimize for these values, as long as they are not in conflict
   with the stated goals above.

## Getting Started

Take a look at the [getting started guide][2].

There are a few examples for keeping Syncthing running in the background
on your system in [the etc directory][3]. There are also several [GUI
implementations][11] for Windows, Mac, and Linux.

## Docker

To run Syncthing in Docker, see [the Docker README][16].

## Getting in Touch

The first and best point of contact is the [Forum][8].
If you've found something that is clearly a
bug, feel free to report it in the [GitHub issue tracker][10].

If you believe that you’ve found a Syncthing-related security vulnerability,
please report it by emailing security@syncthing.net. Do not report it in the
Forum or issue tracker.

## Building

Building Syncthing from source is easy. After extracting the source bundle from
a release or checking out git, you just need to run `go run build.go` and the
binaries are created in `./bin`. There's [a guide][5] with more details on the
build process.

## Signed Releases

Release binaries are GPG signed with the key available from
https://syncthing.net/security/. There is also a built-in automatic
upgrade mechanism (disabled in some distribution channels) which uses a
compiled in ECDSA signature. macOS and Windows binaries are also
code-signed.

## Documentation

Please see the Syncthing [documentation site][6] [[source]][17].

All code is licensed under the [MPLv2 License][7].

[1]: https://docs.syncthing.net/specs/bep-v1.html
[2]: https://docs.syncthing.net/intro/getting-started.html
[3]: https://github.com/syncthing/syncthing/blob/main/etc
[5]: https://docs.syncthing.net/dev/building.html
[6]: https://docs.syncthing.net/
[7]: https://github.com/syncthing/syncthing/blob/main/LICENSE
[8]: https://forum.syncthing.net/
[10]: https://github.com/syncthing/syncthing/issues
[11]: https://docs.syncthing.net/users/contrib.html#gui-wrappers
[13]: https://github.com/syncthing/syncthing/blob/main/GOALS.md
[14]: assets/logo-text-128.png
[15]: https://syncthing.net/
[16]: https://github.com/syncthing/syncthing/blob/main/README-Docker.md
[17]: https://github.com/syncthing/docs
