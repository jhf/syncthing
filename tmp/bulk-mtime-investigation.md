# Bulk per-file metadata investigation

Date: 2026-10-24. Repository: jhf/syncthing, `main`, inspected HEAD `77f137f87add59cc26bfa9a9670f61a9e60c8c3e`.

## Verdict

**macOS: worth a bounded prototype and benchmark, not yet a justified production speedup claim.** `getattrlistbulk` really combines directory enumeration with per-entry metadata. Preserve virtual mtimes, lexical callback order, and fallback Lstat for entries whose bulk attributes cannot reproduce the existing result. The measured constants support a seconds-scale *warm-cache proxy* saving, not a claim that the current 6–9 minute scan becomes seconds. A larger gain requires measuring actual Lstat/path-lookup CPU or cold APFS metadata reads.

**Linux/yarlung: test metadata acquisition order first. Do not patch merely to replace Lstat with statx.** There is no general Linux readdir-plus-bulk-stat syscall equivalent. Readdir-order metadata collection, followed by lexical callback delivery, may improve btrfs locality. At 6.7 ms per avoided random read, 10,000 avoided seeks save about 67 seconds and 50,000 save 5.6 minutes. Neither the achievable seek reduction nor the fraction of current walk time attributable to stat is established by the available measurements. io_uring is a later queue-depth experiment, not the smallest patch.

**No directory-mtime pruning is proposed.** Every enumerated entry still gets fresh per-file metadata. In-place writes must remain visible. Exact historical observation timing cannot be identical after batching: preserving Lstat semantics means the same fields, symlink behavior, errors, wrappers and traversal contract, not an atomic snapshot or identical race outcomes.

## 1. Current call path and exact ordering

### Enumeration path

1. Scanner constructs its callback and calls `w.Filesystem.Walk(".", hashFiles)` or one Walk per requested subpath (`lib/scanner/walk.go:304–338`). The scanner consumes supplied `fs.FileInfo`; it does **not** normally call Lstat again for each ordinary file.
2. Case wrapper, when configured, drops its case cache, checks the supplied root, then delegates Walk (`lib/fs/casefs.go:354–361`). It does not validate the case of every name returned by enumeration.
3. `walkFilesystem.Walk` calls `f.Lstat(root)` once (`lib/fs/walkfs.go:149–158`). Its recursive helper canonicalizes a path, invokes the callback, honors SkipDir, and only then enumerates a directory (`lib/fs/walkfs.go:85–113`). A pruned directory therefore incurs its own stat but not stats of children.
4. Directory enumeration is `f.DirNames(path)`, which BasicFilesystem implements as root-path validation, `os.OpenFile`, `fd.Readdirnames(-1)`, deferred close (`lib/fs/basicfs.go:224–240`). This returns names, not FileInfo. The number of underlying directory-read syscalls depends on buffering and directory size, not one syscall per entry.
5. **Names are sorted lexically first**, then each sibling is joined to its parent and individually `f.Lstat(filename)`ed. On success recursion/callback happens immediately, including the complete subtree of a directory, before the next sibling's Lstat. On failure the callback gets the entry error (`lib/fs/walkfs.go:113–139`). This is lexical depth-first traversal, not readdir-order metadata acquisition.
6. The normal inner call chain is walkfs → metrics → mtimeFS → BasicFilesystem. Trace logging may be above or below walkfs. Constructor deliberately puts mtime inside walking and case checking outside it (`lib/fs/filesystem.go:263–295`). Metrics/log wrappers delegate rather than supplying metadata (`lib/fs/metrics.go:133–141`, `lib/fs/logfs.go:64–74`).
7. BasicFilesystem.Lstat roots/validates the name, invokes `underlyingLstat`, wraps `os.FileInfo` (`lib/fs/basicfs.go:180–189`). Normal Darwin/BSD implementation is exactly `os.Lstat` (`lib/fs/basicfs_lstat_regular.go:7–14`). Linux and Android use `os.Lstat` with up to ten EINTR attempts and increasing millisecond sleeps (`lib/fs/basicfs_lstat_broken.go:7–28`). Windows has a separate implementation. Preserve these platform/error behaviors in fallback paths.
8. After successful underlying Lstat, mtimeFS loads the per-name virtual-time mapping, substitutes virtual time **only if its saved on-disk time equals the current on-disk mtime**, then returns (`lib/fs/mtimefs.go:102–116,186–191`). Thus normal scan enumeration still pays this DB lookup per successful entry. Bulk OS metadata must not silently bypass it.

On this local Go 1.26.4 installation, `os/stat_unix.go:42–52` calls `syscall.Lstat`, and Darwin arm64's `syscall/zsyscall_darwin_arm64.go:1938–1953` uses the libSystem `lstat` trampoline. Linux's exact syscall varies by GOARCH: for example `syscall/syscall_linux_arm64.go:50–51` implements Lstat with Fstatat + AT_SYMLINK_NOFOLLOW. “One Lstat per entry” describes the Go/filesystem operation, not a universal syscall name.

### What happens after the stat

The callback checks cancellation, advances `lastWalked`, applies resume/ignore/normalization rules, and handles the item (`lib/scanner/walk.go:387–488`). Metadata has already been acquired even for many entries ultimately ignored. Unchanged regular files compare against the index and return without hashing (`lib/scanner/walk.go:559–599`). This makes per-entry metadata a real full-walk cost, but not the whole scan cost.

Additional calls not removed by bulk enumeration:

- Special ignore-parent reconstruction re-Lstats parents (`lib/scanner/walk.go:508–524`).
- Normalization checks `normPath` with Lstat (`lib/scanner/walk.go:752`).
- Optional ownership/xattr scanning calls `PlatformData`; symlinks need a separate `ReadSymlink` (`lib/scanner/walk.go:879–895`).
- Files actually hashed are opened and `fd.Stat()`ed both before and after hashing to detect concurrent changes (`lib/scanner/blockqueue.go:19–56`). Keep these checks.
- Current-file index reads, hashing, commits, chunk boundaries, deletion detection and virtual-mtime DB reads are separate cost classes.

### Patch/01 is a different pass

The deleted/ignored sweep calls `osutil.IsDeleted(f.mtimefs, fi.Name)` (`lib/model/folder.go:1467–1472,1640`). That helper uses `fs.LstatExists`, then checks parent symlink traversal (`lib/osutil/osutil.go:134–144`, `lib/osutil/traversessymlink.go:50`). `LstatExists` intentionally skips mtimeFS, handles case/metrics wrappers, unwraps other wrappers and ultimately performs an OS Lstat (`lib/fs/mtimefs.go:123–144`, `lib/fs/casefs.go:221–233`).

Patch/01 removed **DB virtual-time lookups**, not per-file OS stats. It did not convert enumeration to bulk metadata. Nor does adding bulk enumeration automatically optimize this later DB-driven, path-at-a-time sweep. Do not reuse early walk metadata as authoritative existence information for a later sweep across concurrent creates/deletes. Patch/01 measurements are recorded in `PATCHES.md:45`, separate from the proposed optimization.

## 2. macOS: real bulk enumeration, with compatibility work

### Returned attributes

Request these explicitly, rather than assuming a fixed stat struct:

| Needed value | Attribute |
|---|---|
| Name | `ATTR_CMN_NAME` |
| Available-field mask | `ATTR_CMN_RETURNED_ATTRS` |
| Per-entry errno | `ATTR_CMN_ERROR` |
| mtime | `ATTR_CMN_MODTIME` |
| Type | `ATTR_CMN_OBJTYPE` |
| Permission bits | `ATTR_CMN_ACCESSMASK` |
| Inode | `ATTR_CMN_FILEID` |
| BSD file flags | `ATTR_CMN_FLAGS` |
| uid/gid, device identity | `ATTR_CMN_OWNERID`, `ATTR_CMN_GRPID`, `ATTR_CMN_DEVID` |
| Non-directory logical data size | `ATTR_FILE_DATALENGTH` |
| Mount detection | `ATTR_DIR_MOUNTSTATUS` |

`ACCESSMASK` does not supply usable file-type bits, so combine permissions with OBJTYPE and translate to Go FileMode, including special modes. FILEID corresponds to st_ino, not the deprecated OBJID/link identifier. DATALENGTH is logical data-fork length; TOTALSIZE includes forks and is not a safe replacement for regular-file st_size. Directory attributes do not supply ordinary file DATALENGTH. For exact directory Size or unsupported special-object fields, retain Lstat fallback. Apple field definitions: [A2], [A3].

### Semantics and pitfalls

Apple explicitly specifies **link metadata, not link-target metadata**. Entries have variable-length packed records and relative name references. Request NAME and RETURNED_ATTRS, check per-entry errors/masks, bounds-check parsing, and ignore invalid placeholder values even with PACK_INVAL_ATTRS. Unsupported attributes must trigger fallback, not zero-valued “unchanged” decisions. Enumeration order is unspecified. Use a dedicated directory FD and read until zero, never mixing readdir on that FD. [A1]

Mountpoints return underlying mountpoint metadata rather than mounted-root metadata. Firmlinks return the firmlink's metadata rather than its target's. Detect these and obtain today's path-based Lstat result before callback/descent. This is particularly important on modern Mac trees. Bulk enumeration alone is not a drop-in replacement for all Lstat results. [A1]

APFS is the primary benchmark target, not a reason to waive these checks. Keep case-sensitive and case-insensitive volumes in differential tests. Feed raw enumerated names into the existing normalization/case policy, do not lowercase/deduplicate entries. The existing outer case wrapper already distinguishes external input paths from enumeration-derived paths (`lib/fs/filesystem.go:290–295`). Missing-field behavior and filesystem-specific unsupported operations require a generic fallback. No metadata snapshot guarantee is introduced.

### Go availability and invocation

Pinned dependency is `golang.org/x/sys v0.48.0` (`go.mod:49`). Inspection of that installed module found **no Getattrlistbulk Go wrapper**, but `unix/zsysnum_darwin_arm64.go:371` and `unix/zsysnum_darwin_amd64.go:371` both define `SYS_GETATTRLISTBULK = 461`. Apple syscall table also assigns 461 ([A4]).

Feasible options:

- Small `darwin && cgo` wrapper around libSystem `getattrlistbulk`, with a `darwin && !cgo` fallback. Simplest maintainable prototype, but cgo transition overhead belongs in the benchmark.
- Darwin-only `syscall.Syscall6` with syscall 461, pointers to attrlist/buffer, buffer size and 64-bit options. Runtime/ABI details and Go pointer lifetime must be handled carefully. Local Go arm64 assembly implements Syscall6 as a scheduler-aware SVC (`$GOROOT/src/syscall/asm_darwin_arm64.s:52–74`). Do not use RawSyscall for potentially blocking filesystem I/O. A number's presence alone is not a tested portable binding.
- A maintained libc trampoline binding would avoid requiring cgo, but is additional integration work. x/sys's own Darwin implementation uses dynamic libSystem imports ([G1]).

No Go binding or bulk syscall was executed in this investigation. The report establishes availability from source, not runtime acceptance of a proposed binding.

## 3. Linux: statx is richer stat, not bulk stat

`statx` dates from Linux 4.11, with glibc wrapper since 2.28. Pinned x/sys supplies `unix.Statx(dirfd, path, flags, mask, *Statx_t)` directly via SYS_STATX (`unix/zsyscall_linux.go:1607–1617`), so Go does not require that glibc wrapper or cgo. Kernel/filesystem/policy support still needs runtime handling. [L1]

Use `AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT` with default `AT_STATX_SYNC_AS_STAT`, request BASIC_STATS, and verify returned field masks. Relevant fields include mode/type, uid/gid, inode, size, nlink, mtime/ctime/atime, blocks, and device identity. Birth time and additional attributes have separate support. Statx attributes are not a portable copy of Darwin BSD flags. Do **not** request DONT_SYNC as a speed trick: approximate cached metadata is not today's stat freshness contract. Fall back to existing Lstat for unavailable syscall/required fields. [L1]

There is **no general Linux syscall that yields readdir names plus complete Lstat-equivalent metadata for all entries**. `getdents64`/readdir provide names, identifiers/type hints, not size+mtime. Statx remains one object per operation. Filesystem-specific bulk interfaces such as XFS bulkstat are not a btrfs directory-walk replacement. io_uring can batch submissions of individual statx requests, but the kernel still performs one `do_statx` per request ([L3]).

### Why filesystem order may help, but is not guaranteed readahead

Btrfs uses separate filename-hash lookup and sequence-index enumeration keys. Its design document describes the readdir index as favorable to bulk access. More precisely, Linux v6.12 `fs/btrfs/inode.c:5554–5607` walks `BTRFS_DIR_INDEX_KEY` by sequence offset, sets `path->reada = READA_FORWARD`, and returns the target inode number separately. Thus readdir order is **not a guarantee of strictly sorted inode numbers or physical block addresses**. Existing directory enumeration already gets this directory-tree forward readahead. [L2], [L4]

Inference: collecting sibling stats immediately in that order can retain temporal/allocation locality that lexical sorting and intervening depth-first descent discard. It may reduce metadata cache misses or improve batching opportunities. However, hard links, renames, CoW history and fragmentation break simple creation-order locality. Reading directory-index leaves does not imply prefetching every target inode leaf. There is no contract that switching to statx activates an inode readahead optimization. [L2], [L4]

Benchmark three distinct changes separately: (a) existing lexical Lstat, (b) readdir-order **Lstat** acquisition + sorted callback delivery, (c) same acquisition order with relative-dirfd statx. Otherwise a locality improvement may be wrongly credited to statx. Test bounded io_uring depth only after (b), with memory, cancellation and contention limits. More parallel work can harm a busy spindle array and warm-cache CPU performance.

## 4. Cost model: what the constants can and cannot establish

### Inputs and scope

`PATCHES.md:26–35` measures random reads on live **database files**, not Lstat latency. Mac's 2 µs is a warm-cache 4K read; its 9.9 GB/s sequential measurement is RAM-resident, **not cold NVMe bandwidth**. Yarlung's 6.7 ms is a cache-dropped 4K seek, and 150 MB/s is sequential throughput. None gives filesystem metadata cache-miss probability, inode packing, directory leaf count, syscall CPU time, or stat-vs-readdir attribution.

Tree inventory is 2.38M files **plus 394k directories** (`PATCHES.md:101–104`). Let F=2,380,000, D=394,000, N≈F+D=2,774,000 entries, excluding the negligible root adjustment. Ignored/pruned entries and chunk-resume re-traversal alter actual counts. Average is only ≈7.04 children per directory, so “thousands of entries per syscall” in large directories is not a whole-tree batching factor.

An accounting model for an unchanged enumeration pass is:

```
Told = N * c_lstat + D * c_dir + 0.0067 * Qold + Bold / 150e6 + V + R
Tnew = Cbulk * c_bulk + N * c_decode + E * c_fallback
       + 0.0067 * Qnew + Bnew / 150e6 + V + R + overhead
```

For yarlung, Q counts seek-equivalent cold reads and B is transfer bytes, with cache/shared reads counted once. c terms are measured CPU/warm operation costs, not storage constants. V is virtual-mtime lookup cost, R other scanner work. For Linux without io_uring, N individual metadata operations remain, so the principal possible gain is Qold−Qnew, not replacing N syscall crossings by Cbulk. For Mac cold, substitute **as-yet-unmeasured** APFS/NVMe costs. These formulas are a sensitivity model, not a fitted performance prediction.

### Why “one seek per file” is wrong as a forecast

| Deliberately pessimistic serial assumption | Derived time on yarlung |
|---|---:|
| One 6.7 ms seek for every regular file | 15,946 s = 4.43 h |
| One for every file and directory Lstat | 18,585.8 s = 5.16 h |
| One additional directory-read seek per directory | 2,639.8 s = 44.0 min |
| Last two rows combined | 21,225.6 s = 5.90 h |

The naive combined model calls stat 87.6% of the time. **That percentage is not an estimate of the real scan.** The fork records yarlung full walks of 14–45 minutes (`PATCHES.md:48,50,117`), not six hours, and those are historical workload measurements, not isolated cold-stat timings. Cache reuse and metadata packing matter enormously. Even one separate cold seek per directory alone reaches the upper end of that range.

A fully sequential 4K-per-file transfer would take ≈65 s at 150 MB/s, or ≈76 s including directories. That is only byte arithmetic under assumed 4K-per-entry density and perfect contiguity, **not an achievable bulk-walk floor**. Btrfs metadata nodes are shared and can be larger than 4K, and a bulk API does not rewrite their disk layout.

**Actual cold stat-vs-directory-read fraction: unknown from supplied constants.** CPU syscall time must also be separated from I/O waited inside that syscall. Use per-operation timing plus kernel/block tracing to attribute shared inode/directory metadata reads. A stat syscall waiting for a seek is not “syscall overhead” that disappears with batching.

### Per-machine cold/warm expectations and sensitivity

| Machine/regime | Defensible estimate now |
|---|---|
| Mac warm | If one uses 2 µs as an explicitly hypothetical per-entry metadata-operation proxy, all N operations represent 5.55 s (4.76 s for files alone). With roughly one data batch plus EOF call per directory, Cbulk≈788k in a small-directory scenario. Charging the same 2 µs per bulk call leaves 1.58 s of call cost, a ≈3.97 s saving before parsing/fallback/cgo. This is a proxy scenario, not an Lstat benchmark. |
| Mac cold | No numeric forecast: cold filesystem random latency and sequential NVMe bandwidth were not measured. Bulk reduces crossings/path lookup, but not necessarily physical metadata reads. Reusing the warm 2 µs or RAM 9.9 GB/s constants for cold APFS would be false precision. |
| Yarlung warm | No measured warm Lstat/syscall cost. Reordering retains N metadata calls and may save nothing or regress CPU/allocation cost. No minute-scale gain follows from the cold 6.7 ms constant. Benchmark this as a regression regime. |
| Yarlung cold | Linux statx alone has no justified gain. For order/batching, every 10k avoided seeks saves ≈67 s, 50k ≈335 s, 100k ≈670 s, less added overhead. If 10–30% of total serial I/O-dominated time were eliminated, a 14-min baseline becomes 9.8–12.6 min and a 45-min baseline 31.5–40.5 min. The 10–30% is an experiment scenario, not a measured probability or promise. |

For Mac warm, if measured per-entry avoidable work turns out to be 10 µs rather than the 2 µs storage proxy, a 70–90% reduction of that component would save 19–25 s across N entries. At 50 µs it would save 97–125 s. These are **unmeasured sensitivity points**, not new machine constants. The documented 6–9 minute scan would lose only ≈4 s in the 2 µs small-directory proxy scenario, roughly 0.7–1.1%. Large speedups need actual profiles demonstrating a larger avoidable component.

Bulk does not remove V. In the separate, deliberately worst-case assumption of a cold independent DB-page read per file, V alone is 4.43 h. Patch/01 removes that class from existence checks, not normal metadata enumeration (`PATCHES.md:45`, `lib/fs/mtimefs.go:102–116`). Do not count patch/01's already-removed cost as a benefit of this new patch.

## 5. Smallest safe integration

There is currently **no bulk operation** in Filesystem, only DirNames, Lstat, Walk (`lib/fs/filesystem.go:31–65`). `FileInfo` requires Name, Mode, Size, ModTime, type tests, Owner, Group **and Sys**, despite its stale preceding comment (`lib/fs/filesystem.go:88–104`). The abstraction does not prevent bulk acquisition: walk.go already receives FileInfo. `walkfs.go:113–139` is the acquisition loop to change.

Proposed internal operation:

```go
// Sketch, not a committed API.
type DirEntryInfo struct {
    Name string
    Info FileInfo
    Err  error // independent per-entry error, not failure of whole directory
}
// Directory-level failure is separate from individual entry errors.
ReadDirInfo(ctx context.Context, path string) ([]DirEntryInfo, error)
```

1. Generic implementation is today's DirNames followed by Lstat, preserving platform behavior. Darwin implementation enumerates bulk records and falls back per entry where needed. Linux experiment acquires metadata in filesystem order.
2. Keep the directory callback/SkipDir **before** acquisition of children. Sort completed entry records by their original names, then invoke callbacks/recurse exactly in current lexical depth-first order. Do not change `WalkOrderLess` or frontier semantics (`lib/scanner/walk.go:272–301,394–419`).
3. Apply mtime translation **once** to each successful bulk result, preferably via a factored helper shared with mtimeFS.Lstat. Preserve case-insensitive mtime-key normalization. Metrics/logging need explicit bulk delegation/accounting. An optional provider interface asserted on `BasicFilesystem` alone will be hidden by wrappers or bypass their policy. Either extend Filesystem and all concrete/wrapper implementations explicitly, or implement a deliberately wrapper-aware optional capability. Never blindly unwrap to a raw bulk provider.
4. Do not fabricate a partial `os.FileInfo` and assume SameFile still works. BasicFilesystem.SameFile currently type-checks `basicFileInfo` and calls os.SameFile (`lib/fs/basicfs.go:319–328`), while Owner/Group expect `*syscall.Stat_t` (`lib/fs/basicfs_fileinfo_unix.go:20–31`). A synthetic os.FileInfo is not automatically accepted by os.SameFile. Provide explicit identity-aware comparison for mixed bulk/ordinary FileInfos, or fall back in identity-sensitive paths. Test the public Sys/size contract too. Keep platform ownership/xattr collection unchanged initially.
5. Use `darwin` files, plus `darwin && cgo` / `darwin && !cgo` if using cgo. Linux statx/io_uring stays `linux`, with runtime fallback and no impact on BSD, Windows, Android, fakefs or errorfs. All wrapper stacks must compile, including both debug-log orderings.
6. A bulk iterator can bound syscall buffers, but **lexical callbacks still require whole-directory name ordering**. Streaming arbitrary readdir order directly to scanner is incorrect. Retaining all sibling metadata adds O(max directory width) memory and can retain it along the recursion stack. Release processed records and consider lazy metadata windows where possible.
7. Chunk cancellation is the subtle cost: reading/stating a huge directory before any child callback can exceed the scan budget, and resumed chunks can repeat discarded metadata work. Existing Walk has no context parameter, so a context-bearing bulk method needs a walk-context extension or another explicit cancellation path. Check between bulk calls/stat batches. Consider restarting/resampling records held across long subtree traversal, normalization or pause boundaries. A persistent metadata cache across chunks would introduce staleness and requires a separate correctness design.

“Identical semantics” cannot mean observing each sibling at precisely the old instant: all bulk candidates sample later siblings earlier than today's interleaved depth-first walk. The current walk is already non-atomic, but increasing stale-result windows can matter. Keep batching local, avoid cross-chunk result caching, retain hash-time restats, and test mutation cases. If the acceptance requirement is literally one fresh Lstat at each old callback point, arbitrary pre-acquisition cannot meet it and the proposed optimization must be rejected under that stricter interpretation.

## 6. Tests and performance gates

**Baseline tests executed:** targeted existing lexical-order, mtime, LstatExists, symlink and resume tests passed. Full baseline command `go test ./lib/fs ./lib/scanner -count=1` also passed, with verbatim output in `tmp/bulk-mtime-tests.log`:

```text
ok  	github.com/syncthing/syncthing/lib/fs	46.688s
ok  	github.com/syncthing/syncthing/lib/scanner	2.051s
```

These validate today's code, not an implementation of the proposed operation.

Patch acceptance should include:

- Differential static-tree traversal, comparing all exposed FileInfo fields and per-entry errors with existing Lstat. Include nanosecond mtime, resource forks, empty/large files, directories, ownership, flags, hardlinks, FIFO/socket, symlink-to-file/dir, broken symlink, permission failures, mountpoints and firmlinks.
- Case-sensitive and insensitive APFS volumes, composed/decomposed Unicode names, normalization collisions, no symlink following, and mixed bulk/ordinary SameFile identity.
- Virtual mtime matching/nonmatching mappings, exactly-once DB application, case-key normalization, and patch/01 wrapper-order tests. No new DB reads in LstatExists.
- Parser fuzzing: truncated records, bad offsets/lengths, absent required attrs, invalid attribute placeholders, per-entry errno, unexpected type. Directory-level unsupported-op fallback must restart from a fresh FD rather than duplicate/omit partial results.
- Concurrent unlink/rename/replacement, symlink swaps, and in-place writes. Bound batching/cancellation and repeat `TestWalkResumeFrom`, lexical-order tests and model chunk frontier/no-result/error tests (`lib/scanner/walk_test.go:1019`, `lib/fs/walkfs_test.go:39–75`, `lib/model/folder_chunkedscan_test.go:50–386`).
- Build/test Darwin arm64/amd64, cgo on/off where relevant, Linux amd64/arm64, and existing non-Darwin platforms. Cross-compilation alone does not validate syscall ABI.
- Warm/cold acquisition-only A/B plus end-to-end unchanged full scan on each real machine, same paths/ignore/index state, randomized trial order. Record Lstat/statx/bulk counts, fallback counts, virtual-mtime calls, CPU, wall time, allocations, bytes, random I/Os and cancellation latency. Distinguish filesystem metadata cold from database cold. Do not flush production caches just for this report.
- For Linux, compare readdir-order Lstat before adding statx or io_uring. On btrfs test the real aged tree, not only freshly created contiguous files. For Mac, test this tree's many small directories as well as one huge directory.

**Decision gate:** implement an opt-in Darwin prototype first if pursuing the work. Advance Linux acquisition reordering only after measuring avoided seeks, with no warm-cache/chunk-latency regression. Production promotion requires a repeatable end-to-end benefit that outweighs wrapper, identity and race complexity. Present evidence supports these experiments, not an already-quantified multi-fold speedup.

## Primary API/source references

External references were checked on 2026-10-24. Paths/line ranges identify the inspected primary material. URLs are provided as code for reproducibility.

- [A1] Apple XNU `bsd/man/man2/getattrlistbulk.2:32–202`: request/packing, symlinks, errors, mountpoints, firmlinks, iteration.
  `https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/man/man2/getattrlistbulk.2`
- [A2] Apple XNU `bsd/man/man2/getattrlist.2:607–632,703–712,1122–1180`: mode, flags, inode and data-fork size.
  `https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/man/man2/getattrlist.2`
- [A3] Apple XNU `bsd/sys/attr.h:43–98` and attribute definitions: options, attrlist and relative references.
  `https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/sys/attr.h`
- [A4] Apple XNU `bsd/kern/syscalls.master:699`: syscall 461.
  `https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/kern/syscalls.master`
- [G1] Go x/sys `unix/syscall_darwin.go:19–35`: libc binding pattern. Pinned v0.48.0 API availability was checked locally, not inferred from main.
  `https://raw.githubusercontent.com/golang/sys/master/unix/syscall_darwin.go`
- [L1] Linux man-pages source `man/man2/statx.2`, flags/masks and HISTORY (Linux 4.11, glibc 2.28).
  `https://kernel.googlesource.com/pub/scm/docs/man-pages/man-pages/+/master/man/man2/statx.2`
- [L2] Btrfs design, Directories section: filename-hash and sequence indexes.
  `https://btrfs.readthedocs.io/en/latest/dev/dev-btrfs-design.html`
- [L3] Linux `io_uring/statx.c:21–56`: individual statx requests and do_statx execution.
  `https://raw.githubusercontent.com/torvalds/linux/master/io_uring/statx.c`
- [L4] Linux v6.12 `fs/btrfs/inode.c:5535–5633`: actual readdir index ordering and READA_FORWARD.
  `https://raw.githubusercontent.com/torvalds/linux/v6.12/fs/btrfs/inode.c`

## Limitations

No production full-tree scan, cold-cache operation, yarlung remote command or prototype benchmark was run. No implementation was changed. The report distinguishes repository observations, API contracts, arithmetic derived from supplied measurements, and unmeasured scenarios. Missing measurements prevent an honest single-number cold/warm speedup forecast or real stat/directory percentage.
