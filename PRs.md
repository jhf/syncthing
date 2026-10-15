# Upstream PRs that matter to this fork

The PR-side companion to ISSUES.md: what upstream pull requests exist, what they do,
their state, and whether we want them. Kept so that when an issue points at code, we know
where the code lives and why we did or didn't take it.

Status as of 2026-10-15 (scout inventory: tmp/upstream-inventory-prs.md; prior sweep:
tmp/upstream-fix-sweep.md). Fork base: v2.1.6-64-gb8a531f46 (upstream main of ~2026-10-05;
upstream has since added 7 commits, listed at the bottom).

**How to use:** "us" says what we did: adopted (patch number), parked (why), declined,
watching. Full relevance detail lives in ISSUES.md under the linked issue.

## Open PRs we care about

| PR | What | Author | Size | State | Us |
|---|---|---|---|---|---|
| #10906 | fix: optimise and fix a few db indexes — calmh's own index pass, touches `schema/folder/20-files.sql` (the SAME file as our patch/17) | calmh | +44/-15 | OPEN, draft, CONFLICTING | **watching closely** — likely subsumes patch/17; review at next rebase and drop ours if upstream's covers it |
| #10556 | WAL-friendly vacuum: skip vacuum unless freelist >1k pages or 10%; incremental 1k-page chunks (fixes #10554) | bt90 | +63/-6 | OPEN, draft, MERGEABLE | **parked** — sound, but overlaps our rewritten GC (patch/02); revisit if WAL growth (#10559) ever bites us |
| #10814 | Fix stuck dialing loop after machine sleep/wake (QUIC) | pixelspark | +70/-3 | OPEN, MERGEABLE (one failing check) | **parked** — real bug, not our symptom; adopt if our Mac ever goes silent after sleep |
| #10765 | maxDiskIOKbps: token-bucket disk I/O limit for scanning and pulling (fixes #10764), aimed at mechanical HDDs | ViPiC-Ru | +260/-2 | OPEN, MERGEABLE, CHANGES_REQUESTED | **watching** — yarlung is HDD-backed; a candidate if scan I/O ever contends with the receiver's other work |
| #10678 | Rate-based block pull scheduler: pick device by shortest queue in seconds | calmh | +255/-49 | OPEN, draft | **watching** — pull-path performance; single-peer today, so low urgency |
| #10183 | Use the DB instead of in-memory lists for pull queues (forum RFC linked) | imsodin | +533/-388 | OPEN, draft, CONFLICTING | **watching** — the structural answer to #4976 (pull-queue RAM at scale) |
| #10669 | Load remote ignore patterns at folder setup via a RemoteFilesystem shim (fixes #9352) | calmh | +550/-2 | OPEN, draft, CONFLICTING | **watching** — ignore handling; not a pain for us today |
| #10838 | protovalidate validation of incoming protocol messages | calmh | +702/-876 | OPEN, CONFLICTING | **aware** — hardening, no action |

## Closed-unmerged PRs with code worth remembering

These were rejected or abandoned upstream but contain real work. If the corresponding
symptom appears on our machines, the branch is the starting point.

| PR | What | Why it died | Us |
|---|---|---|---|
| #10516 | Streaming chunked scan eliminating per-file DB queries (keyset pagination, 10K chunks, zero-syscall delete detection) | Maintainer objections: memory for ~1M-file p99 folders; long read txn concern | **historical analogue** — our patch/04 shipped the same idea differently; read this if chunking ever needs a redesign |
| #10816 | promoteConnections: release model lock before DB read (+ regression test) | Issue #10798 closed, PR left behind | **watching** — if receiver stalls with goroutine/fd explosion, verify whether upstream fixed it another way |
| #10221 | Cap scan-progress file-list buffering (millions of files → GC thrash) | Closed unmerged | **memory-pressure note** for scan progress |
| #10774 | watchaggregator CPU thrash fix on continuous overflow | Rejected by imsodin ("never called if we already have an event at '.'"); note our patch/08 raised the buffer to 65536, changing this regime | **declined by us too** |

## Merged upstream, already in our base

Verified present in v2.1.6-64-gb8a531f46; listed so we don't re-adopt or misattribute.

| PR | What | Merged |
|---|---|---|
| #10842 | DB connections, puller concurrency, deadlock fix (fixes #10841) | 2026-08-03 |
| #10908 | Remove unnecessary delay in index transmission — index latency improvement, verified in base by merge-base | 2026-10-04 |
| #10849 | fix(api): correctly return metrics/support bundle — diagnostic tooling for #10869, NOT a scan fix | 2026-08-05 |
| #10833 | Don't check existing file twice in rename detection — PARTIAL fix for #10869 | 2026-07-27 |
| #10791 | Avoid warning on vanished-during-scan files (supersedes #10790's casefs analysis) | 2026-07-16 |
| #10379 / #10374 | Skip GC when provably unnecessary | 2025-09-08 |
| #10027 / #10036 | Shorter read transactions + buffered pulled files (smaller WAL) | 2026 |
| #10653 | Rename tracking — INTRODUCED the #10869 regression | 2026-04-23 |

## The 7 upstream commits we are behind (arrive at next rebase)

Per FORK.md procedure. Flagged here because two interact with our patches:

| Commit | What | Interaction |
|---|---|---|
| 5fb192176 | fix(sqlite): no unbounded prepared statements (#10915, refs #10913) | Partial fix for the OOM-mapping issue; review against our db patches at rebase |
| 84329b277 | fix(sqlite): avoid early GC of deleted recv-enc files (#10916) | Touches db_service.go near our GC rewrite — review the rebase conflict carefully |
| 529ced6c6 | fix: handle missing max-sequence file entries (#10912) | — |
| a0fd63763 | fix(discover): skip identical global announces; obey retry-after (#10918) | — |
| aff2db883 | build(deps) bump (#10917) | — |
| 686811986 | build: skip defunct loki sending | — |
| d70d21032 | chore: docs, translations, contributors | — |

## Maintenance

Refresh: re-run the inventory scout (session history: "upstream inventory scout"), which
rewrites tmp/upstream-inventory-prs.md with fresh states; update the tables above. Adopted
code is tracked in PATCHES.md; issue context in ISSUES.md.
