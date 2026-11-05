# patch/60: bounded, self-describing conflict filenames

## Evidence and scope

PATCH59.md reproduced a 248-byte ASCII `.md` filename becoming a 286-byte
conflict filename with a seven-character device ID. On the real host the
component limit was verified as 255 bytes. The destination Lstat in
`caseFilesystem.checkCase` failed before Rename, original bytes stayed intact,
and the remote deletion remained needed. This reproduces conflict preservation
once required, not the origin of the production vector divergence.

The amendment is based on `e4a7a8b57` and changes only this worktree. No push,
deployment, running-daemon interaction, or production performance claim.

## Upstream comparison and design

Upstream `b34734376` appends timestamp/device ID and the extension without any
byte budgeting. Its `existingConflicts` globs the original stem and extension,
and `moveForConflict` attempts one rename and prunes lexically. The initial
patch/60 bounded the stem to 255 bytes, but erased distinguishing trailing
bytes. Two 248-byte originals sharing their first 240 bytes and extension
then shared a conflict prefix, allowing one file's MaxConflicts pruning to
delete the other's copies.

`lib/fs/tempname.go` is an upstream precedent for hashing long names: it uses
SHA-256 of the full original base name once its conservative threshold is
exceeded. Here a conflict name must remain recognizable to people and to
pruning, and must retain the complete `.sync-conflict-` suffix.

There is deliberately no DB mapping. Conflict copies synchronize as ordinary
files, potentially reaching a device that has no originating local database
state. Identity must therefore be self-describing in the filename and
recomputable from the original base name. Hash input is the exact original
base name, including extension, but not parent directories. Parent directories
are unchanged, and discovery stays in the same directory.

## Final format and byte budgets

Let `S = .sync-conflict-YYYYMMDD-HHMMSS-<shortID>` and `E` be the original
extension. `S` is 38 bytes for the normal seven-character ID and 31 bytes for
the empty zero ID.

* If the unmodified base plus `S` fits 255 bytes, retain the upstream spelling
  byte-for-byte. No hash is added.
* Otherwise use `<UTF-8-safe prefix>~<8 lowercase hex SHA-256(base)>S<E>`.
  Reserve 9 bytes for `~` plus hash and the entire `S` before budgeting `E`
  and then the stem. With seven-character ID and `.md`, the prefix budget is
  `255 - 9 - 38 - 3 = 205` bytes. A UTF-8 cut can leave unused bytes.
* Keep the complete extension unless even an empty prefix plus identity,
  suffix and extension cannot fit. Then leave the prefix empty and cut only
  the end of the extension, at a UTF-8 boundary. The existing 230-byte
  extension regression remains covered: 208 extension bytes fit with the
  normal ID, 9-byte identity and 38-byte suffix.

All cuts preserve complete UTF-8 characters and the full timestamp/device
suffix. No parent component is shortened.

## Dynamic fallback and error classification

Attempt the normal spelling first. On a typed, possibly wrapped
`syscall.ENAMETOOLONG`, retry exactly once as
`~<16 lowercase hex SHA-256(base)>S<E>`, with a 143-byte component budget.
Reuse the first attempt's timestamp. There is no prefix. The identity costs
17 bytes, so a normal ID leaves `143 - 17 - 38 = 88` extension bytes.
For `.md`, this spelling is only `17 + 38 + 3 = 58` bytes. Extension cuts are
UTF-8-safe and discovery also recognizes cut extensions.

143 is a conservative retry policy, not a measured universal filesystem
limit: EncFS's own manual documents 143 bytes for block32 encoding over a
255-byte backing limit (Filename Encoding section, EncFS 1.9.5 manual).
It is also below upstream tempname.go's 145-byte original-name threshold.
Reference: `https://man.freebsd.org/cgi/man.cgi?manpath=FreeBSD+13.2-RELEASE+and+Ports&query=encfs&sektion=1`.

Classification uses `errors.Is`, not operation names or message text:
`caseFilesystem.Rename` can return a destination Lstat PathError before the
underlying Rename, while os.Rename can return a LinkError. Darwin supplies
POSIX ENAMETOOLONG, verified by the real-host 255/256-byte probe in the
patch/59 deletion regression. On Windows also recognize native
`ERROR_FILENAME_EXCED_RANGE` (206), independently of Go's synthetic
ENAMETOOLONG. Do not classify ERROR_INVALID_NAME, permission failures or
untyped error messages as length errors. Microsoft documents 206 as the
filename/extension-too-long error:
`https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-`.
Apple's POSIX error reference:
`https://developer.apple.com/documentation/foundation/posixerror/enametoolong`.
Windows native-error tests are added for a Windows runner. Local Windows
cross-compilation cannot establish runtime behavior on NTFS/SMB.

If the retry also fails, propagate that error without another retry or scan
request. Smaller component limits, overlong parent paths, permissions and
other errors can still prevent preservation. No filesystem capability cache
or unbounded probe loop is introduced.

## Discovery, pruning and collision limits

Enumerate the original directory once and compare entries against exact
reconstructed spellings for the entry's timestamp and normal/empty ID.
This avoids glob metacharacters in original names accidentally broadening
ownership. Match upstream names, new 8-hex shortened names, and 16-hex
hash-only fallback names. Shared
reconstruction includes cut extensions, removing initial patch/60's accepted
limitation for degenerate extensions. Match each entry at most once.
Sort matched copies by timestamp/device suffix first, with full-path ties,
so differing hash prefixes do not defeat newest-copy retention.

**No unhashed bounded spelling is matched (coordinator, 2026-11-06).** The
initial unhashed bounded form of patch/60 was never deployed, so no such
copies exist on any device. It was also the only spelling that could match
another original's copies, because it does not encode which original created
it. Dropping it removes that ambiguity entirely. Every bounded copy now
carries the hash of its full original name.

Also, 8 hex digits are a 32-bit truncated digest, not mathematical collision
proof. For a pair already sharing prefix and retained extension, the random
hash collision probability is 2^-32. Hash-only uses 64 bits (pair probability
2^-64). Birthday probability within a shared prefix/extension bucket grows
approximately as n(n-1)/(2*2^bits). Adversarial collisions, same-second
same-device overwrites inherited from upstream, and old ambiguous unhashed
copies remain risks. A literal never-collide guarantee is incompatible with
the approved finite 8/16-hex formats.

## Preserved patch/60 behavior and tests

Keep success-only `Deleted file` logging through the existing deferred
handler, and normalize already-absent deletion to success. Keep the real
patch/59 deletion workflow asserting original bytes in conflict copies,
SQLite tombstones, empty need list and no work on later iterations.

Boundary coverage includes exact fit unchanged from upstream, one byte over,
UTF-8 at the hashed stem cut, no extension, 230-byte extension and multibyte
extension. New regressions exercise two 248-byte originals sharing their
first 240 bytes and injected destination-Lstat ENAMETOOLONG above 143 bytes.
Additional tests cover upstream/hashed/fallback discovery, empty IDs, literal glob
metacharacters, nested paths, retry exhaustion, unrelated errors and wrapped
PathError/LinkError classification.

## Validation evidence

Full logs are retained in ignored `tmp/patch60-amend/`. The baseline run adds
only the two new regression tests to the e4a7a8b57 implementation. Both fail
before implementation changes.

Both runs used:

```sh
go test ./lib/model -run '^(TestConflictShortenedNamesDoNotCrossPrune|TestConflictNameTooLongFallback)$' -count=1 -v
```

Before, verbatim excerpts (`before.log`, exit 1):

```text
    folder_sendrecv_longname_test.go:236: pruning first original deleted other original's conflict: file does not exist
--- FAIL: TestConflictShortenedNamesDoNotCrossPrune (0.02s)
--- FAIL: TestConflictNameTooLongFallback (0.01s)
FAIL
FAIL	github.com/syncthing/syncthing/lib/model	0.648s
FAIL
```

After, verbatim excerpts (`after.log`, exit 0):

```text
=== RUN   TestConflictShortenedNamesDoNotCrossPrune
--- PASS: TestConflictShortenedNamesDoNotCrossPrune (0.01s)
=== RUN   TestConflictNameTooLongFallback
--- PASS: TestConflictNameTooLongFallback (0.01s)
ok  	github.com/syncthing/syncthing/lib/model	0.794s
```

The first `go build ./...` attempt found absent generated GUI assets:

```text
# github.com/syncthing/syncthing/cmd/infra/strelaypoolsrv
cmd/infra/strelaypoolsrv/main.go:294:17: undefined: auto.Assets
# github.com/syncthing/syncthing/lib/api
lib/api/api_statics.go:38:25: undefined: auto.Assets
lib/api/api_statics.go:45:25: undefined: auto.Assets
```

Resolved with the project's standard generation command, confined to this
worktree: `go generate ./lib/api/auto ./cmd/infra/strelaypoolsrv/auto`.
Its output was empty and exit status zero. Both generated `gui.files.go`
files are ignored and not committed. All requested gates were then rerun.

Final gates, verbatim output and harness exit statuses (checkpoint-only
harness lines omitted):

```text
$ go build ./...
exit=0
$ env GOOS=linux GOARCH=amd64 go build ./...
exit=0
$ go test ./lib/model -count=1
ok  	github.com/syncthing/syncthing/lib/model	70.687s
exit=0
$ go test -race ./lib/model -run Conflict\|LongName -count=20
ok  	github.com/syncthing/syncthing/lib/model	93.509s
exit=0
$ env GOOS=windows GOARCH=amd64 go test -c ./lib/model -o tmp/patch60-amend/model-windows.test.exe
exit=0
```

The race selection includes every model test whose name contains `Conflict`
or `LongName`, including the retained patch/59 deletion regression and all
new tests. Windows is compile-only, not a claim of Windows runtime execution.

A separate verbose run confirms the real-host patch/59 workflow passed rather
than skipped (`patch59-deletion.log`, exit 0):

```text
=== RUN   TestDeleteConflictLongNameCurrentlyStaysNeeded
--- PASS: TestDeleteConflictLongNameCurrentlyStaysNeeded (0.04s)
ok  	github.com/syncthing/syncthing/lib/model	0.546s
```
