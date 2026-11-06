# patch/69: one clear warning for directories kept by ignored files

## Problem

When a directory deleted on a remote device still holds locally ignored content without `(?d)`, Syncthing keeps it. That part is correct, and the user chose it: ignored content is not auto-deleted. The reporting was the problem.

- Every pull re-walked the directory and re-failed it, logging `WRN Failed to sync` each time (measured: 3 warnings over 3 pulls on d041da701 + patch/68).
- The message was generic: `contains ignored files (see ignore documentation for (?d) prefix)`. It did not say which file or which pattern. Its sibling, `errDirNotEmpty`, guessed "probably ignored on that remote device" even when nothing was ignored (that case is patch/68).

## Principle

Say it once, precisely, and keep it visible. Do not repeat work or warnings while nothing has changed. Never delete user content on Syncthing's own initiative.

## Change

- `lib/ignore`: `Matcher.MatchPattern(file)` returns the result plus the ignore-file line that decided it, as the user wrote it (`*.local`, not the internal `(?i)**/*.local`). Each `Pattern` records its source line. `Match` is unchanged and still allocation-free. Neither `String()` nor the patterns hash includes the source line.
- `lib/model/folder_ignored_leftover.go`: when `deleteDirOnDisk` reports ignored contents, the folder walks the directory once more. It collects up to 5 blocking ignored entries (the topmost of each ignored subtree), each with its pattern, plus a count of the rest. It builds the Failed Items text, for example:
  `directory has been deleted on a remote device but is kept because it contains locally ignored files: tree/notes.local (pattern "*.local"). They are not deleted because the pattern lacks the (?d) prefix; remove them or add (?d) to let the directory go`
- It logs one `WRN Directory deleted on a remote device is kept because of locally ignored files` with `ignored=[...]`, `patterns=[...]` and `more=N`.
- The result is remembered per directory: the ignore hash, the directory tombstone's version and the named paths. Later pulls re-publish the Failed Items entry without walking or warning. The memory is dropped, and the directory walked afresh, when the ignore patterns change, when the remote deletion changes, or when any named blocker is gone from disk. At most 4096 directories are remembered per folder. A directory past that cap is walked and warned about in every pull, as before, but with the precise message.
- The churn class is `ignored_leftover`, so `Failed to sync` WRN lines and the `Failed to delete directory` INF line are suppressed for it. Items stay in Failed Items (GUI) and in `/rest/folder/errors`.
- No top-level GUI banner: the warning goes to the log at WRN, which the GUI banner (ERROR level) does not show.

## Regression tests

| Test | Base (f25385b32, patch/68) | After |
|---|---|---|
| `TestIgnoredLeftoverWarnsOnce`: kept dir, 3 pulls | FAIL: the message names no path or pattern (`... contains ignored files (see ignore documentation for (?d) prefix)`); with that check disabled, `expected exactly one warning ... over 3 pulls, got 3` | PASS: 1 WRN, Failed Items names `tree/notes.local` and `"*.local"` in every pull, file intact |
| `TestIgnoredLeftoverReleasedWhenBlockerRemoved` | PASS | PASS (control: removing the ignored file lets the directory go) |
| `TestIgnoredLeftoverReleasedWhenIgnoresChange`: `.stignore` becomes `(?d)*.local` | PASS | PASS (control: directory deleted, still 1 WRN total) |
| `lib/ignore` `TestMatchPatternReportsSourceLine` | n/a (new API) | PASS: `*.local`, `!keep.local`, `(?d)build/`, no match gives "" and results equal `Match` |

Suites: `ok lib/model 71.197s`, `ok lib/ignore 0.811s`, `ok lib/api 9.426s`.
