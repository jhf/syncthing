#!/bin/bash
# reweave.sh <amended-branch> — rebase every descendant patch branch and
# main onto an amended stack member, with post-condition checks.
#
# Background: on 2026-10-19 a re-weave after amending patch/22 passed each
# branch's OWN tip as the <old> boundary to `git rebase --onto <new> <old>
# <branch>`. The empty range replayed zero commits, reported success, and
# collapsed patch/23..26 onto patch/22's tip; main kept its docs commits
# but lost the code ancestry. Nothing was deleted (dangling commits were
# recovered via reflog), but the remote agreed with the broken state for a
# while.
#
# This script makes the empty-replay failure loud:
#   1. The range <old-tip>..<branch> must contain exactly the expected
#      commits BEFORE the rebase.
#   2. The range <new-base>..<branch> must contain the same number of
#      commits AFTER, and
#   3. The branch's own delta `git diff <old-parent>..<branch>` must be
#      identical to the replayed delta `git diff <new-base>..<branch>`
#      (content-preserving replay; the tree itself legitimately picks up
#      the amendment).
#
# Usage:
#   ops/reweave.sh patch/22-bulk-delete-collapse
#
# The branch order is read from PATCHES.md (the `patch/NN-...` column), so
# new patches are picked up automatically. Run from anywhere in the repo
# with a clean working tree.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [[ -n "$(git status --porcelain)" ]]; then
	echo "error: working tree not clean" >&2
	exit 1
fi

amended="$1"
mapfile -t branches < <(grep -oE 'patch/[0-9]+[a-z0-9-]+' PATCHES.md | awk '!seen[$0]++')
if [[ ${#branches[@]} -eq 0 ]]; then
	echo "error: no patch branches found in PATCHES.md" >&2
	exit 1
fi

# Locate the amended branch in the order.
start=-1
for i in "${!branches[@]}"; do
	if [[ "${branches[$i]}" == "$amended" ]]; then
		start=$i
		break
	fi
done
if [[ $start -lt 0 ]]; then
	echo "error: $amended not listed in PATCHES.md" >&2
	exit 1
fi

# Record old tips of everything we are about to move.
declare -A oldtip newtip
for b in "${branches[@]}" main; do
	oldtip[$b]=$(git rev-parse "$b")
done

newbase=$(git rev-parse "$amended")

for ((i = start + 1; i < ${#branches[@]}; i++)); do
	b="${branches[$i]}"
	prev="${branches[$((i - 1))]}"
	oldparent="${oldtip[$prev]}"

	# Pre-condition: the range to replay is exactly this branch's commits
	# on top of its parent's OLD tip.
	n=$(git rev-list --count "$oldparent".."${oldtip[$b]}")
	if [[ $n -lt 1 ]]; then
		echo "error: $b has no commits on top of $prev's old tip ($oldparent); refusing empty rebase" >&2
		exit 1
	fi

	echo ">> $b: replaying $n commit(s) onto $newbase"
	git rebase --onto "$newbase" "$oldparent" "$b"
	newtip[$b]=$(git rev-parse "$b")

	# Post-conditions: same commit count, and the branch's own delta is
	# unchanged by the replay.
	m=$(git rev-list --count "$newbase".."${newtip[$b]}")
	if [[ $m -ne $n ]]; then
		echo "error: $b replayed $m commits, expected $n — aborting, branch left at ${newtip[$b]}" >&2
		exit 1
	fi
	if ! diff <(git diff "$oldparent" "${oldtip[$b]}") <(git diff "$newbase" "${newtip[$b]}") >/dev/null; then
		echo "error: $b delta changed across the rebase; inspect before pushing" >&2
		exit 1
	fi
	newbase="${newtip[$b]}"
done

# Finally main: docs chain on top of the last patch branch.
n=$(git rev-list --count "${oldtip[${branches[-1]}]}"..main)
echo ">> main: replaying $n docs commit(s) onto $newbase"
git checkout -q main
git rebase --onto "$newbase" "${oldtip[${branches[-1]}]}" main
newtip[main]=$(git rev-parse main)
m=$(git rev-list --count "$newbase"..main)
if [[ $m -ne $n ]]; then
	echo "error: main replayed $m commits, expected $n" >&2
	exit 1
fi
if ! diff <(git diff "${oldtip[${branches[-1]}]}" "${oldtip[main]}") <(git diff "$newbase" "${newtip[main]}") >/dev/null; then
	echo "error: main delta changed across the rebase; inspect before pushing" >&2
	exit 1
fi

echo "re-weave OK; verify with: git log --oneline ${branches[$start]}..main | tail -5"
echo "push with:    git push fork --force-with-lease main ${branches[*]:$start}"
