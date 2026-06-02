#!/bin/bash
#
# Test-integrity gate for the autoresearch loop. It asks a separate, read-only
# judge sub-agent (cursor-agent in ask mode) whether the current experiment
# diff weakens the test suite. The rules live in evaluator.md.
#
# The implementation agent may fix tests to keep the build green, but may not
# disable, delete, or weaken them. Running the judge as a separate agent stops
# the implementation agent from grading its own homework.
#
# This file is part of the autoresearch fixed infrastructure and must NOT be
# modified by autoresearch experiments.
#
# Usage:
#   hack/autoresearch/evaluate.sh [BASE_REF]   # BASE_REF defaults to HEAD
#
# Exit code 0 = approved, non-zero = rejected (or judge error). The verdict
# reason is printed to stderr.

set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"

base_ref="${1:-HEAD}"
rules_file="hack/autoresearch/evaluator.md"
model="${EVALUATOR_MODEL:-auto}"
max_diff_bytes="${MAX_DIFF_BYTES:-200000}"

# Make newly added (untracked) files visible to `git diff` without staging
# content, so deletions and additions of test files are reviewed too.
git add -A -N >/dev/null 2>&1 || true

status="$(git status --porcelain)"
diff="$(git diff --histogram "$base_ref" | head -c "$max_diff_bytes")"

if [[ -z "$status" && -z "$diff" ]]; then
	echo '{"approved": true, "reason": "No changes to evaluate."}'
	echo "evaluate: approved (empty diff)" >&2
	exit 0
fi

prompt="$(cat "$rules_file")

## Experiment under review

### git status --porcelain

$status

### git diff ${base_ref}

\`\`\`diff
$diff
\`\`\`
"

raw="$(printf '%s' "$prompt" | cursor-agent -p --mode ask --model "$model" --trust --output-format json 2>/dev/null)"

# The print-mode envelope is a JSON object with a .result string; the judge was
# told to make that string a JSON verdict. Extract the first {...} block from it
# to be robust to stray whitespace or fences.
result="$(printf '%s' "$raw" | jq -r '.result // empty' 2>/dev/null || true)"
verdict="$(printf '%s' "$result" | tr '\n' ' ' | grep -oE '\{.*\}' | head -1 || true)"

approved="$(printf '%s' "$verdict" | jq -r '.approved' 2>/dev/null || true)"
reason="$(printf '%s' "$verdict" | jq -r '.reason' 2>/dev/null || true)"

if [[ "$approved" == "true" ]]; then
	echo "$verdict"
	echo "evaluate: APPROVED - ${reason}" >&2
	exit 0
fi

if [[ "$approved" == "false" ]]; then
	echo "$verdict"
	echo "evaluate: REJECTED - ${reason}" >&2
	exit 1
fi

# Fail closed: if we could not get a clear verdict, reject and surface output.
echo '{"approved": false, "reason": "Could not parse a verdict from the judge."}'
echo "evaluate: REJECTED - unparseable judge output:" >&2
printf '%s\n' "$raw" >&2
exit 1
