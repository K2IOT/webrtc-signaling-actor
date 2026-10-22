#!/usr/bin/env bash
# Runs every tests/test_*.sh and every <component>/tests/test_*.sh (for example
# provisioner/tests); exits non-zero when any of them fails.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose="$(cd "$here/.." && pwd)"
failed=0
ran=0
for t in "$compose"/tests/test_*.sh "$compose"/*/tests/test_*.sh; do
  [ -e "$t" ] || continue
  # Files marked `# REQUIRES_DOCKER` (real containers, minutes) run only on request.
  if grep -q '^# REQUIRES_DOCKER' "$t" && [ "${RUN_DOCKER_TESTS:-0}" != "1" ]; then
    printf '== %s\nSKIP (needs Docker; run with RUN_DOCKER_TESTS=1)\n' "${t#"$compose"/}"
    continue
  fi
  ran=$((ran + 1))
  printf '== %s\n' "${t#"$compose"/}"
  if ! bash "$t"; then
    failed=$((failed + 1))
  fi
done
if [ "$ran" -eq 0 ]; then
  echo "no tests found in $here" >&2
  exit 1
fi
printf '%d test files, %d failed\n' "$ran" "$failed"
[ "$failed" -eq 0 ]
