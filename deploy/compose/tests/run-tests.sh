#!/usr/bin/env bash
# Runs every tests/test_*.sh and every <component>/tests/test_*.sh (for example
# provisioner/tests), plus the Node tests of kube-api when `node` is installed (built-ins only,
# run as a file because `node --test <dir>` is not portable across Node majors), and the compose
# lint tests (lint/test_*.py) when python3 and docker compose are installed; exits non-zero when
# any of them fails.
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
if command -v node >/dev/null 2>&1; then
  ran=$((ran + 1))
  printf '== kube-api/server.test.mjs\n'
  if ! node --test "$compose/kube-api/server.test.mjs"; then
    failed=$((failed + 1))
  fi
else
  printf '== kube-api/server.test.mjs\nSKIP (node not installed)\n'
fi
# Compose model lint (python3 stdlib unittest over `docker compose config`; also renders an offline
# tree into a temp dir to compare env.list). Needs python3 and docker compose; the module itself
# skips without compose >= 2.26 (fails under CI).
if command -v python3 >/dev/null 2>&1 && command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  ran=$((ran + 1))
  printf '== lint/test_compose_lint.py\n'
  if ! python3 -m unittest discover -s "$compose/lint" -p 'test_*.py'; then
    failed=$((failed + 1))
  fi
else
  printf '== lint/test_compose_lint.py\nSKIP (needs python3 and docker compose)\n'
fi
if [ "$ran" -eq 0 ]; then
  echo "no tests found in $here" >&2
  exit 1
fi
printf '%d test files, %d failed\n' "$ran" "$failed"
[ "$failed" -eq 0 ]
