#!/usr/bin/env bash
# status.sh table|failures -- reporting for `make up`. LOCAL_TEST_ONLY / NOT_QUALIFIED.
#   table     one line per service: service, state, health, exit code
#   failures  the last LOG_LINES (default 40) log lines of every service that is unhealthy, not
#             running (other than a one-shot that exited 0), or restarting. The logs are JVM and
#             PostgreSQL logs; they carry no secret (launch.sh and the provisioner print file
#             names only).
# Uses the same project/env files as the Makefile (COMPOSE_CMD, split on spaces, may override the compose command).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -n "${COMPOSE_CMD:-}" ]; then
  read -r -a COMPOSE <<<"$COMPOSE_CMD"
else
  COMPOSE=(docker compose --project-directory "$here" --env-file "$here/topology.env" --env-file "$here/.env" -f "$here/compose.yaml")
fi
LOG_LINES="${LOG_LINES:-40}"
FORMAT='{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}'

rows() { "${COMPOSE[@]}" ps -a --format "$FORMAT" | sort; }

case "${1:-}" in
  table)
    printf '%-20s %-11s %-10s %s\n' SERVICE STATE HEALTH EXIT
    rows | awk -F'|' '{ printf "%-20s %-11s %-10s %s\n", $1, $2, ($3 == "" ? "-" : $3), $4 }'
    ;;
  failures)
    bad="$(rows | awk -F'|' '
      $2 == "exited" && $4 == "0" { next }
      $2 != "running" || $3 == "unhealthy" || $3 == "starting" { print $1 }')"
    if [ -z "$bad" ]; then
      echo "status: no failing service found"
      exit 0
    fi
    for s in $bad; do
      printf '\n===== %s (last %s lines) =====\n' "$s" "$LOG_LINES"
      "${COMPOSE[@]}" logs --no-color --tail "$LOG_LINES" "$s" 2>&1 || true
    done
    ;;
  *)
    echo "usage: status.sh table|failures" >&2
    exit 64
    ;;
esac
