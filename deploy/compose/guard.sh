#!/usr/bin/env bash
# Lifecycle guard run by `make up` before `docker compose up`. LOCAL_TEST_ONLY / NOT_QUALIFIED.
#
# Why: provisioning runs with --if-needed, so a completed stage is skipped. Restarting a gateway
# that exited therefore reuses the boot id pinned at provision time, whose lease has expired, and
# it never becomes ready. The supported lifecycle is `make up` -> use -> `make down` (down -v).
#
# Refuses (exit 1, one message, "run make reset first") when the `run` volume exists while
#   - any of the 11 JVM services has no running container (a manual `docker compose down` without
#     -v, or a crashed plane), or
#   - the volume holds an `.offline` marker (a manual offline render with placeholder WAL facts).
# Passes when the volume does not exist (fresh host) or when all 11 JVM services run (a no-op
# `make up` on a healthy stack).
# Environment: COMPOSE_PROJECT (default signaling-compose-local), PROVISIONER_IMAGE.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/topology.sh
source "$here/lib/topology.sh"
# shellcheck source=lib/compose_state.sh
source "$here/lib/compose_state.sh"
load_topology

PROJECT="$COMPOSE_PROJECT"
VOLUME="${PROJECT}_run"

provisioner_image() {
  local f v
  for f in "$here/.env" "$here/.env.example"; do
    [ -r "$f" ] || continue
    v="$(sed -n 's/^PROVISIONER_IMAGE=//p' "$f" | tail -n 1)"
    if [ -n "$v" ]; then printf '%s' "$v"; return; fi
  done
  printf 'signaling-provisioner:local'
}
IMAGE="${PROVISIONER_IMAGE:-$(provisioner_image)}"

refuse() {
  printf 'guard: REFUSED: %s\nguard: run "make reset" first (removes the containers and every volume), then "make up".\n' "$1" >&2
  exit 1
}

# Nothing to protect on a fresh host.
docker volume inspect "$VOLUME" >/dev/null 2>&1 || exit 0

# An offline render (placeholder WAL facts) must never be started. The volume exists, so the
# short-lived container does not create it by accident.
rc=0
docker run --rm -v "$VOLUME:/r:ro" "$IMAGE" test -e /r/.offline >/dev/null 2>&1 || rc=$?
case "$rc" in
  0) refuse "the run volume holds an .offline marker (a manual offline render)" ;;
  1) ;;
  *) echo "guard: cannot inspect volume $VOLUME with image $IMAGE (docker run exit $rc); run 'make build' or 'make reset'" >&2; exit 1 ;;
esac

missing="$(missing_jvm_services)"
if [ -n "$missing" ]; then
  refuse "volume $VOLUME exists but no running container for:$missing (leftover state of a 'docker compose down' without -v, or a crashed plane)"
fi
exit 0
