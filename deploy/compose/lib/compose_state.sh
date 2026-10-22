#!/usr/bin/env bash
# Running-state query shared by guard.sh and preflight.sh (one implementation).
# Needs lib/topology.sh sourced and load_topology called (service_list).
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
#
#   running_services      services of the compose project that have a running container, one per line
#   missing_jvm_services  the services of service_list (the 17 JVM services) that are not running,
#                         space separated and with a leading space; empty when all 17 run
#
# Environment: COMPOSE_PROJECT (default signaling-compose-local).

COMPOSE_PROJECT="${COMPOSE_PROJECT:-signaling-compose-local}"

running_services() {
  docker ps --filter "label=com.docker.compose.project=$COMPOSE_PROJECT" --filter status=running \
    --format '{{.Label "com.docker.compose.service"}}' 2>/dev/null | sort -u
}

missing_jvm_services() {
  local running s missing=""
  running="$(running_services)"
  for s in $(service_list); do
    printf '%s\n' "$running" | grep -qx "$s" || missing="$missing $s"
  done
  printf '%s' "$missing"
}
