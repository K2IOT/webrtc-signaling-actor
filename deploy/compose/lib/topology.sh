#!/usr/bin/env bash
# Helpers over deploy/compose/topology.env. Safe to `source` under `set -euo pipefail`.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
#
#   load_topology            export every topology.env variable into the caller
#   service_list             print the 11 service names, one per line: 6 actors (ACTORS_PER_CELL
#                            per cell), 2 gateways, control (= 9 plane JVMs), then identity,
#                            local-platform (= 11 JVM services in total)
#   ip_of <service> <net>    print the address of <service> on <net>; <net> is
#                            net-c001 | net-c002 | net-shared (or cell | shared, where
#                            "cell" means the service's own cell network); non-zero if none
#   all_addresses            print "<KEY> <ip>" for every addressed endpoint
#   subnet_of_key <KEY>      print the CIDR a topology address key must live in
#   ip_in_cidr <ip> <cidr>   exit 0 when <ip> is inside <cidr> (python3 ipaddress)

_TOPOLOGY_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPOLOGY_ENV="${TOPOLOGY_ENV:-$_TOPOLOGY_LIB_DIR/../topology.env}"

load_topology() {
  if [ ! -r "$TOPOLOGY_ENV" ]; then
    echo "topology: cannot read $TOPOLOGY_ENV" >&2
    return 1
  fi
  set -a
  # shellcheck source=/dev/null
  source "$TOPOLOGY_ENV"
  set +a
}

service_list() {
  local cell n
  for cell in c001 c002; do
    for n in $(seq 1 "$ACTORS_PER_CELL"); do
      printf 'actor-%s-%s\n' "$cell" "$n"
    done
  done
  printf '%s\n' gateway-c001 gateway-c002 control identity local-platform
}

# Prints the variable name that holds the address of <service> on <net>, or fails.
_ip_var() {
  local service="$1" net="$2" cell n upper
  case "$service" in
    actor-c00[12]-[1-6])
      cell="${service#actor-}"
      cell="${cell%-*}"
      n="${service##*-}"
      upper="$(printf '%s' "$cell" | tr '[:lower:]' '[:upper:]')"
      case "$net" in
        cell | "net-$cell") printf 'ACTOR_%s_%s_CELL_IP' "$upper" "$n" ;;
        shared | net-shared) printf 'ACTOR_%s_%s_SHARED_IP' "$upper" "$n" ;;
        *) return 1 ;;
      esac
      ;;
    gateway-c00[12])
      upper="$(printf '%s' "${service#gateway-}" | tr '[:lower:]' '[:upper:]')"
      case "$net" in
        shared | net-shared) printf 'GATEWAY_%s_IP' "$upper" ;;
        *) return 1 ;;
      esac
      ;;
    pg-c00[12]-primary | pg-c00[12]-standby)
      cell="${service#pg-}"
      cell="${cell%-*}"
      upper="$(printf '%s' "$cell" | tr '[:lower:]' '[:upper:]')"
      case "$net" in
        cell | "net-$cell") printf 'PG_%s_%s_IP' "$upper" "$(printf '%s' "${service##*-}" | tr '[:lower:]' '[:upper:]')" ;;
        *) return 1 ;;
      esac
      ;;
    kube-api)
      case "$net" in
        net-c001) printf 'KUBE_C001_IP' ;;
        net-c002) printf 'KUBE_C002_IP' ;;
        *) return 1 ;;
      esac
      ;;
    control)
      case "$net" in
        net-c001) printf 'CONTROL_C001_IP' ;;
        net-c002) printf 'CONTROL_C002_IP' ;;
        shared | net-shared) printf 'CONTROL_IP' ;;
        *) return 1 ;;
      esac
      ;;
    identity | local-platform | pg-directory)
      case "$net" in
        shared | net-shared)
          case "$service" in
            identity) printf 'IDENTITY_IP' ;;
            local-platform) printf 'PLATFORM_IP' ;;
            pg-directory) printf 'PG_DIRECTORY_IP' ;;
          esac
          ;;
        *) return 1 ;;
      esac
      ;;
    *) return 1 ;;
  esac
}

ip_of() {
  if [ "$#" -ne 2 ]; then
    echo "usage: ip_of <service> <network>" >&2
    return 2
  fi
  local var
  var="$(_ip_var "$1" "$2")" || return 1
  if [ -z "${!var-}" ]; then
    echo "topology: $var is not defined (load_topology first?)" >&2
    return 1
  fi
  printf '%s\n' "${!var}"
}

all_addresses() {
  local cell n key
  for key in CONTROL_IP IDENTITY_IP PLATFORM_IP PG_DIRECTORY_IP GATEWAY_C001_IP GATEWAY_C002_IP \
    PG_C001_PRIMARY_IP PG_C001_STANDBY_IP PG_C002_PRIMARY_IP PG_C002_STANDBY_IP KUBE_C001_IP KUBE_C002_IP \
    CONTROL_C001_IP CONTROL_C002_IP; do
    printf '%s %s\n' "$key" "${!key}"
  done
  for cell in C001 C002; do
    for n in $(seq 1 "$ACTORS_PER_CELL"); do
      key="ACTOR_${cell}_${n}_CELL_IP"
      printf '%s %s\n' "$key" "${!key}"
      key="ACTOR_${cell}_${n}_SHARED_IP"
      printf '%s %s\n' "$key" "${!key}"
    done
  done
}

subnet_of_key() {
  case "$1" in
    ACTOR_C001_*_CELL_IP | PG_C001_*_IP | KUBE_C001_IP | CONTROL_C001_IP) printf '%s\n' "$NET_C001" ;;
    ACTOR_C002_*_CELL_IP | PG_C002_*_IP | KUBE_C002_IP | CONTROL_C002_IP) printf '%s\n' "$NET_C002" ;;
    *) printf '%s\n' "$NET_SHARED" ;;
  esac
}

ip_in_cidr() {
  python3 -c '
import ipaddress, sys
ip, net = sys.argv[1:3]
sys.exit(0 if ipaddress.ip_address(ip) in ipaddress.ip_network(net, strict=True) else 1)
' "$1" "$2"
}
