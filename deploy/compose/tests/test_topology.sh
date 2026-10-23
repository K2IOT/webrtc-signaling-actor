#!/usr/bin/env bash
# Topology checks: addresses unique, inside their subnet, one kube-API IP per cell net.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose="$(cd "$here/.." && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"
# shellcheck source=../lib/topology.sh
source "$compose/lib/topology.sh"
load_topology

all_addresses_unique() {
  local dups
  dups="$(all_addresses | awk '{print $2}' | sort | uniq -d | tr '\n' ' ')"
  assert_eq "" "$dups" "duplicate addresses"
  local n
  n="$(all_addresses | wc -l | tr -d ' ')"
  # 6 actors (3 per cell) x 2 addresses + 2 gateways + control/identity/platform/pg-directory + 2 x (2 pg + kube + control cell-net address)
  assert_eq "26" "$n" "number of addressed endpoints"
}

every_address_inside_its_subnet() {
  local key ip subnet
  while read -r key ip; do
    subnet="$(subnet_of_key "$key")"
    assert_true "$key=$ip must be inside $subnet" ip_in_cidr "$ip" "$subnet"
  done < <(all_addresses)
  # negative control: the helper really rejects foreign addresses
  assert_false "172.30.2.11 is not inside NET_C001" ip_in_cidr 172.30.2.11 "$NET_C001"
  assert_eq "172.30.1.0/24" "$NET_C001" "NET_C001"
  assert_eq "172.30.2.0/24" "$NET_C002" "NET_C002"
  assert_eq "172.30.0.0/24" "$NET_SHARED" "NET_SHARED"
}

kube_api_has_one_ip_per_cell_network() {
  assert_eq "172.30.1.250" "$KUBE_C001_IP" "KUBE_C001_IP"
  assert_eq "172.30.2.250" "$KUBE_C002_IP" "KUBE_C002_IP"
  assert_eq "172.30.1.250" "$(ip_of kube-api net-c001)" "kube-api on net-c001"
  assert_eq "172.30.2.250" "$(ip_of kube-api net-c002)" "kube-api on net-c002"
  assert_false "kube-api has no net-shared address" ip_of kube-api net-shared
}

actors_have_cell_and_shared_ips() {
  local cell n var cip sip az want
  for cell in c001 c002; do
    for n in $(seq 1 "$ACTORS_PER_CELL"); do
      assert_eq "172.30.${cell: -1}.1$n" "$(ip_of "actor-$cell-$n" net-$cell)" "actor-$cell-$n cell ip"
    done
  done
  for n in $(seq 1 "$ACTORS_PER_CELL"); do
    assert_eq "172.30.0.3$n" "$(ip_of "actor-c001-$n" shared)" "actor-c001-$n shared ip"
    assert_eq "172.30.0.4$n" "$(ip_of "actor-c002-$n" net-shared)" "actor-c002-$n shared ip"
  done
  # c001 actors never get a c002 address and vice versa
  assert_false "actor-c001-1 is not on net-c002" ip_of actor-c001-1 net-c002
  for cell in C001 C002; do
    for n in $(seq 1 "$ACTORS_PER_CELL"); do
      var="ACTOR_${cell}_${n}_CELL_IP"; cip="${!var}"
      var="ACTOR_${cell}_${n}_SHARED_IP"; sip="${!var}"
      assert_true "actor $cell/$n has distinct cell and shared ips" test "$cip" != "$sip"
      var="ACTOR_${cell}_${n}_AZ"; az="${!var}"
      # one zone per actor: any single actor can be lost and two zones remain
      case "$n" in 1) want=az-a ;; 2) want=az-b ;; *) want=az-c ;; esac
      assert_eq "$want" "$az" "AZ of $cell/$n"
    done
  done
}

control_has_an_address_on_each_cell_network() {
  assert_eq "172.30.1.10" "$CONTROL_C001_IP" "CONTROL_C001_IP"
  assert_eq "172.30.2.10" "$CONTROL_C002_IP" "CONTROL_C002_IP"
  assert_eq "172.30.0.10" "$CONTROL_IP" "CONTROL_IP stays on net-shared"
  assert_eq "172.30.1.10" "$(ip_of control net-c001)" "control on net-c001"
  assert_eq "172.30.2.10" "$(ip_of control net-c002)" "control on net-c002"
  assert_eq "172.30.0.10" "$(ip_of control net-shared)" "control on net-shared"
  assert_true "control c001 address inside NET_C001" ip_in_cidr "$CONTROL_C001_IP" "$NET_C001"
  assert_true "control c002 address inside NET_C002" ip_in_cidr "$CONTROL_C002_IP" "$NET_C002"
  assert_eq "$NET_C001" "$(subnet_of_key CONTROL_C001_IP)" "subnet_of_key CONTROL_C001_IP"
  assert_eq "$NET_C002" "$(subnet_of_key CONTROL_C002_IP)" "subnet_of_key CONTROL_C002_IP"
  # gateways, identity and local-platform stay shared-only
  local s c
  for s in gateway-c001 gateway-c002 identity local-platform; do
    for c in net-c001 net-c002; do
      assert_false "$s has no address on $c" ip_of "$s" "$c"
    done
  done
}

other_fixed_values() {
  assert_eq "c001 c002" "$CELLS" "CELLS"
  assert_eq "3" "$ACTORS_PER_CELL" "ACTORS_PER_CELL"
  assert_eq "172.30.0.10" "$CONTROL_IP" "CONTROL_IP"
  assert_eq "172.30.0.11" "$IDENTITY_IP" "IDENTITY_IP"
  assert_eq "172.30.0.12" "$PLATFORM_IP" "PLATFORM_IP"
  assert_eq "172.30.0.14" "$PG_DIRECTORY_IP" "PG_DIRECTORY_IP"
  assert_eq "172.30.0.21" "$GATEWAY_C001_IP" "GATEWAY_C001_IP"
  assert_eq "172.30.0.22" "$GATEWAY_C002_IP" "GATEWAY_C002_IP"
  assert_eq "172.30.1.101" "$PG_C001_PRIMARY_IP" "PG_C001_PRIMARY_IP"
  assert_eq "172.30.1.102" "$PG_C001_STANDBY_IP" "PG_C001_STANDBY_IP"
  assert_eq "172.30.2.101" "$PG_C002_PRIMARY_IP" "PG_C002_PRIMARY_IP"
  assert_eq "172.30.2.102" "$PG_C002_STANDBY_IP" "PG_C002_STANDBY_IP"
  assert_eq "8443:9443:9443:9444:8559" "$RPC_PORT:$RELAY_PORT:$SOURCE_PORT:$ADMIN_PORT:$HEALTH_PORT" "ports"
  assert_eq "5432:443" "$PG_PORT:$KUBE_API_PORT" "PostgreSQL and kube-API ports"
  assert_eq "signaling.local" "$DOMAIN" "DOMAIN"
  assert_eq "8443:8444:8445:8081" "$GATEWAY_PUBLISH_C001:$GATEWAY_PUBLISH_C002:$CONTROL_PUBLISH:$IDENTITY_PUBLISH" "published ports"
  assert_eq "gw-c001-a:gw-c002-a" "$GATEWAY_ID_C001:$GATEWAY_ID_C002" "gateway ids"
  assert_eq "wsa-c001:wsa-c002" "$KUBE_C001_NAMESPACE:$KUBE_C002_NAMESPACE" "namespaces"
  assert_eq "https://app.signaling.local" "$ORIGIN" "ORIGIN"
  assert_eq "8192" "$SPLIT" "SPLIT"
  assert_eq "http://identity.signaling.local:8081" "$ISSUER" "ISSUER"
  assert_eq "webrtc-signaling" "$AUDIENCE" "AUDIENCE"
  local v
  for v in CLUSTER_FINGERPRINT EXECUTABLE_ID; do
    assert_true "$v is 64 lower-case hex" bash -c '[[ "$1" =~ ^[0-9a-f]{64}$ ]]' _ "${!v}"
  done
  # One fingerprint for both cells (cross-cell compatibility gates compare it for equality).
  assert_eq "$(printf 'TEST_ONLY_COMPOSE_CLUSTER_FINGERPRINT' | sha256sum | cut -d' ' -f1)" "$CLUSTER_FINGERPRINT" \
    "CLUSTER_FINGERPRINT follows its documented derivation"
  assert_eq "unset:unset" "${CLUSTER_FINGERPRINT_C001-unset}:${CLUSTER_FINGERPRINT_C002-unset}" "no per-cell fingerprints"
  # Account user ids: set, distinct, 24 lower-hex; the cell mapping itself is pinned by
  # ComposeUserBucketsTest (SessionRegistryService.bucket is Java only).
  assert_true "ACCOUNT_ADMIN_USER_ID is 24 lower-case hex" bash -c '[[ "$1" =~ ^[0-9a-f]{24}$ ]]' _ "${ACCOUNT_ADMIN_USER_ID-}"
  assert_true "ACCOUNT_MOD_USER_ID is 24 lower-case hex" bash -c '[[ "$1" =~ ^[0-9a-f]{24}$ ]]' _ "${ACCOUNT_MOD_USER_ID-}"
  assert_true "account user ids are distinct" test "${ACCOUNT_ADMIN_USER_ID-}" != "${ACCOUNT_MOD_USER_ID-}"
}

dynamic_ranges_hold_no_static_address() {
  assert_eq "127.0.0.1" "$PUBLISH_HOST" "PUBLISH_HOST is loopback"
  local net key ip range
  for net in C001 C002 SHARED; do
    local subnet="NET_$net" dyn="NET_${net}_DYNAMIC"
    range="${!dyn-}"
    assert_true "$dyn is set" test -n "$range"
    assert_true "$dyn=$range lies inside ${!subnet}" python3 -c \
      'import ipaddress,sys; sys.exit(0 if ipaddress.ip_network(sys.argv[1]).subnet_of(ipaddress.ip_network(sys.argv[2])) else 1)' \
      "$range" "${!subnet}"
    while read -r key ip; do
      assert_false "$key=$ip must lie outside the dynamic range $dyn" ip_in_cidr "$ip" "$range"
    done < <(all_addresses)
  done
  # negative control: an address inside the range is detected
  assert_true "172.30.0.200 is inside NET_SHARED_DYNAMIC" ip_in_cidr 172.30.0.200 "$NET_SHARED_DYNAMIC"
}

service_list_has_11_names() {
  local names n
  names="$(service_list)"
  n="$(printf '%s\n' "$names" | wc -l | tr -d ' ')"
  # 6 actors (3 per cell) + 2 gateways + control (9 plane JVMs) + identity + local-platform.
  assert_eq "11" "$n" "service_list length"
  assert_eq "0" "$(printf '%s\n' "$names" | sort | uniq -d | wc -l | tr -d ' ')" "service names unique"
  assert_eq "actor-c001-1" "$(printf '%s\n' "$names" | sed -n 1p)" "first service"
  assert_eq "actor-c002-3" "$(printf '%s\n' "$names" | sed -n 6p)" "6th service"
  assert_eq "gateway-c001 gateway-c002 control identity local-platform" \
    "$(printf '%s\n' "$names" | sed -n '7,11p' | tr '\n' ' ' | sed 's/ $//')" "services 7..11"
  # every listed service has at least one address
  local s
  while read -r s; do
    assert_true "$s has an address" ip_of "$s" shared
  done < <(printf '%s\n' "$names" | grep -v '^actor-')
}

hosts_sh_prints_four_lines_and_writes_nothing() {
  local tmp before after out
  tmp="$(mktemp -d)"
  before="$(stat -c '%Y %s' /etc/hosts)"
  out="$(HOME="$tmp" bash "$compose/hosts.sh" 2>/dev/null)"
  after="$(stat -c '%Y %s' /etc/hosts)"
  assert_eq "$before" "$after" "/etc/hosts unchanged (mtime,size)"
  assert_eq "4" "$(printf '%s\n' "$out" | wc -l | tr -d ' ')" "hosts.sh prints four lines"
  local h
  for h in c001.signaling.local c002.signaling.local bootstrap.signaling.local identity.signaling.local; do
    assert_eq "127.0.0.1 $h" "$(printf '%s\n' "$out" | grep -x "127.0.0.1 $h")" "hosts line for $h"
  done
  assert_eq "0" "$(find "$tmp" -mindepth 1 | wc -l | tr -d ' ')" "hosts.sh wrote nothing under HOME"
  rm -rf "$tmp"
}

for t in all_addresses_unique every_address_inside_its_subnet kube_api_has_one_ip_per_cell_network \
  actors_have_cell_and_shared_ips control_has_an_address_on_each_cell_network other_fixed_values dynamic_ranges_hold_no_static_address service_list_has_11_names \
  hosts_sh_prints_four_lines_and_writes_nothing; do
  run_test "$t"
done
finish
