#!/usr/bin/env bash
# Provisioner stage 1 (PKI, keys, passwords, pod identities) against a temporary RUN_ROOT.
# Runs on the host with its openssl/keytool/jq; no Docker. LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
prov="$(cd "$here/.." && pwd)"
compose="$(cd "$prov/.." && pwd)"
repo="$(cd "$compose/../.." && pwd)"
# shellcheck source=../../tests/lib.sh
source "$compose/tests/lib.sh"
# shellcheck source=../../lib/topology.sh
source "$compose/lib/topology.sh"
load_topology

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
RUN_ROOT="$TMP/run"
mkdir -p "$RUN_ROOT"
export RUN_ROOT
export IDENTITY_PUBLIC_KEY="$repo/signaling-identity/src/main/resources/keys/public.pem"
STAGE1_OUT="$TMP/stage1.out"

SPIFFE="spiffe://signaling/local-test-only"

stage1() { bash "$prov/provision.sh" stage1; }

san_of() { openssl x509 -in "$1" -noout -ext subjectAltName 2>/dev/null; }
uri_sans() { san_of "$1" | grep -o 'URI:[^ ,]*' || true; }
pub_of() { openssl pkey -in "$1" -pubout 2>/dev/null; }
actors() { service_list | grep '^actor-'; }
planes() { service_list | grep -E '^(actor-|gateway-|control$)'; }
cell_of() { local c="${1#actor-}"; printf '%s' "${c%-*}"; }

setup_run() {
  set +e
  stage1 >"$STAGE1_OUT" 2>&1
  SETUP_RC=$?
  set -e
}

stage1_succeeds() {
  assert_eq "0" "$SETUP_RC" "stage1 exit status"
  assert_true ".provisioned marker exists" test -f "$RUN_ROOT/.provisioned"
}

service_dirs_cover_all_18_names() {
  local s n=0
  for s in $(service_list) pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby pg-directory kube-api-c001 kube-api-c002; do
    assert_true "dir $s" test -d "$RUN_ROOT/$s"
    n=$((n + 1))
  done
  assert_eq "18" "$n" "number of service directories"
  assert_eq "18" "$(find "$RUN_ROOT" -mindepth 1 -maxdepth 1 -type d ! -name '_shared' ! -name '.ca' | wc -l | tr -d ' ')" \
    "no extra service directories"
}

each_workload_leaf_has_exactly_one_uri_san() {
  local s n
  for s in $(planes); do
    n="$(uri_sans "$RUN_ROOT/$s/leaf.crt" | wc -l | tr -d ' ')"
    assert_eq "1" "$n" "$s URI SAN count"
  done
  assert_eq "9" "$(planes | wc -l | tr -d ' ')" "plane count"
  assert_eq "9" "$(for s in $(planes); do uri_sans "$RUN_ROOT/$s/leaf.crt"; done | sort -u | wc -l | tr -d ' ')" \
    "9 distinct SPIFFE ids (no id is shared between pods)"
}

actor_uri_san_is_per_pod_id() {
  local s cell
  for s in $(actors); do
    cell="$(cell_of "$s")"
    assert_eq "URI:$SPIFFE/cell/$cell/actor/$s" "$(uri_sans "$RUN_ROOT/$s/leaf.crt")" "$s URI SAN"
  done
  assert_eq "URI:$SPIFFE/cell/c001/actor/actor-c001-3" "$(uri_sans "$RUN_ROOT/actor-c001-3/leaf.crt")" "actor-c001-3"
  assert_true "actor DNS SAN" grep -q 'DNS:actor.c001.signaling.local' <(san_of "$RUN_ROOT/actor-c001-3/leaf.crt")
  assert_true "actor shared-net IP SAN" grep -q 'IP Address:172.30.0.33' <(san_of "$RUN_ROOT/actor-c001-3/leaf.crt")
  assert_true "actor cell-net IP SAN" grep -q 'IP Address:172.30.1.13' <(san_of "$RUN_ROOT/actor-c001-3/leaf.crt")
}

gateway_uri_san_carries_gateway_id() {
  assert_eq "URI:$SPIFFE/cell/c001/gateway/gw-c001-a" "$(uri_sans "$RUN_ROOT/gateway-c001/leaf.crt")" "gateway-c001"
  assert_eq "URI:$SPIFFE/cell/c002/gateway/gw-c002-a" "$(uri_sans "$RUN_ROOT/gateway-c002/leaf.crt")" "gateway-c002"
  assert_true "gateway IP SAN" grep -q 'IP Address:172.30.0.21' <(san_of "$RUN_ROOT/gateway-c001/leaf.crt")
}

control_leaf_has_control_uri_and_bootstrap_dns() {
  assert_eq "URI:$SPIFFE/cell/c001/control" "$(uri_sans "$RUN_ROOT/control/leaf.crt")" "control URI SAN"
  assert_true "bootstrap DNS" grep -q 'DNS:bootstrap.signaling.local' <(san_of "$RUN_ROOT/control/leaf.crt")
  assert_true "control IP SAN" grep -q 'IP Address:172.30.0.10' <(san_of "$RUN_ROOT/control/leaf.crt")
}

gateway_leaf_has_public_dns_san() {
  local c
  for c in c001 c002; do
    assert_true "gateway-$c public DNS" grep -q "DNS:$c.signaling.local" <(san_of "$RUN_ROOT/gateway-$c/leaf.crt")
    assert_true "gateway-$c tls authority DNS" grep -q "DNS:gateway.$c.signaling.local" <(san_of "$RUN_ROOT/gateway-$c/leaf.crt")
  done
}

leaf_has_server_and_client_eku() {
  local s txt
  for s in $(planes); do
    txt="$(openssl x509 -in "$RUN_ROOT/$s/leaf.crt" -noout -ext extendedKeyUsage 2>/dev/null)"
    assert_true "$s serverAuth" grep -q 'TLS Web Server Authentication' <<<"$txt"
    assert_true "$s clientAuth" grep -q 'TLS Web Client Authentication' <<<"$txt"
  done
}

eku_and_key_usage() {
  local s txt
  for s in $(planes); do
    txt="$(openssl x509 -in "$RUN_ROOT/$s/leaf.crt" -noout -ext keyUsage,basicConstraints 2>/dev/null)"
    assert_true "$s digitalSignature" grep -q 'Digital Signature' <<<"$txt"
    assert_true "$s is not a CA" grep -q 'CA:FALSE' <<<"$txt"
    assert_false "$s has no certSign" grep -q 'Certificate Sign' <<<"$txt"
  done
  txt="$(openssl x509 -in "$RUN_ROOT/actor-c001-1/ca.crt" -noout -ext keyUsage,basicConstraints 2>/dev/null)"
  assert_true "workload CA is a CA" grep -q 'CA:TRUE' <<<"$txt"
  assert_true "workload CA certSign" grep -q 'Certificate Sign' <<<"$txt"
}

leaf_chains_to_workload_ca() {
  local s
  for s in $(planes); do
    assert_true "$s chains to workload CA" openssl verify -CAfile "$RUN_ROOT/$s/ca.crt" "$RUN_ROOT/$s/leaf.crt"
  done
  assert_eq "1" "$(for s in $(planes); do sha256sum <"$RUN_ROOT/$s/ca.crt"; done | sort -u | wc -l | tr -d ' ')" \
    "one CA-workload for every plane"
  # a leaf must NOT verify against another CA
  assert_false "leaf does not chain to the DB CA" openssl verify -CAfile "$RUN_ROOT/actor-c001-1/db-ca.crt" "$RUN_ROOT/actor-c001-1/leaf.crt"
  # PG server leafs chain to the DB CA
  local p
  for p in pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby pg-directory; do
    assert_true "$p chains to db-ca" openssl verify -CAfile "$RUN_ROOT/$p/db-ca.crt" "$RUN_ROOT/$p/server.crt"
  done
}

pg_server_leafs_carry_dns_and_ip_sans() {
  assert_true "pg-c001-primary DNS" grep -q 'DNS:pg-c001-primary' <(san_of "$RUN_ROOT/pg-c001-primary/server.crt")
  assert_true "pg-c001-primary IP" grep -q 'IP Address:172.30.1.101' <(san_of "$RUN_ROOT/pg-c001-primary/server.crt")
  assert_true "pg-c002-standby IP" grep -q 'IP Address:172.30.2.102' <(san_of "$RUN_ROOT/pg-c002-standby/server.crt")
  assert_true "pg-directory IP" grep -q 'IP Address:172.30.0.14' <(san_of "$RUN_ROOT/pg-directory/server.crt")
  assert_true "pg-directory DNS" grep -q 'DNS:pg-directory' <(san_of "$RUN_ROOT/pg-directory/server.crt")
}

# fingerprint_of_pem <file>: SHA-256 fingerprint, hex only, from openssl
fingerprint_of_pem() { openssl x509 -in "$1" -noout -fingerprint -sha256 | sed 's/^.*=//' | tr -d ':' | tr '[:lower:]' '[:upper:]'; }

remoting_p12_has_ip_san_and_loads_with_keytool() {
  local s d out ip cell other leaf sans fp_trust fp_ca
  for s in $(actors); do
    d="$RUN_ROOT/$s"
    cell="$(cell_of "$s")"
    other="c001"
    [ "$cell" = "c001" ] && other="c002"
    ip="$(ip_of "$s" cell)"
    out="$(keytool -list -v -keystore "$d/remoting.p12" -storetype PKCS12 -storepass:file "$d/remoting.pass" 2>&1)" || true
    assert_true "$s remoting.p12 has one entry" grep -q 'Your keystore contains 1 entry' <<<"$out"
    assert_true "$s remoting.p12 PrivateKeyEntry" grep -q 'PrivateKeyEntry' <<<"$out"
    assert_true "$s remoting.p12 IP SAN $ip" grep -q "IPAddress: $ip" <<<"$out"

    # the remoting leaf itself: signed by THIS cell's remoting CA, by no other CA, exact IP SAN
    leaf="$TMP/remoting-leaf-$s.pem"
    openssl pkcs12 -in "$d/remoting.p12" -passin "file:$d/remoting.pass" -nokeys -clcerts 2>/dev/null >"$leaf"
    assert_true "$s remoting leaf verifies against CA-remoting-$cell" openssl verify -CAfile "$RUN_ROOT/_shared/ca-remoting-$cell.crt" "$leaf"
    assert_false "$s remoting leaf does not verify against CA-remoting-$other" openssl verify -CAfile "$RUN_ROOT/_shared/ca-remoting-$other.crt" "$leaf"
    assert_false "$s remoting leaf does not verify against CA-workload" openssl verify -CAfile "$RUN_ROOT/_shared/ca-workload.crt" "$leaf"
    sans="$(openssl x509 -in "$leaf" -noout -ext subjectAltName 2>/dev/null | tail -n +2 | tr -d ' ')"
    assert_eq "IPAddress:$ip" "$sans" "$s remoting leaf SAN is exactly its net-cell address"

    # trust.p12: exactly one entry, and it is this cell's remoting CA (compare fingerprints)
    out="$(keytool -list -v -keystore "$d/trust.p12" -storetype PKCS12 -storepass:file "$d/remoting.pass" 2>&1)" || true
    assert_true "$s trust.p12 has one entry" grep -q 'Your keystore contains 1 entry' <<<"$out"
    assert_true "$s trust.p12 is a trusted cert entry" grep -q 'trustedCertEntry' <<<"$out"
    assert_true "$s trust.p12 alias names the CA" grep -q "Alias name: remoting-ca-$cell\$" <<<"$out"
    assert_true "$s trust.p12 owner is CA-remoting-$cell" grep -q "Owner: .*CN=LOCAL_TEST_ONLY CA-remoting-$cell\b" <<<"$out"
    fp_trust="$(sed -n 's/^[[:space:]]*SHA256:[[:space:]]*//p' <<<"$out" | head -1 | tr -d ':' | tr '[:lower:]' '[:upper:]')"
    fp_ca="$(fingerprint_of_pem "$RUN_ROOT/_shared/ca-remoting-$cell.crt")"
    assert_true "$s trust.p12 fingerprint is non-empty" test -n "$fp_trust"
    assert_eq "$fp_ca" "$fp_trust" "$s trust.p12 holds the CA-remoting-$cell certificate"
    assert_true "$s trust.p12 fingerprint differs from CA-remoting-$other" test "$fp_trust" != "$(fingerprint_of_pem "$RUN_ROOT/_shared/ca-remoting-$other.crt")"
  done
  assert_false "the two cells use different remoting passwords" cmp -s "$RUN_ROOT/actor-c001-1/remoting.pass" "$RUN_ROOT/actor-c002-1/remoting.pass"
  assert_false "c001 trust store does not open with the c002 password" \
    keytool -list -keystore "$RUN_ROOT/actor-c001-1/trust.p12" -storetype PKCS12 -storepass:file "$RUN_ROOT/actor-c002-1/remoting.pass"
}

ca_certs_are_path_length_zero() {
  local f txt
  for f in "$RUN_ROOT"/_shared/ca-*.crt "$RUN_ROOT"/_shared/admin/admin-ca.crt; do
    txt="$(openssl x509 -in "$f" -noout -ext basicConstraints 2>/dev/null | tr -d ' \n')"
    assert_eq "X509v3BasicConstraints:critical" "${txt%%CA:*}" "$(basename "$f") basicConstraints is critical"
    assert_true "$(basename "$f") is CA:TRUE,pathlen:0" grep -q 'CA:TRUE,pathlen:0' <<<"$txt"
  done
  assert_eq "7" "$(ls "$RUN_ROOT"/_shared/ca-*.crt "$RUN_ROOT"/_shared/admin/admin-ca.crt | wc -l | tr -d ' ')" "CA certificates checked"
}

issue_leaf_refuses_an_empty_san_list() {
  local d="$TMP/emptysan" rc
  mkdir -p "$d/tmp"
  cp "$RUN_ROOT/.ca/workload.key" "$RUN_ROOT/.ca/workload.crt" "$d/" 2>/dev/null || { _fail "cannot read CA material"; return 1; }
  set +e
  ( CA_DIR="$d"; source "$prov/lib/pki.sh"; issue_leaf workload x "" "$d/x.key" "$d/x.crt" ) >/dev/null 2>&1
  rc=$?
  set -e
  assert_true "issue_leaf with an empty SAN list fails" test "$rc" -ne 0
  assert_false "no certificate was written" test -e "$d/x.crt"
}

proof_key_is_ed25519_and_pub_matches() {
  local s cell
  for cell in c001 c002; do
    assert_true "$cell proof.key is Ed25519" grep -q 'ED25519' <(openssl pkey -in "$RUN_ROOT/actor-$cell-1/proof.key" -text -noout 2>&1 | tr 'a-z' 'A-Z')
    assert_true "$cell proof.key is PKCS#8" grep -q 'BEGIN PRIVATE KEY' "$RUN_ROOT/actor-$cell-1/proof.key"
  done
  for s in $(actors); do
    cell="$(cell_of "$s")"
    assert_eq "$(pub_of "$RUN_ROOT/actor-$cell-1/proof.key")" "$(pub_of "$RUN_ROOT/$s/proof.key")" "$s shares its cell proof key"
    assert_eq "$(pub_of "$RUN_ROOT/$s/proof.key")" "$(cat "$RUN_ROOT/$s/proof.pub")" "$s proof.pub matches proof.key"
  done
  assert_true "cell proof keys differ" test "$(pub_of "$RUN_ROOT/actor-c001-1/proof.key")" != "$(pub_of "$RUN_ROOT/actor-c002-1/proof.key")"
  for s in $(planes); do
    assert_true "$s has proof.pub" test -s "$RUN_ROOT/$s/proof.pub"
    assert_eq "$(pub_of "$RUN_ROOT/actor-c001-1/proof.key")" "$(cat "$RUN_ROOT/$s/proof-c001.pub")" "$s proof-c001.pub"
    assert_eq "$(pub_of "$RUN_ROOT/actor-c002-1/proof.key")" "$(cat "$RUN_ROOT/$s/proof-c002.pub")" "$s proof-c002.pub"
  done
}

source_pub_matches_source_key() {
  local s want
  assert_true "source.key PKCS#8" grep -q 'BEGIN PRIVATE KEY' "$RUN_ROOT/local-platform/source.key"
  assert_true "source.key Ed25519" grep -q 'ED25519' <(openssl pkey -in "$RUN_ROOT/local-platform/source.key" -text -noout 2>&1 | tr 'a-z' 'A-Z')
  want="$(pub_of "$RUN_ROOT/local-platform/source.key")"
  assert_eq "$want" "$(cat "$RUN_ROOT/local-platform/source.pub")" "local-platform source.pub"
  for s in $(planes); do
    assert_eq "$want" "$(cat "$RUN_ROOT/$s/source.pub")" "$s source.pub"
  done
  assert_false "source.key is not copied to any plane" test -e "$RUN_ROOT/actor-c001-1/source.key"
}

jwt_pub_is_the_identity_public_key() {
  local s
  for s in $(planes); do
    assert_true "$s jwt.pub equals identity public key" cmp -s "$IDENTITY_PUBLIC_KEY" "$RUN_ROOT/$s/jwt.pub"
  done
}

platform_p12_loads_with_keytool_and_has_ip_san() {
  local d="$RUN_ROOT/local-platform" out
  out="$(keytool -list -v -keystore "$d/platform.p12" -storetype PKCS12 -storepass:file "$d/platform.pass" 2>&1)" || true
  assert_true "one entry" grep -q 'Your keystore contains 1 entry' <<<"$out"
  assert_true "PrivateKeyEntry" grep -q 'PrivateKeyEntry' <<<"$out"
  assert_true "IP SAN $PLATFORM_IP" grep -q "IPAddress: $PLATFORM_IP" <<<"$out"
  assert_true "DNS SAN" grep -q 'DNSName: local-platform.signaling.local' <<<"$out"
  assert_true "source-client-trust is the workload CA" cmp -s "$d/source-client-trust.crt" "$RUN_ROOT/actor-c001-1/ca.crt"
  assert_true "admin-ca.crt present" test -s "$d/admin-ca.crt"
  assert_true "admin client verifies against admin CA" openssl verify -CAfile "$d/admin-ca.crt" "$RUN_ROOT/_shared/admin/admin-client.crt"
  assert_eq "URI:$SPIFFE/admin/smoke" "$(uri_sans "$RUN_ROOT/_shared/admin/admin-client.crt")" "admin client URI SAN"
  assert_eq "1" "$(uri_sans "$RUN_ROOT/_shared/admin/admin-client.crt" | wc -l | tr -d ' ')" "admin client has one URI SAN"
}

kube_tokens_differ_per_cell() {
  local t1 t2 n
  t1="$(cat "$RUN_ROOT/kube-api-c001/kube-token")"
  t2="$(cat "$RUN_ROOT/kube-api-c002/kube-token")"
  assert_true "tokens are non-empty" test -n "$t1" -a -n "$t2"
  assert_true "tokens differ between cells" test "$t1" != "$t2"
  for n in $(seq 1 "$ACTORS_PER_CELL"); do
    assert_eq "$t1" "$(cat "$RUN_ROOT/actor-c001-$n/kube-token")" "actor-c001-$n token"
    assert_eq "$t2" "$(cat "$RUN_ROOT/actor-c002-$n/kube-token")" "actor-c002-$n token"
    assert_true "actor-c001-$n kube-ca" cmp -s "$RUN_ROOT/kube-api-c001/kube-ca.crt" "$RUN_ROOT/actor-c001-$n/kube-ca.crt"
  done
  assert_true "kube-api c001 cert has its cell IP" grep -q "IP Address:$KUBE_C001_IP" <(san_of "$RUN_ROOT/kube-api-c001/kube-api.crt")
  assert_true "kube-api c002 cert has its cell IP" grep -q "IP Address:$KUBE_C002_IP" <(san_of "$RUN_ROOT/kube-api-c002/kube-api.crt")
  assert_true "kube-api cert chains to the kube CA" openssl verify -CAfile "$RUN_ROOT/kube-api-c001/kube-ca.crt" "$RUN_ROOT/kube-api-c001/kube-api.crt"
  assert_false "kube CA is not the workload CA" cmp -s "$RUN_ROOT/kube-api-c001/kube-ca.crt" "$RUN_ROOT/actor-c001-1/ca.crt"
}

pod_uids_are_unique_lowercase_uuids() {
  local f="$RUN_ROOT/_shared/pod-uids.env" g="$RUN_ROOT/_shared/gateway-boots.env"
  local re='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  local line k v
  assert_eq "11" "$(wc -l <"$f" | tr -d ' ')" "pod-uids.env lines"
  assert_eq "2" "$(wc -l <"$g" | tr -d ' ')" "gateway-boots.env lines"
  while IFS='=' read -r k v; do
    assert_true "$k is a lowercase uuid" grep -Eq "$re" <<<"$v"
    assert_true "$k name shape" grep -Eq '^[A-Z0-9_]+_POD_UID$' <<<"$k"
  done <"$f"
  while IFS='=' read -r k v; do
    assert_true "$k is a lowercase uuid" grep -Eq "$re" <<<"$v"
  done <"$g"
  assert_eq "13" "$(cat "$f" "$g" | cut -d= -f2 | sort -u | wc -l | tr -d ' ')" "all 13 uuids distinct"
  assert_true "ACTOR_C001_3_POD_UID present" grep -q '^ACTOR_C001_3_POD_UID=' "$f"
  assert_true "LOCAL_PLATFORM_POD_UID present" grep -q '^LOCAL_PLATFORM_POD_UID=' "$f"
  assert_true "GATEWAY_BOOT_C001 present" grep -q '^GATEWAY_BOOT_C001=' "$g"
  assert_true "GATEWAY_BOOT_C002 present" grep -q '^GATEWAY_BOOT_C002=' "$g"
}

privacy_key_is_32_bytes_per_cell() {
  local s cell
  for s in $(actors); do
    cell="$(cell_of "$s")"
    assert_eq "32" "$(wc -c <"$RUN_ROOT/$s/privacy.key" | tr -d ' ')" "$s privacy.key size"
    assert_true "$s privacy.key shared within cell" cmp -s "$RUN_ROOT/actor-$cell-1/privacy.key" "$RUN_ROOT/$s/privacy.key"
  done
  assert_false "privacy.key differs between cells" cmp -s "$RUN_ROOT/actor-c001-1/privacy.key" "$RUN_ROOT/actor-c002-1/privacy.key"
}

db_passwords_are_present_and_random() {
  local f="$RUN_ROOT/_shared/db-passwords.env" k
  for k in DB_PASSWORD_OWNER DB_PASSWORD_APP DB_PASSWORD_NODIR DB_PASSWORD_REPLICATOR DB_PASSWORD_SOURCE_OWNER; do
    assert_true "$k set" grep -Eq "^$k=[0-9a-f]{32,}$" "$f"
  done
  assert_eq "5" "$(cut -d= -f2 "$f" | sort -u | wc -l | tr -d ' ')" "five distinct passwords"
}

no_key_is_world_readable() {
  assert_eq "" "$(find "$RUN_ROOT" -perm /o+rwx | head -5 | tr '\n' ' ')" "world-accessible paths"
  assert_eq "" "$(find "$RUN_ROOT" -type f \( -name '*.key' -o -name '*.p12' -o -name '*.pass' -o -name 'kube-token' -o -name 'db-passwords.env' \) -perm /g+w,o+rwx | tr '\n' ' ')" \
    "writable or world-readable secrets"
}

modes_are_0440_files_and_0750_dirs() {
  assert_eq "" "$(find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -type f ! -perm 0440 -print | tr '\n' ' ')" "files not 0440"
  assert_eq "" "$(find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -type d ! -perm 0750 ! -path "$RUN_ROOT" -print | tr '\n' ' ')" "dirs not 0750"
  assert_eq "700" "$(stat -c '%a' "$RUN_ROOT/.ca")" ".ca mode"
  assert_eq "" "$(find "$RUN_ROOT/.ca" -type f ! -perm 0400 | tr '\n' ' ')" ".ca files not 0400"
}

shared_dir_has_no_ca_private_key() {
  local f ca pubs=""
  while IFS= read -r f; do
    case "$f" in
      "$RUN_ROOT/_shared/admin/admin-client.key") ;;
      *) _fail "private key in _shared: $f"; CHECKS=$((CHECKS + 1)) ;;
    esac
  done < <(grep -rl 'PRIVATE KEY' "$RUN_ROOT/_shared" || true)
  # no CA public key matches any key published under a service or _shared directory
  for ca in "$RUN_ROOT"/.ca/*.key; do pubs="$pubs
$(pub_of "$ca" | sha256sum)"; done
  while IFS= read -r f; do
    assert_false "$f is not a CA key" grep -qF "$(pub_of "$f" | sha256sum)" <<<"$pubs"
  done < <(find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -type f -name '*.key' -print | grep -v -e 'privacy.key' -e 'proof.key' -e 'source.key')
  assert_true "workload CA cert in _shared" test -s "$RUN_ROOT/_shared/ca-workload.crt"
  assert_eq "" "$(find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -type f -name 'ca*.key' -print)" "no CA key outside .ca"
}

no_secret_appears_in_stdout() {
  local f
  assert_false "no PRIVATE KEY in output" grep -q 'PRIVATE KEY' "$STAGE1_OUT"
  for f in "$RUN_ROOT"/actor-c001-1/remoting.pass "$RUN_ROOT"/actor-c002-1/remoting.pass "$RUN_ROOT"/local-platform/platform.pass \
    "$RUN_ROOT"/kube-api-c001/kube-token "$RUN_ROOT"/kube-api-c002/kube-token; do
    assert_false "output leaks $(basename "$f")" grep -qF -- "$(cat "$f")" "$STAGE1_OUT"
  done
  local v
  while IFS='=' read -r _ v; do
    assert_false "output leaks a db password" grep -qF -- "$v" "$STAGE1_OUT"
  done <"$RUN_ROOT/_shared/db-passwords.env"
  assert_true "output names files" grep -q 'actor-c001-1/leaf.crt' "$STAGE1_OUT"
  assert_true "output carries the LOCAL_TEST_ONLY marker" grep -q 'LOCAL_TEST_ONLY' "$STAGE1_OUT"
}

second_stage1_run_is_refused() {
  local before after rc out
  before="$(find "$RUN_ROOT" -type f -exec sha256sum {} + | sort | sha256sum)"
  set +e
  out="$(stage1 2>&1)"
  rc=$?
  set -e
  assert_true "refused with non-zero status" test "$rc" -ne 0
  assert_true "refusal message" grep -q 'PROVISION_REFUSED volume not empty' <<<"$out"
  after="$(find "$RUN_ROOT" -type f -exec sha256sum {} + | sort | sha256sum)"
  assert_eq "$before" "$after" "refused run changed nothing"
}

# Stage 2 itself needs the PostgreSQL servers (test_db.sh); here only its guards, which act
# before any connection: no stage 1, an earlier stage 2, and `all` on a provisioned volume.
stage2_guards_refuse_before_any_work() {
  local rc empty="$TMP/empty-run"
  mkdir -p "$empty"
  set +e
  RUN_ROOT="$empty" bash "$prov/provision.sh" stage2 >/dev/null 2>"$TMP/s2.err"
  rc=$?
  set -e
  assert_eq "1" "$rc" "stage2 before stage1 exit"
  assert_true "stage2 before stage1 message" grep -q 'PROVISION_REFUSED stage 2 needs a completed stage 1' "$TMP/s2.err"
  assert_eq "0" "$(find "$empty" -mindepth 1 | wc -l | tr -d ' ')" "stage2 before stage1 wrote nothing"
  : >"$RUN_ROOT/_shared/.stage2-done"
  set +e
  bash "$prov/provision.sh" stage2 >/dev/null 2>"$TMP/s2.err"
  rc=$?
  set -e
  assert_eq "1" "$rc" "second stage2 exit"
  assert_true "second stage2 message" grep -q 'PROVISION_REFUSED stage 2 already done (_shared/.stage2-done exists)' "$TMP/s2.err"
  assert_true "marker kept" test -e "$RUN_ROOT/_shared/.stage2-done"
  rm -f "$RUN_ROOT/_shared/.stage2-done"
  set +e
  bash "$prov/provision.sh" all >/dev/null 2>"$TMP/all.err"
  rc=$?
  set -e
  assert_eq "1" "$rc" "all on a provisioned volume exit"
  assert_true "all runs stage 1 first, which refuses" grep -q 'PROVISION_REFUSED volume not empty' "$TMP/all.err"
  set +e
  bash "$prov/provision.sh" bogus >/dev/null 2>&1
  rc=$?
  set -e
  assert_true "unknown stage is an error" test "$rc" -ne 0
}

# Compose starts the exited one-shot provisioner containers again on every `docker compose up`,
# so both stages take --if-needed: a completed stage is skipped with exit 0 and changes nothing;
# everything else (foreign directory, missing stage 1) is still refused.
if_needed_skips_completed_stages() {
  local before after rc out
  before="$(find "$RUN_ROOT" -type f -exec sha256sum {} + | sort | sha256sum)"
  set +e
  out="$(bash "$prov/provision.sh" stage1 --if-needed 2>&1)"
  rc=$?
  set -e
  assert_eq "0" "$rc" "stage1 --if-needed on a provisioned volume exit"
  assert_true "stage1 skip message" grep -q 'PROVISION_SKIPPED stage 1 already done' <<<"$out"
  after="$(find "$RUN_ROOT" -type f -exec sha256sum {} + | sort | sha256sum)"
  assert_eq "$before" "$after" "skipped stage 1 changed nothing"

  : >"$RUN_ROOT/_shared/.stage2-done"
  set +e
  out="$(bash "$prov/provision.sh" stage2 --if-needed 2>&1)"
  rc=$?
  set -e
  assert_eq "0" "$rc" "stage2 --if-needed after a completed stage 2 exit"
  assert_true "stage2 skip message" grep -q 'PROVISION_SKIPPED stage 2 already done' <<<"$out"
  assert_true "stage2 marker kept" test -e "$RUN_ROOT/_shared/.stage2-done"
  rm -f "$RUN_ROOT/_shared/.stage2-done"

  local empty="$TMP/empty-if-needed" foreign="$TMP/foreign-if-needed"
  mkdir -p "$empty" "$foreign"
  : >"$foreign/precious"
  set +e
  RUN_ROOT="$empty" bash "$prov/provision.sh" stage2 --if-needed >/dev/null 2>"$TMP/ifn.err"
  rc=$?
  set -e
  assert_eq "1" "$rc" "stage2 --if-needed before stage 1 is still refused"
  assert_true "stage2 --if-needed refusal names stage 1" grep -q 'PROVISION_REFUSED stage 2 needs a completed stage 1' "$TMP/ifn.err"
  set +e
  RUN_ROOT="$foreign" bash "$prov/provision.sh" stage1 --if-needed >/dev/null 2>&1
  rc=$?
  set -e
  assert_eq "1" "$rc" "stage1 --if-needed still refuses a foreign directory"
  assert_true "precious file survived --if-needed" test -f "$foreign/precious"
  set +e
  PROVISION_FORCE=1 bash "$prov/provision.sh" stage1 --if-needed >/dev/null 2>&1
  rc=$?
  set -e
  assert_eq "64" "$rc" "--if-needed together with PROVISION_FORCE=1 is a usage error"
  set +e
  bash "$prov/provision.sh" stage1 --bogus >/dev/null 2>&1
  rc=$?
  set -e
  assert_eq "64" "$rc" "unknown option is a usage error"
}

force_flag_regenerates() {
  local key_before uid_before key_after uid_after
  key_before="$(sha256sum <"$RUN_ROOT/actor-c001-1/leaf.key")"
  uid_before="$(grep '^ACTOR_C001_1_POD_UID=' "$RUN_ROOT/_shared/pod-uids.env")"
  assert_true "force run succeeds" env PROVISION_FORCE=1 bash "$prov/provision.sh" stage1
  key_after="$(sha256sum <"$RUN_ROOT/actor-c001-1/leaf.key")"
  uid_after="$(grep '^ACTOR_C001_1_POD_UID=' "$RUN_ROOT/_shared/pod-uids.env")"
  assert_true "leaf key regenerated" test "$key_before" != "$key_after"
  assert_true "pod uid regenerated" test "$uid_before" != "$uid_after"
  assert_true ".provisioned marker still present" test -f "$RUN_ROOT/.provisioned"
  assert_true "tree is complete after force" test -s "$RUN_ROOT/kube-api-c002/kube-api.key"
}

refuses_a_foreign_non_empty_directory() {
  local d="$TMP/foreign" rc
  mkdir -p "$d"
  : >"$d/precious"
  set +e
  RUN_ROOT="$d" PROVISION_FORCE=1 bash "$prov/provision.sh" stage1 >/dev/null 2>&1
  rc=$?
  set -e
  assert_true "even with force, a directory we did not create is not wiped" test "$rc" -ne 0
  assert_true "precious file survived" test -f "$d/precious"
}

setup_run
for t in stage1_succeeds service_dirs_cover_all_18_names each_workload_leaf_has_exactly_one_uri_san actor_uri_san_is_per_pod_id \
  gateway_uri_san_carries_gateway_id control_leaf_has_control_uri_and_bootstrap_dns gateway_leaf_has_public_dns_san \
  leaf_has_server_and_client_eku eku_and_key_usage leaf_chains_to_workload_ca pg_server_leafs_carry_dns_and_ip_sans \
  remoting_p12_has_ip_san_and_loads_with_keytool ca_certs_are_path_length_zero issue_leaf_refuses_an_empty_san_list proof_key_is_ed25519_and_pub_matches source_pub_matches_source_key \
  jwt_pub_is_the_identity_public_key platform_p12_loads_with_keytool_and_has_ip_san kube_tokens_differ_per_cell \
  pod_uids_are_unique_lowercase_uuids privacy_key_is_32_bytes_per_cell db_passwords_are_present_and_random \
  no_key_is_world_readable modes_are_0440_files_and_0750_dirs shared_dir_has_no_ca_private_key no_secret_appears_in_stdout \
  second_stage1_run_is_refused stage2_guards_refuse_before_any_work if_needed_skips_completed_stages refuses_a_foreign_non_empty_directory force_flag_regenerates; do
  run_test "$t"
done
finish
