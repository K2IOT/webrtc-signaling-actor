#!/usr/bin/env bash
# Contract renderer (lib/render.sh) against a temporary RUN_ROOT produced by stage 1.
# Runs on the host with its openssl/keytool/jq; no Docker. The Java side
# (ComposeRenderedContractsTest) feeds the same output through the real loaders.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
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
RENDER="$prov/lib/render.sh"
OUT="$TMP/render.out"

planes() { service_list | grep -E '^(actor-|gateway-|control$)'; }
actors() { service_list | grep '^actor-'; }
cell_of() {
  case "$1" in
    control) printf 'c001' ;;
    gateway-*) printf '%s' "${1#gateway-}" ;;
    *) local c="${1#actor-}"; printf '%s' "${c%-*}" ;;
  esac
}
up() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | tr '-' '_'; }
kv() { grep "^$2=" "$1" | head -n 1 | cut -d= -f2-; }
j() { jq -r "$2" "$1"; }
render() { bash "$RENDER" render "$@"; }
rendered_digest() {
  find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -type f \
    \( -name provider.json -o -name process.json -o -name runtime.yaml -o -name directory.json \
    -o -name launch.sh -o -name env.list -o -name pod.env -o -name platform.json -o -name db.pass \) -print0 |
    sort -z | xargs -0 sha256sum | sha256sum
}

setup_run() {
  set +e
  bash "$prov/provision.sh" stage1 >"$TMP/stage1.out" 2>&1
  STAGE1_RC=$?
  render --offline >"$OUT" 2>&1
  RENDER_RC=$?
  set -e
}

render_offline_succeeds() {
  assert_eq "0" "$STAGE1_RC" "stage1 exit status"
  assert_eq "0" "$RENDER_RC" "render --offline exit status"
  assert_true "offline note on stderr names the placeholders" grep -q 'RENDER_OFFLINE' "$OUT"
  assert_true ".offline marker written" test -f "$RUN_ROOT/.offline"
  assert_true ".rendered marker after a successful render" test -f "$RUN_ROOT/.rendered"
}

every_plane_has_its_rendered_files() {
  local s f
  for s in $(planes); do
    for f in provider.json process.json runtime.yaml directory.json launch.sh env.list pod.env; do
      assert_true "$s/$f exists" test -s "$RUN_ROOT/$s/$f"
    done
  done
  assert_true "local-platform/platform.json" test -s "$RUN_ROOT/local-platform/platform.json"
  assert_true "local-platform/db.pass" test -s "$RUN_ROOT/local-platform/db.pass"
  assert_true "local-platform/env.list" test -s "$RUN_ROOT/local-platform/env.list"
  assert_false "identity has no contract" test -e "$RUN_ROOT/identity/provider.json"
  assert_false "local-platform has no plane contract" test -e "$RUN_ROOT/local-platform/provider.json"
}

json_is_valid_everywhere() {
  local s f
  for s in $(planes); do
    for f in provider.json process.json directory.json; do
      assert_true "$s/$f parses" jq -e . "$RUN_ROOT/$s/$f"
    done
  done
  assert_true "platform.json parses" jq -e . "$RUN_ROOT/local-platform/platform.json"
}

default_mount_prefix_is_run_signaling() {
  assert_eq "/run/signaling/directory.json" "$(j "$RUN_ROOT/actor-c001-1/provider.json" .directorySnapshot)" "snapshot path"
  assert_eq "/run/signaling/kube-token" "$(j "$RUN_ROOT/actor-c002-4/process.json" .kube.tokenPath)" "kube token path"
  assert_eq "/run/signaling/source.key" "$(j "$RUN_ROOT/local-platform/platform.json" .signingKey)" "platform signing key path"
  assert_true "cell db sslrootcert in the mount" grep -q 'sslrootcert=/run/signaling/db-ca.crt' \
    <(j "$RUN_ROOT/gateway-c002/provider.json" .cellDatabase.jdbcUrl)
}

contract_values_per_plane() {
  local s c cu f
  for s in $(planes); do
    c="$(cell_of "$s")"
    cu="$(up "$c")"
    f="$RUN_ROOT/$s/provider.json"
    assert_eq "$c" "$(j "$f" .cell)" "$s cell"
    assert_eq "$ISSUER" "$(j "$f" .identity.issuer)" "$s issuer"
    assert_eq "$AUDIENCE" "$(j "$f" .identity.audience)" "$s audience"
    local pg="PG_${cu}_PRIMARY_IP"
    assert_eq "jdbc:postgresql://${!pg}:5432/signaling?sslmode=verify-full&sslrootcert=/run/signaling/db-ca.crt" \
      "$(j "$f" .cellDatabase.jdbcUrl)" "$s cell database url"
    assert_eq "signaling_app" "$(j "$f" .cellDatabase.username)" "$s cell database user"
    assert_eq "jdbc:postgresql://$PG_DIRECTORY_IP:5432/directory?sslmode=verify-full&sslrootcert=/run/signaling/db-ca.crt" \
      "$(j "$f" .directoryDatabase.jdbcUrl)" "$s directory url"
    assert_eq "c001 c002" "$(j "$f" '[.cells[].cell] | join(" ")')" "$s enrolls both cells"
    assert_eq "$ACTOR_C001_1_SHARED_IP:$RPC_PORT actor.c001.signaling.local" \
      "$(j "$f" '.cells[0] | "\(.host):\(.port) \(.tlsAuthority)"')" "$s c001 target"
    assert_eq "$ACTOR_C002_1_SHARED_IP:$RPC_PORT actor.c002.signaling.local" \
      "$(j "$f" '.cells[1] | "\(.host):\(.port) \(.tlsAuthority)"')" "$s c002 target"
    assert_eq "c001/$GATEWAY_ID_C001@$GATEWAY_C001_IP:$RELAY_PORT c002/$GATEWAY_ID_C002@$GATEWAY_C002_IP:$RELAY_PORT" \
      "$(j "$f" '[.gateways[] | "\(.cell)/\(.gatewayId)@\(.host):\(.port)"] | join(" ")')" "$s enrolls both gateways"
    assert_eq "https://$PLATFORM_IP:$SOURCE_PORT/v1/clock-bound" "$(j "$f" .sources.clockUrl)" "$s clock url"
    assert_eq "https://$PLATFORM_IP:$SOURCE_PORT/v1/revocations" "$(j "$f" .sources.revocationUrl)" "$s revocation url"
    assert_eq "local-source" "$(j "$f" '.sources.publicKeys | keys | join(",")')" "$s source key id"
    assert_eq "c001/local,c002/local" "$(j "$f" '.proofs.publicKeys | keys | join(",")')" "$s proof keys"
    assert_eq "signaling-identity-key" "$(j "$f" '.jwtPublicKeys | keys | join(",")')" "$s jwt kid"
  done
}

directory_credentials_per_plane() {
  local s f pw_app pw_nodir
  pw_app="$(kv "$RUN_ROOT/_shared/db-passwords.env" DB_PASSWORD_APP)"
  pw_nodir="$(kv "$RUN_ROOT/_shared/db-passwords.env" DB_PASSWORD_NODIR)"
  for s in $(planes); do
    f="$RUN_ROOT/$s/provider.json"
    assert_eq "$pw_app" "$(j "$f" .cellDatabase.password)" "$s cell database password"
    if [ "$s" = control ]; then
      assert_eq "signaling_app" "$(j "$f" .directoryDatabase.username)" "control directory user"
      assert_eq "$pw_app" "$(j "$f" .directoryDatabase.password)" "control directory password"
    else
      assert_eq "signaling_nodir" "$(j "$f" .directoryDatabase.username)" "$s directory user (powerless)"
      assert_eq "$pw_nodir" "$(j "$f" .directoryDatabase.password)" "$s directory password"
    fi
  done
}

control_transfers_to_the_other_cell() {
  local f="$RUN_ROOT/control/process.json"
  assert_eq "CONTROL" "$(j "$f" .plane)" "control plane"
  assert_eq "1" "$(j "$f" '.transferCells | length')" "exactly one transfer cell"
  assert_eq "c002" "$(j "$f" '.transferCells[0].cell')" "transfer cell"
  assert_eq "jdbc:postgresql://$PG_C002_PRIMARY_IP:5432/signaling?sslmode=verify-full&sslrootcert=/run/signaling/db-ca.crt" \
    "$(j "$f" '.transferCells[0].database.jdbcUrl')" "transfer database url"
  assert_eq "signaling_app" "$(j "$f" '.transferCells[0].database.username')" "transfer database user"
  assert_eq "$CONTROL_IP" "$(j "$f" .podIp)" "control pod ip"
}

process_profiles_have_every_key() {
  local s keys
  keys="allowedOrigins azRole businessPort cellAzRoles compatibility gatewayBootId gatewayId healthPort kube plane podIp podUid privacyPseudonymKey relayPort schemaVersion transferCells wal"
  for s in $(planes); do
    assert_eq "$keys" "$(j "$RUN_ROOT/$s/process.json" 'keys | join(" ")')" "$s profile key set"
  done
}

actor_profiles() {
  local s c cu f az kube ns
  for s in $(actors); do
    c="$(cell_of "$s")"
    cu="$(up "$c")"
    f="$RUN_ROOT/$s/process.json"
    az="$(up "$s")_AZ"
    kube="KUBE_${cu}_IP"
    ns="KUBE_${cu}_NAMESPACE"
    assert_eq "ACTOR" "$(j "$f" .plane)" "$s plane"
    assert_eq "$(ip_of "$s" cell)" "$(j "$f" .podIp)" "$s pod ip is the cell-net address"
    assert_eq "${!az}" "$(j "$f" .azRole)" "$s az role"
    assert_eq "az-a,az-b" "$(j "$f" '.cellAzRoles | join(",")')" "$s cell az roles"
    assert_eq "${!kube}" "$(j "$f" .kube.apiHost)" "$s kube host"
    assert_eq "443" "$(j "$f" .kube.apiPort)" "$s kube port"
    assert_eq "${!ns}" "$(j "$f" .kube.namespace)" "$s namespace"
    assert_eq "/run/signaling/privacy.key" "$(j "$f" .privacyPseudonymKey)" "$s privacy key path"
    assert_eq "standby-$c" "$(j "$f" '.wal.replicas | join(",")')" "$s wal replicas"
    assert_eq "1 1" "$(j "$f" '"\(.wal.systemIdentifier) \(.wal.timeline)"')" "$s offline wal placeholders"
    assert_eq "null null" "$(j "$f" '"\(.gatewayId) \(.gatewayBootId)"')" "$s has no gateway identity"
    assert_eq "0 0" "$(j "$f" '"\(.transferCells | length) \(.allowedOrigins | length)"')" "$s no transfer, no origins"
  done
}

gateway_profiles() {
  local c cu f id boot
  for c in c001 c002; do
    cu="$(up "$c")"
    f="$RUN_ROOT/gateway-$c/process.json"
    id="GATEWAY_ID_$cu"
    boot="$(kv "$RUN_ROOT/_shared/gateway-boots.env" "GATEWAY_BOOT_$cu")"
    assert_eq "GATEWAY" "$(j "$f" .plane)" "gateway-$c plane"
    assert_eq "${!id}" "$(j "$f" .gatewayId)" "gateway-$c id"
    assert_eq "$boot" "$(j "$f" .gatewayBootId)" "gateway-$c boot"
    assert_eq "$ORIGIN" "$(j "$f" '.allowedOrigins | join(",")')" "gateway-$c origins"
    assert_eq "$(ip_of "gateway-$c" shared)" "$(j "$f" .podIp)" "gateway-$c pod ip"
    assert_eq "$RPC_PORT $HEALTH_PORT $RELAY_PORT" "$(j "$f" '"\(.businessPort) \(.healthPort) \(.relayPort)"')" "gateway-$c ports"
    assert_eq "null" "$(j "$f" .kube)" "gateway-$c has no kube"
  done
}

pod_uids_reach_each_service_dir() {
  local s uid
  for s in $(planes); do
    uid="$(kv "$RUN_ROOT/_shared/pod-uids.env" "$(up "$s")_POD_UID")"
    assert_eq "$uid" "$(j "$RUN_ROOT/$s/process.json" .podUid)" "$s profile pod uid"
    assert_eq "SIGNALING_POD_UID=$uid" "$(cat "$RUN_ROOT/$s/pod.env")" "$s pod.env"
    assert_eq "$uid" "$(kv "$RUN_ROOT/$s/env.list" SIGNALING_POD_UID)" "$s env.list pod uid"
    assert_true "$s launch.sh exports the literal pod uid" grep -qx "export SIGNALING_POD_UID=$uid" "$RUN_ROOT/$s/launch.sh"
  done
}

snapshot_is_shared_and_pinned_by_digest() {
  local s first sum
  first="$(sha256sum <"$RUN_ROOT/actor-c001-1/directory.json" | cut -d' ' -f1)"
  for s in $(planes); do
    sum="$(sha256sum <"$RUN_ROOT/$s/directory.json" | cut -d' ' -f1)"
    assert_eq "$first" "$sum" "$s snapshot bytes identical"
    assert_eq "$sum" "$(j "$RUN_ROOT/$s/provider.json" .directorySnapshotSha256)" "$s snapshot digest"
  done
  local d="$RUN_ROOT/actor-c001-1/directory.json"
  assert_eq "0-$((SPLIT - 1)):c001 $SPLIT-16383:c002" \
    "$(j "$d" '[.buckets[] | "\(.first)-\(.last):\(.cell)"] | join(" ")')" "bucket split"
  assert_eq "c001/$GATEWAY_ID_C001=$(kv "$RUN_ROOT/_shared/gateway-boots.env" GATEWAY_BOOT_C001) c002/$GATEWAY_ID_C002=$(kv "$RUN_ROOT/_shared/gateway-boots.env" GATEWAY_BOOT_C002)" \
    "$(j "$d" '[.gatewayBoots[] | "\(.cell)/\(.gatewayId)=\(.bootId)"] | join(" ")')" "gateway boots"
}

env_list_matches_contract() {
  local s c f e plane
  for s in $(planes); do
    c="$(cell_of "$s")"
    f="$RUN_ROOT/$s/provider.json"
    e="$RUN_ROOT/$s/env.list"
    plane="$(j "$RUN_ROOT/$s/process.json" .plane | tr '[:upper:]' '[:lower:]')"
    assert_eq "$plane" "$(kv "$e" SPRING_PROFILES_ACTIVE)" "$s spring profile"
    assert_eq "$(j "$f" .cell)" "$(kv "$e" SIGNALING_CELL_ID)" "$s env cell"
    assert_eq "$(j "$f" .identity.issuer)" "$(kv "$e" SIGNALING_IDENTITY_ISSUER)" "$s env issuer"
    assert_eq "$(j "$f" .identity.audience)" "$(kv "$e" SIGNALING_IDENTITY_AUDIENCE)" "$s env audience"
    assert_eq "$CLUSTER_FINGERPRINT" "$(kv "$e" SIGNALING_CLUSTER_FINGERPRINT)" "$s fingerprint"
    assert_eq "local-minikube" "$(kv "$e" SIGNALING_DEPLOYMENT_MODE)" "$s mode"
    assert_eq "LOCAL_TEST_ONLY" "$(kv "$e" SIGNALING_LOCAL_ACKNOWLEDGEMENT)" "$s acknowledgement"
    assert_eq "/run/signaling/provider.json" "$(kv "$e" SIGNALING_NATIVE_PROVIDER_CONTRACT)" "$s contract location"
    assert_eq "/run/signaling/process.json" "$(kv "$e" SIGNALING_LOCAL_PROCESS_PROFILE)" "$s profile location"
    assert_eq "/run/signaling/runtime.yaml" "$(kv "$e" SIGNALING_RUNTIME_CONTRACT)" "$s runtime location"
    assert_false "$s env.list holds no password" grep -qi 'PASSWORD' "$e"
  done
  assert_eq "initial-six" "$(kv "$RUN_ROOT/actor-c002-6/env.list" SIGNALING_FORMATION_PROFILE)" "formation profile"
  assert_eq "$KUBE_C002_IP" "$(kv "$RUN_ROOT/actor-c002-6/env.list" SIGNALING_KUBE_API_HOST)" "kube host env"
  assert_eq "/run/signaling/platform.json" "$(kv "$RUN_ROOT/local-platform/env.list" SIGNALING_PLATFORM_CONFIG)" "platform config env"
}

one_cluster_fingerprint_across_all_planes() {
  local s
  assert_eq "1" "$(for s in $(planes); do kv "$RUN_ROOT/$s/env.list" SIGNALING_CLUSTER_FINGERPRINT; printf '\n'; done |
    sort -u | grep -c .)" "one SIGNALING_CLUSTER_FINGERPRINT over all 15 planes of both cells"
  assert_eq "$CLUSTER_FINGERPRINT" "$(kv "$RUN_ROOT/actor-c002-6/env.list" SIGNALING_CLUSTER_FINGERPRINT)" \
    "the topology value"
}

launch_scripts_export_only_what_they_must() {
  local s f
  for s in $(planes); do
    f="$RUN_ROOT/$s/launch.sh"
    assert_eq "exec java \$JVM_OPTS -jar /opt/signaling/app.jar" "$(tail -n 1 "$f")" "$s launch.sh execs the app"
    assert_eq "set -f" "$(tail -n 2 "$f" | head -n 1)" "$s disables globbing right before the unquoted JVM_OPTS"
    assert_true "$s launch.sh is valid bash" bash -n "$f"
    case "$s" in
      actor-*)
        assert_eq "SIGNALING_KUBE_API_HOST SIGNALING_KUBE_API_PORT SIGNALING_POD_UID SIGNALING_REMOTING_KEYSTORE_PASSWORD SIGNALING_REMOTING_KEY_PASSWORD SIGNALING_REMOTING_TRUSTSTORE_PASSWORD" \
          "$(grep -oE '^export [A-Z_]+' "$f" | cut -d' ' -f2 | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')" "$s exports"
        assert_true "$s reads the password from the mount" grep -q '"$(< /run/signaling/remoting.pass)"' "$f"
        assert_false "$s launch.sh carries no password literal" \
          grep -qF "$(cat "$RUN_ROOT/$s/remoting.pass")" "$f"
        ;;
      *)
        assert_eq "SIGNALING_POD_UID" "$(grep -oE '^export [A-Z_]+' "$f" | cut -d' ' -f2 | tr '\n' ' ' | sed 's/ $//')" "$s exports"
        ;;
    esac
  done
}

launch_script_runs_with_a_fake_java() {
  # The real launch.sh, with /run/signaling rewritten to the temp tree and java on PATH a stub
  # that prints the environment it would receive.
  local s="actor-c001-2" bin="$TMP/bin" out
  mkdir -p "$bin"
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$@"\nenv\n' >"$bin/java"
  chmod +x "$bin/java"
  sed "s#/run/signaling/#$RUN_ROOT/$s/#g" "$RUN_ROOT/$s/launch.sh" >"$TMP/launch.sh"
  out="$(cd "$TMP" && : >"$TMP/-Xglob1" && env -i PATH="$bin:/usr/bin:/bin" JVM_OPTS="-Xmx1m -Dx=y -Xglob*" bash "$TMP/launch.sh")"
  assert_true "jvm options are split into words" grep -qx -- '-Xmx1m' <<<"$out"
  assert_true "a glob in JVM_OPTS stays literal (set -f)" grep -qx -- '-Xglob\*' <<<"$out"
  assert_true "jar is passed" grep -qx -- '/opt/signaling/app.jar' <<<"$out"
  assert_true "keystore password exported" grep -qx "SIGNALING_REMOTING_KEYSTORE_PASSWORD=$(cat "$RUN_ROOT/$s/remoting.pass")" <<<"$out"
  assert_true "truststore password exported" grep -qx "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD=$(cat "$RUN_ROOT/$s/remoting.pass")" <<<"$out"
  assert_true "kube port exported" grep -qx "SIGNALING_KUBE_API_PORT=443" <<<"$out"
  out="$(env -i PATH="$bin:/usr/bin:/bin" bash "$TMP/launch.sh")"
  assert_true "runs without JVM_OPTS" grep -qx -- '-jar' <<<"$out"
}

runtime_yaml_binds_identity() {
  local f="$RUN_ROOT/gateway-c001/runtime.yaml"
  assert_true "issuer bound" grep -qxF "    issuer: \"$ISSUER\"" "$f"
  assert_true "audience bound" grep -qxF "    audience: \"$AUDIENCE\"" "$f"
  assert_eq "$(grep -cv 'issuer:\|audience:\|^#' "$repo/config/production-defaults.yaml")" \
    "$(grep -cv 'issuer:\|audience:\|^#' "$f")" "every other defaults line kept"
  assert_true "sections kept" grep -q '^  rate-limits:' "$f"
}

platform_config_values() {
  local f="$RUN_ROOT/local-platform/platform.json"
  assert_eq "admin cells database health keyId signingKey source" "$(j "$f" 'keys | join(" ")')" "platform keys"
  assert_eq "c001=1 c002=1" "$(j "$f" '.cells | to_entries | map("\(.key)=\(.value)") | join(" ")')" "platform cells"
  assert_eq "local-source" "$(j "$f" .keyId)" "platform key id"
  assert_eq "$PLATFORM_IP:$SOURCE_PORT /run/signaling/platform.p12 /run/signaling/platform.pass /run/signaling/source-client-trust.crt" \
    "$(j "$f" '.source | "\(.bind):\(.port) \(.keystore) \(.keystorePasswordFile) \(.clientTrust)"')" "source listener"
  assert_eq "$PLATFORM_IP:$ADMIN_PORT /run/signaling/platform.p12 /run/signaling/platform.pass /run/signaling/admin-ca.crt" \
    "$(j "$f" '.admin | "\(.bind):\(.port) \(.keystore) \(.keystorePasswordFile) \(.clientTrust)"')" "admin listener"
  assert_eq "$PLATFORM_IP:$HEALTH_PORT" "$(j "$f" '.health | "\(.bind):\(.port)"')" "health listener"
  assert_eq "jdbc:postgresql://$PG_DIRECTORY_IP:5432/source?sslmode=verify-full&sslrootcert=/run/signaling/db-ca.crt source_owner /run/signaling/db.pass" \
    "$(j "$f" '.database | "\(.jdbcUrl) \(.user) \(.passwordFile)"')" "platform database"
  assert_eq "$(kv "$RUN_ROOT/_shared/db-passwords.env" DB_PASSWORD_SOURCE_OWNER)" "$(cat "$RUN_ROOT/local-platform/db.pass")" "db.pass content"
  assert_false "platform.json carries no password" grep -qF "$(cat "$RUN_ROOT/local-platform/db.pass")" "$f"
}

pg_primary_leaf_has_the_url_ip_san() {
  local c ip
  for c in c001 c002; do
    ip="PG_$(up "$c")_PRIMARY_IP"
    assert_true "pg-$c-primary IP SAN ${!ip}" grep -q "IP Address:${!ip}" \
      <(openssl x509 -in "$RUN_ROOT/pg-$c-primary/server.crt" -noout -ext subjectAltName)
  done
  assert_true "pg-directory IP SAN" grep -q "IP Address:$PG_DIRECTORY_IP" \
    <(openssl x509 -in "$RUN_ROOT/pg-directory/server.crt" -noout -ext subjectAltName)
}

# uri_san_count <cert>: the number of URI SANs. The loaders refuse a leaf with a second SPIFFE URI
# SAN (RpcTlsIdentity.extract via LocalTls.rpc -> identity/IDENTITY_MISMATCH; proven through the
# real installer in ComposeRenderedContractsTest); this is the stage-1 side of the same rule.
uri_san_count() { openssl x509 -in "$1" -noout -ext subjectAltName 2>/dev/null | grep -o 'URI:' | wc -l | tr -d ' '; }

leaf_with_a_second_uri_san_is_detected() {
  local s
  for s in $(planes); do
    assert_eq "1" "$(uri_san_count "$RUN_ROOT/$s/leaf.crt")" "$s single URI SAN"
  done
  # The check bites: a leaf with two URI SANs counts 2.
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=TEST_ONLY -days 1 \
    -keyout "$TMP/two.key" -out "$TMP/two.crt" \
    -addext "subjectAltName=URI:spiffe://signaling/local-test-only/cell/c001/actor/a,URI:spiffe://signaling/local-test-only/cell/c001/actor/b" \
    >/dev/null 2>&1
  assert_eq "2" "$(uri_san_count "$TMP/two.crt")" "two URI SANs are counted"
}

# --- rendered names against the stage-1 certificates ---------------------------------------
sans() { openssl x509 -noout -ext subjectAltName "$@" 2>/dev/null | tail -n +2 | tr ',' '\n' | sed 's/^ *//; s/IP Address:/IP:/'; }
url_host() { sed -E 's#^[a-z:]+//([^:/?]+).*#\1#' <<<"$1"; }

rendered_names_match_the_certificates() {
  local s f c n i host auth actor_sans gw kube_ip
  for s in $(planes); do
    f="$RUN_ROOT/$s/provider.json"
    # every actor leaf of a cell carries the DNS name the contract dials that cell by
    for i in 0 1; do
      c="$(j "$f" ".cells[$i].cell")"
      auth="$(j "$f" ".cells[$i].tlsAuthority")"
      for n in 1 2 3 4 5 6; do
        assert_true "$s: actor-$c-$n leaf has DNS:$auth" grep -qx "DNS:$auth" <(sans -in "$RUN_ROOT/actor-$c-$n/leaf.crt")
      done
      gw="$(j "$f" ".gateways[$i].cell")"
      auth="$(j "$f" ".gateways[$i].tlsAuthority")"
      assert_true "$s: gateway-$gw leaf has DNS:$auth" grep -qx "DNS:$auth" <(sans -in "$RUN_ROOT/gateway-$gw/leaf.crt")
    done
    # the source the contract dials is the platform listener's certificate
    host="$(url_host "$(j "$f" .sources.clockUrl)")"
    assert_eq "$host" "$(url_host "$(j "$f" .sources.revocationUrl)")" "$s: one source host"
    assert_true "$s: platform leaf has IP:$host" grep -qx "IP:$host" <(platform_leaf_sans)
    # the databases the contract dials present a certificate for the URL host
    host="$(url_host "$(j "$f" .cellDatabase.jdbcUrl)")"
    assert_true "$s: cell primary leaf has IP:$host" grep -qx "IP:$host" <(sans -in "$RUN_ROOT/pg-$(cell_of "$s")-primary/server.crt")
    host="$(url_host "$(j "$f" .directoryDatabase.jdbcUrl)")"
    assert_true "$s: directory leaf has IP:$host" grep -qx "IP:$host" <(sans -in "$RUN_ROOT/pg-directory/server.crt")
  done
  host="$(url_host "$(j "$RUN_ROOT/control/process.json" '.transferCells[0].database.jdbcUrl')")"
  assert_true "transfer primary leaf has IP:$host" grep -qx "IP:$host" <(sans -in "$RUN_ROOT/pg-c002-primary/server.crt")
  host="$(url_host "$(j "$RUN_ROOT/local-platform/platform.json" .database.jdbcUrl)")"
  assert_true "platform database leaf has IP:$host" grep -qx "IP:$host" <(sans -in "$RUN_ROOT/pg-directory/server.crt")
  # each actor's kube-API address is on its cell's kube-api certificate, which chains to the CA it holds
  for s in $(actors); do
    c="$(cell_of "$s")"
    kube_ip="$(j "$RUN_ROOT/$s/process.json" .kube.apiHost)"
    assert_true "$s: kube-api-$c cert has IP:$kube_ip" grep -qx "IP:$kube_ip" <(sans -in "$RUN_ROOT/kube-api-$c/kube-api.crt")
    assert_true "$s: kube-api-$c cert chains to $s/kube-ca.crt" \
      openssl verify -CAfile "$RUN_ROOT/$s/kube-ca.crt" "$RUN_ROOT/kube-api-$c/kube-api.crt"
  done
}

platform_leaf_sans() {
  openssl pkcs12 -in "$RUN_ROOT/local-platform/platform.p12" -passin "file:$RUN_ROOT/local-platform/platform.pass" \
    -nokeys -clcerts 2>/dev/null | sans
}

no_unresolved_placeholder_is_rendered() {
  assert_false "no \${ in any rendered file" grep -rlF '${' \
    --include=provider.json --include=process.json --include=runtime.yaml --include=directory.json \
    --include=launch.sh --include=env.list --include=pod.env --include=platform.json "$RUN_ROOT"
}

modes_are_0440_files_and_0750_dirs() {
  local bad
  bad="$(find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -type f ! -perm 0440 -print | head -n 3)"
  assert_eq "" "$bad" "every file 0440"
  bad="$(find "$RUN_ROOT" -path "$RUN_ROOT/.ca" -prune -o -mindepth 1 -type d ! -perm 0750 -print | head -n 3)"
  assert_eq "" "$bad" "every directory 0750"
}

no_secret_appears_in_output() {
  local pw
  for pw in $(cut -d= -f2 "$RUN_ROOT/_shared/db-passwords.env") "$(cat "$RUN_ROOT/actor-c001-1/remoting.pass")"; do
    assert_false "secret not on render output" grep -qF "$pw" "$OUT"
  done
  assert_false "no private key on render output" grep -q 'PRIVATE KEY' "$OUT"
}

render_is_deterministic_for_equal_inputs() {
  local before after
  before="$(rendered_digest)"
  assert_true "second offline render succeeds" render --offline
  after="$(rendered_digest)"
  assert_eq "$before" "$after" "byte-identical output for the same stage-1 input"
}

unresolved_placeholder_is_rejected() {
  local before rc
  before="$(rendered_digest)"
  sed "s#^ISSUER=.*#ISSUER='\${SIGNALING_IDENTITY_ISSUER:}'#" "$compose/topology.env" >"$TMP/topology-bad.env"
  set +e
  TOPOLOGY_ENV="$TMP/topology-bad.env" bash "$RENDER" render --offline >"$TMP/bad.out" 2>&1
  rc=$?
  set -e
  assert_true "render refuses an unresolved value" test "$rc" -ne 0
  assert_true "refusal names the key" grep -q 'RENDER_REFUSED.*ISSUER' "$TMP/bad.out"
  assert_eq "$before" "$(rendered_digest)" "nothing was rewritten"
  assert_false "a refused render leaves no .rendered marker" test -e "$RUN_ROOT/.rendered"
  assert_true "a good render restores it" render --offline
  assert_true ".rendered back" test -f "$RUN_ROOT/.rendered"
}

usage_errors() {
  local rc
  set +e
  bash "$RENDER" >/dev/null 2>&1
  rc=$?
  set -e
  assert_eq "64" "$rc" "no command is a usage error"
  set +e
  bash "$RENDER" render --mount-prefix "$TMP/x" >/dev/null 2>&1
  rc=$?
  set -e
  assert_eq "64" "$rc" "--mount-prefix needs --offline"
  set +e
  bash "$RENDER" render --offline --mount-prefix relative >/dev/null 2>&1
  rc=$?
  set -e
  assert_eq "64" "$rc" "--mount-prefix must be absolute"
  local bad
  for bad in "$TMP/a/../b" "$TMP//b" "$TMP/./b"; do
    set +e
    bash "$RENDER" render --offline --mount-prefix "$bad" >/dev/null 2>&1
    rc=$?
    set -e
    assert_eq "64" "$rc" "--mount-prefix $bad is refused"
  done
}

missing_stage1_file_fails_before_writing() {
  local d="$TMP/missing" rc before
  cp -a "$RUN_ROOT" "$d"
  rm -f "$d/gateway-c001/db-ca.crt"
  before="$(RUN_ROOT="$d" rendered_digest)"
  set +e
  RUN_ROOT="$d" bash "$RENDER" render --offline >"$TMP/missing.out" 2>&1
  rc=$?
  set -e
  assert_true "render refuses a partial stage-1 tree" test "$rc" -ne 0
  assert_eq "1" "$(grep -c 'RENDER_REFUSED' "$TMP/missing.out")" "one refusal line"
  assert_true "it names the missing file" grep -q 'RENDER_REFUSED stage-1 files missing.*gateway-c001/db-ca.crt' "$TMP/missing.out"
  assert_eq "$before" "$(RUN_ROOT="$d" rendered_digest)" "nothing was rewritten"
  assert_false "no .rendered marker after a failed render" test -e "$d/.rendered"
  assert_eq "" "$(find "$d" -name '*.render-tmp' -o -name .render-work | head -n 1)" "no temporary file left"
}

refuses_a_volume_without_stage1() {
  local d="$TMP/empty" rc
  mkdir -p "$d"
  set +e
  RUN_ROOT="$d" bash "$RENDER" render --offline >"$TMP/empty.out" 2>&1
  rc=$?
  set -e
  assert_true "render refuses before stage 1" test "$rc" -ne 0
  assert_true "says why" grep -q 'RENDER_REFUSED' "$TMP/empty.out"
}

mount_prefix_rewrites_every_path() {
  local d="$TMP/prefixed" s f
  cp -a "$RUN_ROOT" "$d"
  assert_true "prefixed render succeeds" env RUN_ROOT="$d" bash "$RENDER" render --offline --mount-prefix "$d"
  f="$d/actor-c001-5/provider.json"
  assert_eq "$d/actor-c001-5/directory.json" "$(j "$f" .directorySnapshot)" "snapshot path under prefix"
  assert_eq "$d/actor-c001-5/kube-ca.crt" "$(j "$d/actor-c001-5/process.json" .kube.caPath)" "kube ca under prefix"
  assert_true "sslrootcert under prefix" grep -q "sslrootcert=$d/control/db-ca.crt" <(j "$d/control/provider.json" .cellDatabase.jdbcUrl)
  assert_eq "$d/local-platform/source.key" "$(j "$d/local-platform/platform.json" .signingKey)" "platform path under prefix"
  for s in $(planes); do
    assert_false "$s: no /run/signaling path left" grep -q '/run/signaling' "$d/$s/provider.json" "$d/$s/process.json" "$d/$s/env.list"
  done
}

online_render_needs_wal_facts_and_clears_the_offline_marker() {
  local d="$TMP/online" rc
  cp -a "$RUN_ROOT" "$d"
  set +e
  RUN_ROOT="$d" bash "$RENDER" render >"$TMP/online.out" 2>&1
  rc=$?
  set -e
  assert_true "online render without wal.env is refused" test "$rc" -ne 0
  assert_true "refusal names the WAL facts" grep -q 'RENDER_REFUSED.*wal' "$TMP/online.out"
  assert_true "offline marker kept after a refused online render" test -f "$d/.offline"
  chmod u+w "$d/_shared"
  printf 'WAL_C001_SYSTEM_ID=7412345678901234567\nWAL_C001_TIMELINE=1\nWAL_C002_SYSTEM_ID=7412345678901234999\nWAL_C002_TIMELINE=2\n' \
    >"$d/_shared/wal.env"
  assert_true "online render succeeds with wal.env" env RUN_ROOT="$d" bash "$RENDER" render
  assert_false "offline marker removed" test -e "$d/.offline"
  assert_eq "7412345678901234999 2" "$(j "$d/actor-c002-3/process.json" '"\(.wal.systemIdentifier) \(.wal.timeline)"')" "c002 wal facts"
  assert_eq "7412345678901234567 1" "$(j "$d/actor-c001-3/process.json" '"\(.wal.systemIdentifier) \(.wal.timeline)"')" "c001 wal facts"
  printf 'WAL_C001_SYSTEM_ID=12x\nWAL_C001_TIMELINE=1\nWAL_C002_SYSTEM_ID=1\nWAL_C002_TIMELINE=1\n' >"$d/_shared/wal.env"
  assert_false "malformed wal facts are refused" env RUN_ROOT="$d" bash "$RENDER" render
}

setup_run
for t in render_offline_succeeds every_plane_has_its_rendered_files json_is_valid_everywhere default_mount_prefix_is_run_signaling \
  contract_values_per_plane directory_credentials_per_plane control_transfers_to_the_other_cell process_profiles_have_every_key \
  actor_profiles gateway_profiles pod_uids_reach_each_service_dir snapshot_is_shared_and_pinned_by_digest env_list_matches_contract \
  one_cluster_fingerprint_across_all_planes launch_scripts_export_only_what_they_must launch_script_runs_with_a_fake_java runtime_yaml_binds_identity platform_config_values \
  pg_primary_leaf_has_the_url_ip_san rendered_names_match_the_certificates leaf_with_a_second_uri_san_is_detected no_unresolved_placeholder_is_rendered \
  modes_are_0440_files_and_0750_dirs no_secret_appears_in_output render_is_deterministic_for_equal_inputs \
  unresolved_placeholder_is_rejected missing_stage1_file_fails_before_writing \
  usage_errors refuses_a_volume_without_stage1 mount_prefix_rewrites_every_path \
  online_render_needs_wal_facts_and_clears_the_offline_marker; do
  run_test "$t"
done
finish
