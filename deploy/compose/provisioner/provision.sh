#!/usr/bin/env bash
# Provisioner: provision.sh stage1|stage2|all
#   stage1  PKI, keys, passwords, pod identities, PostgreSQL mount files (this file, lib/pki.sh,
#           lib/dbprep.sh); marker .provisioned
#   stage2  databases (lib/db.sh migrate-and-seed, wal-facts), then the contracts (lib/render.sh
#           render, online); marker _shared/.stage2-done, written last. Needs the five PostgreSQL
#           servers started from the stage-1 mounts and reachable by IP.
#   all     stage1 then stage2
# Writes under $RUN_ROOT (default /run/signaling-src, the root of the `run` volume).
# Logs file names only, never contents. LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=../lib/topology.sh
source "$here/../lib/topology.sh"
# shellcheck source=lib/pki.sh
source "$here/lib/pki.sh"
# shellcheck source=lib/dbprep.sh
source "$here/lib/dbprep.sh"
load_topology

RUN_ROOT="${RUN_ROOT:-/run/signaling-src}"
IDENTITY_PUBLIC_KEY="${IDENTITY_PUBLIC_KEY:-/opt/provisioner/identity-public.pem}"
GROUP_ID="${PROVISION_GROUP_ID:-10001}"
MARKER=".provisioned"
SENTINEL=".stage1-started"
STAGE2_MARKER="_shared/.stage2-done"

DB_SERVICES="pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby pg-directory"
KUBE_SERVICES="kube-api-c001 kube-api-c002"

die() { printf 'provision: %s\n' "$*" >&2; exit 1; }

# --- guards ----------------------------------------------------------------------------

check_run_root() {
  case "$RUN_ROOT" in
    /?*) ;;
    *) die "RUN_ROOT must be an absolute path other than / (got an unusable value)" ;;
  esac
  [ -d "$RUN_ROOT" ] || mkdir -p "$RUN_ROOT"
}

# wipe_children: empty the volume root (never the root itself, which may be a mount point).
wipe_children() {
  find "${RUN_ROOT:?}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
}

guard_volume() {
  local entries
  entries="$(find "$RUN_ROOT" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')"
  if [ "$entries" -eq 0 ]; then
    return 0
  fi
  if [ -e "$RUN_ROOT/$MARKER" ]; then
    if [ "${PROVISION_FORCE:-0}" = "1" ]; then
      pki_log "PROVISION_FORCE=1: discarding the provisioned volume contents"
      wipe_children
      return 0
    fi
    printf 'PROVISION_REFUSED volume not empty (%s exists); only `make reset` (PROVISION_FORCE=1) may regenerate\n' "$MARKER" >&2
    exit 1
  fi
  if [ -e "$RUN_ROOT/$SENTINEL" ]; then
    pki_log "discarding the leftovers of an interrupted earlier stage 1"
    wipe_children
    return 0
  fi
  printf 'PROVISION_REFUSED volume not empty and not created by this provisioner; refusing to touch it\n' >&2
  exit 1
}

# --- layout ----------------------------------------------------------------------------

all_dirs() {
  service_list
  local d
  for d in $DB_SERVICES $KUBE_SERVICES; do printf '%s\n' "$d"; done
}

plane_list() { service_list | grep -E '^(actor-|gateway-|control$)'; }
actor_list() { service_list | grep '^actor-'; }

# finalize_modes: files 0440, dirs 0750, root:10001 (best effort when not root); .ca 0700/0400.
finalize_modes() {
  find "$RUN_ROOT" -path "$CA_DIR" -prune -o -type d -exec chmod 0750 {} +
  find "$RUN_ROOT" -path "$CA_DIR" -prune -o -type f -exec chmod 0440 {} +
  find "$CA_DIR" -type d -exec chmod 0700 {} +
  find "$CA_DIR" -type f -exec chmod 0400 {} +
  if [ "$(id -u)" -eq 0 ]; then
    find "$RUN_ROOT" -path "$CA_DIR" -prune -o -exec chown "root:$GROUP_ID" {} +
    chown -R root:root "$CA_DIR"
  else
    pki_log "NOTE: not running as root; skipping chown to root:$GROUP_ID (test runs only)"
  fi
}

# --- stage 1 ---------------------------------------------------------------------------

stage1() {
  check_run_root
  guard_volume
  umask 077
  : >"$RUN_ROOT/$SENTINEL"
  pki_log "stage1 start (LOCAL_TEST_ONLY / NOT_QUALIFIED)"

  [ -r "$IDENTITY_PUBLIC_KEY" ] || die "identity public key is not readable (IDENTITY_PUBLIC_KEY)"

  local d s cell cu ipv
  for d in $(all_dirs); do mkdir -p "$RUN_ROOT/$d"; done
  mkdir -p "$RUN_ROOT/_shared/admin"
  pki_init
  local tmp="$CA_DIR/tmp"

  # certificate authorities
  new_ca workload "LOCAL_TEST_ONLY CA-workload"
  new_ca db "LOCAL_TEST_ONLY CA-db"
  new_ca kube "LOCAL_TEST_ONLY CA-kube"
  new_ca admin "LOCAL_TEST_ONLY CA-admin"
  for cell in $CELLS; do new_ca "remoting-$cell" "LOCAL_TEST_ONLY CA-remoting-$cell"; done
  for d in workload db kube admin; do cp "$CA_DIR/$d.crt" "$RUN_ROOT/_shared/ca-$d.crt"; done
  for cell in $CELLS; do cp "$CA_DIR/remoting-$cell.crt" "$RUN_ROOT/_shared/ca-remoting-$cell.crt"; done
  cp "$CA_DIR/admin.crt" "$RUN_ROOT/_shared/admin/admin-ca.crt"

  # signing keys: one proof key per cell, one source key
  for cell in $CELLS; do
    new_ed25519 "$tmp/proof-$cell.key" "$tmp/proof-$cell.pub"
  done
  new_ed25519 "$RUN_ROOT/local-platform/source.key" "$RUN_ROOT/local-platform/source.pub"
  pki_log "created proof and source signing keys"

  # per-cell secrets: remoting password, privacy pseudonym key, kube bearer token
  for cell in $CELLS; do
    write_secret "$tmp/remoting-$cell.pass" "$(rand_hex 24)"
    head -c 32 /dev/urandom >"$tmp/privacy-$cell.key"
    write_secret "$tmp/kube-token-$cell" "$(rand_hex 32)"
    make_truststore "$CA_DIR/remoting-$cell.crt" "remoting-ca-$cell" "$tmp/remoting-$cell.pass" "$tmp/trust-$cell.p12"
  done

  # --- planes (actors, gateways, control) ---
  for s in $(plane_list); do
    d="$RUN_ROOT/$s"
    cp "$CA_DIR/workload.crt" "$d/ca.crt"
    issue_leaf workload "$s" "$(workload_san "$s")" "$d/leaf.key" "$d/leaf.crt"
    cp "$RUN_ROOT/local-platform/source.pub" "$d/source.pub"
    cp "$IDENTITY_PUBLIC_KEY" "$d/jwt.pub"
    cp "$CA_DIR/db.crt" "$d/db-ca.crt"
    for cell in $CELLS; do cp "$tmp/proof-$cell.pub" "$d/proof-$cell.pub"; done
    case "$s" in
      control) cell="c001" ;;
      *) cell="$(cell_of_service "$s")" ;;
    esac
    cp "$tmp/proof-$cell.pub" "$d/proof.pub"
    pki_log "wrote $s/leaf.crt $s/leaf.key $s/ca.crt $s/proof.pub $s/source.pub $s/jwt.pub $s/db-ca.crt"
  done

  # --- actors: remoting stores, kube access, privacy and proof keys ---
  for s in $(actor_list); do
    d="$RUN_ROOT/$s"
    cell="$(cell_of_service "$s")"
    issue_leaf "remoting-$cell" "$s-remoting" "IP:$(ip_of "$s" cell)" "$tmp/remoting.key" "$tmp/remoting.crt"
    cp "$tmp/remoting-$cell.pass" "$d/remoting.pass"
    make_keystore "$tmp/remoting.key" "$tmp/remoting.crt" "$CA_DIR/remoting-$cell.crt" remoting "$d/remoting.pass" "$d/remoting.p12"
    cp "$tmp/trust-$cell.p12" "$d/trust.p12"
    rm -f "$tmp/remoting.key" "$tmp/remoting.crt"
    cp "$CA_DIR/kube.crt" "$d/kube-ca.crt"
    cp "$tmp/kube-token-$cell" "$d/kube-token"
    cp "$tmp/privacy-$cell.key" "$d/privacy.key"
    cp "$tmp/proof-$cell.key" "$d/proof.key"
    pki_log "wrote $s/remoting.p12 $s/trust.p12 $s/remoting.pass $s/kube-ca.crt $s/kube-token $s/privacy.key $s/proof.key"
  done

  # --- fake kube-API, one per cell ---
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    d="$RUN_ROOT/kube-api-$cell"
    ipv="KUBE_${cu}_IP"
    cp "$CA_DIR/kube.crt" "$d/kube-ca.crt"
    issue_leaf kube "kube-api-$cell" "IP:${!ipv}" "$d/kube-api.key" "$d/kube-api.crt"
    cp "$tmp/kube-token-$cell" "$d/kube-token"
    pki_log "wrote kube-api-$cell/kube-ca.crt kube-api-$cell/kube-api.crt kube-api-$cell/kube-api.key kube-api-$cell/kube-token"
  done

  # --- PostgreSQL servers ---
  for s in $DB_SERVICES; do
    d="$RUN_ROOT/$s"
    case "$s" in
      pg-directory) ipv="$(ip_of "$s" shared)" ;;
      *) ipv="$(ip_of "$s" cell)" ;;
    esac
    issue_leaf db "$s" "DNS:$s,DNS:localhost,IP:$ipv,IP:127.0.0.1" "$d/server.key" "$d/server.crt"
    cp "$CA_DIR/db.crt" "$d/db-ca.crt"
    pki_log "wrote $s/server.crt $s/server.key $s/db-ca.crt"
  done

  # --- local-platform: source/admin listener store, trust anchors ---
  d="$RUN_ROOT/local-platform"
  cp "$CA_DIR/workload.crt" "$d/ca.crt"
  cp "$CA_DIR/workload.crt" "$d/source-client-trust.crt"
  cp "$CA_DIR/admin.crt" "$d/admin-ca.crt"
  cp "$CA_DIR/db.crt" "$d/db-ca.crt"
  write_secret "$d/platform.pass" "$(rand_hex 24)"
  issue_leaf workload "local-platform" "DNS:local-platform.${DOMAIN:?},IP:$(ip_of local-platform shared)" \
    "$tmp/platform.key" "$tmp/platform.crt"
  make_keystore "$tmp/platform.key" "$tmp/platform.crt" "$CA_DIR/workload.crt" platform "$d/platform.pass" "$d/platform.p12"
  rm -f "$tmp/platform.key" "$tmp/platform.crt"
  pki_log "wrote local-platform/platform.p12 local-platform/platform.pass local-platform/source.key local-platform/source.pub local-platform/ca.crt local-platform/source-client-trust.crt local-platform/admin-ca.crt local-platform/db-ca.crt"

  # --- admin client (smoke and local-platform's admin trust), single URI SAN ---
  issue_leaf admin "admin-smoke" "URI:$SPIFFE_PREFIX/admin/smoke" \
    "$RUN_ROOT/_shared/admin/admin-client.key" "$RUN_ROOT/_shared/admin/admin-client.crt"
  pki_log "wrote _shared/admin/admin-ca.crt _shared/admin/admin-client.crt _shared/admin/admin-client.key"

  # --- database passwords ---
  {
    printf 'DB_PASSWORD_OWNER=%s\n' "$(rand_hex 24)"
    printf 'DB_PASSWORD_APP=%s\n' "$(rand_hex 24)"
    printf 'DB_PASSWORD_NODIR=%s\n' "$(rand_hex 24)"
    printf 'DB_PASSWORD_REPLICATOR=%s\n' "$(rand_hex 24)"
    printf 'DB_PASSWORD_SOURCE_OWNER=%s\n' "$(rand_hex 24)"
  } >"$RUN_ROOT/_shared/db-passwords.env"
  pki_log "wrote _shared/db-passwords.env"

  # --- PostgreSQL mounts: start.sh, hba.conf, password files (the servers start before stage 2) ---
  for s in $DB_SERVICES; do prepare_pg_mount "$s"; done

  # --- pod identities and gateway boot ids (generated once, here) ---
  : >"$RUN_ROOT/_shared/pod-uids.env"
  for s in $(service_list); do
    printf '%s_POD_UID=%s\n' "$(env_name "$s")" "$(new_uuid)" >>"$RUN_ROOT/_shared/pod-uids.env"
  done
  : >"$RUN_ROOT/_shared/gateway-boots.env"
  for cell in $CELLS; do
    printf 'GATEWAY_BOOT_%s=%s\n' "$(upper "$cell")" "$(new_uuid)" >>"$RUN_ROOT/_shared/gateway-boots.env"
  done
  pki_log "wrote _shared/pod-uids.env _shared/gateway-boots.env"

  rm -rf "${tmp:?}"
  printf 'LOCAL_TEST_ONLY stage1 complete %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$RUN_ROOT/$MARKER"
  rm -f "$RUN_ROOT/$SENTINEL"
  finalize_modes
  pki_log "stage1 done: $(find "$RUN_ROOT" -type f | wc -l | tr -d ' ') files (LOCAL_TEST_ONLY / NOT_QUALIFIED)"
}

# --- stage 2 ---------------------------------------------------------------------------

# stage2: refuses before stage 1 is complete, and refuses a second run (its own marker,
# independent of .provisioned) unless PROVISION_FORCE=1. The marker is removed before any work
# and written last, so a failed run never looks complete.
stage2() {
  check_run_root
  if [ ! -f "$RUN_ROOT/$MARKER" ] || [ -e "$RUN_ROOT/$SENTINEL" ]; then
    printf 'PROVISION_REFUSED stage 2 needs a completed stage 1 (%s missing or stage 1 interrupted)\n' "$MARKER" >&2
    exit 1
  fi
  if [ -e "$RUN_ROOT/$STAGE2_MARKER" ]; then
    if [ "${PROVISION_FORCE:-0}" != "1" ]; then
      printf 'PROVISION_REFUSED stage 2 already done (%s exists); PROVISION_FORCE=1 runs it again\n' "$STAGE2_MARKER" >&2
      exit 1
    fi
    pki_log "PROVISION_FORCE=1: running stage 2 again"
  fi
  rm -f "$RUN_ROOT/$STAGE2_MARKER"
  pki_log "stage2 start (LOCAL_TEST_ONLY / NOT_QUALIFIED)"
  bash "$here/lib/db.sh" migrate-and-seed || die "stage 2 failed: databases (cause above)"
  bash "$here/lib/db.sh" wal-facts || die "stage 2 failed: WAL facts (cause above)"
  bash "$here/lib/render.sh" render || die "stage 2 failed: contract rendering (cause above)"
  local tmp="$RUN_ROOT/$STAGE2_MARKER.tmp"
  printf 'LOCAL_TEST_ONLY stage2 complete %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$tmp"
  chmod 0440 "$tmp"
  [ "$(id -u)" -ne 0 ] || chown "root:$GROUP_ID" "$tmp"
  mv -f "$tmp" "$RUN_ROOT/$STAGE2_MARKER"
  pki_log "stage2 done (LOCAL_TEST_ONLY / NOT_QUALIFIED)"
}

case "${1:-}" in
  stage1) stage1 ;;
  stage2) stage2 ;;
  all)
    stage1
    stage2
    ;;
  *)
    printf 'usage: provision.sh stage1|stage2|all\n' >&2
    exit 64
    ;;
esac
