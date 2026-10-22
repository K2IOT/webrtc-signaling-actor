#!/usr/bin/env bash
# Contract renderer: render.sh render [--offline [--mount-prefix <dir>]]
#
# Reads topology.env and the stage-1 tree under $RUN_ROOT and writes, for each of the 15 plane
# services (12 actors, 2 gateways, control), into $RUN_ROOT/<service>/:
#   provider.json  process.json  runtime.yaml  directory.json  env.list  pod.env  launch.sh
# and into $RUN_ROOT/local-platform/: platform.json  db.pass  env.list. identity gets nothing.
# Every JSON document is built by a jq template in ../templates; shell only validates and feeds
# values. Secrets reach jq through its environment, never its argv, and are never printed.
#
#   --offline            WAL facts are placeholders (systemIdentifier 1, timeline 1) instead of
#                        _shared/wal.env (Task 4 writes it once the standbys are in sync). Writes
#                        $RUN_ROOT/.offline, which compose (`make up`) refuses; an online render
#                        removes it. For validation of the rendered files only.
#   --mount-prefix <dir> (offline only) paths become <dir>/<service>/... instead of the in-container
#                        /run/signaling/..., so the real loaders can read the tree in place.
#
# LOCAL_TEST_ONLY / NOT_QUALIFIED. Running it twice on the same stage-1 output gives
# byte-identical files.
set -euo pipefail

here="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
prov="$(cd "$here/.." && pwd)"
compose="$(cd "$prov/.." && pwd)"
# shellcheck source=../../lib/topology.sh
source "$compose/lib/topology.sh"
# shellcheck source=pki.sh
source "$here/pki.sh"

TEMPLATES="$prov/templates"
RUN_ROOT="${RUN_ROOT:-/run/signaling-src}"
GROUP_ID="${PROVISION_GROUP_ID:-10001}"
CONTAINER_MOUNT="/run/signaling"
# Names stage 1 puts into the DNS SANs (pki.sh workload_san): actor.<cell>.<domain>.
DOMAIN="signaling.local"
# Fixed by the compose design, not by topology.env: PostgreSQL and the fake kube-API listen here.
PG_PORT=5432
KUBE_API_PORT=443
CELL_AZ_ROLES='["az-a","az-b"]'
APP_JAR="/opt/signaling/app.jar"

UUID_RE='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
IP_RE='^[0-9]{1,3}(\.[0-9]{1,3}){3}$'
PORT_RE='^[1-9][0-9]{0,4}$'
HEX64_RE='^[0-9a-f]{64}$'
SECRET_RE='^[0-9a-f]{16,128}$'

usage() {
  [ "$#" -eq 0 ] || printf 'render: %s\n' "$*" >&2
  printf 'usage: render.sh render [--offline [--mount-prefix <absolute dir>]]\n' >&2
  exit 64
}

refuse() {
  printf 'RENDER_REFUSED %s\n' "$*" >&2
  exit 1
}

log() { printf 'render: %s\n' "$*"; }

# --- inputs ------------------------------------------------------------------------------

# check <NAME> <regex>: the variable is set, carries no placeholder and matches.
check() {
  local name="$1" re="$2" value
  value="${!name-}"
  [ -n "$value" ] || refuse "$name is empty or unset"
  case "$value" in *'${'*) refuse "unresolved placeholder in $name" ;; esac
  [[ "$value" =~ $re ]] || refuse "$name is malformed"
}

# kv <file> <KEY>: the single value of KEY in a KEY=value file (values are never printed).
kv() {
  local file="$1" key="$2" line value="" n=0
  [ -r "$file" ] || refuse "cannot read ${file#"$RUN_ROOT"/}"
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in "$key="*)
      value="${line#*=}"
      n=$((n + 1))
      ;;
    esac
  done <"$file"
  [ "$n" -eq 1 ] || refuse "$key must appear exactly once in ${file#"$RUN_ROOT"/}"
  printf '%s' "$value"
}

# kv_checked <file> <KEY> <regex> <label>
kv_checked() {
  local value
  value="$(kv "$1" "$2")"
  [[ "$value" =~ $3 ]] || refuse "$2 in ${1#"$RUN_ROOT"/} is malformed"
  printf '%s' "$value"
}

plane_list() { service_list | grep -E '^(actor-|gateway-|control$)'; }

# cell_of <plane service>: control's own cell is the first cell (c001).
cell_of() {
  case "$1" in
    control) printf '%s' "$FIRST_CELL" ;;
    *) cell_of_service "$1" ;;
  esac
}

# mount_of <service>: the directory the service sees its mount as.
mount_of() {
  if [ -n "$MOUNT_PREFIX" ]; then
    printf '%s/%s' "$MOUNT_PREFIX" "$1"
  else
    printf '%s' "$CONTAINER_MOUNT"
  fi
}

production_defaults() {
  local candidate
  for candidate in "${PRODUCTION_DEFAULTS:-}" "$compose/../../config/production-defaults.yaml" \
    /opt/provisioner/production-defaults.yaml; do
    if [ -n "$candidate" ] && [ -r "$candidate" ]; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  refuse "production-defaults.yaml not found (set PRODUCTION_DEFAULTS)"
}

validate_topology() {
  local c cu s n key
  check CELLS '^c[0-9]{3} c[0-9]{3}$'
  read -r FIRST_CELL SECOND_CELL <<<"$CELLS"
  for key in ISSUER AUDIENCE; do check "$key" '^[[:print:]]{1,512}$'; done
  check ORIGIN '^https?://[A-Za-z0-9.-]{1,253}(:[0-9]{1,5})?$'
  check EXECUTABLE_ID "$HEX64_RE"
  check SPLIT '^[1-9][0-9]{0,4}$'
  [ "$SPLIT" -le 16383 ] || refuse "SPLIT is out of range"
  for key in RPC_PORT RELAY_PORT SOURCE_PORT ADMIN_PORT HEALTH_PORT; do check "$key" "$PORT_RE"; done
  for key in CONTROL_IP PLATFORM_IP PG_DIRECTORY_IP; do check "$key" "$IP_RE"; done
  for c in $CELLS; do
    cu="$(upper "$c")"
    for key in "GATEWAY_${cu}_IP" "PG_${cu}_PRIMARY_IP" "KUBE_${cu}_IP"; do check "$key" "$IP_RE"; done
    check "GATEWAY_ID_$cu" '^[A-Za-z0-9_.-]{1,128}$'
    check "CLUSTER_FINGERPRINT_$cu" "$HEX64_RE"
    check "KUBE_${cu}_NAMESPACE" '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
    for n in 1 2 3 4 5 6; do
      for key in "ACTOR_${cu}_${n}_CELL_IP" "ACTOR_${cu}_${n}_SHARED_IP"; do check "$key" "$IP_RE"; done
      check "ACTOR_${cu}_${n}_AZ" '^az-[a-z0-9][a-z0-9-]{0,62}$'
    done
  done
}

require_stage1() {
  case "$RUN_ROOT" in /?*) ;; *) refuse "RUN_ROOT must be an absolute path" ;; esac
  [ -f "$RUN_ROOT/.provisioned" ] && [ ! -e "$RUN_ROOT/.stage1-started" ] ||
    refuse "stage 1 has not completed in RUN_ROOT (no .provisioned marker)"
}

# read_shared: stage-1 identifiers and passwords, and the WAL facts, into globals.
read_shared() {
  local shared="$RUN_ROOT/_shared" c cu s
  declare -gA POD_UID=() BOOT=() WAL_SYSTEM=() WAL_TIMELINE=()
  for s in $(plane_list); do
    POD_UID[$s]="$(kv_checked "$shared/pod-uids.env" "$(env_name "$s")_POD_UID" "$UUID_RE")"
  done
  for c in $CELLS; do
    cu="$(upper "$c")"
    BOOT[$c]="$(kv_checked "$shared/gateway-boots.env" "GATEWAY_BOOT_$cu" "$UUID_RE")"
  done
  DB_PASSWORD_APP="$(kv_checked "$shared/db-passwords.env" DB_PASSWORD_APP "$SECRET_RE")"
  DB_PASSWORD_NODIR="$(kv_checked "$shared/db-passwords.env" DB_PASSWORD_NODIR "$SECRET_RE")"
  DB_PASSWORD_SOURCE_OWNER="$(kv_checked "$shared/db-passwords.env" DB_PASSWORD_SOURCE_OWNER "$SECRET_RE")"
  if [ "$OFFLINE" -eq 1 ]; then
    printf 'RENDER_OFFLINE WAL facts are placeholders (systemIdentifier 1, timeline 1); %s marks this volume and compose must refuse it\n' \
      "$RUN_ROOT/.offline" >&2
    for c in $CELLS; do
      WAL_SYSTEM[$c]=1
      WAL_TIMELINE[$c]=1
    done
    return 0
  fi
  [ -r "$shared/wal.env" ] ||
    refuse "wal facts missing: _shared/wal.env (stage 2 writes it once the standbys are in sync); use --offline to validate only"
  for c in $CELLS; do
    cu="$(upper "$c")"
    WAL_SYSTEM[$c]="$(kv_checked "$shared/wal.env" "WAL_${cu}_SYSTEM_ID" '^[0-9]{1,20}$')"
    WAL_TIMELINE[$c]="$(kv_checked "$shared/wal.env" "WAL_${cu}_TIMELINE" '^[1-9][0-9]{0,9}$')"
  done
}

# --- output ------------------------------------------------------------------------------

# emit <dest> <command...>: the command's stdout to <dest>, through a temporary file in the same
# directory (which also replaces a 0440 file). A failing command leaves <dest> untouched.
emit() {
  local dest="$1" tmp="$1.render-tmp"
  shift
  if ! "$@" >"$tmp"; then
    rm -f "$tmp"
    refuse "could not render ${dest#"$RUN_ROOT"/}"
  fi
  mv -f "$tmp" "$dest"
  RENDERED+=("$dest")
}

# text <string>: print the string as is (an emit source).
text() { printf '%s' "$1"; }

# tpl <template> [jq options...]: run a template with no input.
tpl() {
  local template="$1"
  shift
  jq -n -L "$TEMPLATES" -f "$TEMPLATES/$template.jq" "$@"
}

# named <--arg k v ...>: a compact JSON object of the named string arguments.
named() { jq -cn '$ARGS.named' "$@"; }

build_shared_documents() {
  local c cu id ip gw args=() hosts=() gws=()
  for c in $CELLS; do
    cu="$(upper "$c")"
    id="GATEWAY_ID_$cu"
    gw="GATEWAY_${cu}_IP"
    args+=(--arg "$c" "${!id}")
    hosts+=(--arg "$c" "$(ip_of "actor-$c-1" shared)")
    gws+=(--arg "$c" "${!gw}")
  done
  CELLS_JSON="$(jq -cn '$ARGS.positional' --args $CELLS)"
  GATEWAY_IDS="$(named "${args[@]}")"
  ACTOR_HOSTS="$(named "${hosts[@]}")"
  GATEWAY_HOSTS="$(named "${gws[@]}")"
  args=()
  for c in $CELLS; do args+=(--arg "$c" "${BOOT[$c]}"); done
  GATEWAY_BOOTS="$(named "${args[@]}")"

  tpl directory --argjson cells "$CELLS_JSON" --argjson split "$SPLIT" \
    --argjson gatewayIds "$GATEWAY_IDS" --argjson gatewayBoots "$GATEWAY_BOOTS" >"$WORK/directory.json"
  SNAPSHOT_SHA="$(sha256sum <"$WORK/directory.json" | cut -d' ' -f1)"
  jq -n -j -f "$TEMPLATES/runtime.jq" --rawfile defaults "$(production_defaults)" \
    --arg issuer "$ISSUER" --arg audience "$AUDIENCE" >"$WORK/runtime.yaml"
}

provider_doc() {
  local s="$1" cell="$2" mount="$3" dir_user dir_password cu pg
  cu="$(upper "$cell")"
  pg="PG_${cu}_PRIMARY_IP"
  if [ "$s" = control ]; then
    dir_user=signaling_app
    dir_password="$DB_PASSWORD_APP"
  else
    # signaling_nodir has no CONNECT on directory: actors and gateways hold no directory credential.
    dir_user=signaling_nodir
    dir_password="$DB_PASSWORD_NODIR"
  fi
  RENDER_CELL_DB_PASSWORD="$DB_PASSWORD_APP" RENDER_DIR_DB_PASSWORD="$dir_password" \
    tpl provider \
    --arg cell "$cell" --arg mount "$mount" --arg issuer "$ISSUER" --arg audience "$AUDIENCE" \
    --argjson pgPort "$PG_PORT" --arg cellDbHost "${!pg}" --arg cellDbUser signaling_app \
    --arg dirDbHost "$PG_DIRECTORY_IP" --arg dirDbUser "$dir_user" \
    --arg sourceBase "https://$PLATFORM_IP:$SOURCE_PORT" --arg domain "$DOMAIN" \
    --arg snapshotSha "$SNAPSHOT_SHA" --argjson cells "$CELLS_JSON" --argjson actorHosts "$ACTOR_HOSTS" \
    --argjson rpcPort "$RPC_PORT" --argjson gatewayIds "$GATEWAY_IDS" --argjson gatewayHosts "$GATEWAY_HOSTS" \
    --argjson relayPort "$RELAY_PORT"
}

# profile_args <service> <pod ip>: the arguments every process template takes.
profile_args() {
  PROFILE_ARGS=(--arg podUid "${POD_UID[$1]}" --arg podIp "$2" --argjson businessPort "$RPC_PORT"
    --argjson healthPort "$HEALTH_PORT" --argjson relayPort "$RELAY_PORT" --arg executableId "$EXECUTABLE_ID")
}

process_doc() {
  local s="$1" cell="$2" mount="$3" cu az kube ns id other pg
  cu="$(upper "$cell")"
  case "$s" in
    actor-*)
      az="$(env_name "$s")_AZ"
      kube="KUBE_${cu}_IP"
      ns="KUBE_${cu}_NAMESPACE"
      profile_args "$s" "$(ip_of "$s" cell)"
      tpl process-actor "${PROFILE_ARGS[@]}" --arg mount "$mount" --arg cell "$cell" --arg azRole "${!az}" \
        --argjson cellAzRoles "$CELL_AZ_ROLES" --arg walSystemId "${WAL_SYSTEM[$cell]}" \
        --argjson walTimeline "${WAL_TIMELINE[$cell]}" --arg kubeHost "${!kube}" \
        --argjson kubePort "$KUBE_API_PORT" --arg namespace "${!ns}"
      ;;
    gateway-*)
      id="GATEWAY_ID_$cu"
      profile_args "$s" "$(ip_of "$s" shared)"
      tpl process-gateway "${PROFILE_ARGS[@]}" --arg gatewayId "${!id}" --arg gatewayBootId "${BOOT[$cell]}" \
        --arg origin "$ORIGIN"
      ;;
    control)
      other="$SECOND_CELL"
      pg="PG_$(upper "$other")_PRIMARY_IP"
      profile_args "$s" "$CONTROL_IP"
      RENDER_TRANSFER_DB_PASSWORD="$DB_PASSWORD_APP" \
        tpl process-control "${PROFILE_ARGS[@]}" --arg mount "$mount" --argjson pgPort "$PG_PORT" \
        --arg transferCell "$other" --arg transferDbHost "${!pg}" --arg transferDbUser signaling_app
      ;;
  esac
}

# env.list: the non-secret environment of spec 4.2 that compose sets (for comparison by tests).
env_list_doc() {
  local s="$1" cell="$2" mount="$3" plane fp kube extra=()
  plane="${s%%-*}"
  fp="CLUSTER_FINGERPRINT_$(upper "$cell")"
  if [ "$plane" = actor ]; then
    kube="KUBE_$(upper "$cell")_IP"
    extra=(--arg SIGNALING_FORMATION_PROFILE initial-six --arg SIGNALING_KUBE_API_HOST "${!kube}"
      --arg SIGNALING_KUBE_API_PORT "$KUBE_API_PORT")
  fi
  jq -rn '$ARGS.named | to_entries[] | "\(.key)=\(.value)"' \
    --arg SPRING_PROFILES_ACTIVE "$plane" \
    --arg SIGNALING_DEPLOYMENT_MODE local-minikube \
    --arg SIGNALING_LOCAL_ACKNOWLEDGEMENT LOCAL_TEST_ONLY \
    --arg SIGNALING_NATIVE_PROVIDER_CONTRACT "$mount/provider.json" \
    --arg SIGNALING_LOCAL_PROCESS_PROFILE "$mount/process.json" \
    --arg SIGNALING_RUNTIME_CONTRACT "$mount/runtime.yaml" \
    --arg SIGNALING_CELL_ID "$cell" \
    --arg SIGNALING_POD_UID "${POD_UID[$s]}" \
    --arg SIGNALING_CLUSTER_FINGERPRINT "${!fp}" \
    --arg SIGNALING_IDENTITY_ISSUER "$ISSUER" \
    --arg SIGNALING_IDENTITY_AUDIENCE "$AUDIENCE" \
    "${extra[@]}"
}

# launch.sh: what compose cannot hold. Every embedded literal is validated above (UUID, dotted
# quad, port, a prefix of [A-Za-z0-9._/-]), so no quoting is needed.
launch_doc() {
  local s="$1" cell="$2" mount="$3" kube
    printf '#!/usr/bin/env bash\n'
    printf '# LOCAL_TEST_ONLY / NOT_QUALIFIED. Rendered by provisioner/lib/render.sh for %s; do not edit.\n' "$s"
    printf '# Exports the pod identity and, on actors, the remoting store passwords (read from the\n'
    printf '# mount at start, never stored in compose) and the Kubernetes API address the adapter\n'
    printf '# requires in the real process environment. Compose sets the rest (see env.list).\n'
    printf 'set -eo pipefail\n'
    printf 'export SIGNALING_POD_UID=%s\n' "${POD_UID[$s]}"
    case "$s" in
      actor-*)
        kube="KUBE_$(upper "$cell")_IP"
        printf 'SIGNALING_REMOTING_KEYSTORE_PASSWORD="$(< %s/remoting.pass)"\n' "$mount"
        printf 'export SIGNALING_REMOTING_KEYSTORE_PASSWORD\n'
        printf 'export SIGNALING_REMOTING_KEY_PASSWORD="$SIGNALING_REMOTING_KEYSTORE_PASSWORD"\n'
        printf 'export SIGNALING_REMOTING_TRUSTSTORE_PASSWORD="$SIGNALING_REMOTING_KEYSTORE_PASSWORD"\n'
        printf 'export SIGNALING_KUBE_API_HOST=%s\n' "${!kube}"
        printf 'export SIGNALING_KUBE_API_PORT=%s\n' "$KUBE_API_PORT"
        ;;
    esac
    printf '# JVM_OPTS is split into words on purpose.\n'
    printf 'exec java $JVM_OPTS -jar %s\n' "$APP_JAR"
}

render_plane() {
  local s="$1" cell mount
  cell="$(cell_of "$s")"
  mount="$(mount_of "$s")"
  emit "$RUN_ROOT/$s/directory.json" cat "$WORK/directory.json"
  emit "$RUN_ROOT/$s/runtime.yaml" cat "$WORK/runtime.yaml"
  emit "$RUN_ROOT/$s/provider.json" provider_doc "$s" "$cell" "$mount"
  emit "$RUN_ROOT/$s/process.json" process_doc "$s" "$cell" "$mount"
  emit "$RUN_ROOT/$s/env.list" env_list_doc "$s" "$cell" "$mount"
  emit "$RUN_ROOT/$s/pod.env" text "SIGNALING_POD_UID=${POD_UID[$s]}"$'\n'
  emit "$RUN_ROOT/$s/launch.sh" launch_doc "$s" "$cell" "$mount"
  log "wrote $s/provider.json $s/process.json $s/runtime.yaml $s/directory.json $s/env.list $s/pod.env $s/launch.sh"
}

render_platform() {
  local s=local-platform mount
  mount="$(mount_of "$s")"
  emit "$RUN_ROOT/$s/platform.json" tpl platform-config --argjson cells "$CELLS_JSON" --arg mount "$mount" \
    --arg bind "$PLATFORM_IP" --argjson sourcePort "$SOURCE_PORT" --argjson adminPort "$ADMIN_PORT" \
    --argjson healthPort "$HEALTH_PORT" --argjson pgPort "$PG_PORT" --arg dbHost "$PG_DIRECTORY_IP" \
    --arg dbUser source_owner
  # The password file (no trailing newline): printf is a builtin, so it never reaches an argv.
  emit "$RUN_ROOT/$s/db.pass" text "$DB_PASSWORD_SOURCE_OWNER"
  emit "$RUN_ROOT/$s/env.list" jq -rn '$ARGS.named | to_entries[] | "\(.key)=\(.value)"' \
    --arg SIGNALING_LOCAL_ACKNOWLEDGEMENT LOCAL_TEST_ONLY \
    --arg SIGNALING_IDENTITY_ISSUER "$ISSUER" \
    --arg SIGNALING_PLATFORM_CONFIG "$mount/platform.json"
  log "wrote $s/platform.json $s/db.pass $s/env.list"
}

# finalize: 0440 root:10001 like every stage-1 file; refuse output with a placeholder left.
finalize() {
  local f
  for f in "${RENDERED[@]}"; do
    case "$(basename "$f")" in
      db.pass) ;;
      *) if grep -qF '${' "$f"; then refuse "unresolved placeholder rendered into ${f#"$RUN_ROOT"/}"; fi ;;
    esac
  done
  chmod 0440 "${RENDERED[@]}"
  if [ "$(id -u)" -eq 0 ]; then
    chown "root:$GROUP_ID" "${RENDERED[@]}"
  else
    log "NOTE: not running as root; skipping chown to root:$GROUP_ID (test runs only)"
  fi
}

render() {
  require_stage1
  load_topology
  validate_topology
  read_shared
  umask 077
  WORK="$RUN_ROOT/.render-work"
  rm -rf "${WORK:?}"
  mkdir -p "$WORK"
  trap 'rm -rf "${WORK:?}"' EXIT
  RENDERED=()
  build_shared_documents
  local s
  for s in $(plane_list); do render_plane "$s"; done
  render_platform
  if [ "$OFFLINE" -eq 1 ]; then
    emit "$RUN_ROOT/.offline" text $'LOCAL_TEST_ONLY offline render: WAL facts are placeholders; compose must refuse this volume\n'
  else
    rm -f "$RUN_ROOT/.offline"
  fi
  finalize
  log "render done: ${#RENDERED[@]} files (LOCAL_TEST_ONLY / NOT_QUALIFIED$([ "$OFFLINE" -eq 1 ] && printf ', OFFLINE'))"
}

main() {
  [ "${1:-}" = render ] || usage
  shift
  OFFLINE=0
  MOUNT_PREFIX=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --offline) OFFLINE=1 ;;
      --mount-prefix)
        [ "$#" -ge 2 ] || usage "--mount-prefix needs a directory"
        MOUNT_PREFIX="$2"
        shift
        ;;
      *) usage "unknown argument" ;;
    esac
    shift
  done
  if [ -n "$MOUNT_PREFIX" ]; then
    [ "$OFFLINE" -eq 1 ] || usage "--mount-prefix is for --offline validation trees only"
    [[ "$MOUNT_PREFIX" =~ ^/[A-Za-z0-9._/-]+$ ]] || usage "--mount-prefix must be an absolute path of [A-Za-z0-9._/-]"
    MOUNT_PREFIX="${MOUNT_PREFIX%/}"
  fi
  render
}

main "$@"
