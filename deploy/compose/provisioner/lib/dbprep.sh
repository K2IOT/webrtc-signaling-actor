#!/usr/bin/env bash
# Provisioner stage 1, PostgreSQL mounts: renders start.sh and hba.conf and copies the password
# files each pg-* server needs before provisioner stage 2 exists (spec 6: the servers start,
# and the standbys take their base backups, between the two stages).
#
#   pg-<cell>-primary  start.sh (start-common + start-primary), hba.conf, owner.pass, replicator.pass
#   pg-<cell>-standby  start.sh (start-common + start-standby), hba.conf, owner.pass, replicator.pass
#   pg-directory       start.sh (start-common + start-directory), hba.conf, owner.pass
#
# Sourced by provision.sh after topology.env, pki.sh and db-passwords.env exist. Templates live in
# deploy/compose/postgres. Values substituted into them are topology values checked against a
# narrow character set; passwords are copied by bash builtins, never substituted or printed.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
# shellcheck shell=bash

DBPREP_TEMPLATES="${DBPREP_TEMPLATES:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../postgres" && pwd)}"
PLACEHOLDER_VALUE_RE='^[A-Za-z0-9._/:() -]+$'

dbprep_fail() {
  printf 'provision: %s\n' "$*" >&2
  return 1
}

# render_placeholders <out> <template...> -- KEY=VALUE...: concatenates the templates, replaces
# every {{KEY}}, refuses a value outside PLACEHOLDER_VALUE_RE or any {{ left over. Writes <out>
# only on success.
render_placeholders() {
  local out="$1" text="" f kv key value
  shift
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
    [ -r "$1" ] || dbprep_fail "template $1 is not readable" || return 1
    text+="$(<"$1")"$'\n'
    shift
  done
  [ "${1:-}" = "--" ] || dbprep_fail "render_placeholders: missing -- before the values" || return 1
  shift
  for kv in "$@"; do
    key="${kv%%=*}"
    value="${kv#*=}"
    [[ "$key" =~ ^[A-Z_]+$ ]] || dbprep_fail "render_placeholders: bad placeholder name" || return 1
    [[ "$value" =~ $PLACEHOLDER_VALUE_RE ]] || dbprep_fail "render_placeholders: value of $key has characters outside [A-Za-z0-9._/:() -]" || return 1
    text="${text//"{{$key}}"/$value}"
  done
  if [[ "$text" == *"{{"* ]]; then
    f="${text#*\{\{}"
    dbprep_fail "render_placeholders: unresolved placeholder {{${f%%\}\}*}}} in $out" || return 1
  fi
  printf '%s' "$text" >"$out"
}

pg_kind() {
  case "$1" in
    pg-c00[0-9]-primary) printf 'primary' ;;
    pg-c00[0-9]-standby) printf 'standby' ;;
    pg-directory) printf 'directory' ;;
    *) return 1 ;;
  esac
}

# hba_select <kind> <template>: keeps the lines tagged for <kind> (or [all]) with the tag
# removed, drops lines tagged for other kinds and ## documentation lines, copies the rest.
hba_select() {
  local kind="$1" line tags rest
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" == "##"* ]]; then
      continue
    elif [[ "$line" =~ ^\[([a-z,]+)\][[:space:]]+(.*)$ ]]; then
      tags=",${BASH_REMATCH[1]},"
      rest="${BASH_REMATCH[2]}"
      if [[ "$tags" == *",$kind,"* || "$tags" == *",all,"* ]]; then
        printf '%s\n' "$rest"
      fi
    else
      printf '%s\n' "$line"
    fi
  done <"$2"
}

# kv_value <file> <KEY>: the value of KEY=value in an env file (bash only).
kv_value() {
  local line
  while IFS= read -r line || [ -n "$line" ]; do
    if [ "${line%%=*}" = "$2" ]; then
      printf '%s' "${line#*=}"
      return 0
    fi
  done <"$1"
  return 1
}

# prepare_pg_mount <service>
prepare_pg_mount() {
  local s="$1" d="$RUN_ROOT/$1" kind cell cu net standby_ip primary_ip ip tmp
  kind="$(pg_kind "$s")" || dbprep_fail "not a PostgreSQL service: $s" || return 1
  local values=("SERVER=$s" "KIND=$kind" "PG_PORT=${PG_PORT:?}")
  if [ "$kind" = directory ]; then
    net="${NET_SHARED:?}"
    values+=("CONTROL_IP=${CONTROL_IP:?}" "PLATFORM_IP=${PLATFORM_IP:?}")
  else
    cell="${s#pg-}"
    cell="${cell%-*}"
    cu="$(upper "$cell")"
    ip="NET_$cu"
    net="${!ip:?}"
    ip="PG_${cu}_STANDBY_IP"
    standby_ip="${!ip:?}"
    ip="PG_${cu}_PRIMARY_IP"
    primary_ip="${!ip:?}"
    values+=("CELL=$cell" "STANDBY_NAME=standby-$cell" "PRIMARY_IP=$primary_ip" "STANDBY_IP=$standby_ip")
  fi
  values+=("CLIENT_NET=$net")
  render_placeholders "$d/start.sh" "$DBPREP_TEMPLATES/start-common.sh" "$DBPREP_TEMPLATES/start-$kind.sh" -- "${values[@]}" || return 1
  tmp="$CA_DIR/tmp/hba-$s.tmpl"
  hba_select "$kind" "$DBPREP_TEMPLATES/pg_hba.conf.tmpl" >"$tmp"
  render_placeholders "$d/hba.conf" "$tmp" -- "${values[@]}" || return 1
  rm -f "$tmp"
  local passwords="$RUN_ROOT/_shared/db-passwords.env" pw
  pw="$(kv_value "$passwords" DB_PASSWORD_OWNER)" && [ -n "$pw" ] ||
    dbprep_fail "DB_PASSWORD_OWNER missing from _shared/db-passwords.env" || return 1
  write_secret "$d/owner.pass" "$pw"
  if [ "$kind" != directory ]; then
    pw="$(kv_value "$passwords" DB_PASSWORD_REPLICATOR)" && [ -n "$pw" ] ||
      dbprep_fail "DB_PASSWORD_REPLICATOR missing from _shared/db-passwords.env" || return 1
    write_secret "$d/replicator.pass" "$pw"
    pki_log "wrote $s/start.sh $s/hba.conf $s/owner.pass $s/replicator.pass"
  else
    pki_log "wrote $s/start.sh $s/hba.conf $s/owner.pass"
  fi
}
