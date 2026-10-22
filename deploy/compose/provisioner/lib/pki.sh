#!/usr/bin/env bash
# PKI, key, password and identifier generation for provisioner stage 1.
# Pure openssl + keytool. LOCAL_TEST_ONLY / NOT_QUALIFIED. Sourced by provision.sh.
#
# Rules this file keeps:
#   * CA private keys live only under $CA_DIR (= $RUN_ROOT/.ca); nothing in a service
#     mount directory or in _shared holds one.
#   * Secrets are passed to openssl and keytool through files, never through argv, and
#     nothing here prints key, password or token content: log lines name files only.
#   * Leaf keys are PKCS#8 PEM (openssl genpkey). TLS leafs use EC P-256, proof and
#     source signing keys use Ed25519.
# shellcheck shell=bash

LEAF_DAYS="${LEAF_DAYS:-825}"
CA_DAYS="${CA_DAYS:-3650}"
SPIFFE_PREFIX="spiffe://signaling/local-test-only"

pki_log() { printf 'provision: %s\n' "$*"; }

# quiet <cmd...>: run a command, hide its output unless it fails (openssl never prints keys).
quiet() {
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  if [ "$rc" -ne 0 ]; then
    printf 'provision: command failed (%s): %s\n' "$1" "$out" >&2
    return "$rc"
  fi
}

rand_hex() { openssl rand -hex "$1"; }

# new_uuid: random (version 4) lower-case UUID.
new_uuid() {
  local u
  if [ -r /proc/sys/kernel/random/uuid ]; then
    u="$(cat /proc/sys/kernel/random/uuid)"
  else
    u="$(openssl rand -hex 16 | sed -E 's/^(.{8})(.{4}).(.{3}).(.{3})(.{12})$/\1-\2-4\3-a\4-\5/')"
  fi
  printf '%s\n' "$u" | tr '[:upper:]' '[:lower:]'
}

# write_secret <path> <content>: file with no trailing newline, created under umask 077.
write_secret() { printf '%s' "$2" >"$1"; }

# ---------------------------------------------------------------------------------------
# certificate authorities (keys stay in $CA_DIR)
# ---------------------------------------------------------------------------------------

pki_init() {
  CA_DIR="${RUN_ROOT:?}/.ca"
  mkdir -p "$CA_DIR/tmp"
  cat >"$CA_DIR/openssl.cnf" <<'CNF'
[req]
distinguished_name = dn
prompt = no
[dn]
CN = unused
CNF
}

# new_ca <name> <common-name>: $CA_DIR/<name>.key + <name>.crt (EC P-256, self-signed)
new_ca() {
  local name="$1" cn="$2"
  quiet openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$CA_DIR/$name.key"
  quiet openssl req -x509 -new -config "$CA_DIR/openssl.cnf" -key "$CA_DIR/$name.key" \
    -subj "/O=LOCAL_TEST_ONLY/CN=$cn" -days "$CA_DAYS" -sha256 \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash" \
    -out "$CA_DIR/$name.crt"
  pki_log "created CA $name"
}

# issue_leaf <ca> <common-name> <san-list> <key-out> <crt-out>
#   san-list: "URI:...,DNS:...,IP:..." ; EKU serverAuth+clientAuth, keyUsage digitalSignature.
issue_leaf() {
  local ca="$1" cn="$2" san="$3" keyout="$4" crtout="$5"
  # fail closed: a leaf without SANs must never be issued (an empty value usually means an
  # upstream lookup failed inside a command substitution, where set -e does not fire)
  if [ -z "$san" ]; then
    printf 'provision: refusing to issue a leaf without SANs (%s)\n' "$cn" >&2
    return 1
  fi
  local csr="$CA_DIR/tmp/leaf.csr" ext="$CA_DIR/tmp/leaf.ext"
  quiet openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$keyout"
  quiet openssl req -new -config "$CA_DIR/openssl.cnf" -key "$keyout" -subj "/O=LOCAL_TEST_ONLY/CN=$cn" -out "$csr"
  printf '%s\n' \
    "basicConstraints=critical,CA:FALSE" \
    "keyUsage=critical,digitalSignature" \
    "extendedKeyUsage=serverAuth,clientAuth" \
    "subjectKeyIdentifier=hash" \
    "authorityKeyIdentifier=keyid,issuer" \
    "subjectAltName=$san" >"$ext"
  quiet openssl x509 -req -in "$csr" -CA "$CA_DIR/$ca.crt" -CAkey "$CA_DIR/$ca.key" \
    -set_serial "0x$(rand_hex 15)" -days "$LEAF_DAYS" -sha256 -extfile "$ext" -out "$crtout"
  rm -f "$csr" "$ext"
}

# new_ed25519 <key-out> <pub-out>: PKCS#8 PEM private key and its SubjectPublicKeyInfo PEM.
new_ed25519() {
  quiet openssl genpkey -algorithm ED25519 -out "$1"
  openssl pkey -in "$1" -pubout -out "$2"
}

# ---------------------------------------------------------------------------------------
# PKCS12 stores
# ---------------------------------------------------------------------------------------

# make_keystore <key> <crt> <ca-crt> <alias> <passfile> <out>: one private-key entry + chain
make_keystore() {
  quiet openssl pkcs12 -export -inkey "$1" -in "$2" -certfile "$3" -name "$4" \
    -passout "file:$5" -out "$6"
}

# make_truststore <ca-crt> <alias> <passfile> <out>: one trustedCertEntry
make_truststore() {
  rm -f "$4"
  quiet keytool -importcert -noprompt -storetype PKCS12 -alias "$2" -file "$1" \
    -keystore "$4" -storepass:file "$3"
}

# ---------------------------------------------------------------------------------------
# helpers over the topology
# ---------------------------------------------------------------------------------------

# cell_of_service <actor-cNNN-N | gateway-cNNN>
cell_of_service() {
  local c
  case "$1" in
    actor-*)
      c="${1#actor-}"
      printf '%s' "${c%-*}"
      ;;
    gateway-*) printf '%s' "${1#gateway-}" ;;
    *) return 1 ;;
  esac
}

upper() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }

# env_name <service>: UPPER_SNAKE form of a service name (actor-c001-3 -> ACTOR_C001_3)
env_name() { upper "$1" | tr '-' '_'; }

# workload_san <service>: the SAN list of the workload leaf (spec 4.6)
workload_san() {
  local svc="$1" cell cu gid_var host_var
  case "$svc" in
    actor-*)
      cell="$(cell_of_service "$svc")"
      printf 'URI:%s/cell/%s/actor/%s,DNS:actor.%s.signaling.local,IP:%s,IP:%s' \
        "$SPIFFE_PREFIX" "$cell" "$svc" "$cell" "$(ip_of "$svc" shared)" "$(ip_of "$svc" cell)"
      ;;
    gateway-*)
      cell="$(cell_of_service "$svc")"
      cu="$(upper "$cell")"
      gid_var="GATEWAY_ID_$cu"
      host_var="HOSTNAME_$cu"
      printf 'URI:%s/cell/%s/gateway/%s,DNS:gateway.%s.signaling.local,DNS:%s,IP:%s' \
        "$SPIFFE_PREFIX" "$cell" "${!gid_var}" "$cell" "${!host_var}" "$(ip_of "$svc" shared)"
      ;;
    control)
      # control's home cell is c001 (its process profile names c001, spec 4.6)
      printf 'URI:%s/cell/c001/control,DNS:%s,IP:%s' \
        "$SPIFFE_PREFIX" "$HOSTNAME_BOOTSTRAP" "$(ip_of control shared)"
      ;;
    *) return 1 ;;
  esac
}
