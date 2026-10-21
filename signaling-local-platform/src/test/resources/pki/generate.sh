#!/usr/bin/env bash
# TEST_ONLY / LOCAL_TEST_ONLY / NOT_QUALIFIED.
# Generates the throw-away certificates used by PeerIdentityTest. Only the public certificates
# are kept; every private key lives in a temporary directory that is deleted on exit, so nothing
# here can authenticate anything. Validity is 100 years so the tests never expire.
# Re-running creates NEW CAs and leaves different bytes; the tests only depend on the SAN shapes.
set -euo pipefail
out="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
days=36500
base="spiffe://signaling/local-test-only"

ca() { # name
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days "$days" \
    -keyout "$work/$1.key" -out "$out/$1.pem" -subj "/O=TEST_ONLY/CN=TEST_ONLY $1" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
}

leaf() { # caName fileName sanList...   (sanList entries look like URI:spiffe://... or DNS:host)
  local issuer="$1" name="$2"; shift 2
  local san; san="$(IFS=,; echo "$*")"
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$work/$name.key" -out "$work/$name.csr" -subj "/O=TEST_ONLY/CN=TEST_ONLY $name" 2>/dev/null
  printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=clientAuth\n' > "$work/$name.ext"
  [ -n "$san" ] && printf 'subjectAltName=%s\n' "$san" >> "$work/$name.ext"
  openssl x509 -req -in "$work/$name.csr" -CA "$out/$issuer.pem" -CAkey "$work/$issuer.key" \
    -CAcreateserial -CAserial "$work/serial" -days "$days" -extfile "$work/$name.ext" \
    -out "$out/$name.pem" 2>/dev/null
}

ca workload-ca
ca admin-ca

leaf workload-ca actor-cell-a          "URI:$base/cell/cell-a/actor"
leaf workload-ca gateway-cell-a-gw-1   "URI:$base/cell/cell-a/gateway/gw-1"
leaf workload-ca control-cell-b        "URI:$base/cell/cell-b/control"
leaf workload-ca no-uri-san            "DNS:test-only.invalid"
leaf workload-ca no-san
leaf workload-ca two-uri-sans          "URI:$base/cell/cell-a/actor" "URI:$base/cell/cell-a/gateway"
leaf workload-ca foreign-trust-domain  "URI:spiffe://other/local-test-only/cell/cell-a/actor"
leaf workload-ca unknown-cell          "URI:$base/cell/cell-z/actor"
leaf workload-ca unknown-plane         "URI:$base/cell/cell-a/relay"
leaf workload-ca extra-path-segment    "URI:$base/cell/cell-a/gateway/gw-1/extra"
leaf workload-ca trailing-slash        "URI:$base/cell/cell-a/actor/"
leaf workload-ca uppercase-instance    "URI:$base/cell/cell-a/gateway/GW-1"
leaf workload-ca dns-and-actor-uri     "DNS:test-only.invalid" "URI:$base/cell/cell-a/actor"
leaf workload-ca admin-path-on-workload-ca "URI:$base/admin/ops"
leaf admin-ca    admin-ops             "URI:$base/admin/ops"
leaf admin-ca    admin-two-uri-sans    "URI:$base/admin/ops" "URI:$base/admin/other"
leaf admin-ca    admin-no-uri-san      "DNS:test-only.invalid"
leaf admin-ca    admin-uppercase-name  "URI:$base/admin/Ops"
leaf admin-ca    admin-name-too-long   "URI:$base/admin/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
leaf admin-ca    admin-extra-segment   "URI:$base/admin/ops/extra"
leaf admin-ca    admin-foreign-domain  "URI:spiffe://other/local-test-only/admin/ops"
leaf admin-ca    workload-path-on-admin-ca "URI:$base/cell/cell-a/actor"
