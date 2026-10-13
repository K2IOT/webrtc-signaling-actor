#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$(mktemp -d)"
trap 'rm -rf -- "$fixture"' EXIT
mkdir -p "$fixture/scripts"
cp "$repo_root/scripts/generate-rsa-keys.sh" "$fixture/scripts/"

key_dir="$fixture/signaling-identity/src/main/resources/keys"
private_key="$key_dir/private.pem"
public_key="$key_dir/public.pem"

(cd / && bash "$fixture/scripts/generate-rsa-keys.sh")
openssl pkey -in "$private_key" -check -noout >/dev/null
openssl pkey -pubin -in "$public_key" -text -noout | rg -q 'Public-Key: \(2048 bit\)'
cmp <(openssl pkey -in "$private_key" -pubout) "$public_key"
before="$(sha256sum "$private_key" "$public_key")"

if bash "$fixture/scripts/generate-rsa-keys.sh" >"$fixture/overwrite.out" 2>&1; then
  echo "generator unexpectedly overwrote existing keys" >&2
  exit 1
fi
after="$(sha256sum "$private_key" "$public_key")"
[[ "$before" == "$after" ]]

bash "$fixture/scripts/generate-rsa-keys.sh" --force
openssl pkey -in "$private_key" -check -noout >/dev/null
cmp <(openssl pkey -in "$private_key" -pubout) "$public_key"

if bash "$fixture/scripts/generate-rsa-keys.sh" --unknown >"$fixture/unknown.out" 2>&1; then
  echo "generator unexpectedly accepted an unknown option" >&2
  exit 1
fi

echo "RSA key generator regression checks passed"
