#!/usr/bin/env bash
set -euo pipefail

force=false
case "${1:-}" in
  "") ;;
  --force) force=true ;;
  *) echo "usage: $0 [--force]" >&2; exit 2 ;;
esac
if [[ "$#" -gt 1 ]]; then
  echo "usage: $0 [--force]" >&2
  exit 2
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate development RSA keys" >&2
  exit 1
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
key_dir="$repo_root/signaling-identity/src/main/resources/keys"
private_key="$key_dir/private.pem"
public_key="$key_dir/public.pem"
mkdir -p "$key_dir"
if [[ "$force" != true && ( -e "$private_key" || -e "$public_key" ) ]]; then
  echo "key output already exists; pass --force to replace it" >&2
  exit 1
fi

temporary_dir="$(mktemp -d "$key_dir/.rsa-keygen.XXXXXX")"
installed_private=false
installed_public=false
cleanup() {
  local result=$?
  if [[ "$result" -ne 0 && -d "$temporary_dir" ]]; then
    if [[ "$installed_private" == true ]]; then rm -f "$private_key"; fi
    if [[ "$installed_public" == true ]]; then rm -f "$public_key"; fi
    if [[ -e "$temporary_dir/private.previous" ]]; then mv -f "$temporary_dir/private.previous" "$private_key"; fi
    if [[ -e "$temporary_dir/public.previous" ]]; then mv -f "$temporary_dir/public.previous" "$public_key"; fi
  fi
  rm -rf -- "$temporary_dir"
  return "$result"
}
trap cleanup EXIT

openssl genpkey -quiet -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$temporary_dir/private.pem"
openssl pkey -in "$temporary_dir/private.pem" -check -noout >/dev/null
openssl pkey -in "$temporary_dir/private.pem" -pubout -out "$temporary_dir/public.pem"
openssl pkey -pubin -in "$temporary_dir/public.pem" -text -noout | grep -Fq 'Public-Key: (2048 bit)'
cmp <(openssl pkey -in "$temporary_dir/private.pem" -pubout) "$temporary_dir/public.pem"
chmod 600 "$temporary_dir/private.pem"

if [[ -e "$private_key" ]]; then mv "$private_key" "$temporary_dir/private.previous"; fi
if [[ -e "$public_key" ]]; then mv "$public_key" "$temporary_dir/public.previous"; fi
mv "$temporary_dir/private.pem" "$private_key"
installed_private=true
mv "$temporary_dir/public.pem" "$public_key"
installed_public=true
echo "Generated development RSA key pair in $key_dir"
