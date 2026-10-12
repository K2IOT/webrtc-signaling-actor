#!/usr/bin/env bash
# TEST ONLY. Installs Ubuntu packages and an isolated pinned Node runtime, then runs local gates.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
source /etc/os-release
[[ $ID == ubuntu && $VERSION_ID == 24.04 ]] || { echo 'Ubuntu 24.04 required' >&2; exit 1; }
command -v docker >/dev/null && docker info >/dev/null
privilege=()
if [[ $(id -u) != 0 ]]; then privilege=(sudo); fi
"${privilege[@]}" apt-get update
"${privilege[@]}" apt-get install -y openjdk-21-jdk-headless python3-venv git curl ca-certificates xz-utils \
  libnss3 libatk1.0-0t64 libatk-bridge2.0-0t64 libcups2t64 libdrm2 libxkbcommon0 \
  libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2t64 \
  libpango-1.0-0 libcairo2 libatspi2.0-0t64 fonts-liberation
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture)
export PATH="$JAVA_HOME/bin:$PATH"
if ! command -v node >/dev/null || ! command -v npm >/dev/null || ! node -e 'if(Number(process.versions.node.split(".")[0])<20)process.exit(1)'; then
  case $(uname -m) in x86_64) node_arch=x64;; aarch64) node_arch=arm64;; *) echo 'Unsupported Node architecture' >&2; exit 1;; esac
  node_version=24.14.0
  archive="node-v$node_version-linux-$node_arch.tar.xz"
  tools_dir="$root/.superpowers/local-tools"
  mkdir -p "$tools_dir"
  temp=$(mktemp -d "$tools_dir/download.XXXXXX")
  trap 'rm -rf -- "$temp"' EXIT
  curl --fail --location --proto '=https' --tlsv1.2 "https://nodejs.org/dist/v$node_version/$archive" -o "$temp/$archive"
  curl --fail --location --proto '=https' --tlsv1.2 "https://nodejs.org/dist/v$node_version/SHASUMS256.txt" -o "$temp/SHASUMS256.txt"
  (cd "$temp"; awk -v file="$archive" '$2==file {print; found=1} END {if(!found)exit 1}' SHASUMS256.txt > selected.sha256; sha256sum -c selected.sha256)
  tar -xJf "$temp/$archive" -C "$tools_dir"
  export PATH="$tools_dir/node-v$node_version-linux-$node_arch/bin:$PATH"
  rm -rf -- "$temp"
  trap - EXIT
fi
exec "$root/qualification/scenarios/local/run-pending-native.sh"
