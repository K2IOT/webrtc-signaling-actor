#!/usr/bin/env bash
# Prints the /etc/hosts lines the compose environment needs. It NEVER writes a file:
# review the output and append it yourself (for example `./hosts.sh | sudo tee -a /etc/hosts`).
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/topology.sh
source "$here/lib/topology.sh"
load_topology

echo "# add these lines to /etc/hosts yourself; this script does not modify any file" >&2
for h in "$HOSTNAME_C001" "$HOSTNAME_C002" "$HOSTNAME_BOOTSTRAP" "$HOSTNAME_IDENTITY"; do
  printf '127.0.0.1 %s\n' "$h"
done
