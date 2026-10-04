#!/usr/bin/env bash
set -euo pipefail
exec "${QUALIFICATION_PYTHON:-python3}" "$(dirname "$0")/evidence_verifier.py" "$@"
