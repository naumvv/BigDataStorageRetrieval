#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-${DATA_DIR:-./cleaned_data}}"
if [[ $# -gt 0 ]]; then
  shift
fi

python3 "$SCRIPT_DIR/load_data_psql.py" --data-dir "$DATA_DIR" --drop "$@"
