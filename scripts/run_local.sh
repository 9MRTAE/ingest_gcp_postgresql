#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH="${PYTHONPATH:-$(pwd)}" python run_local.py "$@"
