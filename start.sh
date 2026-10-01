#!/usr/bin/env bash
# Start (or restart) the monitor under pm2.
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
fi
pm2 startOrRestart ecosystem.config.cjs
pm2 save >/dev/null
echo "Logs: pm2 logs sn-monitor"
