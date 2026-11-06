#!/usr/bin/env bash
# Show the DMs the news account got (kept even when the sender deleted them). Times are UTC.
#   ./dms.sh            the last 50 events
#   ./dms.sh 200        the last 200
#   ./dms.sh deleted    only deleted and edited messages
cd "$(dirname "$0")"
exec .venv/bin/python -m snmon.dmlog "$@"
