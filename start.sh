#!/bin/bash
# start.sh — runs discover.py, stream_opensea.py, and eth_mint_watcher.py
# together in a single process, for a single combined Railway service.
#
# Each script gets its own supervisory loop:
#   - stream_opensea.py / eth_mint_watcher.py are meant to run forever.
#     If either one crashes for any reason, its loop restarts it after
#     a short pause, rather than the whole container going down.
#   - discover.py is meant to run once and exit (that's correct
#     behavior, not a crash) — its loop re-runs it once every 24 hours.
#
# All three share this container's filesystem, so the existing
# projects.json + lock-file coordination between them works exactly
# as it does locally — no extra setup needed for that part. What DOES
# need setup is a Railway Volume mounted here, so projects.json and
# the state files (seen_opensea_collections.json,
# eth_mint_watcher_state.json) survive redeploys instead of resetting
# to empty every time.

set -uo pipefail
cd "$(dirname "$0")"

supervise_persistent() {
    local name="$1"
    local script="$2"

    while true; do
        echo "[supervisor] Starting $name..."
        python3 "$script"
        exit_code=$?
        echo "[supervisor] $name exited (code $exit_code) — restarting in 10s..."
        sleep 10
    done
}

supervise_daily() {
    while true; do
        echo "[supervisor] Running discover.py..."
        python3 discover.py
        echo "[supervisor] discover.py finished — sleeping 24h until next run..."
        sleep 86400
    done
}

supervise_persistent "stream_opensea" "stream_opensea.py" &
supervise_persistent "eth_mint_watcher" "eth_mint_watcher.py" &
supervise_daily &

# Each loop above is infinite under normal operation, so this should
# never return. If one of them somehow does exit (a bug in the
# supervisor logic itself, not a normal script crash — those are
# already handled by the loops), bring the whole container down so
# Railway's own platform-level restart kicks in as a last resort.
wait -n
echo "[supervisor] A supervisor loop exited unexpectedly — exiting so Railway restarts the service."
exit 1
