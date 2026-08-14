"""
watchdog.py

Monitors heartbeats.json (written by discover.py, stream_opensea.py,
and eth_mint_watcher.py) and posts a Discord alert if any source goes
silent longer than its expected interval — so you don't have to guess
from log gaps whether something crashed or is just quietly waiting.

Each source has a different expected quiet window:
- OpenSea / Ethereum / Robinhood: these loop every 20-30 seconds, so
  going more than 5 minutes without a heartbeat means something is
  genuinely wrong.
- NFTCalendar (discover.py): runs once, then sleeps 24 hours by
  design — 25 hours of silence is normal, not a problem. Only flag it
  if it's quiet noticeably LONGER than its own schedule accounts for.

Alerts fire once when a source goes stale, and once again when it
recovers — not on every single check, to avoid spamming the channel.
"""

import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from heartbeat import read_heartbeats

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

CHECK_INTERVAL_SECONDS = 300  # check every 5 minutes

# name -> (display label, max allowed silence in minutes)
EXPECTED_SOURCES = {
    "opensea": ("OpenSea (stream_opensea.py)", 5),
    "eth_ethereum": ("Ethereum (eth_mint_watcher.py)", 5),
    "eth_robinhood": ("Robinhood Chain (eth_mint_watcher.py)", 5),
    "discover": ("NFTCalendar (discover.py)", 25 * 60),  # 25 hours
}

_currently_down = set()


def send_alert(message):
    print(f"[watchdog] {message}")

    if not DISCORD_WEBHOOK_URL:
        return

    try:
        requests.post(
            DISCORD_WEBHOOK_URL,
            json={"username": "MintRadar - Watchdog", "content": message},
            timeout=30,
        )
    except requests.RequestException as error:
        print(f"[watchdog] Alert send failed: {error}")


def check_once():
    heartbeats = read_heartbeats()
    now = datetime.now(timezone.utc)

    for key, (label, max_minutes) in EXPECTED_SOURCES.items():
        last_seen_raw = heartbeats.get(key)

        if last_seen_raw is None:
            # No heartbeat recorded yet at all — likely still starting
            # up for the first time. Don't alert on this; only alert
            # once we've seen it alive and then lost it.
            continue

        try:
            last_seen = datetime.fromisoformat(last_seen_raw)
        except ValueError:
            continue

        age_minutes = (now - last_seen).total_seconds() / 60

        if age_minutes > max_minutes:
            if key not in _currently_down:
                send_alert(
                    f"⚠️ **{label}** hasn't reported in "
                    f"{int(age_minutes)} minutes (expected within "
                    f"{max_minutes}). It may have crashed or stalled."
                )
                _currently_down.add(key)
        else:
            if key in _currently_down:
                send_alert(f"✅ **{label}** is back — reporting normally again.")
                _currently_down.discard(key)


def run():
    print(f"[watchdog] Starting health monitor "
          f"(checking every {CHECK_INTERVAL_SECONDS}s)...")

    while True:
        try:
            check_once()
        except Exception as error:
            print(f"[watchdog] Unexpected error this cycle: {error}")

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
