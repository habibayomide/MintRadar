"""
heartbeat.py

Shared helper for the "is this source actually alive" signal used by
watchdog.py. Each of the other scripts calls write_heartbeat(name) at
a point in their loop that proves they're genuinely still running —
not just that the process exists, but that it's actually doing its
job.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEARTBEAT_FILE = DATA_DIR / "heartbeats.json"
HEARTBEAT_LOCK = DATA_DIR / "heartbeats.json.lock"


def _acquire_lock(timeout=5):
    start = time.time()
    while HEARTBEAT_LOCK.exists():
        if time.time() - start > timeout:
            break
        time.sleep(0.1)
    HEARTBEAT_LOCK.touch()


def _release_lock():
    HEARTBEAT_LOCK.unlink(missing_ok=True)


def write_heartbeat(name):
    _acquire_lock()
    try:
        if HEARTBEAT_FILE.exists():
            try:
                with open(HEARTBEAT_FILE, "r", encoding="utf-8") as file:
                    data = json.load(file)
            except json.JSONDecodeError:
                data = {}
        else:
            data = {}

        data[name] = datetime.now(timezone.utc).isoformat()

        with open(HEARTBEAT_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
    finally:
        _release_lock()


def read_heartbeats():
    if not HEARTBEAT_FILE.exists():
        return {}
    try:
        with open(HEARTBEAT_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    except (json.JSONDecodeError, OSError):
        return {}
