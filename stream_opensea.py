"""
stream_opensea.py

Real-time OpenSea mint detector. Unlike discover.py (which runs
periodically via cron), this is meant to run continuously in the
background — it opens one WebSocket connection to OpenSea's Stream API
and listens for "item transferred" events across ALL collections. When
it sees a transfer whose sender is the zero address, that's a mint. The
FIRST time we see a mint from a collection we haven't seen before, we
alert immediately with the collection's name and details.

Run it separately from discover.py, e.g. in its own terminal / tmux
session / systemd service:

    python3 stream_opensea.py

WHY A DIRECT WEBSOCKET CLIENT INSTEAD OF THE opensea-stream PACKAGE:
That package (and its `realtime` dependency) turned out to be
unmaintained and broken in three separate ways in a row — wrong
dependency pin, wrong API names, and a raw protocol-parsing bug. Rather
than keep patching an abandoned wrapper, this talks to OpenSea's
documented WebSocket protocol directly (it's a Phoenix Channels socket,
same family used by Elixir/Phoenix apps generally):

    Endpoint:  wss://stream-api.opensea.io/socket/websocket?token=<API_KEY>
    Heartbeat: {"topic": "phoenix", "event": "heartbeat",
                "payload": {}, "ref": <n>}   every 30 seconds
    Subscribe: {"topic": "collection:<slug-or-*>", "event": "phx_join",
                "payload": {}, "ref": <n>}

This removes the `opensea-stream` and `realtime` dependencies entirely
— just needs `websockets`, which is a mainstream, actively maintained
library.

CAVEATS (please read before relying on this):
- I don't have network access in my sandbox, so this hasn't been
  tested against a live connection. The message shapes below are
  taken directly from OpenSea's current official docs and documented
  example payloads, not guessed — but a live run is still the first
  real test.
- The Stream API is "best effort delivery" per OpenSea's own docs —
  messages can be dropped or arrive out of order on reconnects.
- "First mint we've seen" is tracked in seen_opensea_collections.json,
  a small local file separate from projects.json — avoids two
  processes (this one, and discover.py's cron) writing to the same
  file at the same time.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import websockets
from dotenv import load_dotenv

from heartbeat import write_heartbeat

load_dotenv()

OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

WS_URL = f"wss://stream-api.opensea.io/socket/websocket?token={OPENSEA_API_KEY}"
HEARTBEAT_INTERVAL = 30  # seconds, per OpenSea's docs

DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SEEN_FILE = DATA_DIR / "seen_opensea_collections.json"
DATA_FILE = DATA_DIR / "projects.json"
LOCK_FILE = DATA_DIR / "projects.json.lock"

RECONNECT_BASE_DELAY = 5  # seconds
RECONNECT_MAX_DELAY = 300  # seconds


# ---------------------------------------------------------------------
# Local "seen collections" tracking (so we only alert once per drop)
# ---------------------------------------------------------------------

def load_seen_collections():
    if not SEEN_FILE.exists():
        return set()

    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as file:
            return set(json.load(file))
    except (json.JSONDecodeError, OSError):
        return set()


def save_seen_collections(seen):
    with open(SEEN_FILE, "w", encoding="utf-8") as file:
        json.dump(sorted(seen), file, indent=2)


SEEN_COLLECTIONS = load_seen_collections()

OPENSEA_REST_BASE = "https://api.opensea.io/api/v2"
NEW_COLLECTION_MAX_AGE_HOURS = float(
    os.getenv("NEW_COLLECTION_MAX_AGE_HOURS", "12")
)

# Candidate field names for a collection's creation timestamp — OpenSea's
# v2 docs don't publish the exact field name for GET /collections/{slug}
# anywhere I could verify without a live call, so we check a few likely
# ones. If NONE of these match on your first real alert, tell me what
# field the response actually has and I'll fix this to use the right one.
_CREATED_DATE_FIELDS = ("created_date", "created_at", "createdDate")

# Collections matching any of these (case-insensitive, checked against
# both the collection slug and the item name) are skipped entirely —
# no age check, no alert. These are typically DeFi/utility tokens
# wrapped as NFTs rather than the kind of drop you're actually
# tracking.
EXCLUDE_KEYWORDS = (
    "position",       # LP position NFTs, e.g. "uniswap-v3-positions-nft"
    "liquidity",       # LP position NFTs phrased differently
    "badge",          # achievement/participation badges
    "sbt",             # soulbound tokens
    "soulbound",       # soulbound tokens, spelled out
    "lp token",        # another common LP-position phrasing
    "vault receipt",   # DeFi vault deposit receipts wrapped as NFTs
)


def is_low_quality_name(name):
    """
    A resolved collection name that's just digits (e.g. "1", "42") is
    a strong signal of a low-effort/spam collection — a real project
    almost always sets an actual name. Catches exactly the "1" / "9"
    style titles you flagged, which turned out to be real collection
    names, not a lookup bug.
    """
    return name.strip().isdigit()


# Chains where you've specifically flagged noise as a problem get a
# stricter bar: a real, non-trivial description. Spam/bot-deployed
# contracts almost never bother writing one; legitimate projects
# almost always do.
STRICT_QUALITY_CHAINS = {"monad"}
MIN_DESCRIPTION_LENGTH = 20


def passes_quality_bar(chain, description):
    if chain not in STRICT_QUALITY_CHAINS:
        return True
    return len((description or "").strip()) >= MIN_DESCRIPTION_LENGTH


def matches_excluded_keyword(*texts):
    combined = " ".join(t.lower() for t in texts if t)
    return any(keyword in combined for keyword in EXCLUDE_KEYWORDS)


# Chains to skip alerting on entirely (case-insensitive match against
# OpenSea's chain.name). "matic" is included defensively since Polygon
# is sometimes labeled that way instead of "polygon" depending on the
# API surface.
EXCLUDED_CHAINS = {"polygon", "matic"}


def get_collection_info(collection_slug):
    """
    Calls OpenSea's REST API once to get both (a) whether the
    collection is actually new, and (b) its real display name — the
    Stream API only gives us the individual token's metadata name
    (often just "#1089"), not the collection's actual name.

    Returns (is_new: bool, name: str | None, description: str,
    has_twitter: bool). Fails OPEN on is_new (True) if the lookup fails
    — better to occasionally alert on an old collection than to
    silently swallow a real new one. has_twitter defaults to False on
    lookup failure (safest default: don't bypass the age filter based
    on data we don't actually have).
    """

    if not OPENSEA_API_KEY:
        return True, None, "", False

    data = None
    last_error = None

    for attempt in range(3):
        try:
            response = requests.get(
                f"{OPENSEA_REST_BASE}/collections/{collection_slug}",
                headers={"x-api-key": OPENSEA_API_KEY},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            break

        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as error:
            # Transient network/TLS hiccups — worth a quick retry.
            last_error = error
            if attempt < 2:
                time.sleep(1 + attempt)  # 1s, then 2s
                continue

        except requests.RequestException as error:
            # A stable response like 404 (collection genuinely not found)
            # won't be fixed by retrying — fail immediately.
            last_error = error
            break

    if data is None:
        print(f"[stream_opensea] Collection lookup failed for "
              f"'{collection_slug}' after retries: {last_error} — "
              f"alerting anyway.")
        return True, None, "", False

    collection_name = data.get("name")
    description = data.get("description") or ""

    # Field name unverified live (same caveat as created_date/description
    # above) — checking a few likely candidates, including a possible
    # nested "socials" structure.
    has_twitter = bool(
        data.get("twitter_username")
        or data.get("twitter")
        or (isinstance(data.get("socials"), dict) and data["socials"].get("twitter"))
    )

    created_raw = None
    for field in _CREATED_DATE_FIELDS:
        if data.get(field):
            created_raw = data[field]
            break

    if created_raw is None:
        print(f"[stream_opensea] No recognized creation-date field in "
              f"collection response for '{collection_slug}' (tried "
              f"{_CREATED_DATE_FIELDS}) — alerting anyway. If this "
              f"collection is actually old, tell me the field name from "
              f"the real response and I'll fix the check.")
        return True, collection_name, description, has_twitter

    try:
        created_at = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        print(f"[stream_opensea] Couldn't parse creation date "
              f"'{created_raw}' for '{collection_slug}' — alerting anyway.")
        return True, collection_name, description, has_twitter

    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600

    is_new = age_hours <= NEW_COLLECTION_MAX_AGE_HOURS

    return is_new, collection_name, description, has_twitter


# ---------------------------------------------------------------------
# projects.json read-modify-write with a simple file lock, since
# discover.py's cron job may write to the same file
# ---------------------------------------------------------------------

def _acquire_lock(timeout=10):
    start = time.time()
    while LOCK_FILE.exists():
        if time.time() - start > timeout:
            print("[stream_opensea] Lock file stuck — proceeding anyway.")
            break
        time.sleep(0.2)
    LOCK_FILE.touch()


def _release_lock():
    LOCK_FILE.unlink(missing_ok=True)


def append_project(project):
    _acquire_lock()

    try:
        if DATA_FILE.exists():
            try:
                with open(DATA_FILE, "r", encoding="utf-8") as file:
                    projects = json.load(file)
            except json.JSONDecodeError:
                projects = []
        else:
            projects = []

        projects_by_url = {p["url"]: p for p in projects if p.get("url")}
        projects_by_url[project["url"]] = project

        with open(DATA_FILE, "w", encoding="utf-8") as file:
            json.dump(
                list(projects_by_url.values()), file, indent=4, ensure_ascii=False
            )

    finally:
        _release_lock()


# ---------------------------------------------------------------------
# Discord alert (immediate, single-project ping — separate from
# alerts.py's batched digest format)
# ---------------------------------------------------------------------

EMBED_COLOR_OPENSEA = 0x2081E2  # OpenSea's own blue


def send_instant_alert(project):
    contract_address = project.get("contract_address") or "Unknown"

    embed = {
        "title": f"🆕 {project['name']}",
        "url": project["url"],
        "color": EMBED_COLOR_OPENSEA,
        "fields": [
            {"name": "⛓️ Chain", "value": project["blockchain"], "inline": True},
            {"name": "📜 Contract", "value": f"`{contract_address}`", "inline": False},
            {"name": "🔗 OpenSea", "value": project["url"], "inline": False},
        ],
        "footer": {"text": "MintRadar • OpenSea"},
        # Discord renders this natively in the viewer's own timezone/format —
        # no manual date formatting needed.
        "timestamp": project["launch_datetime"],
    }

    print(f"🆕 NEW OPENSEA MINT DETECTED — {project['name']} "
          f"({project['blockchain']}) — {contract_address}")

    if not DISCORD_WEBHOOK_URL:
        print("[stream_opensea] Discord webhook not configured.")
        return

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"username": "MintRadar", "embeds": [embed]},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            print(
                f"[stream_opensea] Discord alert failed: "
                f"{response.status_code} {response.text}"
            )
    except requests.RequestException as error:
        print(f"[stream_opensea] Discord alert failed to send: {error}")


# ---------------------------------------------------------------------
# Event handling
# ---------------------------------------------------------------------

def handle_item_transferred(frame):
    """
    frame is the full Phoenix channel message:

    {
        "topic": "collection:*",
        "event": "item_transferred",
        "payload": {
            "event_type": "item_transferred",
            "sent_at": "...",
            "payload": {
                "collection": {"slug": "..."},
                "event_timestamp": "...",
                "from_account": {"address": "0x0000...0000"},
                "to_account": {"address": "0x..."},
                "item": {
                    "chain": {"name": "ethereum"},
                    "metadata": {"name": "..."},
                    "nft_id": "ethereum/0xcontract/tokenid",
                    "permalink": "https://opensea.io/assets/..."
                }
            }
        },
        "ref": null
    }

    (Two levels of "payload" nesting — OpenSea's own event envelope
    sits inside the Phoenix channel envelope.)
    """

    outer_payload = frame.get("payload", {})
    data = outer_payload.get("payload", outer_payload)

    from_address = data.get("from_account", {}).get("address", "")

    if from_address.lower() != ZERO_ADDRESS:
        return  # not a mint — just a regular transfer/sale

    collection_slug = data.get("collection", {}).get("slug", "")

    if not collection_slug or collection_slug in SEEN_COLLECTIONS:
        return  # already checked this collection before

    item = data.get("item", {})
    item_name = item.get("metadata", {}).get("name", "")
    chain = item.get("chain", {}).get("name", "unknown")

    # Mark as seen regardless of the outcome below, so we don't re-check
    # this same (possibly old, possibly excluded) collection again.
    SEEN_COLLECTIONS.add(collection_slug)
    save_seen_collections(SEEN_COLLECTIONS)

    if chain.lower() in EXCLUDED_CHAINS:
        print(f"[stream_opensea] '{collection_slug}' is on an excluded "
              f"chain ({chain}) — skipping.")
        return

    if matches_excluded_keyword(collection_slug, item_name):
        print(f"[stream_opensea] '{collection_slug}' matches an excluded "
              f"keyword (LP position / badge) — skipping.")
        return

    is_new, collection_name, description, has_twitter = get_collection_info(collection_slug)

    if not is_new and not has_twitter:
        print(f"[stream_opensea] '{collection_slug}' minted, but isn't "
              f"a new collection (older than {NEW_COLLECTION_MAX_AGE_HOURS}h) "
              f"and has no linked X profile to justify the exception — "
              f"skipping alert.")
        return
    elif not is_new and has_twitter:
        print(f"[stream_opensea] '{collection_slug}' is older than "
              f"{NEW_COLLECTION_MAX_AGE_HOURS}h but has a linked X profile "
              f"— likely a phased mint, allowing it through.")

    if not passes_quality_bar(chain, description):
        print(f"[stream_opensea] '{collection_slug}' on {chain} has no "
              f"real description ({MIN_DESCRIPTION_LENGTH}+ chars required "
              f"for this chain) — skipping as likely low-effort/spam.")
        return

    nft_id = item.get("nft_id", "")
    contract_address = nft_id.split("/")[1] if nft_id.count("/") >= 2 else ""

    display_name = collection_name or item_name or "Unknown Project"

    if is_low_quality_name(display_name):
        print(f"[stream_opensea] '{collection_slug}' resolved name "
              f"('{display_name}') is just a number — low-effort/spam "
              f"signal, skipping.")
        return

    project = {
        # Prefer the collection's real name (from the REST lookup) over
        # the individual token's metadata name, which is often just a
        # bare number like "#1089".
        "name": display_name,
        "url": f"https://opensea.io/collection/{collection_slug}",
        "source": "OpenSea",
        "blockchain": chain,
        "contract_address": contract_address,
        "launch_datetime": data.get(
            "event_timestamp", datetime.now(timezone.utc).isoformat()
        ),
        "description": f"First mint detected for '{collection_slug}'.",
        "verified": False,
        "status": "live",  # it just minted, so it's live as of detection
    }

    print(f"[stream_opensea] New collection detected: {project['name']}")

    append_project(project)
    send_instant_alert(project)


# ---------------------------------------------------------------------
# Raw WebSocket connection loop
# ---------------------------------------------------------------------

async def heartbeat_loop(ws):
    ref = 1
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await ws.send(json.dumps(
            {"topic": "phoenix", "event": "heartbeat", "payload": {}, "ref": ref}
        ))
        ref += 1
        write_heartbeat("opensea")


def normalize_frame(parsed):
    """
    Phoenix Channels has two wire formats: the object format OpenSea's
    docs show ({"topic":..., "event":..., "payload":..., "ref":...}),
    and a newer array format some Phoenix servers default to
    ([join_ref, ref, topic, event, payload]). OpenSea's server rejects
    the connection outright (HTTP 400 at handshake) if we explicitly
    request the object format via ?vsn=1.0.0, so we take whatever
    format it sends by default and normalize it here instead.
    """

    if isinstance(parsed, dict):
        return parsed

    if isinstance(parsed, list) and len(parsed) == 5:
        _join_ref, ref, topic, event, payload = parsed
        return {"topic": topic, "event": event, "payload": payload, "ref": ref}

    return None


async def listen():
    async with websockets.connect(WS_URL) as ws:

        # Subscribe to ALL collections via the "*" wildcard slug —
        # this is what makes "detect it the instant it's minted"
        # possible without knowing the collection in advance.
        await ws.send(json.dumps({
            "topic": "collection:*",
            "event": "phx_join",
            "payload": {},
            "ref": 0,
        }))

        heartbeat_task = asyncio.create_task(heartbeat_loop(ws))

        try:
            async for raw_message in ws:
                try:
                    parsed = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue

                frame = normalize_frame(parsed)

                if frame is None:
                    print(f"[stream_opensea] Unrecognized frame shape: {parsed!r}")
                    continue

                event_name = frame.get("event")

                if event_name == "phx_reply":
                    status = frame.get("payload", {}).get("status")
                    print(f"[stream_opensea] Join reply: {status}")
                    if status != "ok":
                        print(f"[stream_opensea] Join failed: {frame}")
                    continue

                if event_name == "item_transferred":
                    handle_item_transferred(frame)

        finally:
            heartbeat_task.cancel()


def run():
    if not OPENSEA_API_KEY:
        print(
            "[stream_opensea] OPENSEA_API_KEY not set in .env — cannot "
            "connect. Add OPENSEA_API_KEY=your_key to .env and retry."
        )
        return

    delay = RECONNECT_BASE_DELAY

    while True:
        try:
            print("[stream_opensea] Connecting to OpenSea Stream API...")
            asyncio.run(listen())

        except KeyboardInterrupt:
            print("[stream_opensea] Stopped by user.")
            break

        except Exception as error:
            print(f"[stream_opensea] Connection error: {error}")
            print(f"[stream_opensea] Reconnecting in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
            continue

        # A clean exit from listen() (e.g. server closed the socket)
        # also triggers a reconnect, with the backoff reset.
        delay = RECONNECT_BASE_DELAY
        print("[stream_opensea] Connection closed, reconnecting...")
        time.sleep(RECONNECT_BASE_DELAY)


if __name__ == "__main__":
    run()
