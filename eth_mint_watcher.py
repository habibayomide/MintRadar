"""
eth_mint_watcher.py

Watches EVM chains for brand-new NFT contracts by polling for mint
events (Transfer events where the sender is the zero address) across
the ENTIRE chain — not one contract at a time. Different mechanism
from stream_opensea.py (which gets events pushed to it); neither
Etherscan nor Blockscout offer a push/webhook option, so this polls on
an interval instead.

Currently watches: Ethereum (via Etherscan) and Robinhood Chain (via
Blockscout — Etherscan doesn't index Robinhood Chain at all). Base can
be added later as one more entry in CHAINS below, since it uses the
same Etherscan API as Ethereum, just a different chain ID.

Run it in its own terminal/session, separate from discover.py and
stream_opensea.py:

    python3 eth_mint_watcher.py

HOW IT WORKS (same logic for every configured chain):
1. Every POLL_INTERVAL seconds, ask the chain's block explorer for the
   latest block, then query logs in that new block range for:
   - ERC-721 mints: Transfer(address,address,uint256) where topic1
     (from) is the zero address AND the log has 4 topics (topic3 =
     indexed tokenId). Checking topics length matters — ERC-20 uses
     the EXACT SAME event signature, but only has 3 topics (the
     amount is unindexed, in the data field, not a topic). Without
     this check we'd alert on every new memecoin too.
   - ERC-1155 mints: TransferSingle(...) where topic2 (from — NOT
     topic1, operator is topic1 for this event) is the zero address.
     (TransferBatch, used for admin/airdrop-style multi-mints, is not
     covered yet — most first-ever mints are single-token calls.)
2. For every mint seen, increment a persisted per-contract counter
   (kept separately per chain). This counts mints WE'VE OBSERVED since
   this script started watching that chain, not the contract's
   lifetime total.
3. The first time a contract's count crosses MINT_COUNT_THRESHOLD:
   - Check ONCE whether it's source-verified (skip if not, and never
     re-check that contract again).
   - Check ONCE whether it was actually deployed recently
     (MAX_CONTRACT_AGE_HOURS) — without this, an old-but-currently-busy
     contract (e.g. ENS's registrar, which mints constantly as people
     register domains) looks "new" just because it's the first time
     WE'VE seen it cross the threshold.
   - Read the contract's own name() function on-chain for the real
     project name, since verified "ContractName" is often just a
     shared template's class name (e.g. "ERC721SeaDrop" — hundreds of
     different projects use that same template).

CHAIN BACKENDS:
Etherscan (Ethereum, and Base if added later) uses their unified V2
API — one base URL, chain selected via a `chainid` query param.
Blockscout (Robinhood Chain) uses the older Etherscan-COMPATIBLE
classic API style (same module=/action= parameters), but each
Blockscout instance is already chain-specific by its base URL, so no
chainid param is needed or accepted. Both need their own API key —
Etherscan's and Blockscout's keys are NOT interchangeable.

CAVEATS:
- I don't have network access in my sandbox. Ethereum's logic has been
  confirmed against real runs already; Robinhood/Blockscout is new and
  untested against a live connection — same caveat as always applied
  to every new source when we first turn it on.
- Etherscan/Blockscout log endpoints cap results at 1000 per call and
  5000 blocks per range. At a short poll interval this shouldn't
  matter in normal operation, but a long downtime on restart could hit
  those caps — full pagination isn't implemented for the initial cut.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from heartbeat import write_heartbeat

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

POLL_INTERVAL_SECONDS = 20
MINT_COUNT_THRESHOLD = 30
MAX_CONTRACT_AGE_HOURS = float(os.getenv("MAX_CONTRACT_AGE_HOURS", "12"))

ZERO_ADDRESS_TOPIC = "0x" + "0" * 64

TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRANSFER_SINGLE_SIG = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
NAME_FUNCTION_SELECTOR = "0x06fdde03"  # keccak256("name()")[:4]

DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "eth_mint_watcher_state.json"
DATA_FILE = DATA_DIR / "projects.json"
LOCK_FILE = DATA_DIR / "projects.json.lock"


CHAINS = [
    {
        "key": "ethereum",
        "label": "Ethereum",
        "api_base": "https://api.etherscan.io/v2/api",
        "chain_id": 1,
        "chain_id_param": "chainid",  # confirmed via real runs
        "rpc_url": None,  # module=proxy works fine on Etherscan
        "api_key": os.getenv("ETHERSCAN_API_KEY"),
        "explorer_url": "https://etherscan.io/address/{address}",
        "embed_color": 0x21325B,
    },
    {
        "key": "robinhood",
        "label": "Robinhood Chain",
        "api_base": "https://api.blockscout.com/v2/api",
        "chain_id": 4663,
        "chain_id_param": "chain_id",  # confirmed via live curl test — NOT "chainid"
        "rpc_url": "https://api.blockscout.com/4663/json-rpc",  # module=proxy
                    # doesn't exist on Blockscout's Pro API (confirmed live:
                    # "Unknown module") — eth_blockNumber/eth_call go
                    # through this dedicated endpoint instead. module=logs
                    # and module=contract DO work on the regular api_base.
        "api_key": os.getenv("ROBINHOOD_API_KEY"),
        "explorer_url": "https://robinhoodchain.blockscout.com/address/{address}",
        "embed_color": 0x00C805,  # Robinhood's brand green
    },
]


def _empty_chain_state():
    return {"last_block": None, "counts": {}, "checked": []}


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2)


STATE = load_state()
for chain in CHAINS:
    STATE.setdefault(chain["key"], _empty_chain_state())


def api_get(chain, params, retries=3):
    params = {**params, "apikey": chain["api_key"]}
    if chain["chain_id"] is not None:
        params[chain["chain_id_param"]] = chain["chain_id"]

    last_error = None
    for attempt in range(retries):
        try:
            response = requests.get(chain["api_base"], params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as error:
            last_error = error
            if attempt < retries - 1:
                time.sleep(1 + attempt)
                continue
        except requests.RequestException as error:
            last_error = error
            break

    print(f"[eth_mint_watcher] [{chain['key']}] Request failed after retries: {last_error}")
    return None


def json_rpc_call(chain, method, rpc_params, retries=3):
    """
    Some chains (confirmed: Blockscout's Pro API) don't support
    eth_blockNumber/eth_call via the module=proxy REST-compatible
    style — those need to go through a dedicated JSON-RPC POST
    endpoint instead, authenticated with a Bearer token rather than
    an apikey query param.
    """

    payload = {"jsonrpc": "2.0", "method": method, "params": rpc_params, "id": 1}
    headers = {"authorization": f"Bearer {chain['api_key']}"}

    last_error = None
    for attempt in range(retries):
        try:
            response = requests.post(
                chain["rpc_url"], json=payload, headers=headers, timeout=15
            )
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as error:
            last_error = error
            if attempt < retries - 1:
                time.sleep(1 + attempt)
                continue
        except requests.RequestException as error:
            last_error = error
            break

    print(f"[eth_mint_watcher] [{chain['key']}] JSON-RPC request failed after retries: {last_error}")
    return None


def get_latest_block(chain):
    if chain["rpc_url"]:
        result = json_rpc_call(chain, "eth_blockNumber", [])
    else:
        result = api_get(chain, {"module": "proxy", "action": "eth_blockNumber"})

    if result is None or "result" not in result:
        return None
    try:
        return int(result["result"], 16)
    except (TypeError, ValueError):
        return None


def get_logs(chain, topic_index, from_block, to_block, event_sig):
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": from_block,
        "toBlock": to_block,
        "topic0": event_sig,
        f"topic0_{topic_index}_opr": "and",
        f"topic{topic_index}": ZERO_ADDRESS_TOPIC,
    }
    result = api_get(chain, params)
    if result is None or result.get("status") != "1":
        return []
    logs = result.get("result", [])
    return logs if isinstance(logs, list) else []


def check_verification(chain, contract_address):
    result = api_get(chain, {
        "module": "contract",
        "action": "getsourcecode",
        "address": contract_address,
    })
    if result is None or result.get("status") != "1":
        return False, "Unknown"
    entries = result.get("result", [])
    if not entries:
        return False, "Unknown"
    entry = entries[0]
    source_code = entry.get("SourceCode", "")
    contract_name = entry.get("ContractName", "Unknown") or "Unknown"
    return bool(source_code), contract_name


def is_contract_actually_new(chain, contract_address):
    result = api_get(chain, {
        "module": "contract",
        "action": "getcontractcreation",
        "contractaddresses": contract_address,
    })
    if result is None or result.get("status") != "1":
        return True
    entries = result.get("result", [])
    if not entries:
        return True
    timestamp_raw = entries[0].get("timestamp")
    if not timestamp_raw:
        return True
    try:
        created_at = datetime.fromtimestamp(int(timestamp_raw), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return True
    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    return age_hours <= MAX_CONTRACT_AGE_HOURS


def _decode_abi_string(hex_result):
    if not hex_result or hex_result in ("0x", "0x0"):
        return None
    data = hex_result[2:] if hex_result.startswith("0x") else hex_result
    if len(data) < 128:
        return None
    try:
        length = int(data[64:128], 16)
        string_hex = data[128:128 + length * 2]
        return bytes.fromhex(string_hex).decode("utf-8", errors="replace").strip()
    except (ValueError, IndexError):
        return None


def get_onchain_name(chain, contract_address):
    call_params = [
        {"to": contract_address, "data": NAME_FUNCTION_SELECTOR},
        "latest",
    ]

    if chain["rpc_url"]:
        result = json_rpc_call(chain, "eth_call", call_params)
    else:
        result = api_get(chain, {
            "module": "proxy",
            "action": "eth_call",
            "to": contract_address,
            "data": NAME_FUNCTION_SELECTOR,
            "tag": "latest",
        })

    if result is None or "result" not in result:
        return None
    return _decode_abi_string(result["result"])


def _acquire_lock(timeout=10):
    start = time.time()
    while LOCK_FILE.exists():
        if time.time() - start > timeout:
            print("[eth_mint_watcher] Lock file stuck — proceeding anyway.")
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
            json.dump(list(projects_by_url.values()), file, indent=4, ensure_ascii=False)
    finally:
        _release_lock()


def send_instant_alert(chain, project):
    embed = {
        "title": f"🆕 {project['name']}",
        "url": project["url"],
        "color": chain["embed_color"],
        "fields": [
            {"name": "⛓️ Chain", "value": chain["label"], "inline": True},
            {"name": "🏷️ Standard", "value": project.get("token_standard", "Unknown"), "inline": True},
            {"name": "✅ Verified", "value": "Yes", "inline": True},
            {"name": "📜 Contract", "value": f"`{project['contract_address']}`", "inline": False},
        ],
        "footer": {"text": f"MintRadar • {chain['label']}"},
        "timestamp": project["launch_datetime"],
    }

    print(f"🆕 NEW {chain['label'].upper()} NFT CONTRACT DETECTED — {project['name']} "
          f"({project.get('token_standard', 'Unknown')}) — {project['contract_address']}")

    if not DISCORD_WEBHOOK_URL:
        print("[eth_mint_watcher] Discord webhook not configured.")
        return

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"username": "MintRadar", "embeds": [embed]},
            timeout=30,
        )
        if response.status_code not in (200, 204):
            print(f"[eth_mint_watcher] Discord alert failed: "
                  f"{response.status_code} {response.text}")
    except requests.RequestException as error:
        print(f"[eth_mint_watcher] Discord alert failed to send: {error}")


def process_mint_log(chain, log, standard):
    contract_address = log.get("address", "").lower()
    if not contract_address:
        return

    topics = log.get("topics", [])
    if standard == "ERC-721" and len(topics) != 4:
        return

    chain_state = STATE[chain["key"]]

    if contract_address in chain_state["checked"]:
        return

    chain_state["counts"][contract_address] = chain_state["counts"].get(contract_address, 0) + 1

    if chain_state["counts"][contract_address] < MINT_COUNT_THRESHOLD:
        return

    chain_state["checked"].append(contract_address)

    is_verified, contract_name = check_verification(chain, contract_address)

    if not is_verified:
        print(f"[eth_mint_watcher] [{chain['key']}] {contract_address} crossed "
              f"{MINT_COUNT_THRESHOLD} mints but isn't source-verified — skipping.")
        return

    if not is_contract_actually_new(chain, contract_address):
        print(f"[eth_mint_watcher] [{chain['key']}] {contract_address} crossed "
              f"{MINT_COUNT_THRESHOLD} mints but was deployed more than "
              f"{MAX_CONTRACT_AGE_HOURS}h ago — skipping.")
        return

    onchain_name = get_onchain_name(chain, contract_address)
    display_name = onchain_name or contract_name

    project = {
        "name": display_name,
        "url": chain["explorer_url"].format(address=contract_address),
        "source": "Etherscan" if chain["key"] == "ethereum" else "Blockscout",
        "blockchain": chain["key"],
        "contract_address": contract_address,
        "token_standard": standard,
        "launch_datetime": datetime.now(timezone.utc).isoformat(),
        "description": f"{standard} contract crossed {MINT_COUNT_THRESHOLD} "
                        f"observed mints, source-verified.",
        "verified": True,
        "status": "live",
    }

    print(f"[eth_mint_watcher] [{chain['key']}] New contract detected: "
          f"{display_name} ({contract_address})")

    append_project(project)
    send_instant_alert(chain, project)


def poll_once(chain):
    chain_state = STATE[chain["key"]]

    latest_block = get_latest_block(chain)
    if latest_block is None:
        print(f"[eth_mint_watcher] [{chain['key']}] Couldn't fetch latest block — skipping.")
        return

    write_heartbeat(f"eth_{chain['key']}")

    if chain_state["last_block"] is None:
        chain_state["last_block"] = latest_block
        save_state(STATE)
        return

    from_block = chain_state["last_block"] + 1
    to_block = latest_block

    if from_block > to_block:
        return

    if to_block - from_block > 5000:
        print(f"[eth_mint_watcher] [{chain['key']}] Behind by "
              f"{to_block - from_block} blocks — clamping to a 5000-block "
              f"window (some older mints in the gap will be missed).")
        to_block = from_block + 5000

    erc721_logs = get_logs(chain, 1, from_block, to_block, TRANSFER_SIG)
    time.sleep(0.25)
    erc1155_logs = get_logs(chain, 2, from_block, to_block, TRANSFER_SINGLE_SIG)

    for log in erc721_logs:
        process_mint_log(chain, log, "ERC-721")

    for log in erc1155_logs:
        process_mint_log(chain, log, "ERC-1155")

    chain_state["last_block"] = to_block
    save_state(STATE)


def run():
    active_chains = [c for c in CHAINS if c["api_key"]]

    missing = [c["label"] for c in CHAINS if not c["api_key"]]
    if missing:
        print(f"[eth_mint_watcher] Skipping {', '.join(missing)} — missing API key in .env.")

    if not active_chains:
        print("[eth_mint_watcher] No chains configured with an API key — "
              "nothing to watch. Add ETHERSCAN_API_KEY and/or "
              "ROBINHOOD_API_KEY to .env.")
        return

    print(f"[eth_mint_watcher] Starting watcher for: "
          f"{', '.join(c['label'] for c in active_chains)} "
          f"(poll every {POLL_INTERVAL_SECONDS}s, "
          f"threshold {MINT_COUNT_THRESHOLD} mints, verified-only)...")

    while True:
        for chain in active_chains:
            try:
                poll_once(chain)
            except KeyboardInterrupt:
                raise
            except Exception as error:
                print(f"[eth_mint_watcher] [{chain['key']}] Unexpected error this cycle: {error}")

        try:
            time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("[eth_mint_watcher] Stopped by user.")
            break


if __name__ == "__main__":
    run()
