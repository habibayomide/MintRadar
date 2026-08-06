# MintRadar

Watches for new NFT drops across multiple sources and alerts to Discord
the moment something looks like a genuine new mint — filtered to cut
spam, LP-position tokens, soulbound tokens, and stale/established
contracts that just happen to be active.

## What it watches

| Script | Source | How | Runs |
|---|---|---|---|
| `discover.py` | NFTCalendar | Scrapes listing pages | Once, on a schedule (daily) |
| `stream_opensea.py` | OpenSea (all chains) | Real-time WebSocket stream | Continuously |
| `eth_mint_watcher.py` | Ethereum + Robinhood Chain | Polls block explorer logs for mint events | Continuously |

All three write into a shared `projects.json` and alert independently
to the same Discord channel via webhook.

## Filtering, at a glance

- **OpenSea**: only alerts on collections created within the last N
  hours (`NEW_COLLECTION_MAX_AGE_HOURS`, default 12h) — excludes
  Polygon entirely, excludes LP-position/badge/SBT-style collections
  by keyword, rejects collections whose resolved name is just a bare
  number (a strong spam signal), and requires a real description for
  Monad specifically (configurable via `STRICT_QUALITY_CHAINS`).
- **Ethereum/Robinhood**: only alerts on contracts that are
  source-verified, deployed within the last N hours
  (`MAX_CONTRACT_AGE_HOURS`, default 12h), and have crossed a minimum
  observed-mint threshold (30, hardcoded in `MINT_COUNT_THRESHOLD`) —
  filters out ERC-20 tokens (which share ERC-721's Transfer event
  signature) and old-but-currently-busy contracts (e.g. ENS's
  registrar, which mints constantly but isn't a new drop).

## Project layout

```
MintRadar/
├── discover.py            # NFTCalendar — run once, exits
├── stream_opensea.py      # OpenSea — runs forever
├── eth_mint_watcher.py    # Ethereum + Robinhood Chain — runs forever
├── dashboard.py           # CLI summary of everything in projects.json
├── alerts.py              # Discord digest builder for discover.py
├── organizer.py           # Groups projects by chain
├── filters.py             # Test/duplicate project filtering
├── status.py              # upcoming/live/unknown status logic
├── start.sh               # Supervisor script — runs all three together
│                           # (for single-service deployment, e.g. Railway)
├── sources/
│   └── nftcalendar.py      # NFTCalendar scraper (uses curl_cffi)
├── web/                    # Flask dashboard (in progress, not wired up)
├── requirements.txt
└── .env.example
```

## Setup (local / WSL)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your real values, see below
```

### Required environment variables

```
DISCORD_WEBHOOK_URL=            # required by all three scripts
OPENSEA_API_KEY=                # required by stream_opensea.py
ETHERSCAN_API_KEY=              # required for Ethereum in eth_mint_watcher.py
ROBINHOOD_API_KEY=              # Blockscout Pro API key, required for Robinhood Chain
                                  # (NOT the same key type as Etherscan's)
```

Optional tuning (all have working defaults):

```
NEW_COLLECTION_MAX_AGE_HOURS=12   # OpenSea freshness window
MAX_CONTRACT_AGE_HOURS=12         # Ethereum/Robinhood freshness window
DATA_DIR=.                        # where projects.json + state files live
                                    # (set to a mounted volume path in production —
                                    # see Deployment below)
```

## Running locally

Each script is independent — run each in its own terminal:

```bash
python3 discover.py          # run once; re-run periodically (cron) to catch new listings
python3 stream_opensea.py    # leave running
python3 eth_mint_watcher.py  # leave running
python3 dashboard.py         # check current state anytime
```

## Deployment (Railway)

`start.sh` runs all three scripts together as one process — each
persistent script gets auto-restarted if it crashes, and `discover.py`
re-runs itself once every 24 hours internally (it's designed to exit
after one run, not loop on its own).

Setup:
1. Deploy from this repo, set the **Custom Start Command** to `bash start.sh`.
2. Add all the environment variables listed above, **plus**
   `DATA_DIR=/data`.
3. Attach a **Volume** mounted at `/data` — not the project root.
   Mounting a volume directly over the working directory hides the
   application code sitting there (standard container behavior, not
   a Railway-specific quirk) — `DATA_DIR` exists specifically so the
   volume and the code never overlap.

## Known limitations

- `sources/nftcalendar.py` scrapes rendered HTML with best-effort
  heuristics (no official API exists) — occasional bad names/dates
  are possible if the site's markup shifts.
- Etherscan and Blockscout log endpoints cap at 1000 results / 5000
  blocks per call — a long watcher downtime before restart could miss
  some mints in the gap (no pagination implemented for this yet).
- The OpenSea Stream API is "best effort delivery" per OpenSea's own
  docs — occasional missed events are possible, not just a bug here.
- `web/` (Flask dashboard) exists but isn't part of the automated
  pipeline yet.
