"""
sources/nftcalendar.py

Scrapes nftcalendar.io's per-blockchain listing pages and converts each
drop into MintRadar's project schema:

    {
        "name": str,
        "url": str,
        "source": "NFTCalendar",
        "blockchain": str,
        "launch_datetime": str (ISO 8601) | None,
        "description": str,
        "verified": bool,
    }

NFTCalendar has no public API, so this scrapes rendered HTML with
BeautifulSoup. It targets each event card by finding links to
"/event/<slug>/" and reading the card's text around that link — this
avoids depending on exact CSS class names (which I can't verify without
a live browser), but it does mean you should spot-check the output
after the first run and adjust `_extract_card_fields` if a field
comes back empty.

Per your call: for the "Jul 26 – Aug 02, 2026" style date ranges,
we use the END date as launch_datetime (not the start).

UPDATE (2nd revision): `cloudscraper` stopped working — nftcalendar.io's
bot detection has moved past what cloudscraper's bypass technique
handles (this is a known, ongoing problem with cloudscraper broadly in
2026: Cloudflare-style protection keeps evolving, and older solvers
fall behind). Switched to `curl_cffi`, which impersonates a real
browser's actual TLS fingerprint at the network/socket level, not just
HTTP headers — a fundamentally stronger technique. It exposes a
requests-compatible API (same .get() interface), added to
requirements.txt.
"""

import re

from bs4 import BeautifulSoup

from datetime import datetime, timezone

try:
    from curl_cffi import requests as _curl_requests
    _SESSION = _curl_requests.Session()
    _IMPERSONATE = "chrome"
except ImportError:
    import requests as _SESSION
    _IMPERSONATE = None
    print(
        "[nftcalendar] curl_cffi isn't installed — falling back to "
        "plain requests, which nftcalendar.io will likely block with a "
        "403. Run: pip install curl_cffi"
    )


REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://nftcalendar.io/",
}

BASE_URL = "https://nftcalendar.io"

# Chain slug -> the blockchain value MintRadar should store.
# These map to organizer.py's get_chain_group() buckets, plus a couple
# of extras that will fall into "Others".
CHAIN_PAGES = {
    "ethereum": "ethereum",
    "solana": "solana",
    "base-coinbase": "base",
    "robinhood": "robinhood",
    "avax-network": "avalanche",
}

DATE_RANGE_PATTERN = re.compile(
    r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\s*[–-]\s*([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})"
)

SINGLE_DATE_PATTERN = re.compile(
    r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})"
)

EVENT_HREF_PATTERN = re.compile(r"^/event/[^/]+/?$")

RELATIVE_TIME_PATTERN = re.compile(
    r"^\d+\s+(second|minute|hour|day|week|month|year)s?\s+ago$",
    re.IGNORECASE,
)


def _parse_end_date(text):
    """
    Extract the launch_datetime from a card's text block, using the END
    of a date range (per your preference), or a single date if that's
    all that's present. Returns an ISO 8601 UTC string, or None.
    """

    range_match = DATE_RANGE_PATTERN.search(text)

    if range_match:
        date_str = range_match.group(2)

    else:
        single_match = SINGLE_DATE_PATTERN.search(text)

        if not single_match:
            return None

        date_str = single_match.group(1)

    try:
        parsed = datetime.strptime(date_str, "%b %d, %Y")

        parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.isoformat()

    except ValueError:
        return None


def _extract_card_fields(anchor, blockchain):
    """
    Given an <a href="/event/..."> anchor tag, walk up to a container
    that holds the full card (image, title, date, description) and
    pull out the fields we need.
    """

    href = anchor.get("href", "")

    url = href if href.startswith("http") else BASE_URL + href

    # Walk up until we hit a container with a decent amount of text,
    # or run out of parents. 6 levels is a generous ceiling for a
    # typical card/grid layout.
    container = anchor

    for _ in range(6):
        if container.parent is None:
            break
        container = container.parent
        if len(container.get_text(strip=True)) > 40:
            break

    text = container.get_text("\n", strip=True)

    # Name: prefer a heading element within the container
    heading = container.find(["h1", "h2", "h3", "h4"])

    name = heading.get_text(strip=True) if heading else anchor.get_text(
        strip=True
    )

    if not name:
        # Fall back to the slug in the URL
        name = href.strip("/").split("/")[-1].replace("-", " ").title()

    # Some cards apparently have a "posted X hours/days ago" badge that
    # our heading-selection sometimes picks up instead of the real
    # title. If the extracted name IS just that badge text, it's not a
    # usable name — fall back to the URL slug instead.
    if RELATIVE_TIME_PATTERN.match(name):
        name = href.strip("/").split("/")[-1].replace("-", " ").title()

    verified = "verified" in text.lower()

    launch_datetime = _parse_end_date(text)

    # Description: take the text, strip out the name/date/"verified"/
    # "Read More" noise, and use the longest remaining line as a
    # rough description.
    noise_pattern = re.compile(
        r"verified|read more", re.IGNORECASE
    )

    lines = [
        line for line in text.split("\n")
        if line != name
        and not DATE_RANGE_PATTERN.search(line)
        and not SINGLE_DATE_PATTERN.fullmatch(line)
        and not noise_pattern.search(line)
    ]

    description = max(lines, key=len, default="")

    return {
        "name": name,
        "url": url,
        "source": "NFTCalendar",
        "blockchain": blockchain,
        "launch_datetime": launch_datetime,
        "description": description,
        "verified": verified,
    }


def _discover_chain(slug, blockchain):

    page_url = f"{BASE_URL}/b/{slug}/"

    get_kwargs = {"headers": HEADERS, "timeout": REQUEST_TIMEOUT}
    if _IMPERSONATE:
        get_kwargs["impersonate"] = _IMPERSONATE

    try:
        response = _SESSION.get(page_url, **get_kwargs)
        response.raise_for_status()

    except Exception as error:
        # Broad catch: curl_cffi has its own exception hierarchy,
        # separate from the standard `requests` library's, so we can't
        # rely on a specific exception type here.
        print(f"[nftcalendar] Request to {page_url} failed: {error}")

        return []

    soup = BeautifulSoup(response.text, "html.parser")

    seen_urls = set()

    projects = []

    for anchor in soup.find_all("a", href=EVENT_HREF_PATTERN):

        href = anchor["href"]

        if href in seen_urls:
            continue

        seen_urls.add(href)

        projects.append(
            _extract_card_fields(anchor, blockchain)
        )

    return projects


def discover_projects():
    """
    Returns a list of upcoming NFTCalendar drops, tagged with blockchain,
    in MintRadar's project schema.
    """

    projects = []

    for slug, blockchain in CHAIN_PAGES.items():

        projects.extend(_discover_chain(slug, blockchain))

    return projects


if __name__ == "__main__":

    found = discover_projects()

    print(f"Found {len(found)} NFTCalendar projects.")

    for p in found[:5]:

        print(p)
