import json
import os
import time
from datetime import datetime, timezone
import requests

from dotenv import load_dotenv

from organizer import organize_projects


load_dotenv()


DISCORD_WEBHOOK_URL = os.getenv(
    "DISCORD_WEBHOOK_URL"
)


MAX_CHUNK_LENGTH = 1900  # stay comfortably under Discord's 2000-char cap


def _format_discord_timestamp(launch_datetime):
    """
    Converts an ISO 8601 string into Discord's native <t:UNIX:f> markup,
    which Discord renders as a properly formatted, localized date/time
    for each viewer — works in plain message content, not just embeds.
    Falls back to the raw string if it can't be parsed.
    """

    if not launch_datetime or launch_datetime == "Unknown":
        return "Unknown"

    try:
        parsed = datetime.fromisoformat(launch_datetime.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        unix_ts = int(parsed.timestamp())
        return f"<t:{unix_ts}:f>"
    except (ValueError, AttributeError):
        return launch_datetime


def _format_project_block(index, project):

    name = project.get("name", "Unknown Project")
    launch_display = _format_discord_timestamp(project.get("launch_datetime"))
    price = project.get("price")
    currency = project.get("currency")
    url = project.get("url", "")
    twitter_url = project.get("twitter_url")

    if price is not None:
        price_text = f"{price} {currency}" if currency else str(price)
    else:
        price_text = "Unknown"

    block = (
        f"**{index}. {name}**\n"
        f"🕒 {launch_display}\n"
        f"💰 {price_text}\n"
        f"🔗 {url}\n"
    )

    if twitter_url:
        block += f"🐦 {twitter_url}\n"

    return block + "\n"


def build_discord_message(projects):
    """
    Returns a LIST of message strings, each under Discord's 2000-char
    limit, instead of one giant string. With enough new projects in a
    single run (e.g. NFTCalendar catching up after being down), one
    message easily blows past that limit and gets rejected outright —
    this splits it into as many messages as needed, re-showing the
    chain header if a chain's listings get split across a chunk
    boundary so each message still reads clearly on its own.
    """

    organized_projects = organize_projects(projects)

    chain_order = ["Ethereum", "Robinhood", "Base", "Solana", "Others"]

    chunks = []
    current_chunk = "🚨 **MINTRADAR - NFTCALENDAR UPCOMING MINTS**\n\n"

    total_projects = 0

    for chain in chain_order:
        chain_projects = organized_projects.get(chain, [])

        if not chain_projects:
            continue

        chain_header = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"⛓️ **{chain.upper()}**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
        )

        if len(current_chunk) + len(chain_header) > MAX_CHUNK_LENGTH:
            chunks.append(current_chunk)
            current_chunk = ""

        current_chunk += chain_header
        header_just_added = True

        for index, project in enumerate(chain_projects, start=1):
            block = _format_project_block(index, project)

            if len(current_chunk) + len(block) > MAX_CHUNK_LENGTH:
                chunks.append(current_chunk)
                # Re-show the chain header in the new chunk, unless we
                # literally just added it (avoids an immediate repeat).
                current_chunk = "" if header_just_added else chain_header
                header_just_added = False

            current_chunk += block
            header_just_added = False
            total_projects += 1

    if total_projects == 0:
        current_chunk += "No upcoming mints found."

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def send_discord_alert(projects):
    chunks = build_discord_message(projects)

    # Label every chunk as "(Part X/Y)" when there's more than one, so
    # a repeated chain header across messages reads as intentional
    # continuation rather than a glitch. Prepended rather than
    # string-replaced into the main header, since only the FIRST chunk
    # actually contains that header text.
    if len(chunks) > 1:
        chunks = [
            f"**(Part {i}/{len(chunks)})**\n{chunk}"
            for i, chunk in enumerate(chunks, start=1)
        ]

    for chunk in chunks:
        print(chunk)

    if not DISCORD_WEBHOOK_URL:
        print("Discord webhook URL not configured.")
        return

    for i, chunk in enumerate(chunks):
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"username": "MintRadar - NFTCalendar", "content": chunk},
            timeout=30
        )

        if response.status_code in (200, 204):
            print(f"Discord alert sent successfully ({i + 1}/{len(chunks)}).")
        else:
            print("Discord alert failed.")
            print("Status code:", response.status_code)
            print(response.text)

        if i < len(chunks) - 1:
            time.sleep(1)  # be polite to Discord's rate limit between sends


if __name__ == "__main__":
    with open("projects.json", "r", encoding="utf-8") as file:
        projects = json.load(file)

    send_discord_alert(projects)