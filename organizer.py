from datetime import datetime, timezone


CHAIN_ORDER = [
    "Ethereum",
    "Robinhood",
    "Base",
    "Solana",
    "Others"
]


# Magic Eden's API returns EVM chains as numeric chain IDs (as strings),
# not names, so we need to translate them before grouping.
EVM_CHAIN_ID_MAP = {
    "1": "ethereum",
    "8453": "base",
    "137": "polygon",
    "43114": "avalanche",
    "143": "monad",
    "2741": "abstract",
    "1329": "sei",
    "4326": "megaeth",
}


def get_chain_group(project):

    blockchain = project.get(
        "blockchain",
        ""
    )

    blockchain = str(
        blockchain
    ).strip().lower()

    # Resolve numeric EVM chain IDs (e.g. "1", "137") to chain names
    blockchain = EVM_CHAIN_ID_MAP.get(
        blockchain,
        blockchain
    )


    if blockchain == "ethereum":

        return "Ethereum"


    if blockchain == "robinhood":

        return "Robinhood"


    if blockchain == "base":

        return "Base"


    if blockchain == "solana":

        return "Solana"


    return "Others"


def get_launch_time(project):

    launch_datetime = project.get(
        "launch_datetime"
    )


    if not launch_datetime:

        return None


    try:

        launch_time = datetime.fromisoformat(

            launch_datetime.replace(
                "Z",
                "+00:00"
            )

        )


        if launch_time.tzinfo is None:

            launch_time = launch_time.replace(

                tzinfo=timezone.utc

            )


        return launch_time


    except ValueError:

        return None


def organize_projects(projects):

    organized = {

        "Ethereum": [],

        "Robinhood": [],

        "Base": [],

        "Solana": [],

        "Others": []

    }


    now = datetime.now(

        timezone.utc

    )


    for project in projects:

        launch_time = get_launch_time(

            project

        )


        # Skip projects with no valid launch date
        if launch_time is None:

            continue


        # Skip projects that have already launched
        if launch_time <= now:

            continue


        chain = get_chain_group(

            project

        )


        organized[chain].append(

            project

        )


    # Sort each chain from nearest launch
    # to furthest launch

    for chain in organized:

        organized[chain].sort(

            key=get_launch_time

        )


    return organized