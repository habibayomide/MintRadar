import json
import os
from pathlib import Path

from sources.nftcalendar import (
    discover_projects as discover_nftcalendar
)

from filters import filter_projects

from status import get_project_status

from alerts import send_discord_alert


DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATA_FILE = DATA_DIR / "projects.json"


def load_existing_projects():

    if not DATA_FILE.exists():

        return []

    try:

        with open(
            DATA_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            return json.load(file)

    except json.JSONDecodeError:

        return []


def save_projects(projects):

    with open(
        DATA_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            projects,
            file,
            indent=4,
            ensure_ascii=False
        )


def main():

    print(
        "Starting MintRadar discovery..."
    )


    # DISCOVER

    nftcalendar_projects = (
        discover_nftcalendar()
    )

    print(
        f"NFTCalendar projects found: "
        f"{len(nftcalendar_projects)}"
    )


    discovered_projects = (

        nftcalendar_projects

    )


    print(
        f"Total projects discovered: "
        f"{len(discovered_projects)}"
    )


    # FILTER

    clean_projects = (

        filter_projects(
            discovered_projects
        )

    )


    print(
        f"Projects after filtering: "
        f"{len(clean_projects)}"
    )


    # ADD STATUS

    for project in clean_projects:

        project["status"] = (

            get_project_status(
                project
            )

        )


    # LOAD EXISTING

    existing_projects = (

        load_existing_projects()

    )


    projects_by_url = {

        project["url"]: project

        for project in existing_projects

        if project.get("url")

    }


    new_projects = 0

    updated_projects = 0

    newly_upcoming = []


    # ADD OR UPDATE

    for project in clean_projects:

        url = project.get(
            "url"
        )


        if not url:

            continue


        if url in projects_by_url:

            projects_by_url[url].update(
                project
            )

            updated_projects += 1


        else:

            projects_by_url[url] = project

            new_projects += 1


            print(
                f"New project found: "
                f"{project.get('name')}"
            )


            # Only alert for drops that actually have a future launch
            # date. Sources like Magic Eden's launchpad endpoint return
            # their FULL history, so most "new" entries (new to us,
            # not new to the world) are years-old and already launched.
            # Alerting on every one of those would spam Discord with
            # empty "No upcoming mints found" messages.
            if project.get("status") == "upcoming":

                newly_upcoming.append(project)


    # Send ONE batched alert for everything newly upcoming, instead of
    # one Discord message per project.
    if newly_upcoming:

        send_discord_alert(
            newly_upcoming
        )


    # SAVE

    all_projects = list(
        projects_by_url.values()
    )


    save_projects(
        all_projects
    )


    print(
        f"New projects added: "
        f"{new_projects}"
    )


    print(
        f"Existing projects updated: "
        f"{updated_projects}"
    )


    print(
        f"Total projects saved: "
        f"{len(all_projects)}"
    )


if __name__ == "__main__":

    main()