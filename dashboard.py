import json
from pathlib import Path


DATA_FILE = Path("projects.json")


def load_projects():

    if not DATA_FILE.exists():

        print(
            "projects.json was not found."
        )

        return []


    with open(
        DATA_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


def display_project(
    number,
    project
):

    name = project.get(
        "name",
        "Unknown"
    )


    source = project.get(
        "source",
        "Unknown"
    )


    blockchain = project.get(
        "blockchain",
        "Unknown"
    )


    price = project.get(
        "price",
        "Unknown"
    )


    launch_datetime = project.get(
        "launch_datetime",
        "Unknown"
    )


    url = project.get(
        "url",
        "No URL"
    )


    print(
        f"{number}. {name}"
    )


    print(
        f"   Source: {source}"
    )


    print(
        f"   Blockchain: {blockchain}"
    )


    print(
        f"   Price: {price}"
    )


    print(
        f"   Launch: {launch_datetime}"
    )


    print(
        f"   URL: {url}"
    )


    print()


def main():

    projects = load_projects()


    if not projects:

        print(
            "No projects found."
        )

        return


    upcoming = [

        project

        for project in projects

        if project.get(
            "status"
        ) == "upcoming"

    ]


    live = [

        project

        for project in projects

        if project.get(
            "status"
        ) == "live"

    ]


    unknown = [

        project

        for project in projects

        if project.get(
            "status"
        ) == "unknown"

    ]


    print()


    print(
        "=" * 60
    )


    print(
        "                    MINTRADAR"
    )


    print(
        "=" * 60
    )


    print()


    print(
        f"TOTAL PROJECTS: {len(projects)}"
    )


    print(
        f"UPCOMING: {len(upcoming)}"
    )


    print(
        f"LIVE: {len(live)}"
    )


    print(
        f"UNKNOWN: {len(unknown)}"
    )


    print()


    print(
        "=" * 60
    )


    print(
        "                    UPCOMING PROJECTS"
    )


    print(
        "=" * 60
    )


    if upcoming:

        for number, project in enumerate(
            upcoming,
            start=1
        ):

            display_project(
                number,
                project
            )

    else:

        print(
            "No upcoming projects found."
        )


    print()


    print(
        "=" * 60
    )


    print(
        "                    LIVE PROJECTS"
    )


    print(
        "=" * 60
    )


    if live:

        for number, project in enumerate(
            live,
            start=1
        ):

            display_project(
                number,
                project
            )

    else:

        print(
            "No live projects found."
        )


if __name__ == "__main__":

    main()