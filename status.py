from datetime import datetime, timezone


def get_project_status(project):

    launch_datetime = project.get(
        "launch_datetime"
    )


    if not launch_datetime:

        return "unknown"


    try:

        launch_time = datetime.fromisoformat(
            launch_datetime.replace(
                "Z",
                "+00:00"
            )
        )

    except ValueError:

        return "unknown"


    now = datetime.now(
        timezone.utc
    )


    if launch_time > now:

        return "upcoming"


    return "live"