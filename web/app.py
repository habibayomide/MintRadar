from flask import Flask, render_template, request
import json
from pathlib import Path


app = Flask(__name__)


DATA_FILE = Path(__file__).parent.parent / "projects.json"


def load_projects():

    if not DATA_FILE.exists():

        return []


    with open(
        DATA_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


@app.route("/")
def index():

    projects = load_projects()


    search = request.args.get(
        "search",
        ""
    ).lower()


    status = request.args.get(
        "status",
        "all"
    )


    source = request.args.get(
        "source",
        "all"
    )


    blockchain = request.args.get(
        "blockchain",
        "all"
    )


    if search:

        projects = [

            project

            for project in projects

            if search in project.get(
                "name",
                ""
            ).lower()

        ]


    if status != "all":

        projects = [

            project

            for project in projects

            if project.get(
                "status"
            ) == status

        ]


    if source != "all":

        projects = [

            project

            for project in projects

            if project.get(
                "source"
            ) == source

        ]


    if blockchain != "all":

        projects = [

            project

            for project in projects

            if project.get(
                "blockchain"
            ) == blockchain

        ]


    all_projects = load_projects()


    sources = sorted({

        project.get(
            "source"
        )

        for project in all_projects

        if project.get(
            "source"
        )

    })


    blockchains = sorted({

        project.get(
            "blockchain"
        )

        for project in all_projects

        if project.get(
            "blockchain"
        )

    })


    return render_template(

        "index.html",

        projects=projects,

        sources=sources,

        blockchains=blockchains,

        selected_search=search,

        selected_status=status,

        selected_source=source,

        selected_blockchain=blockchain

    )


if __name__ == "__main__":

    app.run(

        debug=True

    )