# filters.py


# Words that commonly indicate a test collection
TEST_KEYWORDS = [
    "test",
    "testing",
    "demo",
    "example",
    "sandbox"
]


def is_test_project(project):

    name = project.get("name", "").lower()

    for keyword in TEST_KEYWORDS:

        if keyword in name:
            return True

    return False


def remove_test_projects(projects):

    clean_projects = []

    for project in projects:

        if not is_test_project(project):

            clean_projects.append(project)

    return clean_projects


def remove_duplicate_projects(projects):

    unique_projects = []

    seen_urls = set()

    for project in projects:

        url = project.get("url")

        if not url:
            continue

        if url not in seen_urls:

            seen_urls.add(url)

            unique_projects.append(project)

    return unique_projects


def filter_projects(projects):

    """
    Apply all filtering rules to the discovered projects.
    """

    projects = remove_test_projects(projects)

    projects = remove_duplicate_projects(projects)

    return projects