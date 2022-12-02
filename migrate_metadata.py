import json
import os
import requests

# Script to help identify issues related to problems or observations made during import.

github_user = os.environ.get("GITHUB_USER")
github_token = os.environ.get("GITHUB_TOKEN")
github_target_repo = os.environ.get("GITHUB_TARGET_REPO")
component_labels = []
fix_versions = {}
COMPONENT_LABEL_COLOR = "ededed"
ISSUE_TYPE_LABEL_COLOR = "a2eeef"

def identify_leading_space_hash_text(issue_dict):
    if has_hash_with_leading_space(issue_dict['fields']['description']):
        print(f"{issue_dict['key']}\tHASH_LEADING_SPACES\tDESCRIPTION")
    else:
        for comment in [c for c in issue_dict['fields']['comment']['comments'] if c['author']['name'] != "githubbot"]:
            if has_hash_with_leading_space(comment['body']):
                print(f"{issue_dict['key']}\tHASH_LEADING_SPACES\tCOMMENT")

def stage_version_metadata(jira_json):
    issue_fix_versions = jira_json['fields']['fixVersions']
    for issue_fix_version in issue_fix_versions:
        if not issue_fix_version['name'] in fix_versions.keys():
            fix_versions[issue_fix_version['name']] = issue_fix_version

def stage_component_metadata(jira_json):
    for component in jira_json['fields']['components']:
        component_label = f"Component: {component['name']}"
        if not component_label in component_labels:
            component_labels.append(component_label)

def stage_from_source_files():
    source_dir = os.path.join(os.path.dirname(__file__), "issues", "source")
    for filename in os.listdir(source_dir):
        if filename.endswith(".json"):
            with open(os.path.join(source_dir, filename), "r+") as fp:
                jira_json = json.load(fp)
                stage_component_metadata(jira_json)
                stage_version_metadata(jira_json)
    component_labels_filename = os.path.join(os.path.dirname(__file__), "metadata", "component_labels.json")
    with open(component_labels_filename, "w+") as fp:
        json.dump(component_labels, fp, indent=4)
        fp.close()
    versions_filename = os.path.join(os.path.dirname(__file__), "metadata", "versions.json")
    with open(versions_filename, "w+") as fp:
        json.dump([version for version in fix_versions.values()], fp, indent=4)
        fp.close()

def get_github_headers():
    return {
        "Authorization": f'token {github_token}',
        'User-Agent': github_user
    }

def add_missing_metadata():
    add_missing_versions()
    add_missing_component_labels()
    add_missing_issue_type_labels()

def add_missing_versions():
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/milestones"
    resp = requests.get(url + "?state=all&per_page=100", headers=get_github_headers())
    resp.raise_for_status()
    existing_versions = [version["title"] for version in resp.json()]
    needed_versions = []
    versions_filename = os.path.join(os.path.dirname(__file__), "metadata", "versions.json")
    with open(versions_filename, "r") as fp:
        needed_versions = json.load(fp)
        fp.close()
    print(f"Needed versions: {[version['name'] for version in needed_versions]}")
    print(f"Existing versions: {existing_versions}")
    print(len(existing_versions))

    for needed_version in needed_versions:
        if not needed_version["name"] in existing_versions:
            version_state = "open" if not (needed_version["archived"] or needed_version["released"]) else "closed"
            version_payload = {
                "title" : needed_version["name"],
                "state" : version_state
            }
            if needed_version["released"] and needed_version["releaseDate"]:
                version_payload["due_on"] = needed_version["releaseDate"]+"T00:00:00Z"
            add_version_resp = requests.post(url, headers=get_github_headers(), json=version_payload)
            print(f"Adding milestone: {version_payload}")
            add_version_resp.raise_for_status()

def get_existing_labels():
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/labels?per_page=100"
    resp = requests.get(url, headers=get_github_headers())
    resp.raise_for_status()
    return [label["name"] for label in resp.json()]

def add_missing_component_labels():
    existing_labels = get_existing_labels()
    needed_labels = []
    versions_filename = os.path.join(os.path.dirname(__file__), "metadata", "component_labels.json")
    with open(versions_filename, "r") as fp:
        needed_labels = json.load(fp)
        fp.close()

    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/labels"
    for needed_label in needed_labels:
        if not needed_label in existing_labels:
            label_payload = {
                "name" : needed_label,
                "color" : COMPONENT_LABEL_COLOR,
                "description" : "Affects " + needed_label[0].lower() + needed_label[1:]
            }
            add_version_resp = requests.post(url, headers=get_github_headers(), json=label_payload)
            add_version_resp.raise_for_status()

def add_missing_issue_type_labels():
    needed_labels = [
        "Type: bug",
        "Type: enhancement",
        "Type: task",
        "Type: test"
    ]
    existing_labels = get_existing_labels()
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/labels"
    for needed_label in needed_labels:
        if not needed_label in existing_labels:
            label_payload = {
                "name" : needed_label,
                "color" : ISSUE_TYPE_LABEL_COLOR,
                "description" : "Issue is of " + needed_label[0].lower() + needed_label[1:]
            }
            add_version_resp = requests.post(url, headers=get_github_headers(), json=label_payload)
            try:
                add_version_resp.raise_for_status()
            except Exception as ex:
                print(f"Exception adding label {needed_label} with payload {label_payload} {ex}")
            print(f"Added label: {needed_label}")

def print_add_metadata():
    needed_labels = []
    filename = os.path.join(os.path.dirname(__file__), "metadata", "component_labels.json")
    with open(filename, "r") as fp:
        needed_labels = json.load(fp)
        fp.close()
    for label in sorted(needed_labels):
        print(label)

def clear_labels(prefix=None):
    for label in get_existing_labels():
        if not prefix or label.startswith(prefix):
            print(f"Deleting label: {label}")
            url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/labels/{label}"
            resp = requests.delete(url, headers=get_github_headers())
            try:
                resp.raise_for_status()
            except Exception as ex:
                print(ex)

def main():
    # stage_from_source_files()
    # clear_labels()
    # add_missing_metadata()
    print_add_metadata()

if __name__ == "__main__":
    main()