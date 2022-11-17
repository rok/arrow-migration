from jira import JIRA
import os
import json
import csv
import argparse
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import math
import re

from datetime import datetime, timezone
from string import punctuation

from pyparsing import (
    CaselessLiteral,
    Char,
    Combine,
    FollowedBy,
    Optional,
    ParserElement,
    ParseResults,
    PrecededBy,
    SkipTo,
    StringEnd,
    StringStart,
    Suppress,
    White,
    Word,
    alphanums,
)

import logging
from jira2markdown import convert
from jira2markdown.elements import MarkupElements
from jira2markdown.markup.links import Mention
from jira2markdown.markup.base import AbstractMarkup

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

jira_user = os.environ.get("ISSUES_USER")
jira_token = os.environ.get("ISSUES_TOKEN")
github_user = os.environ.get("GITHUB_USER")
github_token = os.environ.get("GITHUB_TOKEN")
github_target_repo = os.environ.get("GITHUB_TARGET_REPO")
ALLOW_JIRA_UPDATES = False
username_mapping = {"toddfarmer" : "toddfarmer"}
cached_issue = None
issues_retrieved_from_jira = []
issues_imported_to_github = []
issues_pending_post_updates = []
mapped_issues = {}
subtask_issues = {}
related_issues = {}
related_users = {}
issue_attachments = {}
retrieval_process_complete = False
import_process_complete = False
post_import_update_complete = False
issue_mapping_complete = False

class MigratedMention(AbstractMarkup):
    def action(self, tokens: ParseResults) -> str:
        username = self.usernames.get(tokens.accountid)
        return f"`[~{tokens.accountid}]`" if username is None else f"@{username}"

    @property
    def expr(self) -> ParserElement:
        MENTION = Combine(
            "["
            + Optional(
                SkipTo("|", failOn="]") + Suppress("|"),
                default="",
                )
            + "~"
            + Optional(CaselessLiteral("accountid:"))
            + Word(alphanums + ":-").setResultsName("accountid")
            + "]",
            )
        return (
                (StringStart() | Optional(PrecededBy(White(), retreat=1), default=" "))
                + MENTION.setParseAction(self.action)
                + (StringEnd() | Optional(FollowedBy(White() | Char(punctuation, excludeChars="[") | MENTION), default=" "))
        )


def dump(issues, path):
    out = []
    for item in issues:
        out.append(item)
    with open(path, "wt") as f:
        f.write(json.dumps(out))

def get_jira_connection():
    jira_url = "https://issues.apache.org/jira/"
    conn_opts = {
        "token_auth" : (jira_user, jira_token),
        "server" : jira_url
    }

    headers = JIRA.DEFAULT_OPTIONS["headers"].copy()
    headers["Authorization"] = f"Bearer {jira_token}"
    return JIRA(server=jira_url, options={"headers": headers})

    # return JIRA({"server" : "https://issues.apache.org/jira/"}, basic_auth=("todd@voltrondata.com", jira_token))
    # return JIRA(**conn_opts)

def collect_jira_issues(filter):
    conn = get_jira_connection()
    issues = conn.search_issues(filter, maxResults = False)
    [save_related_metadata(issue.raw) for issue in issues]
    global issues_retrieved_from_jira
    issues_retrieved_from_jira = [issue.key for issue in issues]
    global retrieval_process_complete
    retrieval_process_complete = True
    logging.info(f"Completed Jira issue collection")
    return [issue for issue in issues]

def save_source_issue(issue_dict):
    filename= get_local_file_path(issue_dict["key"], dir="source")
    with open(filename, "w") as fp:
        fp.write(json.dumps(issue_dict, indent=4))
        fp.close()

def save_related_metadata(issue_dict):
    save_related_issues(issue_dict)
    save_related_users(issue_dict)
    save_issue_attachments(issue_dict)
    save_source_issue(issue_dict)

def save_related_issues(issue_dict):
    issue_subtasks = issue_dict['fields']['subtasks']
    if len(issue_subtasks):
        global subtask_issues
        subtask_issues[issue_dict['key']] = [
            {"key" : subtask["key"], "status" : subtask["fields"]["status"]["name"]}
            for subtask in issue_subtasks
        ]
    issue_relations = issue_dict["fields"]["issuelinks"]
    if len(issue_relations) > 0:
        global related_issues
        collected_issues = []
        for related_issue in issue_relations:
            direction_outward = True
            target_issue = None
            if "inwardIssue" in related_issue.keys():
                direction_outward = False
            relationship = None
            if direction_outward:
                relationship = related_issue["type"]["outward"]
                target_issue = related_issue["outwardIssue"]["key"]
            else:
                relationship = related_issue["type"]["inward"]
                target_issue = related_issue["inwardIssue"]["key"]
            collected_issues.append({"key" : target_issue, "relation" : relationship})
        related_issues[issue_dict["key"]] = collected_issues

def save_related_users(issue_dict):
    text = f"""\n### Migrated issue participants:
**Reporter**: {get_user_string_raw(issue_dict["fields"]["creator"])}
**Assignee**: {get_user_string_raw(issue_dict["fields"]["assignee"])}
<!--- WATCHERS HERE --->
"""
    related_users[issue_dict["key"]] = text

def save_issue_attachments(issue_dict):
    if 'attachment' not in issue_dict["fields"].keys():
        return
    if len(issue_dict["fields"]["attachment"]) == 0:
        return
    global issue_attachments
    issue_attachments[issue_dict["key"]] = issue_dict["fields"]["attachment"]

def collect_jira_issue(key):
    conn = get_jira_connection()
    return conn.issue(key)

def clean():
    source_dir = os.path.join(os.path.dirname(__file__), "issues", "source")
    for file in os.listdir(source_dir):
        if file.endswith(".json"):
            os.remove(os.path.join(source_dir, file))
    staged_dir = os.path.join(os.path.dirname(__file__), "issues", "staged")
    for file in os.listdir(staged_dir):
        if file.endswith(".json"):
            os.remove(os.path.join(staged_dir, file))

def get_github_format_json(jira_issue):
    issue_url = f"https://issues.apache.org/jira/browse/{jira_issue.key}"
    labels = ["Migrated from Jira"]
    closed = jira_issue.fields.status.name in ["Closed", "Resolved"]
    labels += get_component_labels(jira_issue)
    labels.append(get_issue_type_label(jira_issue))
    details = {
        "title": get_summary(jira_issue),
        "body": get_description(jira_issue.raw),
        "created_at": fix_timestamp(jira_issue.fields.created),
        "updated_at": fix_timestamp(jira_issue.fields.updated),
        "labels": labels,
        "closed": closed
    }
    # GitHub throws errors if empty string or None:
    closed_date = fix_timestamp(jira_issue.fields.resolutiondate)
    if closed_date:
        details["closed_at"] = closed_date
    comments = []
    for comment in jira_issue.fields.comment.comments:
        # Skip ASF GitHub Bot comments per https://github.com/apache/arrow/issues/14648
        if comment.author.key == "githubbot":
            continue
        comments.append({
            "created_at": fix_timestamp(comment.created),
            "body" : f"***Note**: [Comment]({issue_url}?focusedCommentId={comment.id}) by {get_user_string(comment.author)}:*\n{translate_markup(comment.body, attachments=jira_issue.raw['fields']['attachment'])}"
        })
    return {"issue": details, "comments": comments}

def get_summary(jira_issue):
    return jira_issue.fields.summary

def get_description(jira_issue_dict):
    issue_url = f"https://issues.apache.org/jira/browse/{jira_issue_dict['key']}"
    return f"""***Note**: This issue was originally created as [{jira_issue_dict['key']}]({issue_url}). Please see the [migration documentation](https://gist.github.com/toddfarmer/12aa88361532d21902818a6044fda4c3) for further details.*

### Original Issue Description:
{translate_markup(jira_issue_dict['fields']['description'], attachments=jira_issue_dict['fields']['attachment'])}"""

def save_issue_json(jira_key, issue_dict):
    file_location = get_local_file_path(jira_key, dir="staged")
    with open(file_location, 'w') as fp:
        json.dump(issue_dict, fp, indent=4)

def get_issue_type_label(jira_issue):
    issue_type = jira_issue.fields.issuetype.name
    if issue_type in ["Bug"]:
        return "Type: bug"
    if issue_type in ["Improvement", "Wish", "New Feature"]:
        return "Type: enhancement"
    if issue_type in ["Task", "Sub-task"]:
        return "Type: task"
    if issue_type in ["Test"]:
        return "Type: test"
    return None

def get_component_labels(jira_issue):
    return [f"Component: {component.name}" for component in jira_issue.fields.components]

def fix_timestamp(jira_ts):
    if not jira_ts:
        return None
    return jira_ts.split("+")[0] + "Z"

def get_user_string(user):
    if user.name in username_mapping.keys():
        return f"@{username_mapping[user.name]}"
    return f"{user.displayName} ({user.name})"

def get_user_string_raw(user):
    if not user:
        return ""
    if user["name"] in username_mapping.keys():
        return f"@{username_mapping[user['name']]}"
    return f"{user['displayName']} ({user['name']})"

def translate_markup(content, attachments=None):
    if not content:
        return ""
    elements = MarkupElements()
    elements.replace(Mention, MigratedMention)
    converted_text = convert(handle_leading_space_before_hash(content), elements=elements, usernames=username_mapping)
    if not attachments:
        return converted_text
    for attachment in attachments:
        converted_text = converted_text.replace(f"![{attachment['filename']}]({attachment['filename']})", f"![{attachment['filename']}]({attachment['content']})")
    return converted_text

def handle_leading_space_before_hash(content):
    lines = content.splitlines(True)
    returned_content = ""
    for line in lines:
        if re.match(r'\s#+\s+\S.*', line):
            returned_content += line.lstrip()
        else:
            returned_content += line
    return returned_content

def get_github_headers():
    return {
        "Authorization": f'token {github_token}',
        'User-Agent': github_user
    }

def get_local_file_path(issue, dir="staged"):
    parent_dir = os.path.dirname(__file__)
    return os.path.join(parent_dir, "issues", dir, f"{issue}.json")

def import_issue(github_issue, jira_issue_id):
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/import/issues"
    resp = None
    while True:
        try:
            resp = requests.post(url, headers=get_github_headers(), json=github_issue)
            resp.raise_for_status()
            return resp.json()['url']
        except Exception as ex:
            if github_rate_limit_exceeded(resp):
                continue
            # Payload too large:
            elif resp.status_code == 413:
                logging.error(f"Payload too large when importing issue {jira_issue_id}.")
            else:
                logging.error(f"Error importing issue {jira_issue_id} into GitHub. Rrror: {ex}, headers: {resp.headers}")
            return None

def get_github_issue_id(github_import_url):
    while True:
        resp = requests.get(github_import_url, headers=get_github_headers())
        if resp.ok:
            response_json = resp.json()
            if response_json['status'] == 'pending':
                pass
            else:
                return response_json['issue_url'].split("/")[-1]
        else:
            if github_rate_limit_exceeded(resp):
                pass
            else:
                logging.error(f"Unable to retrieve GitHub issue id from {github_import_url}, retrying.  Response headers: {resp.headers}")
        # Throttle API requests if the issue import is not complete yet:
        time.sleep(5)

def add_comment_to_migrated_issue(issue_id, comment_text):
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/issues/{issue_id}/comments"
    resp = None
    while True:
        try:
            resp = requests.post(url, headers=get_github_headers(), json={"body":comment_text})
            resp.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if github_rate_limit_exceeded(resp):
                continue
            else:
                logging.error(f"Error adding comment to GitHub issue {issue_id}.  Comment: {comment_text} Error: {err}, response headers: {resp.headers}")
                break

def get_jira_watchers(jira_issue):
    # TODO: Get the real list of Jira issue watchers
    return []

def get_github_issue(issue_id):
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/issues/{issue_id}"
    resp = None
    while True:
        try:
            resp = requests.get(url, headers=get_github_headers())
            resp.raise_for_status()
            return resp.json()
        except Exception as ex:
            if github_rate_limit_exceeded(resp):
                continue
            else:
                logging.error(f"Unable to get GitHub issue details for {issue_id}. Response headers: {resp.headers}")
                raise(ex)
# curl -X POST -H "Authorization: token ${GITHUB_TOKEN}" \
#                 -H "Accept: application/vnd.github.golden-comet-preview+json" \
#                    -d '{"issue":{"title":"My money, mo issues"}}' \
#     https://api.github.com/repos/${GITHUB_USERNAME}/foo/import/issues

def init(args):
    if args.enable_user_mapping:
        load_user_mapping_file()
    if args.clean:
        clean()
    global ALLOW_JIRA_UPDATES
    ALLOW_JIRA_UPDATES = args.enable_jira_updates

def load_user_mapping_file():
    global username_mapping
    loaded_mapping = {}
    with open('jira-to-github-user-mapping.csv', newline='') as csvfile:
        user_reader = csv.reader(csvfile)
        for row in user_reader:
            loaded_mapping[row[1]] = row[2]
    username_mapping = loaded_mapping

def import_staged_issues():
    global retrieval_process_complete
    global issues_retrieved_from_jira
    global issues_imported_to_github
    global import_process_complete
    while (not retrieval_process_complete) or len(issues_retrieved_from_jira) > 0:
        try:
            jira_issue_id = issues_retrieved_from_jira.pop(0)
            logging.info(f"Importing staged issue {jira_issue_id}")
            filename = get_local_file_path(jira_issue_id)
            with open(filename) as fp:
                issue_to_import = json.load(fp)
                import_url = import_issue(issue_to_import, jira_issue_id)
                # set_external_url_on_source_issue(jira_issue_id, import_url)
                if import_url:
                    issues_imported_to_github.append((jira_issue_id, import_url))
                else:
                    logging.error(f"Failed to import issue {jira_issue_id}, continuing.")
                fp.close()
        except IndexError:
            time.sleep(1)
        except Exception as ex:
            logging.error(f"Unhandled exception in import_staged_issues(): {ex}")
    import_process_complete = True
    logging.info(f"Completed initial import process")

def set_external_url_on_source_issue(jira_id, url):
    filename = get_local_file_path(jira_id, dir="source")
    with open(filename, "r+") as fp:
        jira_json = json.load(fp)
        jira_json['fields']['customfield_12311020'] = url
        json.dump(jira_json, fp, indent=4)
        fp.close()

def get_external_url_from_source_issue(jira_id):
    filename = get_local_file_path(jira_id, dir="source")
    with open(filename, "r+") as fp:
        jira_json = json.load(fp)
        fp.close()
        return jira_json['fields']['customfield_12311020']

def import_to_github():
    with ThreadPoolExecutor() as e:
        e.submit(import_staged_issues)
        e.submit(second_pass)
        e.submit(post_import_updates)

def stage(filter="project = ARROW order by key"):
    jira_issues = collect_jira_issues(filter)
    for jira_issue in jira_issues:
        github_format_issue = get_github_format_json(jira_issue)
        save_issue_json(jira_issue, github_format_issue)
    logging.info("Completed conversion of Jira issues to GitHub format")

def set_issue_mapping(jira_id, github_id):
    global mapped_issues
    mapped_issues[jira_id] = github_id

def second_pass():
    global issues_imported_to_github
    global import_process_complete
    global issues_pending_post_updates
    global issue_mapping_complete
    while (not import_process_complete) or len(issues_imported_to_github) > 0:
        try:
            next_issue = issues_imported_to_github.pop(0)
            jira_issue_id = next_issue[0]
            import_task_url = next_issue[1]
            logging.info(f"Processing issue {jira_issue_id} in second pass")
            github_issue_id = get_github_issue_id(import_task_url)
            set_issue_mapping(jira_issue_id, github_issue_id)
            github_issue_url = f"https://github.com/{github_user}/{github_target_repo}/issues/{github_issue_id}"
            # set_external_url_on_source_issue(jira_issue_id, github_issue_url)
            issues_pending_post_updates.append((jira_issue_id, github_issue_url))
        except IndexError:
            time.sleep(1)
        except Exception as ex:
            logging.error(f"Unhandled exception in second pass: {ex}")
    issue_mapping_complete = True
    logging.info(f"Completed second pass")

def check_issue_ready(jira_issue_id):
    global mapped_issues
    global related_issues
    global subtask_issues
    if not jira_issue_id in mapped_issues.keys():
        return False
    if jira_issue_id in related_issues.keys():
        for related_issue in related_issues[jira_issue_id]:
            if related_issue["key"] not in mapped_issues.keys():
                return False
    if jira_issue_id in subtask_issues.keys():
        for subtask_issue in subtask_issues[jira_issue_id]:
            if subtask_issue["key"] not in mapped_issues.keys():
                return False
    return True

def update_source_jira(jira, jira_issue_id, migrated_gh_issue_url):
    if not ALLOW_JIRA_UPDATES:
        return
    try:
        jira_issue = jira.issue(jira_issue_id)
        migrated_id = migrated_gh_issue_url.split("/")[-1]
        comment = f"""This issue has been migrated as [issue #{migrated_id}|{migrated_gh_issue_url}] in GitHub. Please see the [migration documentation|https://gist.github.com/toddfarmer/12aa88361532d21902818a6044fda4c3] for further details."""
        jira.add_comment(jira_issue_id, comment)
        jira_issue.update(fields={"customfield_12311020" : migrated_gh_issue_url})
    except Exception as ex:
        logging.error(f"Failure to update source issue {jira_issue_id} with URL {migrated_gh_issue_url} and added comment {comment}: {ex}")

def post_import_updates():
    global issues_pending_post_updates
    global post_import_update_complete
    jira = None
    if ALLOW_JIRA_UPDATES:
        logging.info("creating JIRA connection to update source issue(s)...")
        jira = get_jira_connection()
    while (not issue_mapping_complete) or len(issues_pending_post_updates) > 0:
        try:
            next_issue = issues_pending_post_updates.pop(0)
            jira_issue_id = next_issue[0]
            migrated_gh_issue_url = next_issue[1]
            if not check_issue_ready(jira_issue_id):
                issues_pending_post_updates.append(next_issue)
                continue
            update_source_jira(jira, jira_issue_id, migrated_gh_issue_url)
            finalize_migrated_issue(jira_issue_id, migrated_gh_issue_url)
        except IndexError as ex:
            time.sleep(5)
            continue
        except Exception as ex:
            logging.error(f"Unhandled exception during post_import_updates(): {ex}")
            return
    post_import_update_complete = True
    logging.info(f"Completed post import updates")

def finalize_migrated_issue(jira_issue_id, migrated_github_issue_url):
    global subtask_issues
    global related_issues
    global mapped_issues
    added_description_text = ""

    try:
        if jira_issue_id in subtask_issues.keys():
            added_description_text += generate_subtask_list_for_description(jira_issue_id)
        if jira_issue_id in related_issues.keys():
            added_description_text += generate_related_issue_list_for_description(jira_issue_id)
        if jira_issue_id in issue_attachments.keys():
            added_description_text += generate_issue_attachment_list_for_description(jira_issue_id)
        added_description_text += related_users[jira_issue_id]

        github_issue_id = migrated_github_issue_url.split("/")[-1]
        github_issue = get_github_issue(github_issue_id)
        update_github_issue(github_issue_id, {"body" : github_issue["body"] + added_description_text})
        logging.info(f"Issue {jira_issue_id} import completed: {migrated_github_issue_url}")
    except Exception as ex:
        logging.error(f"Unhandled exception finalizing migrated issue {jira_issue_id} with URL: {migrated_github_issue_url}")

def generate_related_issue_list_for_description(jira_parent_issue_id):
    text = "\n### Related issues:\n" + "\n".join(
        [f" - #{mapped_issues[related_issue['key']]} ({related_issue['relation']})" for related_issue in related_issues[jira_parent_issue_id]]
    )
    return text + "\n"

def generate_subtask_list_for_description(jira_parent_issue_id):
    text = "\n### Subtasks:\n" + "\n".join(
        [f" - [{'X' if is_status_completed(subtask['status']) else ' '}] #{mapped_issues[subtask['key']]}" for subtask in subtask_issues[jira_parent_issue_id]]
    )
    return text + "\n"

def generate_issue_attachment_list_for_description(jira_parent_issue_id):
    text = "\n### Original Issue Attachments:\n" + "\n".join(
        [f" - [{attachment['filename']}]({attachment['content']})" for attachment in issue_attachments[jira_parent_issue_id]]
    )
    return text + "\n"


def is_status_completed(status_text):
    return status_text in ["Closed", "Resolved"]

def update_github_issue(issue_id, details):
    url = f"https://api.github.com/repos/{github_user}/{github_target_repo}/issues/{issue_id}"
    resp = None
    while True:
        try:
            resp = requests.patch(url, headers=get_github_headers(), json=details)
            resp.raise_for_status()
            return
        except Exception as err:
            if github_rate_limit_exceeded(resp):
                continue
            else:
                logging.error(f"Error while updating GitHub issue {issue_id} with details {details}: {err}")
                break

def github_rate_limit_exceeded(resp):
    if resp.status_code != 403:
        return False
    if int(resp.headers["X-RateLimit-Remaining"]) > 0:
        return False
    try:
        reset_time = int(resp.headers["X-RateLimit-Reset"])
        wait_time = reset_time - math.ceil(time.time())
        if wait_time > 0:
            logging.info(f"GitHub rate throttling detected, need to wait {wait_time} seconds before retrying.")
            time.sleep(wait_time)
        else:
            # Clock drift may cause rate limit errors even after waiting calculated time. Sleep a few seconds:
            logging.error(f"GitHub rate throttling detected, but no further wait required. Computed wait time is {wait_time}")
            time.sleep(3)
        return True
    except Exception as ex:
        logging.error(f"Failed to sleep: {ex}")
        return False


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--issue")
    parser.add_argument("--skip-jira-fetch", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--enable-user-mapping", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--skip-github-load", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--enable-jira-updates", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--stage", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--import-staged", action=argparse.BooleanOptionalAction, default=False)

    args = parser.parse_args()

    init(args)

    if args.stage:
        stage()
    elif args.import_staged:
        import_to_github()
    elif args.clean:
        clean()
    elif args.issue:
            github_format_issue = None
            if args.skip_jira_fetch:
                global issues_retrieved_from_jira
                issues_retrieved_from_jira.append(args.issue)
                global retrieval_process_complete
                retrieval_process_complete = True
                logging.info(f"Skipping fetching from JIRA, using existing file: {get_local_file_path(args.issue)}")
            else:
                logging.info(f"Getting details from Jira for issue {args.issue}")
                stage(filter=f"key = {args.issue}")
            if not args.skip_github_load:
                logging.info("Staring import to GitHub...")
                import_to_github()
    else:
        # stage(filter="key = ARROW-15635 or parent = ARROW-15635")
        # import_to_github()
        # stage(filter = "key in (ARROW-18308, ARROW-18323)")
        logging.info(f"Starting import")
        with ThreadPoolExecutor() as e:
            e.submit(stage)
            e.submit(import_to_github)
        logging.info(f"Completed")

if __name__ == "__main__":
    main()
