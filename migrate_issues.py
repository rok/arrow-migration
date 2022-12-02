from jira import JIRA
import asyncio
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

class RelatedIssues:
    def __init__(self, issue_dict):
        self.referenced_issues = get_related_issues(issue_dict)
        self.subtasks = get_subtasks(issue_dict)

    def all_imported(self):
        for issue in self.referenced_issues:
            if not issue["key"] in mapped_issues:
                return False
        for issue["key"] in self.subtasks:
            if not issue in mapped_issues.keys():
                return False
        return True

    def get_referenced_issues_text(self):
        if len(self.referenced_issues) == 0:
            return ""
        text = "\n### Related issues:\n" + "\n".join(
            [f" - #{mapped_issues[referenced_issue['key']]} ({referenced_issue['relation']})" for referenced_issue in self.referenced_issues]
        )
        return text + "\n"


    def get_subtask_text(self):
        if len(self.subtasks) == 0:
            return ""
        text = "\n### Subtasks:\n" + "\n".join(
                [f" - [{'X' if is_status_completed(subtask['status']) else ' '}] #{mapped_issues[subtask['key']]}" for subtask in self.subtasks]
            )
        return text + "\n"

class MigratingIssueMetadata:
     def __init__(self, issue_dict):
         issue_id = issue_dict["key"]
         self.source_issue_id = issue_id
         self.import_url = None
         self.target_issue_id = None
         self.related_issues = RelatedIssues(issue_dict)
         self.related_users = get_related_user_text(issue_dict)
         self.attachments = get_issue_attachments(issue_dict)

     async def get_staged_issue(self):
         with open(get_local_file_path(self.source_issue_id)) as fp:
             return json.load(fp)
     async def get_source_issue(self):
         with open(get_local_file_path(self.source_issue_id, dir="source")) as fp:
             return json.load(fp)
     async def initiate_import(self):
         staged_issue = await self.get_staged_issue()
         self.import_url = await import_issue(staged_issue, self.source_issue_id)
         logging.info(f"URL for {self.source_issue_id} is {self.import_url}")

     async def stage_in_github_format(self):
         issue_dict = await self.get_source_issue()
         github_format = get_github_format_json(issue_dict)
         filename = get_local_file_path(issue_dict["key"], dir="staged")
         with open(filename, "w") as fp:
            fp.write(json.dumps(github_format, indent=4))
            fp.close()

     async def poll_import_complete(self):
         if not self.import_url:
             raise RuntimeError(f"Issue {self.source_issue_id} has no import URL to poll for completion")
         self.target_issue_id = await get_github_issue_id(self.import_url)
         mapped_issues[self.source_issue_id] = self.target_issue_id
         # await issue_post_import_queue.put(self)
         logging.info(f"Issue {self.source_issue_id} has been successfully imported as GitHub issue {self.target_issue_id}")

     def is_ready_for_post_import_update(self):
         if not self.source_issue_id:
             return False
         if not self.target_issue_id:
             return False
         return self.related_issues.all_imported()

     def get_jira_url(self):
         return f"https://issues.apache.org/jira/browse/{self.source_issue_id}"

     def get_github_issue_url(self):
         return f"https://api.github.com/repos/{github_user}/{github_target_repo}/issues/{self.target_issue_id}"

     def get_attachment_text(self):
         if len(self.attachments) == 0:
             return ""
         text = "\n### Original Issue Attachments:\n" + "\n".join(
             [f" - [{attachment['filename']}]({attachment['content']})" for attachment in self.attachments]
         )
         return text + "\n"

     def get_post_update_additional_text(self):
         return (
            self.related_issues.get_subtask_text() +
            self.related_issues.get_referenced_issues_text() +
            self.get_attachment_text() +
            self.related_users)

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

async def collect_jira_issues(queue, filter):
    conn = get_jira_connection()
    issues = conn.search_issues(filter, maxResults = False)
    import_tasks = []
    for issue in issues:
        issue_metadata = MigratingIssueMetadata(issue.raw)
        import_tasks.append(asyncio.create_task(issue_metadata.stage_in_github_format(issue.raw)))
        await queue.put(issue_metadata)
        logging.info(f"Collected issue {issue.key}")
    await asyncio.gather(*import_tasks)
    logging.info(f"Completed Jira issue collection")


def save_source_issue(issue_dict):
    filename= get_local_file_path(issue_dict["key"], dir="source")
    with open(filename, "w") as fp:
        fp.write(json.dumps(issue_dict, indent=4))
        fp.close()

def get_related_metadata(issue_dict):
    issue_metatdata = MigratingIssueMetadata(issue_dict)
    return issue_metatdata

def get_related_issues(issue_dict):
        collected_issues = []
        for related_issue in issue_dict["fields"]["issuelinks"]:
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
        return collected_issues

def get_subtasks(issue_dict):
    return [
        {"key" : subtask["key"], "status" : subtask["fields"]["status"]["name"]}
        for subtask in issue_dict["fields"]["subtasks"]
    ]

def get_related_user_text(issue_dict):
    return f"""\n### Migrated issue participants:
**Reporter**: {get_user_string_raw(issue_dict["fields"]["creator"])}
**Assignee**: {get_user_string_raw(issue_dict["fields"]["assignee"])}
<!--- WATCHERS HERE --->
"""

def get_issue_attachments(issue_dict):
    if 'attachment' not in issue_dict["fields"].keys():
        return []
    if len(issue_dict["fields"]["attachment"]) == 0:
        return []
    return issue_dict["fields"]["attachment"]

def collect_jira_issue(key):
    conn = get_jira_connection()
    return conn.issue(key)

def clean():
    source_dir = os.path.join(os.path.dirname(__file__), "issues", "source")
    for file in os.listdir(source_dir):
        os.remove(os.path.join(source_dir, file))
    staged_dir = os.path.join(os.path.dirname(__file__), "issues", "staged")
    for file in os.listdir(staged_dir):
        os.remove(os.path.join(staged_dir, file))
    completed_dir = os.path.join(os.path.dirname(__file__), "issues", "completed")
    for file in os.listdir(completed_dir):
        os.remove(os.path.join(completed_dir, file))
    logs_dir = os.path.join(os.path.dirname(__file__), "logs")
    for file in os.listdir(logs_dir):
        os.remove(os.path.join(logs_dir, file))

def get_github_format_json(issue_dict):
    issue_url = f"https://issues.apache.org/jira/browse/{issue_dict['key']}"
    labels = ["Migrated from Jira"]
    closed = issue_dict['fields']['status']['name'] in ["Closed", "Resolved"]
    labels += get_component_labels(issue_dict)
    labels.append(get_issue_type_label(issue_dict))
    details = {
        "title": get_summary(issue_dict),
        "body": get_description(issue_dict),
        "created_at": fix_timestamp(issue_dict['fields']['created']),
        "updated_at": fix_timestamp(issue_dict['fields']['updated']),
        "labels": labels,
        "closed": closed
    }
    # GitHub throws errors if empty string or None:
    closed_date = fix_timestamp(issue_dict['fields']['resolutiondate'])
    if closed_date:
        details["closed_at"] = closed_date
    comments = []
    for comment in issue_dict['fields']['comment']['comments']:
        # Skip ASF GitHub Bot comments per https://github.com/apache/arrow/issues/14648
        if comment['author']['key'] == "githubbot":
            continue
        comments.append({
            "created_at": fix_timestamp(comment['created']),
            "body" : f"***Note**: [Comment]({issue_url}?focusedCommentId={comment['id']}) by {get_user_string_raw(comment['author'])}:*\n{translate_markup(comment['body'], attachments=issue_dict['fields']['attachment'])}"
        })
    return {"issue": details, "comments": comments}

def get_summary(jira_issue):
    return jira_issue['fields']['summary']

def get_description(jira_issue_dict):
    issue_url = f"https://issues.apache.org/jira/browse/{jira_issue_dict['key']}"
    return f"""***Note**: This issue was originally created as [{jira_issue_dict['key']}]({issue_url}). Please see the [migration documentation](https://gist.github.com/toddfarmer/12aa88361532d21902818a6044fda4c3) for further details.*

### Original Issue Description:
{translate_markup(jira_issue_dict['fields']['description'], attachments=jira_issue_dict['fields']['attachment'])}"""

def save_issue_json(jira_key, issue_dict):
    file_location = get_local_file_path(jira_key, dir="staged")
    with open(file_location, 'w') as fp:
        json.dump(issue_dict, fp, indent=4)

def get_issue_type_label(raw_jira_issue):
    issue_type = raw_jira_issue['fields']['issuetype']['name']
    if issue_type in ["Bug"]:
        return "Type: bug"
    if issue_type in ["Improvement", "Wish", "New Feature"]:
        return "Type: enhancement"
    if issue_type in ["Task", "Sub-task"]:
        return "Type: task"
    if issue_type in ["Test"]:
        return "Type: test"
    return None

def get_component_labels(raw_jira_issue):
    return [f"Component: {component['name']}" for component in raw_jira_issue['fields']['components']]

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
        if re.match(r'\s+#+\s+\S.*', line):
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

async def import_issue(github_issue, jira_issue_id):
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

async def get_github_issue_id(github_import_url):
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
        await asyncio.sleep(60)

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

async def get_github_issue(issue_id):
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

async def import_to_github(filter="project = ARROW order by key"):
    ready_to_import_queue = asyncio.Queue()
    import_stage_task = asyncio.create_task(stage(ready_to_import_queue, filter=filter))
    issue_import_poll_queue = asyncio.Queue()
    issue_check_ready_queue = asyncio.Queue()
    issue_update_post_import_queue = asyncio.Queue()
    import_staged_task = asyncio.create_task(import_staged_issues(ready_to_import_queue, issue_import_poll_queue))
    poll_complete_task = asyncio.create_task(poll_completed_import(issue_import_poll_queue, issue_check_ready_queue))
    wait_ready_task = asyncio.create_task(await_issue_ready(issue_check_ready_queue, issue_update_post_import_queue))
    post_import_task = asyncio.create_task(post_import_updates(issue_update_post_import_queue))

    await import_stage_task
    logging.info("All issues staged for import")
    await cancel_task_when_queue_empty(import_staged_task, ready_to_import_queue)
    logging.info("Importing staged issues completed")
    await cancel_task_when_queue_empty(poll_complete_task, issue_import_poll_queue)
    logging.info("GitHub issue importing work completed")
    await cancel_task_when_queue_empty(wait_ready_task, issue_check_ready_queue)
    logging.info("All issues are now ready for post-import work")
    await cancel_task_when_queue_empty(post_import_task, issue_update_post_import_queue)
    logging.info("Post-import updates complete")

async def cancel_task_when_queue_empty(task, queue):
    await queue.join()
    task.cancel()

async def import_staged_issues(source_queue, target_queue):
    while True:
        # try:
        issue_metadata = await source_queue.get()
        jira_issue_id = issue_metadata.source_issue_id
        logging.info(f"Importing staged issue {jira_issue_id}")
        await issue_metadata.initiate_import()
        await target_queue.put(issue_metadata)
        # await issue_import_poll_queue.put(issue_metadata)
        source_queue.task_done()
        # except Exception as ex:
        #     logging.error(f"Unhandled exception in import_staged_issues(): {ex}")
    logging.info(f"Completed initial import process")

async def poll_completed_import(source_queue, target_queue):
    while True:
        # try:
        issue_metadata = await source_queue.get()
        await issue_metadata.poll_import_complete()
        await target_queue.put(issue_metadata)
        source_queue.task_done()
        # except Exception as ex:
        #     logging.error(f"Unhandled exception while polling for completed import: {ex}")
    logging.info("Polling for import completion is done")

async def await_issue_ready(source_queue, target_queue):
    while True:
        issue_metadata = await source_queue.get()
        if not issue_metadata.is_ready_for_post_import_update():
            await source_queue.put(issue_metadata)
            logging.info()
        else:
            await target_queue.put(issue_metadata)
        source_queue.task_done()
        await asyncio.sleep(1)

async def post_import_updates(source_queue):
    jira = None
    if ALLOW_JIRA_UPDATES:
        logging.info("creating JIRA connection to update source issue(s)...")
        jira = get_jira_connection()
    while True:
        issue_metadata = await source_queue.get()
        jira_issue_id = issue_metadata.source_issue_id
        migrated_gh_issue_url = issue_metadata.get_github_issue_url()
        await update_source_jira(jira, jira_issue_id, migrated_gh_issue_url)
        await finalize_migrated_issue(issue_metadata)
        logging.info(f"Final update completed on {jira_issue_id}")
        source_queue.task_done()
    logging.info(f"Completed post import updates")

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

async def stage(queue, filter="project = ARROW order by key"):
    await collect_jira_issues(queue, filter)

def set_issue_mapping(jira_id, github_id):
    global mapped_issues
    mapped_issues[jira_id] = github_id


async def update_source_jira(jira, jira_issue_id, migrated_gh_issue_url):
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


async def finalize_migrated_issue(issue_metadata):
    try:
        added_description_text = issue_metadata.get_post_update_additional_text()
        migrated_github_issue_url = issue_metadata.get_github_issue_url()
        github_issue_id = migrated_github_issue_url.split("/")[-1]
        github_issue = await get_github_issue(github_issue_id)
        update_github_issue(github_issue_id, {"body" : github_issue["body"] + added_description_text})
    except Exception as ex:
        logging.error(f"Unhandled exception finalizing migrated issue {issue_metadata.get_github_issue_url()} with URL: {issue_metadata.get_github_issue_url()}: {ex}")

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
            asyncio.sleep(wait_time)
        else:
            # Clock drift may cause rate limit errors even after waiting calculated time. Sleep a few seconds:
            logging.error(f"GitHub rate throttling detected, but no further wait required. Computed wait time is {wait_time}")
            asyncio.sleep(3)
        return True
    except Exception as ex:
        logging.error(f"Failed to sleep: {ex}")
        return False

def queueless_import():
    parent_dir = os.path.dirname(__file__)
    source_dir = os.path.join(parent_dir, "issues", "source")
    tasks = []
    for file in os.listdir(source_dir):
        with open(os.path.join(source_dir, file)) as fp:
            source_json = json.load(fp)
            metadata = MigratingIssueMetadata(source_json)
            tasks.append(asyncio.create_task(single_issue_import(metadata)))
    asyncio.gather(*tasks)

async def single_issue_import(metadata):
    await metadata.stage_in_github_format()
    logging.info(f"Staged issue {metadata.source_issue_id} in GitHub format")
    try:
        issue_to_import = await metadata.get_staged_issue()
        metadata.import_url = await import_issue(issue_to_import, metadata.source_issue_id)
        logging.info(f"Import URL for {metadata.source_issue_id} is {metadata.import_url}")

    except Exception as ex:
        logging.error(ex)
        logging.error(metadata.get_source_issue())
        sys.exit(0)
    logging.info(f"Imported issue {metadata.source_issue_id} into GitHub")
    github_issue_id = await get_github_issue_id(metadata.import_url)
    logging.info(f"Imported {metadata.source_issue_id} as {github_issue_id}")


async def main():

    queueless_import()
    sys.exit(0)

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
        # import_ready_queue = await asyncio.create_task(stage(filter = "key in (ARROW-18308, ARROW-18323)"))
        await import_to_github()
        logging.info(f"Completed")

if __name__ == "__main__":
    asyncio.run(main())
