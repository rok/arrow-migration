from jira import JIRA
import os
import pickle
import re
import csv
import time
from functools import partial
from multiprocessing import Pool, cpu_count
from string import punctuation
from jinja2 import Template
import jira2markdown
from jira2markdown.markup.links import Mention
from jira2markdown.markup.base import AbstractMarkup
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
import requests

JIRA_USER_PROFILE_URL ="https://issues.apache.org/jira/secure/ViewProfile.jspa?name="

COMMENT_TEMPLATE = Template(
    "[{{ comment.author.displayName }}]({{ jira_url }}?#comment-{{ comment.id }})" \
    "{% if comment.author.name in user_mapping %} / @{{ user_mapping[comment.author.name] }}{% endif %}:\n" \
    "{{ comment_content[comment.id] }}")

BODY_TEMPLATE = Template(
    "{{ description }}\n"
    "{%if environment %}"
        "\n**Environment**: {{ environment }}"
    "{% endif %}"
    "\n**Reporter**: [{{ reporter.displayName }}]({{ jira_user_url }}{{ reporter.name.replace(' ', '+') }})"
        "{% if reporter.name in user_mapping %} / @{{ user_mapping[reporter.name] }}{% endif %}"
    "{%if assignee %}"
        "\n**Assignee**: [{{ assignee.displayName }}]({{ jira_user_url }}{{ assignee.name.replace(' ', '+') }})"
            "{% if assignee.name in user_mapping %} / @{{ user_mapping[assignee.name] }}{% endif %}"
    "{% endif %}"
    "{%if watchers %}"
        "\n**Watchers**: {% for w in watchers %}"
            "{% if w.name != 'githubbot'%} [{{ w.displayName }}]({{ jira_user_url }}{{ w.name.replace(' ', '+') }}){% endif %}"
            "{% if w.name in user_mapping %} / @{{ user_mapping[w.name] }}{% endif %}"
            "{{ ', ' if not loop.last else '' }}"
        "{% endfor %}"
    "{% endif %}"
    "{%if subtasks %}"
        "\n#### Subtasks:{% for s in subtasks %}"
            "\n- {%if closed %}[X]{% else %}[ ]{% endif %} "
            "[{{ s.fields.summary }}]({{ s.permalink() }})"
        "{% endfor %}"
    "{% endif %}"
    "{%if linked_issues %}"
        "\n#### Related issues:{% for li in linked_issues %}"
            "\n- [{{ li['summary'] }}]({{ li['url'] }}) ({{ li['relationship'] }})"
        "{% endfor %}"
    "{% endif %}"
    "{%if attachment %}"
        "\n#### Original Issue Attachments:{% for a in attachment %}"
            "\n- [{{ a.filename }}]({{ a.content }})"
        "{% endfor %}"
    "{% endif %}"
    "{%if externally_tracked %}"
        "\n#### Externally tracked issue: [{{ externally_tracked }}]({{ externally_tracked }})"
    "{% endif %}"
    "{%if remote_links %}"
        "\n#### PRs and other links:{% for rl in remote_links %}"
        "\n- [{{ rl.object.title }}]({{ rl.object.url }})"
    "{% endfor %}"
    "{% endif %}"
    "\n\n<sub>**Note**: *This issue was originally created as [{{ jira_key }}]({{ jira_url }}). "
    "Please see the "
    "[migration documentation]({{ migration_doc_url }}) "
    "for further details.*</sub>"
)


class MigratedMention(AbstractMarkup):
    def action(self, tokens: ParseResults) -> str:
        username = self.usernames.get(tokens.accountid)
        return f"`[~{tokens.accountid}]`" if username is None else f"@{username}"

    @property
    def expr(self) -> ParserElement:
        mention = Combine(
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
                + mention.setParseAction(self.action)
                + (StringEnd() | Optional(FollowedBy(White() | Char(punctuation, excludeChars="[") | mention), default=" "))
        )


LEADING_SPACE_HASH_PATTERN = re.compile(r"\n\s(#+\s+\S.*)")
ELEMENTS = jira2markdown.elements.MarkupElements()
ELEMENTS.replace(Mention, MigratedMention)


def get_jira_client(jira_credentials):
    return JIRA(**jira_credentials)


def _extract_linked_issues(linked_issue):
    if hasattr(linked_issue, "outwardIssue"):
        return {
            "key": linked_issue.outwardIssue.key,
            "relationship": linked_issue.type.outward,
            "summary": linked_issue.outwardIssue.fields.summary,
            "url": linked_issue.outwardIssue.permalink(),
            "completed": _is_completed(linked_issue.outwardIssue)
        }
    else:
        return {
            "key": linked_issue.inwardIssue.key,
            "relationship": linked_issue.type.inward,
            "summary": linked_issue.inwardIssue.fields.summary,
            "url": linked_issue.inwardIssue.permalink(),
            "completed": _is_completed(linked_issue.inwardIssue)
        }


def _is_completed(item):
    return item.fields.status.name in ("Closed", "Resolved")


def get_assignable_users(owner, repo, users, github_credentials):
    user_can_be_assignee = []
    with requests.Session() as s:
        for user in users:
            url = f"https://api.github.com/repos/{owner}/{repo}/assignees/{user}"
            params = {"method": "GET", "url": url, "headers": github_credentials}
            response = s.request(**params)
            if response.status_code == 204:
                user_can_be_assignee.append(user)

    return user_can_be_assignee


def get_milestone_map(owner, repo, github_credentials):
    milestone_url = f"https://api.github.com/repos/{owner}/{repo}/milestones"
    raw_milestone_map = requests.get(
        milestone_url, params={"state": "all", "per_page": 100}, headers=github_credentials)
    return {x["title"]: x["number"] for x in raw_milestone_map.json()}


def make_milestones(milestones, owner, repo, github_credentials):
    milestone_url = f"https://api.github.com/repos/{owner}/{repo}/milestones"
    results = []
    for milestone in milestones:
        results.append(requests.post(milestone_url, json=milestone, headers=github_credentials))
    return {x.json()["title"]: x.json()["number"] for x in results}


def make_labels(labels, owner, repo, github_credentials):
    label_url = f"https://api.github.com/repos/{owner}/{repo}/labels"
    results = []
    for label in labels:
        results.append(requests.post(label_url, json=label, headers=github_credentials))
    return [x.json() for x in results]


def request_to_github(params, session):
    while True:
        r = session.request(**params)
        request_data = ", ".join((params["method"], params["url"], params.get("body", "")))

        if r.status_code in (200, 202, 204):
            # all is good
            return r
        elif r.status_code == 403:
            # throttling
            print("Response was: ", r.json())
            reset_time = int(r.headers["X-RateLimit-Reset"])
            wait_time = reset_time - round(time.time() + .5)
            if wait_time > 0:
                print(f"Throttled on {request_data}, call:  {r.text}\nSleeping for {wait_time // 60} minutes.")
                time.sleep(wait_time)
            else:
                time.sleep(1)
        else:
            # something is wrong
            print(f"Request {request_data} returned status code {r.status_code} and {r.text}")
            r.raise_for_status()


def get_and_cache_jira_data(jira_project_name, jira_credentials, jira_to_github_user_mapping_file, cache_folder):
    raw_jira_issues_filename = os.path.join(cache_folder, "raw_jira_issues.pickle")
    raw_jira_watchers_filename = os.path.join(cache_folder, 'raw_jira_watchers.pickle')
    raw_jira_remote_links_filename = os.path.join(cache_folder, "raw_jira_remote_links.pickle")
    translated_markup_filename = os.path.join(cache_folder, "translated_markdown.pickle")
    print(f"Cache folder: {cache_folder}")
    print(raw_jira_issues_filename)
    print(raw_jira_watchers_filename)
    print(raw_jira_remote_links_filename)
    print(translated_markup_filename)

    os.makedirs(cache_folder, exist_ok=True)
    conn = get_jira_client(jira_credentials)

    user_mapping = {}
    with open(jira_to_github_user_mapping_file, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            user_mapping[row[0]] = row[2]
            user_mapping[row[1]] = row[2]

    if not os.path.exists(raw_jira_issues_filename):
        issues = conn.search_issues(f"project = {jira_project_name} order by key", maxResults=False, fields='*all')

        print(f"Writing {raw_jira_issues_filename}")
        with open(raw_jira_issues_filename, 'wb') as handle:
            pickle.dump(issues, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(raw_jira_issues_filename, 'rb') as handle:
            issues = pickle.load(handle)

    if not os.path.exists(raw_jira_watchers_filename):
        watchers = {}
        for i, issue in enumerate(issues):
            if i % 1000 == 0:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Getting watchers for {issue.key} [{i}/{len(issues)}].")
            watchers[issue.id] = conn.watchers(issue.id)

        with open(raw_jira_watchers_filename, 'wb') as handle:
            pickle.dump(watchers, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(raw_jira_watchers_filename, 'rb') as handle:
            watchers = pickle.load(handle)

    if not os.path.exists(raw_jira_remote_links_filename):
        remote_links = {}
        for i, issue in enumerate(issues):
            if i % 1000 == 0:
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Getting remote links for {issue.key} [{i}/{len(issues)}].")
            remote_links[issue.id] = conn.remote_links(issue)

        with open(raw_jira_remote_links_filename, 'wb') as handle:
            pickle.dump(remote_links, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(raw_jira_remote_links_filename, 'rb') as handle:
            remote_links = pickle.load(handle)

    # Jira -> GitHub markdown translation
    if not os.path.exists(translated_markup_filename):
        translate_markup = partial(_translate_markup, user_mapping)
        with Pool(processes=int(cpu_count() / 2)) as pool:
            translated_markup = pool.map_async(translate_markup, issues, chunksize=100).get()
        translated_markup = {k: v for k, v in translated_markup}

        with open(translated_markup_filename, 'wb') as handle:
            pickle.dump(translated_markup, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(translated_markup_filename, 'rb') as handle:
            translated_markup = pickle.load(handle)

    return issues, watchers, remote_links, translated_markup, user_mapping


def _translate_markup(user_mapping, issue):
    if issue.fields.description:
        description = issue.fields.description
    else:
        description = ""

    description = re.sub(LEADING_SPACE_HASH_PATTERN, r"\n\1", description)
    text = jira2markdown.convert(description, elements=ELEMENTS, usernames=user_mapping)

    for attachment in issue.fields.attachment:
        text = text.replace(f"![{attachment.filename}]({attachment.filename})",
                            f"![{attachment.filename}]({attachment.content})")

    comments = {}
    for comment in issue.fields.comment.comments:
        # Skip ASF GitHub Bot comments per https://github.com/apache/arrow/issues/14648
        if comment.author.name == "githubbot":
            continue
        comment_body = re.sub(LEADING_SPACE_HASH_PATTERN, r"\n\1", comment.body)
        comment_text = jira2markdown.convert(comment_body, elements=ELEMENTS, usernames=user_mapping)

        for attachment in issue.fields.attachment:
            comment_text = comment_text.replace(f"![{attachment.filename}]({attachment.filename})",
                                                f"![{attachment.filename}]({attachment.content})")
        comments[comment.id] = comment_text

    return issue.key, {"description": text, "comments": comments}


def munge_issues(issues, remote_links, watchers):
    return [{
        "title": issue.fields.summary,
        "issuetype": issue.fields.issuetype.name,
        "jira_key": issue.key,
        "id": issue.id,
        "components": issue.fields.components,
        "labels": issue.fields.labels,
        "priority": issue.fields.priority.name,
        "fixVersion": issue.fields.fixVersions,
        "jira_url": issue.permalink(),
        "remote_links": remote_links[issue.id],
        "environment": issue.fields.environment,
        "reporter": issue.fields.reporter,
        "assignee": issue.fields.assignee,
        "watchers": watchers[issue.id].watchers,
        "subtasks": issue.fields.subtasks,
        "attachment": issue.fields.attachment,
        "externally_tracked_issues": issue.fields.customfield_12311020,
        "created_at": issue.fields.created,
        "updated_at": issue.fields.updated,
        "closed_at": issue.fields.resolutiondate,
        "closed": _is_completed(issue),
        "comments": issue.fields.comment.comments,
        "linked_issues": [_extract_linked_issues(linked_issue) for linked_issue in issue.fields.issuelinks],
        "externally_tracked": issue.fields.customfield_12311020
    } for issue in issues]


def generate_payload(x, user_mapping, translated_markup, watchers, migration_doc_url, user_can_be_assignee):
    data = {
        "issue": {
            "title": x["title"],
            "created_at": x["created_at"][:-5] + "Z",
            "updated_at": x["updated_at"][:-5] + "Z",
            "closed": x["closed"],
            "body": BODY_TEMPLATE.render(
                description=translated_markup[x["jira_key"]]["description"],
                environment=x["environment"],
                reporter=x["reporter"],
                assignee=x["assignee"],
                jira_url=x["jira_url"],
                jira_key=x["jira_key"],
                user_mapping=user_mapping,
                watchers=watchers[x["id"]].watchers,
                subtasks=x["subtasks"],
                closed=x["closed"],
                linked_issues=x["linked_issues"],
                attachment=x["attachment"],
                externally_tracked=x["externally_tracked"],
                remote_links=x["remote_links"],
                jira_user_url=JIRA_USER_PROFILE_URL,
                migration_doc_url=migration_doc_url,
            ),
        },
        "comments": [{
            "body": COMMENT_TEMPLATE.render(
                comment=c,
                comment_content=translated_markup[x["jira_key"]]["comments"],
                jira_url=x["jira_url"],
                user_mapping=user_mapping),
            "created_at": c.created[:-5] + "Z"
        }
            for c in x["comments"] if c.author.name != "githubbot"]
    }

    if "milestone" in x:
        data["issue"]["milestone"] = x["milestone"]

    if "labels" in x:
        data["issue"]["labels"] = x["labels"]

    if x["closed_at"]:
        data["issue"]["closed_at"] = x["closed_at"][:-5] + "Z"

    if x["assignee"] and x["assignee"].name in user_mapping:
        assignee = user_mapping[x["assignee"].name]
        if assignee in user_can_be_assignee:
            data["issue"]["assignee"] = assignee

    return x["jira_key"], data


def fix_issue_bodies(issues, payloads, github_urls):
    issue_bodies = {key: payload["issue"]["body"] for key, payload in payloads}
    new_issue_bodies = {}

    for issue in issues:
        if issue.key not in github_urls:
            continue

        if issue.fields.issuelinks or issue.fields.subtasks:
            body = issue_bodies[issue.key]

            if issue.fields.issuelinks:
                for li in issue.fields.issuelinks:
                    linked_issue = li.outwardIssue if hasattr(li, "outwardIssue") else li.inwardIssue
                    jira_url = linked_issue.permalink()
                    github_url = github_urls.get(linked_issue.key, jira_url)
                    body = body.replace(jira_url, github_url)

            if issue.fields.subtasks:
                for subtask in issue.fields.subtasks:
                    jira_url = subtask.permalink()
                    github_url = github_urls.get(subtask.key, jira_url)
                    body = body.replace(jira_url, github_url)

            new_issue_bodies[issue.key] = body

    return new_issue_bodies


def update_gh_issue_links(issue_bodies, github_urls, github_credentials):
    responses = {}

    with requests.Session() as s:
        for i, (key, body) in enumerate(issue_bodies.items()):
            url = github_urls[key].replace("https://github.com/", "https://api.github.com/repos/")
            params = {"method": "POST", "url": url, "json": {"body": body}, "headers": github_credentials}
            print(i, "/", len(issue_bodies), params["url"])
            responses[key] = request_to_github(params, s)

    return responses


def run_query(query, github_credentials): # A simple function to use requests.post to make the API call. Note the json= section.
    request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=github_credentials)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))


def get_issues(owner, repo, interval, github_credentials):
    get_issue_texts = """
    query get_issue_texts {{
      search(query: "repo:{owner}/{repo} author:{created_by} is:issue created:{interval}",
                     type: ISSUE, first: 100{cursor}) {{
        edges {{
          node {{
            ... on Issue {{
              id
              bodyText
              url
              databaseId
              milestone {{
                id
                number
                title
              }}
            }}
          }}
        }}
        pageInfo {{
          endCursor
          hasNextPage
        }}
        issueCount
      }}
    }}
    """

    cursor = ""
    has_next_page = True
    responses = []
    page = 0

    while has_next_page:
        page += 1

        q = get_issue_texts.format(owner=owner, repo=repo, cursor=cursor, interval=interval,
                                   created_by=github_credentials["User-Agent"])
        response = run_query(q, github_credentials)
        responses.append(response)

        if int(response['data']["search"]["issueCount"]) > 1000:
            print(f"Query for {interval} is not granular enough and will not capture all issues!")

        has_next_page = response["data"]["search"]["pageInfo"]["hasNextPage"]
        cursor = f', after: "{response["data"]["search"]["pageInfo"]["endCursor"]}"'

    return [e["node"] for x in responses for e in x["data"]["search"]["edges"]]


def update_source_jira(issue, gh_url, jira_migration_note, conn):
    gh_id = gh_url.split("/")[-1]
    comment = jira_migration_note.format(gh_id=gh_id, gh_url=gh_url)
    conn.add_comment(issue, comment)

    # if not issue.fields.customfield_12311020:
    #     issue.update(fields={"customfield_12311020" : gh_url})
