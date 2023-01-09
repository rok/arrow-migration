#!/usr/bin/env python3
import requests
import csv
import sys

SCRIPT_DESCRIPTION = """
This script uses GitHub API to subscribe you to migrated Apache Arrow issues.
You will be prompted for your GitHub username and a GitHub token with notification permissions.
You can create one here: https://github.com/settings/tokens/new
"""
DATA_URL = "https://raw.githubusercontent.com/rok/arrow-migration/main/issue_subscriptions.csv"
API_URL = "https://api.github.com/graphql"
SUBSCRIBE_QUERY = """
mutation subscribe_to_issues {{
  updateSubscription(input: {{subscribableId: "{}", state: SUBSCRIBED}}) {{
    clientMutationId
  }}
}}
"""

def get_subscriptions(git_username):
    subscriptions = []

    with requests.Session() as s:
        data = s.get(DATA_URL).content.decode()
        reader = csv.reader(data.splitlines(), delimiter=',')

        for row in reader:
            if row[0] == git_username:
                subscriptions.append(row)
    return subscriptions

def subscribe(subscriptions):
    with requests.Session() as s:
        for subscription in subscriptions:
            print(subscription[2])
            query = SUBSCRIBE_QUERY.format(subscription[3])
            request = requests.post(API_URL, json={'query': query}, headers=HEADERS)
            if request.status_code != 200:
                raise Exception(query, "returned", request.status_code, "\nSubscription process not complete.")

                
if __name__ == "__main__":
    print(SCRIPT_DESCRIPTION)
    github_username = input("\nPlease input your GitHub username: ")
    subscriptions = get_subscriptions(github_username)

    if len(subscriptions) == 0:
        print("No subscriptions found for", gitub_username)
        sys.exit(0)
    print("Found", len(subscriptions), "issues.")

    GITHUB_API_TOKEN = input("Please input your GitHub API token. It should have notification permissions: ")
    HEADERS = {"Authorization": "token " + GITHUB_API_TOKEN}

    print("Subscribing:")
    print("=========================================================")
    subscribe(subscriptions)
    print("=========================================================")
    print("Successfully subscribed to", len(subscriptions), "GitHub issues.")
