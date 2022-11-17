import json
import os
import re

def identify_leading_space_hash_text(issue_dict):
    if has_hash_with_leading_space(issue_dict['fields']['description']):
        print(f"{issue_dict['key']}\tHASH_LEADING_SPACES\tDESCRIPTION")
    else:
        for comment in [c for c in issue_dict['fields']['comment']['comments'] if c['author']['name'] != "githubbot"]:
            if has_hash_with_leading_space(comment['body']):
                print(f"{issue_dict['key']}\tHASH_LEADING_SPACES\tCOMMENT")


def has_hash_with_leading_space(text):
    if not text:
        return False
    lines = text.splitlines()
    for line in lines:
        if re.match(r'\s#+\s+\S.*', line):
            print(line)
            return True
    return False

def identify_missing_issues():
    source_dir = os.path.join(os.path.dirname(__file__), "issues", "source")
    for i in range(1,18348):
        if not os.path.exists(os.path.join(source_dir, f"ARROW-{i}.json")):
            print(f"ARROW-{i} does not exist")

def check_all_source_files():
    source_dir = os.path.join(os.path.dirname(__file__), "issues", "source")
    for filename in os.listdir(source_dir):
        if filename.endswith(".json"):
            with open(os.path.join(source_dir, filename), "r+") as fp:
                jira_json = json.load(fp)
                identify_leading_space_hash_text(jira_json)

    print("Completed checking all files.")
def main():
    identify_missing_issues()

if __name__ == "__main__":
    main()