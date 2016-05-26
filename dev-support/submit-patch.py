#!/usr/bin/env python
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Makes a patch for the current branch, creates/updates the review board request and uploads new
# patch to jira. Patch is named as JIRAID_BRANCH_VERSION.patch. If no jira is specified,
# patch will be named BRANCH_v0.patch and jira and review board are not updated.
# Review board id is retrieved from the remote link in the jira.
# Print help: submit-patch.py --h
import argparse
import getpass
import git
import json
import logging
import os
import re
import requests
import subprocess
import sys

parser = argparse.ArgumentParser(
    epilog = "To avoid having to enter jira/review board username/password every time, setup an "
             "encrypted ~/.apache-cred files as follows:\n"
             "1) Create a file with following single "
             "line: \n{\"jira_username\" : \"appy\", \"jira_password\":\"123\", "
             "\"rb_username\":\"appy\", \"rb_password\" : \"@#$\"}\n"
             "2) Encrypt it with openssl.\n"
             "openssl enc -aes-256-cbc -in <file> -out ~/.apache-creds\n"
             "3) Delete original file.\n"
             "Now onwards, you'll need to enter this encryption key only once per run. If you "
             "forget the key, simply regenerate ~/.apache-cred file again.",
    formatter_class=argparse.RawTextHelpFormatter
)
parser.add_argument("-b", "--branch",
                    help = "Branch to use for generating diff. If not specified, tracking branch "
                         "is used. If there is no tracking branch, error will be thrown.")

# Arguments related to Jira.
parser.add_argument("-jid", "--jira-id",
                    help = "Jira id of the issue. If set, we deduce next patch version from "
                           "attachments in the jira and also upload the new patch. Script will "
                           "ask for jira username/password for authentication. If not set, "
                           "patch is named <branch>_v0.patch.")

# Arguments related to Review Board.
parser.add_argument("-srb", "--skip-review-board",
                    help = "Don't create/update the review board.",
                    default = False, action = "store_true")
parser.add_argument("--reviewers",
                    help = "Comma separated list of users to add as reviewers.")

# Misc arguments
parser.add_argument("--patch-dir", default = "~/patches",
                    help = "Directory to store patch files. If it doesn't exist, it will be "
                          "created. Default: ~/patches")
parser.add_argument("--rb-repo", default = "hbase-git",
                    help = "Review board repository. Default: hbase-git")
args = parser.parse_args()

# Setup logger
logging.basicConfig()
logger = logging.getLogger("submit-patch")
logger.setLevel(logging.INFO)


def log_fatal_and_exit(*arg):
    logger.fatal(*arg)
    sys.exit(1)


def assert_status_code(response, expected_status_code, description):
    if response.status_code != expected_status_code:
        log_fatal_and_exit(" Oops, something went wrong when %s. \nResponse: %s %s\nExiting..",
                           description, response.status_code, response.reason)


# Make repo instance to interact with git repo.
try:
    repo = git.Repo(os.getcwd())
    git = repo.git
except git.exc.InvalidGitRepositoryError as e:
    log_fatal_and_exit(" '%s' is not valid git repo directory.\nRun from base directory of "
                       "HBase's git repo.", e)

logger.info(" Active branch: %s", repo.active_branch.name)
# Do not proceed if there are uncommitted changes.
if repo.is_dirty():
    log_fatal_and_exit(" Git status is dirty. Commit locally first.")


# Returns base branch for creating diff.
def get_base_branch():
    # if --branch is set, use it as base branch for computing diff. Also check that it's a valid branch.
    if args.branch is not None:
        base_branch = args.branch
        # Check that given branch exists.
        for ref in repo.refs:
            if ref.name == base_branch:
                return base_branch
        log_fatal_and_exit(" Branch '%s' does not exist in refs.", base_branch)
    else:
        # if --branch is not set, use tracking branch as base branch for computing diff.
        # If there is no tracking branch, log error and quit.
        tracking_branch = repo.active_branch.tracking_branch()
        if tracking_branch is None:
            log_fatal_and_exit(" Active branch doesn't have a tracking_branch. Please specify base "
                               " branch for computing diff using --branch flag.")
        logger.info(" Using tracking branch as base branch")
        return tracking_branch.name


# Returns patch name having format "JIRAID_BRANCH_VERSION.patch". If no jira is specified,
# the format changes to "BRANCH_v0.patch".
def get_patch_name(branch):
    patch_name = ""
    if args.jira_id is not None:
        patch_name = args.jira_id.upper() + "_"

    patch_name = patch_name + branch
    patch_version = get_next_patch_version(patch_name)
    return patch_name + "_v" + patch_version + ".patch"


# Fetches list of attachments in specified jira (--jira-id) and deduces next version of patch for
# the given patch_name_prefix. If no jira is specified, returns 0.
def get_next_patch_version(patch_name_prefix):
    if args.jira_id is None:
        return "0"

    # JIRA's rest api is broken wrt to attachments. https://jira.atlassian.com/browse/JRA-27637.
    # Using crude way to get list of attachments.
    url = "https://issues.apache.org/jira/browse/" + args.jira_id
    logger.info("Getting list of attachments for jira %s from %s", args.jira_id, url)
    html = requests.get(url)
    if html.status_code == 404:
        log_fatal_and_exit(" Invalid jira id : %s", args.jira_id)
    if html.status_code != 200:
        log_fatal_and_exit(" Cannot fetch jira information. Status code %s", html.status_code)
    # Iterate over patch names starting from version 1 and return when name is not already used.
    content = unicode(html.content, 'utf-8')
    for i in range(1, 100):
        name = patch_name_prefix + "_v" + str(i) + ".patch"
        print name
        if content.find(name) == -1:
            return str(i)


# Validates that patch directory exists, if not, creates it.
def validate_patch_dir(patch_dir):
    # Create patch_dir if it doesn't exist.
    if not os.path.exists(patch_dir):
        logger.warn(" Patch directory doesn't exist. Creating it.")
        os.mkdir(patch_dir)
    else:
        # If patch_dir exists, make sure it's a directory.
        if not os.path.isdir(patch_dir):
            log_fatal_and_exit(" '%s' exists but is not a directory. Specify another directory.",
                               patch_dir)


# Make sure current branch is ahead of base_branch by exactly 1 commit. Quits if
# - base_branch has commits not in current branch
# - current branch is same as base branch
# - current branch is ahead of base_branch by more than 1 commits
def check_diff_between_branches(base_branch):
    only_in_base_branch = git.log("HEAD.." + base_branch, oneline = True)
    only_in_active_branch = git.log(base_branch + "..HEAD", oneline = True)
    if len(only_in_base_branch) != 0:
        log_fatal_and_exit(" '%s' is ahead of current branch by %s commits. Rebase "
                           "and try again.", base_branch, len(only_in_base_branch.split("\n")))
    if len(only_in_active_branch) == 0:
        log_fatal_and_exit(" Current branch is same as '%s'. Exiting...", base_branch)
    if len(only_in_active_branch.split("\n")) > 1:
        log_fatal_and_exit(" Current branch is ahead of '%s' by %s commits. Squash into single "
                           "commit and try again.",
                           base_branch, len(only_in_active_branch.split("\n")))


# If ~/.apache-creds is present, load credentials from it otherwise prompt user.
def get_credentials():
    creds = dict()
    creds_filepath = os.path.expanduser("~/.apache-creds")
    if os.path.exists(creds_filepath):
        try:
            content = subprocess.check_output("openssl enc -aes-256-cbc -d -in " + creds_filepath,
                                              shell=True)
        except subprocess.CalledProcessError as e:
            log_fatal_and_exit(" Couldn't decrypt ~/.apache-creds file. Exiting..")
        creds = json.loads(content)
    else:
        creds['jira_username'] = raw_input("Jira username:")
        creds['jira_password'] = getpass.getpass("Jira password:")
        if not args.skip_review_board:
            creds['rb_username'] = raw_input("Review Board username:")
            creds['rb_password'] = getpass.getpass("Review Board password:")
    return creds


def attach_patch_to_jira(issue_url, patch_filepath, creds):
    # Upload patch to jira using REST API.
    headers = {'X-Atlassian-Token': 'no-check'}
    files = {'file': open(patch_filepath, 'rb')}
    jira_auth = requests.auth.HTTPBasicAuth(creds['jira_username'], creds['jira_password'])
    attachment_url = issue_url +  "/attachments"
    r = requests.post(attachment_url, headers = headers, files = files, auth = jira_auth)
    assert_status_code(r, 200, "uploading patch to jira")


def get_jira_summary(issue_url):
    r = requests.get(issue_url + "?fields=summary")
    assert_status_code(r, 200, "fetching jira summary")
    return json.loads(r.content)["fields"]["summary"]


def get_review_board_id_if_present(issue_url, rb_link_title):
    r = requests.get(issue_url + "/remotelink")
    assert_status_code(r, 200, "fetching remote links")
    links = json.loads(r.content)
    for link in links:
        if link["object"]["title"] == rb_link_title:
            res = re.search("reviews.apache.org/r/([0-9]+)", link["object"]["url"])
            return res.group(1)
    return None


base_branch = get_base_branch()
# Remove remote repo name from branch name if present. This assumes that we don't use '/' in
# actual branch names.
base_branch_without_remote = base_branch.split('/')[-1]
logger.info(" Base branch: %s", base_branch)

check_diff_between_branches(base_branch)

patch_dir = os.path.abspath(os.path.expanduser(args.patch_dir))
logger.info(" Patch directory: %s", patch_dir)
validate_patch_dir(patch_dir)

patch_filename = get_patch_name(base_branch_without_remote)
logger.info(" Patch name: %s", patch_filename)
patch_filepath = os.path.join(patch_dir, patch_filename)

diff = git.format_patch(base_branch, stdout = True)
with open(patch_filepath, "w") as f:
    f.write(diff)

if args.jira_id is not None:
    creds = get_credentials()
    issue_url = "https://issues.apache.org/jira/rest/api/2/issue/" + args.jira_id

    attach_patch_to_jira(issue_url, patch_filepath, creds)

    if not args.skip_review_board:
        rb_auth = requests.auth.HTTPBasicAuth(creds['rb_username'], creds['rb_password'])

        rb_link_title = "Review Board (" + base_branch_without_remote + ")"
        rb_id = get_review_board_id_if_present(issue_url, rb_link_title)

        # If no review board link found, create new review request and add its link to jira.
        if rb_id is None:
            reviews_url = "https://reviews.apache.org/api/review-requests/"
            data = {"repository" : "hbase-git"}
            r = requests.post(reviews_url, data = data, auth = rb_auth)
            assert_status_code(r, 201, "creating new review request")
            review_request = json.loads(r.content)["review_request"]
            absolute_url = review_request["absolute_url"]
            logger.info(" Created new review request: %s", absolute_url)

            # Use jira summary as review's summary too.
            summary = get_jira_summary(issue_url)
            # Use commit message as description.
            description = git.log("-1", pretty="%B")
            update_draft_data = {"bugs_closed" : [args.jira_id.upper()], "target_groups" : "hbase",
                                 "target_people" : args.reviewers, "summary" : summary,
                                 "description" : description }
            draft_url = review_request["links"]["draft"]["href"]
            r = requests.put(draft_url, data = update_draft_data, auth = rb_auth)
            assert_status_code(r, 200, "updating review draft")

            draft_request = json.loads(r.content)["draft"]
            diff_url = draft_request["links"]["draft_diffs"]["href"]
            files = {'path' : (patch_filename, open(patch_filepath, 'rb'))}
            r = requests.post(diff_url, files = files, auth = rb_auth)
            assert_status_code(r, 201, "uploading diff to review draft")

            r = requests.put(draft_url, data = {"public" : True}, auth = rb_auth)
            assert_status_code(r, 200, "publishing review request")

            # Add link to review board in the jira.
            remote_link = json.dumps({'object': {'url': absolute_url, 'title': rb_link_title}})
            jira_auth = requests.auth.HTTPBasicAuth(creds['jira_username'], creds['jira_password'])
            r = requests.post(issue_url + "/remotelink", data = remote_link, auth = jira_auth,
                              headers={'Content-Type':'application/json'})
        else:
            logger.info(" Updating existing review board: https://reviews.apache.org/r/%s", rb_id)
            draft_url = "https://reviews.apache.org/api/review-requests/" + rb_id + "/draft/"
            diff_url = draft_url + "diffs/"
            files = {'path' : (patch_filename, open(patch_filepath, 'rb'))}
            r = requests.post(diff_url, files = files, auth = rb_auth)
            assert_status_code(r, 201, "uploading diff to review draft")

            r = requests.put(draft_url, data = {"public" : True}, auth = rb_auth)
            assert_status_code(r, 200, "publishing review request")
