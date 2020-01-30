<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Git / JIRA Release Audit

This is an application for performing an audit between the histories on our git
branches and the `fixVersion` field set on issues in JIRA. It does this by
building a Sqlite database from the commits found on each git branch,
identifying Jira IDs and release tags, and then requesting information about
those issues from Jira. Once both sources have been collected, queries can be
performed against the database to look for discrepancies between the sources of
truth (and, possibly, bugs in this script).

## Setup

The system prerequisites are Python3 with VirtualEnv available and Sqlite. Also,
you'll need the content of this directory and a local checkout of git repository.

Build a VirtualEnv with the script's dependencies with:

```shell script
$ python3 --version
Python 3.7.6
$ python3 -m venv ./venv
$ ./venv/bin/pip install -r ./requirements.txt
...
Successfully installed...
```

## Usage

The tool provides basic help docs.

```shell script
$ ./venv/bin/python ./git_jira_release_audit.py --help
usage: git_jira_release_audit.py [-h] [--populate-from-git POPULATE_FROM_GIT]
                                 [--populate-from-jira POPULATE_FROM_JIRA]
                                 [--db-path DB_PATH]
                                 [--initialize-db INITIALIZE_DB]
                                 [--report-new-for-release-line REPORT_NEW_FOR_RELEASE_LINE]
                                 [--git-repo-path GIT_REPO_PATH]
                                 [--remote-name REMOTE_NAME]
                                 [--development-branch DEVELOPMENT_BRANCH]
                                 [--development-branch-fix-version DEVELOPMENT_BRANCH_FIX_VERSION]
                                 [--release-line-regexp RELEASE_LINE_REGEXP]
                                 [--parse-release-tags PARSE_RELEASE_TAGS]
                                 [--fallback-actions-path FALLBACK_ACTIONS_PATH]
                                 [--jira-url JIRA_URL]

optional arguments:
  -h, --help            show this help message and exit

Building the audit database:
  --populate-from-git POPULATE_FROM_GIT
                        When true, populate the audit database from the Git
                        repository.
  --populate-from-jira POPULATE_FROM_JIRA
                        When true, populate the audit database from Jira.
  --db-path DB_PATH     Path to the database file, or leave unspecified for a
                        transient db.
  --initialize-db INITIALIZE_DB
                        When true, initialize the database tables. This is
                        destructive to the contents of an existing database.

Generating reports:
  --report-new-for-release-line REPORT_NEW_FOR_RELEASE_LINE
                        Builds a report of the Jira issues that are new on the
                        target release line, not present on any of the
                        associated release branches. (i.e., on branch-2 but
                        not branch-{2.0,2.1,...})

Interactions with the Git repo:
  --git-repo-path GIT_REPO_PATH
                        Path to the git repo, or leave unspecified to infer
                        from the current file's path.
  --remote-name REMOTE_NAME
                        The name of the git remote to use when identifying
                        branches. Default: 'origin'
  --development-branch DEVELOPMENT_BRANCH
                        The name of the branch from which all release lines
                        originate. Default: 'master'
  --development-branch-fix-version DEVELOPMENT_BRANCH_FIX_VERSION
                        The Jira fixVersion used to indicate an issue is
                        committed to the development branch. Default: '3.0.0'
  --release-line-regexp RELEASE_LINE_REGEXP
                        A regexp used to identify release lines.
  --parse-release-tags PARSE_RELEASE_TAGS
                        When true, look for release tags and annotate commits
                        according to their release version. An Expensive
                        calculation, disabled by default.
  --fallback-actions-path FALLBACK_ACTIONS_PATH
                        Path to a file containing _DB.Actions applicable to
                        specific git shas.
  --branch-1-fix-version BRANCH_1_FIX_VERSION
                        The Jira fixVersion used to indicate an issue is
                        committed to the specified release line branch
  --branch-2-fix-version BRANCH_2_FIX_VERSION
                        The Jira fixVersion used to indicate an issue is
                        committed to the specified release line branch

Interactions with Jira:
  --jira-url JIRA_URL   A URL locating the target JIRA instance.
```

Example Run:

```shell script
$ ./venv/bin/python3 ./git_jira_release_audit.py \
  --db-path=audit.db \
  --remote-name=apache-rw \
  --development-branch-fix-version=3.0.0 \
  --branch-1-fix-version=1.5.0 \
  --branch-2-fix-version=2.3.0
INFO:root:apache-rw/branch-1 has 4046 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:root:apache-rw/branch-1.0 has 1433 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:root:apache-rw/branch-1.1 has 2111 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:root:apache-rw/branch-1.2 has 2738 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:root:apache-rw/branch-1.3 has 3287 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:root:apache-rw/branch-1.4 has 3912 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:root:apache-rw/branch-2 has 3080 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:root:apache-rw/branch-2.0 has 2194 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:root:apache-rw/branch-2.1 has 2705 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:root:apache-rw/branch-2.2 has 2927 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:root:retrieving 5653 jira_ids from the issue tracker

apache-rw/branch-1 100%|██████████████████████████████████████████████████████| 4046/4046 [08:23<00:00, 8.04 commit/s]
apache-rw/branch-1.0 100%|████████████████████████████████████████████████████| 1433/1433 [03:49<00:00, 6.26 commit/s]
apache-rw/branch-1.1 100%|████████████████████████████████████████████████████| 2111/2111 [05:16<00:00, 6.68 commit/s]
apache-rw/branch-1.2 100%|████████████████████████████████████████████████████| 2738/2738 [06:26<00:00, 7.10 commit/s]
apache-rw/branch-1.3 100%|████████████████████████████████████████████████████| 3287/3287 [07:21<00:00, 7.46 commit/s]
apache-rw/branch-1.4 100%|████████████████████████████████████████████████████| 3912/3912 [08:08<00:00, 8.02 commit/s]
apache-rw/branch-2 100%|█████████████████████████████████████████████████████| 3080/3080 [03:29<00:00, 14.74 commit/s]
apache-rw/branch-2.0 100%|████████████████████████████████████████████████████| 2194/2194 [04:56<00:00, 7.42 commit/s]
apache-rw/branch-2.1 100%|███████████████████████████████████████████████████| 2705/2705 [03:17<00:00, 13.75 commit/s]
apache-rw/branch-2.2 100%|███████████████████████████████████████████████████| 2927/2927 [03:28<00:00, 14.09 commit/s]
fetch from Jira 100%|█████████████████████████████████████████████████████████| 5653/5653 [00:58<00:00, 98.29 issue/s]
```

With a populated database, query with sqlite:

```shell script
$ sqlite3 audit.db
SQLite version 3.24.0 2018-06-04 14:10:15
Enter ".help" for usage hints.
sqlite> -- count the number of distinct commits on a release branch
sqlite> select count(distinct jira_id), branch from git_commits group by branch;
3406|apache-rw/branch-1
1189|apache-rw/branch-1.0
1728|apache-rw/branch-1.1
2289|apache-rw/branch-1.2
2779|apache-rw/branch-1.3
3277|apache-rw/branch-1.4
2666|apache-rw/branch-2
1809|apache-rw/branch-2.0
2289|apache-rw/branch-2.1
2511|apache-rw/branch-2.2

sqlite> -- count the number of issues that will be in 2.3.0 that have not been released on any earlier
sqlite> -- version.
sqlite> select count(1) from (
  select distinct jira_id from git_commits where branch = 'apache-rw/branch-2' except
  select distinct jira_id from git_commits where branch in
    ('apache-rw/branch-2.0', 'apache-rw/branch-2.1', 'apache-rw/branch-2.2'));
169

sqlite> -- find the issues for which the git commit record and JIRA fixVersion disagree
sqlite> select g.jira_id, g.git_tag, j.fix_version
  from git_commits g
  inner join jira_versions j
     on g.jira_id = j.jira_id
    and g.branch = 'apache-rw/branch-2.2'
    and g.git_tag is not null
    and j.fix_version like '2.2.%'
    and g.git_tag != j.fix_version;
HBASE-22941|2.2.2|2.2.1

sqlite> -- show jira non-1.x fixVersions for all issues on branch-2 but not on any
sqlite> -- branch-2.x release branch; i.e., issues that are missing a fixVersion or
sqlite> -- are marked for a release other than (3.0.0, 2.3.0)
sqlite> select g.jira_id, j.fix_version
from (
  select distinct jira_id from git_commits where branch = 'apache-rw/branch-2' except
  select distinct jira_id from git_commits where branch in
    (select distinct branch from git_commits where branch like 'apache-rw/branch-2.%')) g
left join jira_versions j
  on g.jira_id = j.jira_id
  and j.fix_version not like '1.%'
where (
  j.fix_version is null
  OR j.fix_version not in ('3.0.0', '2.3.0'))
order by g.jira_id desc;
HBASE-23683|2.2.4
HBASE-23032|connector-1.0.1
HBASE-23032|hbase-filesystem-1.0.0-alpha2
HBASE-22405|2.2.0
HBASE-22360|2.2.0
HBASE-22321|
HBASE-22283|2.2.0
```
