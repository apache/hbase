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

## Basic Usage

The tool provides basic help docs.

```shell script
$ ./venv/bin/python ./git_jira_release_audit.py --help
usage: git_jira_release_audit.py [-h] [--populate-from-git POPULATE_FROM_GIT]
                                 [--populate-from-jira POPULATE_FROM_JIRA]
                                 [--db-path DB_PATH]
                                 [--initialize-db INITIALIZE_DB]
                                 [--report-new-for-release-line REPORT_NEW_FOR_RELEASE_LINE]
                                 [--report-new-for-release-branch REPORT_NEW_FOR_RELEASE_BRANCH]
                                 [--git-repo-path GIT_REPO_PATH]
                                 [--remote-name REMOTE_NAME]
                                 [--development-branch DEVELOPMENT_BRANCH]
                                 [--development-branch-fix-version DEVELOPMENT_BRANCH_FIX_VERSION]
                                 [--release-line-regexp RELEASE_LINE_REGEXP]
                                 [--parse-release-tags PARSE_RELEASE_TAGS]
                                 [--fallback-actions-path FALLBACK_ACTIONS_PATH]
                                 [--branch-filter-regexp BRANCH_FILTER_REGEXP]
                                 [--jira-url JIRA_URL] --branch-1-fix-version
                                 BRANCH_1_FIX_VERSION --branch-2-fix-version
                                 BRANCH_2_FIX_VERSION

optional arguments:
  -h, --help            show this help message and exit

Building the audit database:
  --populate-from-git POPULATE_FROM_GIT
                        When true, populate the audit database from the Git
                        repository. (default: True)
  --populate-from-jira POPULATE_FROM_JIRA
                        When true, populate the audit database from Jira.
                        (default: True)
  --db-path DB_PATH     Path to the database file, or leave unspecified for a
                        transient db. (default: audit.db)
  --initialize-db INITIALIZE_DB
                        When true, initialize the database tables. This is
                        destructive to the contents of an existing database.
                        (default: False)

Generating reports:
  --report-new-for-release-line REPORT_NEW_FOR_RELEASE_LINE
                        Builds a report of the Jira issues that are new on the
                        target release line, not present on any of the
                        associated release branches. (i.e., on branch-2 but
                        not branch-{2.0,2.1,...}) (default: None)
  --report-new-for-release-branch REPORT_NEW_FOR_RELEASE_BRANCH
                        Builds a report of the Jira issues that are new on the
                        target release branch, not present on any of the
                        previous release branches. (i.e., on branch-2.3 but
                        not branch-{2.0,2.1,...}) (default: None)

Interactions with the Git repo:
  --git-repo-path GIT_REPO_PATH
                        Path to the git repo, or leave unspecified to infer
                        from the current file's path. (default:
                        ./git_jira_release_audit.py)
  --remote-name REMOTE_NAME
                        The name of the git remote to use when identifying
                        branches. Default: 'origin' (default: origin)
  --development-branch DEVELOPMENT_BRANCH
                        The name of the branch from which all release lines
                        originate. Default: 'master' (default: master)
  --development-branch-fix-version DEVELOPMENT_BRANCH_FIX_VERSION
                        The Jira fixVersion used to indicate an issue is
                        committed to the development branch. (default: 3.0.0)
  --release-line-regexp RELEASE_LINE_REGEXP
                        A regexp used to identify release lines. (default:
                        branch-\d+$)
  --parse-release-tags PARSE_RELEASE_TAGS
                        When true, look for release tags and annotate commits
                        according to their release version. An Expensive
                        calculation, disabled by default. (default: False)
  --fallback-actions-path FALLBACK_ACTIONS_PATH
                        Path to a file containing _DB.Actions applicable to
                        specific git shas. (default: fallback_actions.csv)
  --branch-filter-regexp BRANCH_FILTER_REGEXP
                        Limit repo parsing to branch names that match this
                        filter expression. (default: .*)
  --branch-1-fix-version BRANCH_1_FIX_VERSION
                        The Jira fixVersion used to indicate an issue is
                        committed to the specified release line branch
                        (default: None)
  --branch-2-fix-version BRANCH_2_FIX_VERSION
                        The Jira fixVersion used to indicate an issue is
                        committed to the specified release line branch
                        (default: None)

Interactions with Jira:
  --jira-url JIRA_URL   A URL locating the target JIRA instance. (default:
                        https://issues.apache.org/jira)
```

### Build a Database

This invocation will build a "simple" database, correlating commits to
branches. It omits gathering the detailed release tag data, so it runs pretty
quickly. 

Example Run:

```shell script
$ ./venv/bin/python3 ./git_jira_release_audit.py \
  --db-path=audit.db \
  --development-branch-fix-version=3.0.0 \
  --branch-1-fix-version=1.7.0 \
  --branch-2-fix-version=2.4.0
INFO:git_jira_release_audit.py:origin/branch-1.0 has 1433 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:git_jira_release_audit.py:origin/branch-1.1 has 2111 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:git_jira_release_audit.py:origin/branch-1.2 has 2738 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:git_jira_release_audit.py:origin/branch-1.3 has 3296 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:git_jira_release_audit.py:origin/branch-1.4 has 3926 commits since its origin at 0167558eb31ff48308d592ef70b6d005ba6d21fb.
INFO:git_jira_release_audit.py:origin/branch-2 has 3325 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:git_jira_release_audit.py:origin/branch-2.0 has 2198 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:git_jira_release_audit.py:origin/branch-2.1 has 2749 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:git_jira_release_audit.py:origin/branch-2.2 has 2991 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:git_jira_release_audit.py:origin/branch-2.3 has 3312 commits since its origin at 0d0c330401ade938bf934aafd79ec23705edcc60.
INFO:git_jira_release_audit.py:retrieving 5850 jira_ids from the issue tracker

origin/branch-1 100%|████████████████████████████████████| 4084/4084 [00:00<00:00, 9805.33 commit/s]
origin/branch-1.0 100%|█████████████████████████████████| 1433/1433 [00:00<00:00, 10479.89 commit/s]
origin/branch-1.1 100%|█████████████████████████████████| 2111/2111 [00:00<00:00, 10280.60 commit/s]
origin/branch-1.2 100%|██████████████████████████████████| 2738/2738 [00:00<00:00, 8833.51 commit/s]
origin/branch-1.3 100%|██████████████████████████████████| 3296/3296 [00:00<00:00, 9746.93 commit/s]
origin/branch-1.4 100%|██████████████████████████████████| 3926/3926 [00:00<00:00, 9750.96 commit/s]
origin/branch-2 100%|████████████████████████████████████| 3325/3325 [00:00<00:00, 9688.14 commit/s]
origin/branch-2.0 100%|██████████████████████████████████| 2198/2198 [00:00<00:00, 8804.18 commit/s]
origin/branch-2.1 100%|██████████████████████████████████| 2749/2749 [00:00<00:00, 9328.67 commit/s]
origin/branch-2.2 100%|██████████████████████████████████| 2991/2991 [00:00<00:00, 9215.56 commit/s]
origin/branch-2.3 100%|██████████████████████████████████| 3312/3312 [00:00<00:00, 9063.19 commit/s]
fetch from Jira 100%|████████████████████████████████████████| 5850/5850 [10:40<00:00, 9.14 issue/s]
```

Optionally, the database can be build to include release tags, by specifying
`--parse-release-tags=true`. This is more time-consuming, but is necessary for
auditing discrepancies between git and Jira. Optionally, limit the branches
under consideration by specifying a regex filter with `--branch-filter-regexp`.
Running the same command but including this flag looks like this:

```shell script
origin/branch-1 100%|███████████████████████████████████████| 4084/4084 [08:58<00:00, 7.59 commit/s]
origin/branch-1.0 100%|█████████████████████████████████████| 1433/1433 [03:54<00:00, 6.13 commit/s]
origin/branch-1.1 100%|█████████████████████████████████████| 2111/2111 [41:26<00:00, 0.85 commit/s]
origin/branch-1.2 100%|█████████████████████████████████████| 2738/2738 [07:10<00:00, 6.37 commit/s]
origin/branch-1.3 100%|██████████████████████████████████| 3296/3296 [2h 33:13<00:00, 0.36 commit/s]
origin/branch-1.4 100%|██████████████████████████████████| 3926/3926 [7h 22:41<00:00, 0.15 commit/s]
origin/branch-2 100%|████████████████████████████████████| 3325/3325 [2h 05:43<00:00, 0.44 commit/s]
origin/branch-2.0 100%|█████████████████████████████████████| 2198/2198 [52:18<00:00, 0.70 commit/s]
origin/branch-2.1 100%|█████████████████████████████████████| 2749/2749 [17:09<00:00, 2.67 commit/s]
origin/branch-2.2 100%|█████████████████████████████████████| 2991/2991 [52:15<00:00, 0.95 commit/s]
origin/branch-2.3 100%|████████████████████████████████████| 3312/3312 [05:08<00:00, 10.74 commit/s]
fetch from Jira 100%|████████████████████████████████████████| 5850/5850 [10:46<00:00, 9.06 issue/s]
```

### Run a Report

With a database populated with branch information, the build-in reports can be
run.

`--report-new-for-release-line`
> Builds a report of the Jira issues that are new on the target release line,
not present on any of the associated release branches. (i.e., on branch-2 but
not branch-{2.0,2.1,...})

`--report-new-for-release-branch`
> Builds a report of the Jira issues that are new on the target release branch,
not present on any of the previous release branches. (i.e., on branch-2.3 but
not branch-{2.0,2.1,...})

Either way, the output is a csv file containing a summary of each JIRA id found
matching the report criteria.

Example Run:

```shell script
$ ./venv/bin/python3.7 ./git_jira_release_audit.py \
  --populate-from-git=false \
  --populate-from-jira=false \
  --branch-1-fix-version=1.7.0 \
  --branch-2-fix-version=2.4.0 \
  --report-new-for-release-branch=origin/branch-2.3
INFO:git_jira_release_audit.py:retrieving 292 jira_ids from the issue tracker
INFO:git_jira_release_audit.py:generated report at new_for_origin-branch-2.3.csv

fetch from Jira 100%|████████████████████████████████████████| 292/292 [00:03<00:00, 114.01 issue/s]
$ head -n5 new_for_origin-branch-2.3.csv
key,issue_type,priority,summary,resolution,components
HBASE-21070,Bug,Critical,SnapshotFileCache won't update for snapshots stored in S3,Fixed,['snapshots']
HBASE-21773,Bug,Critical,rowcounter utility should respond to pleas for help,Fixed,['tooling']
HBASE-21505,Bug,Major,Several inconsistencies on information reported for Replication Sources by hbase shell status 'replication' command.,Fixed,['Replication']
HBASE-22057,Bug,Major,Impose upper-bound on size of ZK ops sent in a single multi(),Fixed,[]
```

### Explore the Database

With a populated database, query it with sqlite:

```shell script
$ sqlite3 audit.db
SQLite version 3.24.0 2018-06-04 14:10:15
Enter ".help" for usage hints.
sqlite> -- count the number of distinct commits on a release branch
sqlite> select count(distinct jira_id), branch from git_commits group by branch;
3437|origin/branch-1
1189|origin/branch-1.0
1728|origin/branch-1.1
2289|origin/branch-1.2
2788|origin/branch-1.3
3289|origin/branch-1.4
2846|origin/branch-2
1813|origin/branch-2.0
2327|origin/branch-2.1
2566|origin/branch-2.2
2839|origin/branch-2.3

sqlite> -- find the issues for which the git commit record and JIRA fixVersion disagree
sqlite> -- this query requires the database be built with --parse-release-tags
sqlite> select g.jira_id, g.git_tag, j.fix_version
  from git_commits g
  inner join jira_versions j
     on g.jira_id = j.jira_id
    and g.branch = 'origin/branch-2.2'
    and g.git_tag is not null
    and j.fix_version like '2.2.%'
    and g.git_tag != j.fix_version;
HBASE-22941|2.2.2|2.2.1

sqlite> -- show jira fixVersions for all issues on branch-2.3 but not on any earlier
sqlite> -- branch; i.e., issues that are missing a fixVersion or are marked for
sqlite> -- a release other than the expected (3.0.0, 2.3.0).
sqlite> -- this query requires the database be built with --parse-release-tags
sqlite> select jira_id, fix_version
  FROM jira_versions
  WHERE jira_id in (
    SELECT distinct jira_id
    FROM git_commits
    WHERE branch = 'origin/branch-2.3'
    EXCEPT SELECT distinct jira_id
      FROM git_commits
      WHERE branch IN (
        SELECT distinct branch
        FROM git_commits
        WHERE branch != 'origin/branch-2.3'))
  AND fix_version NOT IN ('3.0.0', '2.3.0')
  ORDER BY jira_id;
HBASE-22321|1.5.0
HBASE-22360|2.2.0
HBASE-22405|2.2.0
HBASE-22555|2.4.0
HBASE-23032|connector-1.0.1
HBASE-23032|hbase-filesystem-1.0.0-alpha2
HBASE-23604|HBASE-18095
HBASE-23633|2.4.0
HBASE-23647|HBASE-18095
HBASE-23648|HBASE-18095
HBASE-23731|HBASE-18095
HBASE-23741|2.4.0
HBASE-23752|HBASE-18095
HBASE-23804|HBASE-18095
HBASE-23851|master
HBASE-23936|2.4.0
HBASE-23937|2.4.0
HBASE-23977|2.4.0
HBASE-24002|2.4.0
HBASE-24033|2.4.0
HBASE-24037|2.4.0
HBASE-24073|master
HBASE-24075|2.4.0
HBASE-24080|2.4.0
HBASE-24080|master
```
