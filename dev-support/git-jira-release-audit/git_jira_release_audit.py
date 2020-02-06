#!/usr/bin/env python3
#
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
# Build a database from git commit histories. Can be used to audit git vs. jira. For usage,
# see README.md.
"""An application to assist Release Managers with ensuring that histories in Git and fixVersions in
JIRA are in agreement. See README.md for a detailed explanation.
"""

import argparse
import csv
import enum
import logging
import pathlib
import re
import sqlite3
import time

import enlighten
import git
import jira


class _DB:
    """Manages an instance of Sqlite on behalf of the application.

    Args:
        db_path (str): Path to the Sqlite database file. ':memory:' for an ephemeral database.
        **_kwargs: Convenience for CLI argument parsing. Ignored.

    Attributes:
        conn (:obj:`sqlite3.db2api.Connection`): The underlying connection object.
    """
    class Action(enum.Enum):
        """Describes an action to be taken against the database."""
        ADD = 'ADD'
        REVERT = 'REVERT'
        SKIP = 'SKIP'

    def __init__(self, db_path, initialize_db, **_kwargs):
        self._conn = sqlite3.connect(db_path)

        if initialize_db:
            for table in 'git_commits', 'jira_versions':
                self._conn.execute("DROP TABLE IF EXISTS %s" % table)

        self._conn.execute("""
        CREATE TABLE IF NOT EXISTS "git_commits"(
          jira_id TEXT NOT NULL,
          branch TEXT NOT NULL,
          git_sha TEXT NOT NULL,
          git_tag TEXT,
          CONSTRAINT pk PRIMARY KEY (jira_id, branch, git_sha)
        );""")
        self._conn.execute("""
        CREATE TABLE IF NOT EXISTS "jira_versions"(
          jira_id TEXT NOT NULL,
          fix_version TEXT NOT NULL,
          CONSTRAINT pk PRIMARY KEY (jira_id, fix_version)
        );""")
        self._conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    @property
    def conn(self):
        """:obj:`sqlite3.db2api.Connection`: Underlying database handle."""
        return self._conn

    def apply_commit(self, action, jira_id, branch, git_sha):
        """Apply an edit to the commits database.

        Args:
            action (:obj:`_DB.Action`): The action to execute.
            jira_id (str): The applicable Issue ID from JIRA.
            branch (str): The name of the git branch from which the commit originates.
            git_sha (str): The commit's SHA.
        """
        if action == _DB.Action.ADD:
            self.conn.execute(
                "INSERT INTO git_commits(jira_id, branch, git_sha) VALUES (upper(?),?,?)",
                (jira_id, branch, git_sha))
        elif action == _DB.Action.REVERT:
            self.conn.execute("""
            DELETE FROM git_commits WHERE
              jira_id=upper(?)
              AND branch=?
            """, (jira_id.upper(), branch))

    def flush_commits(self):
        """Commit any pending changes to the database."""
        self.conn.commit()

    def apply_git_tag(self, branch, git_sha, git_tag):
        """Annotate a commit in the commits database as being a part of the specified release.

        Args:
            branch (str): The name of the git branch from which the commit originates.
            git_sha (str): The commit's SHA.
            git_tag (str): The first release tag following the commit.
        """
        self.conn.execute("UPDATE git_commits SET git_tag = ? WHERE branch = ? AND git_sha = ?",
                          (git_tag, branch, git_sha))

    def apply_fix_version(self, jira_id, fix_version):
        """Annotate a Jira issue in the jira database as being part of the specified release
        version.

        Args:
            jira_id (str): The applicable Issue ID from JIRA.
            fix_version (str): The annotated `fixVersion` as seen in JIRA.
        """
        self.conn.execute("INSERT INTO jira_versions(jira_id, fix_version) VALUES (upper(?),?)",
                          (jira_id, fix_version))

    def unique_jira_ids_from_git(self):
        """Query the commits database for the population of Jira Issue IDs."""
        results = self.conn.execute("SELECT distinct jira_id FROM git_commits").fetchall()
        return [x[0] for x in results]

    def backup(self, target):
        """Write a copy of the database to the `target` destination.

        Args:
            target (str): The backup target, a filesystem path.
        """
        dst = sqlite3.connect(target)
        with dst:
            self._conn.backup(dst)
        dst.close()


class _RepoReader:
    """This class interacts with the git repo, and encapsulates actions specific to HBase's git
    history.

    Args:
        db (:obj:`_DB`): A handle to the database manager.
        fallback_actions_path (str): Path to the file containing sha-specific actions
            (see README.md).
        remote_name (str): The name of the remote to query for branches and histories
            (i.e., "origin").
        development_branch (str): The name of the branch on which active development occurs
            (i.e., "master").
        release_line_regexp (str): Filter criteria used to select "release line" branches (such
            as "branch-1," "branch-2," &c.).
        **_kwargs: Convenience for CLI argument parsing. Ignored.
    """
    _extract_release_tag_pattern = re.compile(r'^rel/(\d+\.\d+\.\d+)(\^0)?$', re.IGNORECASE)
    _skip_patterns = [
        re.compile(r'^preparing development version.+', re.IGNORECASE),
        re.compile(r'^preparing hbase release.+', re.IGNORECASE),
        re.compile(r'^\s*updated? pom.xml version (for|to) .+', re.IGNORECASE),
        re.compile(r'^\s*updated? chang', re.IGNORECASE),
        re.compile(r'^\s*updated? (book|docs|documentation)', re.IGNORECASE),
        re.compile(r'^\s*updating (docs|changes).+', re.IGNORECASE),
        re.compile(r'^\s*bump (pom )?versions?', re.IGNORECASE),
        re.compile(r'^\s*updated? (version|poms|changes).+', re.IGNORECASE),
    ]
    _identify_leading_jira_id_pattern = re.compile(r'^[\s\[]*(hbase-\d+)', re.IGNORECASE)
    _identify_backport_jira_id_patterns = [
        re.compile(r'^backport "(.+)".*', re.IGNORECASE),
        re.compile(r'^backport (.+)', re.IGNORECASE),
    ]
    _identify_revert_jira_id_pattern = re.compile(r'^revert:? "(.+)"', re.IGNORECASE)
    _identify_revert_revert_jira_id_pattern = re.compile(
        '^revert "revert "(.+)"\\.?"\\.?', re.IGNORECASE)
    _identify_amend_jira_id_pattern = re.compile(r'^amend (.+)', re.IGNORECASE)

    def __init__(self, db, fallback_actions_path, remote_name, development_branch,
                 release_line_regexp, parse_release_tags, **_kwargs):
        self._db = db
        self._repo = _RepoReader._open_repo()
        self._fallback_actions = _RepoReader._load_fallback_actions(fallback_actions_path)
        self._remote_name = remote_name
        self._development_branch = development_branch
        self._release_line_regexp = release_line_regexp
        self._parse_release_tags = parse_release_tags

    @property
    def repo(self):
        """:obj:`git.repo.base.Repo`: Underlying Repo handle."""
        return self._repo

    @property
    def remote_name(self):
        """str: The name of the remote used for querying branches and histories."""
        return self._remote_name

    @property
    def development_branch_ref(self):
        """:obj:`git.refs.reference.Reference`: The git branch where active development occurs."""
        refs = self.repo.remote(self._remote_name).refs
        return [ref for ref in refs
                if ref.name == '%s/%s' % (self._remote_name, self._development_branch)][0]

    @property
    def release_line_refs(self):
        """:obj:`list` of :obj:`git.refs.reference.Reference`: The git branches identified as
        "release lines", i.e., "branch-2"."""
        refs = self.repo.remote(self._remote_name).refs
        pattern = re.compile('%s/%s' % (self._remote_name, self._release_line_regexp))
        return [ref for ref in refs if pattern.match(ref.name)]

    @property
    def release_branch_refs(self):
        """:obj:`list` of :obj:`git.refs.reference.Reference`: The git branches identified as
        "release branches", i.e., "branch-2.2"."""
        refs = self.repo.remote(self._remote_name).refs
        release_line_refs = self.release_line_refs
        return [ref for ref in refs
                if any([ref.name.startswith(release_line.name + '.')
                        for release_line in release_line_refs])]

    @staticmethod
    def _open_repo():
        return git.Repo(pathlib.Path(__file__).parent.absolute(), search_parent_directories=True)

    def identify_least_common_commit(self, ref_a, ref_b):
        """Given a pair of references, attempt to identify the commit that they have in common,
        i.e., the commit at which a "release branch" originates from a "release line" branch.
        """
        commits = self._repo.merge_base(ref_a, ref_b, "--all")
        if commits:
            return commits[0]
        raise Exception("could not identify merge base between %s, %s" % (ref_a, ref_b))

    @staticmethod
    def _skip(summary):
        return any([p.match(summary) for p in _RepoReader._skip_patterns])

    @staticmethod
    def _identify_leading_jira_id(summary):
        match = _RepoReader._identify_leading_jira_id_pattern.match(summary)
        if match:
            return match.groups()[0]
        return None

    @staticmethod
    def _identify_backport_jira_id(summary):
        for pattern in _RepoReader._identify_backport_jira_id_patterns:
            match = pattern.match(summary)
            if match:
                return _RepoReader._identify_leading_jira_id(match.groups()[0])
        return None

    @staticmethod
    def _identify_revert_jira_id(summary):
        match = _RepoReader._identify_revert_jira_id_pattern.match(summary)
        if match:
            return _RepoReader._identify_leading_jira_id(match.groups()[0])
        return None

    @staticmethod
    def _identify_revert_revert_jira_id(summary):
        match = _RepoReader._identify_revert_revert_jira_id_pattern.match(summary)
        if match:
            return _RepoReader._identify_leading_jira_id(match.groups()[0])
        return None

    @staticmethod
    def _identify_amend_jira_id(summary):
        match = _RepoReader._identify_amend_jira_id_pattern.match(summary)
        if match:
            return _RepoReader._identify_leading_jira_id(match.groups()[0])
        return None

    @staticmethod
    def _action_jira_id_for(summary):
        jira_id = _RepoReader._identify_leading_jira_id(summary)
        if jira_id:
            return _DB.Action.ADD, jira_id
        jira_id = _RepoReader._identify_backport_jira_id(summary)
        if jira_id:
            return _DB.Action.ADD, jira_id
        jira_id = _RepoReader._identify_revert_jira_id(summary)
        if jira_id:
            return _DB.Action.REVERT, jira_id
        jira_id = _RepoReader._identify_revert_revert_jira_id(summary)
        if jira_id:
            return _DB.Action.ADD, jira_id
        jira_id = _RepoReader._identify_amend_jira_id(summary)
        if jira_id:
            return _DB.Action.ADD, jira_id
        return None

    def _extract_release_tag(self, commit):
        """works for extracting the tag, but need a way to retro-actively tag
        commits we've already seen."""
        names = self._repo.git.name_rev(commit, tags=True, refs='rel/*')
        for name in names.split(' '):
            match = _RepoReader._extract_release_tag_pattern.match(name)
            if match:
                return match.groups()[0]
        return None

    def _set_release_tag(self, branch, tag, shas):
        cnt = 0
        for sha in shas:
            self._db.apply_git_tag(branch, sha, tag)
            cnt += 1
            if cnt % 50 == 0:
                self._db.flush_commits()
        self._db.flush_commits()

    def _resolve_ambiguity(self, commit):
        if commit.hexsha not in self._fallback_actions:
            logging.warning('Unable to resolve action for %s: %s', commit.hexsha, commit.summary)
            return _DB.Action.SKIP, None
        action, jira_id = self._fallback_actions[commit.hexsha]
        if not jira_id:
            jira_id = None
        return _DB.Action[action], jira_id

    def _row_generator(self, branch, commit):
        if _RepoReader._skip(commit.summary):
            return None
        result = _RepoReader._action_jira_id_for(commit.summary)
        if not result:
            result = self._resolve_ambiguity(commit)
        if not result:
            raise Exception('Cannot resolve action for %s: %s' % (commit.hexsha, commit.summary))
        action, jira_id = result
        return action, jira_id, branch, commit.hexsha

    def populate_db_release_branch(self, origin_commit, release_branch):
        """List all commits on `release_branch` since `origin_commit`, recording them as
        observations in the commits database.

        Args:
            origin_commit (:obj:`git.objects.commit.Commit`): The sha of the first commit to
                consider.
            release_branch (str): The name of the ref whose history is to be parsed.
        """
        global MANAGER
        commits = list(self._repo.iter_commits(
            "%s...%s" % (origin_commit.hexsha, release_branch), reverse=True))
        logging.info("%s has %d commits since its origin at %s.", release_branch, len(commits),
                     origin_commit)
        counter = MANAGER.counter(total=len(commits), desc=release_branch, unit='commit')
        commits_since_release = list()
        cnt = 0
        for commit in counter(commits):
            row = self._row_generator(release_branch, commit)
            if row:
                self._db.apply_commit(*row)
            cnt += 1
            if cnt % 50 == 0:
                self._db.flush_commits()
            commits_since_release.append(commit.hexsha)
            if self._parse_release_tags:
                tag = self._extract_release_tag(commit)
                if tag:
                    self._set_release_tag(release_branch, tag, commits_since_release)
                    commits_since_release = list()
        self._db.flush_commits()

    @staticmethod
    def _load_fallback_actions(file):
        result = dict()
        if pathlib.Path(file).exists():
            with open(file, 'r') as handle:
                reader = csv.DictReader(filter(lambda line: line[0] != '#', handle))
                result = dict()
                for row in reader:
                    result[row['hexsha']] = (row['action'], row['jira_id'])
        return result


class _JiraReader:
    """This class interacts with the Jira instance.

    Args:
        db (:obj:`_DB`): A handle to the database manager.
        jira_url (str): URL of the Jira instance to query.
        **_kwargs: Convenience for CLI argument parsing. Ignored.
    """
    def __init__(self, db, jira_url, **_kwargs):
        self._db = db
        self.client = jira.JIRA(jira_url)
        self.throttle_time_in_sec = 1

    def populate_db(self):
        """Query Jira for issue IDs found in the commits database, writing them to the jira
        database."""
        global MANAGER
        jira_ids = self._db.unique_jira_ids_from_git()
        logging.info("retrieving %s jira_ids from the issue tracker", len(jira_ids))
        counter = MANAGER.counter(total=len(jira_ids), desc='fetch from Jira', unit='issue')
        chunk_size = 50
        chunks = [jira_ids[i:i + chunk_size] for i in range(0, len(jira_ids), chunk_size)]

        cnt = 0
        for chunk in chunks:
            query = "key in (" + ",".join([("'" + jira_id + "'") for jira_id in chunk]) + ")"
            results = self.client.search_issues(jql_str=query, maxResults=chunk_size,
                                                fields='fixVersions')
            for result in results:
                jira_id = result.key
                fix_versions = [version.name for version in result.fields.fixVersions]
                for fix_version in fix_versions:
                    self._db.apply_fix_version(jira_id, fix_version)
                    cnt += 1
                    if cnt % 50:
                        self._db.flush_commits()
            counter.update(incr=len(chunk))
            time.sleep(5)
        self._db.flush_commits()

    def fetch_issues(self, jira_ids):
        """Retrieve the specified jira Ids."""
        global MANAGER
        logging.info("retrieving %s jira_ids from the issue tracker", len(jira_ids))
        counter = MANAGER.counter(total=len(jira_ids), desc='fetch from Jira', unit='issue')
        chunk_size = 50
        chunks = [jira_ids[i:i + chunk_size] for i in range(0, len(jira_ids), chunk_size)]
        ret = list()
        for chunk in chunks:
            query = "key IN (" + ",".join([("'" + jira_id + "'") for jira_id in chunk]) + ")"\
                    + " ORDER BY issuetype ASC, priority DESC, key ASC"
            results = self.client.search_issues(
                jql_str=query, maxResults=chunk_size,
                fields='summary,issuetype,priority,resolution,components')
            for result in results:
                val = dict()
                val['key'] = result.key
                val['summary'] = result.fields.summary.strip()
                val['priority'] = result.fields.priority.name.strip()
                val['issue_type'] = result.fields.issuetype.name.strip() \
                    if result.fields.issuetype else None
                val['resolution'] = result.fields.resolution.name.strip() \
                    if result.fields.resolution else None
                val['components'] = [x.name.strip() for x in result.fields.components if x] \
                    if result.fields.components else []
                ret.append(val)
            counter.update(incr=len(chunk))
        return ret


class Auditor:
    """This class builds databases from git and Jira, making it possible to audit the two for
    discrepancies. At some point, it will provide pre-canned audit queries against those databases.
    It is the entrypoint to this application.

    Args:
        repo_reader (:obj:`_RepoReader`): An instance of the `_RepoReader`.
        jira_reader (:obj:`_JiraReader`): An instance of the `JiraReader`.
        db (:obj:`_DB`): A handle to the database manager.
        **_kwargs: Convenience for CLI argument parsing. Ignored.
    """
    def __init__(self, repo_reader, jira_reader, db, **_kwargs):
        self._repo_reader = repo_reader
        self._jira_reader = jira_reader
        self._db = db
        self._release_line_fix_versions = dict()
        for k, v in _kwargs.items():
            if k.endswith('_fix_version'):
                release_line = k[:-len('_fix_version')]
                self._release_line_fix_versions[release_line] = v

    def populate_db_from_git(self):
        """Process the git repository, populating the commits database."""
        for release_line in self._repo_reader.release_line_refs:
            branch_origin = self._repo_reader.identify_least_common_commit(
                self._repo_reader.development_branch_ref.name, release_line.name)
            self._repo_reader.populate_db_release_branch(branch_origin, release_line.name)
            for release_branch in self._repo_reader.release_branch_refs:
                if not release_branch.name.startswith(release_line.name):
                    continue
                self._repo_reader.populate_db_release_branch(branch_origin, release_branch.name)

    def populate_db_from_jira(self):
        """Process the Jira issues identified by the commits database, populating the jira
        database."""
        self._jira_reader.populate_db()

    @staticmethod
    def _write_report(filename, issues):
        with open(filename, 'w') as file:
            fieldnames = ['key', 'issue_type', 'priority', 'summary', 'resolution', 'components']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for issue in issues:
                writer.writerow(issue)
        logging.info('generated report at %s', filename)

    def report_new_for_release_line(self, release_line):
        """Builds a report of the Jira issues that are new on the target release line, not present
        on any of the associated release branches. (i.e., on branch-2 but not
        branch-{2.0,2.1,...})"""
        matches = [x for x in self._repo_reader.release_line_refs
                   if x.name == release_line or x.name.endswith('/%s' % release_line)]
        release_line_ref = next(iter(matches), None)
        if not release_line_ref:
            logging.error('release line %s not found. available options are %s.',
                          release_line, [x.name for x in self._repo_reader.release_line_refs])
            return
        cursor = self._db.conn.execute("""
        SELECT distinct jira_id FROM git_commits
        WHERE branch = ?
        EXCEPT SELECT distinct jira_id FROM git_commits
          WHERE branch LIKE ?
        """, (release_line_ref.name, '%s.%%' % release_line_ref.name))
        jira_ids = [x[0] for x in cursor.fetchall()]
        issues = self._jira_reader.fetch_issues(jira_ids)
        filename = 'new_for_%s.csv' % release_line.replace('/', '-')
        Auditor._write_report(filename, issues)

    @staticmethod
    def _str_to_bool(val):
        if not val:
            return False
        return val.lower() in ['true', 't', 'yes', 'y']

    @staticmethod
    def _build_first_pass_parser():
        parser = argparse.ArgumentParser(add_help=False)
        building_group = parser.add_argument_group(title='Building the audit database')
        building_group.add_argument(
            '--populate-from-git',
            help='When true, populate the audit database from the Git repository.',
            type=Auditor._str_to_bool,
            default=True)
        building_group.add_argument(
            '--populate-from-jira',
            help='When true, populate the audit database from Jira.',
            type=Auditor._str_to_bool,
            default=True)
        building_group.add_argument(
            '--db-path',
            help='Path to the database file, or leave unspecified for a transient db.',
            default=':memory:')
        building_group.add_argument(
            '--initialize-db',
            help='When true, initialize the database tables. This is destructive to the contents'
            + ' of an existing database.',
            type=Auditor._str_to_bool,
            default=False)
        report_group = parser.add_argument_group('Generating reports')
        report_group.add_argument(
            '--report-new-for-release-line',
            help=Auditor.report_new_for_release_line.__doc__,
            type=str,
            default=None)
        git_repo_group = parser.add_argument_group('Interactions with the Git repo')
        git_repo_group.add_argument(
            '--git-repo-path',
            help='Path to the git repo, or leave unspecified to infer from the current'
            + ' file\'s path.',
            default=__file__)
        git_repo_group.add_argument(
            '--remote-name',
            help='The name of the git remote to use when identifying branches.'
            + ' Default: \'origin\'',
            default='origin')
        git_repo_group.add_argument(
            '--development-branch',
            help='The name of the branch from which all release lines originate.'
            + ' Default: \'master\'',
            default='master')
        git_repo_group.add_argument(
            '--development-branch-fix-version',
            help='The Jira fixVersion used to indicate an issue is committed to the development'
            + ' branch. Default: \'3.0.0\'',
            default='3.0.0')
        git_repo_group.add_argument(
            '--release-line-regexp',
            help='A regexp used to identify release lines.',
            default=r'branch-\d+$')
        git_repo_group.add_argument(
            '--parse-release-tags',
            help='When true, look for release tags and annotate commits according to their release'
            + ' version. An Expensive calculation, disabled by default.',
            type=Auditor._str_to_bool,
            default=False)
        git_repo_group.add_argument(
            '--fallback-actions-path',
            help='Path to a file containing _DB.Actions applicable to specific git shas.',
            default='fallback_actions.csv')
        jira_group = parser.add_argument_group('Interactions with Jira')
        jira_group.add_argument(
            '--jira-url',
            help='A URL locating the target JIRA instance.',
            default='https://issues.apache.org/jira')
        return parser, git_repo_group

    @staticmethod
    def _build_second_pass_parser(repo_reader, parent_parser, git_repo_group):
        for release_line in repo_reader.release_line_refs:
            name = release_line.name
            git_repo_group.add_argument(
                '--%s-fix-version' % name[len(repo_reader.remote_name) + 1:],
                help='The Jira fixVersion used to indicate an issue is committed to the specified '
                + 'release line branch',
                required=True)
        return argparse.ArgumentParser(parents=[parent_parser])


MANAGER = None


def main():
    global MANAGER

    first_pass_parser, git_repo_group = Auditor._build_first_pass_parser()
    first_pass_args, extras = first_pass_parser.parse_known_args()
    first_pass_args_dict = vars(first_pass_args)
    with _DB(**first_pass_args_dict) as db:
        logging.basicConfig(level=logging.INFO)
        repo_reader = _RepoReader(db, **first_pass_args_dict)
        jira_reader = _JiraReader(db, **first_pass_args_dict)
        second_pass_parser = Auditor._build_second_pass_parser(
            repo_reader, first_pass_parser, git_repo_group)
        second_pass_args = second_pass_parser.parse_args(extras, first_pass_args)
        second_pass_args_dict = vars(second_pass_args)
        auditor = Auditor(repo_reader, jira_reader, db, **second_pass_args_dict)
        with enlighten.get_manager() as MANAGER:
            if second_pass_args.populate_from_git:
                auditor.populate_db_from_git()
            if second_pass_args.populate_from_jira:
                auditor.populate_db_from_jira()
            if second_pass_args.report_new_for_release_line:
                release_line = second_pass_args.report_new_for_release_line
                auditor.report_new_for_release_line(release_line)


if __name__ == '__main__':
    main()
