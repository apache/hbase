<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# dev-support

Developer and maintainer tooling for the HBase project. This directory
contains CI configuration, release automation, code analysis, and various
utility scripts.

## CI

PR-level CI runs via GitHub Actions (see `../.github/workflows/`). Nightly
builds, branch validation, and precommit checks still use Jenkins
configurations in this directory:

- `Jenkinsfile`, `Jenkinsfile_GitHub` -- Pipeline definitions
- `hbase_nightly_yetus.sh`, `jenkins_precommit_github_yetus.sh` -- Yetus-based
  precommit and nightly check scripts
- `hbase-personality.sh` -- Yetus personality plugin that customizes checks for
  HBase
- `jenkinsEnv.sh`, `jenkins-scripts/` -- Shared Jenkins environment setup
- `HOW_TO_YETUS_LOCAL.md` -- Guide for running Yetus checks locally

## Release Automation

- `create-release/` -- Docker-based release candidate builder (tags, builds,
  signs, publishes). Entry point is `do-release-docker.sh`.
- `make_rc.sh` -- Older release candidate script (superseded by
  `create-release/`)
- `hbase-vote.sh` -- Generates release vote email content
- `git-jira-release-audit/` -- Audits git history against JIRA fixVersion
  fields to find discrepancies between what was committed and what JIRA says
  shipped

## Code Quality and Analysis

- `checkcompatibility.py` -- Checks API/ABI compatibility between versions
- `checkstyle_report.py` -- Generates checkstyle reports
- `spotbugs-exclude.xml` -- SpotBugs exclusion rules
- `code-coverage/` -- Scripts for generating code coverage reports
- `flaky-tests/` -- Flaky test detection, reporting, and dashboards
- `license-header` -- Apache License header template

## Docker and Test Environments

- `docker/` -- Dockerfile for CI build environment
- `hbase_docker/`, `hbase_docker.sh` -- Docker-based local test cluster
- `adhoc_run_tests/` -- Scripts for running test suites outside CI
- `integration-test/` -- Integration test support

## Utility Scripts

- `smart-apply-patch.sh`, `make_patch.sh` -- Patch creation and application
- `rebase_all_git_branches.sh` -- Rebases all local tracking branches
- `zombie-detector.sh` -- Detects leaked processes from test runs
- `gather_machine_environment.sh` -- Captures build machine info for debugging
- `gh_hide_old_comments.sh` -- Hides outdated bot comments on GitHub PRs

## IDE Configuration

- `hbase_eclipse_formatter.xml` -- Eclipse code formatter settings
- `eclipse.importorder` -- Eclipse import ordering
- `HBase Code Template.xml` -- IntelliJ code template

## Design Documents

`design-docs/` collects design documents and proposals for major features.
These capture the rationale behind complex subsystems and are useful for
understanding why the code is structured the way it is.
