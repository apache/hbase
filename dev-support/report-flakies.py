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

"""
This script uses Jenkins REST api to collect test result(s) of given build/builds and generates
flakyness data about unittests.
Print help: report-flakies.py -h
"""
import argparse
from jinja2 import Template
import logging
import os
import requests

import findHangingTests

parser = argparse.ArgumentParser()
parser.add_argument(
    '--urls', metavar='URL', action='append', required=True,
    help='Urls to analyze, which can refer to simple projects, multi-configuration projects or '
         'individual build run.')
parser.add_argument('--excluded-builds', metavar='n1,n2', action='append',
                    help='List of build numbers to exclude (or "None"). Not required, '
                         'but if specified, number of uses should be same as that of --urls '
                         'since the values are matched.')
parser.add_argument('--max-builds', metavar='n', action='append', type=int,
                    help='The maximum number of builds to use (if available on jenkins). Specify '
                         '0 to analyze all builds. Not required, but if specified, number of uses '
                         'should be same as that of --urls since the values are matched.')
parser.add_argument(
    "--mvn", action="store_true",
    help="Writes two strings for including/excluding these flaky tests using maven flags. These "
         "strings are written to files so they can be saved as artifacts and easily imported in "
         "other projects. Also writes timeout and failing tests in separate files for "
         "reference.")
parser.add_argument("-v", "--verbose", help="Prints more logs.", action="store_true")
args = parser.parse_args()

logging.basicConfig()
logger = logging.getLogger(__name__)
if args.verbose:
    logger.setLevel(logging.INFO)


def get_bad_tests(build_url):
    """
    Given url of an executed build, analyzes its console text, and returns
    [list of all tests, list of timeout tests, list of failed tests].
    Returns None if can't get console text or if there is any other error.
    """
    logger.info("Analyzing %s", build_url)
    response = requests.get(build_url + "/api/json").json()
    if response["building"]:
        logger.info("Skipping this build since it is in progress.")
        return {}
    console_url = build_url + "/consoleText"
    build_result = findHangingTests.get_bad_tests(console_url)
    if not build_result:
        logger.info("Ignoring build %s", build_url)
        return
    return build_result


def expand_multi_config_projects(cli_args):
    """
    If any url is of type multi-configuration project (i.e. has key 'activeConfigurations'),
    get urls for individual jobs.
    """
    job_urls = cli_args.urls
    excluded_builds_arg = cli_args.excluded_builds
    max_builds_arg = cli_args.max_builds
    if excluded_builds_arg is not None and len(excluded_builds_arg) != len(job_urls):
        raise Exception("Number of --excluded-builds arguments should be same as that of --urls "
                        "since values are matched.")
    if max_builds_arg is not None and len(max_builds_arg) != len(job_urls):
        raise Exception("Number of --max-builds arguments should be same as that of --urls "
                        "since values are matched.")
    final_expanded_urls = []
    for (i, job_url) in enumerate(job_urls):
        max_builds = 10000  # Some high number
        if max_builds_arg is not None and max_builds_arg[i] != 0:
            max_builds = int(max_builds_arg[i])
        excluded_builds = []
        if excluded_builds_arg is not None and excluded_builds_arg[i] != "None":
            excluded_builds = [int(x) for x in excluded_builds_arg[i].split(",")]
        response = requests.get(job_url + "/api/json").json()
        if response.has_key("activeConfigurations"):
            for config in response["activeConfigurations"]:
                final_expanded_urls.append({'url':config["url"], 'max_builds': max_builds,
                                            'excludes': excluded_builds})
        else:
            final_expanded_urls.append({'url':job_url, 'max_builds': max_builds,
                                        'excludes': excluded_builds})
    return final_expanded_urls


# Set of timeout/failed tests across all given urls.
all_timeout_tests = set()
all_failed_tests = set()
all_hanging_tests = set()
# Contains { <url> : { <bad_test> : { 'all': [<build ids>], 'failed': [<build ids>],
#                                     'timeout': [<build ids>], 'hanging': [<builds ids>] } } }
url_to_bad_test_results = {}

# Iterates over each url, gets test results and prints flaky tests.
expanded_urls = expand_multi_config_projects(args)
for url_max_build in expanded_urls:
    url = url_max_build["url"]
    excludes = url_max_build["excludes"]
    json_response = requests.get(url + "/api/json").json()
    if json_response.has_key("builds"):
        builds = json_response["builds"]
        logger.info("Analyzing job: %s", url)
    else:
        builds = [{'number' : json_response["id"], 'url': url}]
        logger.info("Analyzing build : %s", url)
    build_id_to_results = {}
    num_builds = 0
    build_ids = []
    build_ids_without_tests_run = []
    for build in builds:
        build_id = build["number"]
        if build_id in excludes:
            continue
        result = get_bad_tests(build["url"])
        if not result:
            continue
        if len(result[0]) > 0:
            build_id_to_results[build_id] = result
        else:
            build_ids_without_tests_run.append(build_id)
        num_builds += 1
        build_ids.append(build_id)
        if num_builds == url_max_build["max_builds"]:
            break

    # Collect list of bad tests.
    bad_tests = set()
    for build in build_id_to_results:
        [_, failed_tests, timeout_tests, hanging_tests] = build_id_to_results[build]
        all_timeout_tests.update(timeout_tests)
        all_failed_tests.update(failed_tests)
        all_hanging_tests.update(hanging_tests)
        # Note that timedout tests are already included in failed tests.
        bad_tests.update(failed_tests.union(hanging_tests))

    # For each bad test, get build ids where it ran, timed out, failed or hanged.
    test_to_build_ids = {key : {'all' : set(), 'timeout': set(), 'failed': set(), 'hanging' : set()}
                         for key in bad_tests}
    for build in build_id_to_results:
        [all_tests, failed_tests, timeout_tests, hanging_tests] = build_id_to_results[build]
        for bad_test in test_to_build_ids:
            if all_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]["all"].add(build)
            if timeout_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]['timeout'].add(build)
            if failed_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]['failed'].add(build)
            if hanging_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]['hanging'].add(build)
    url_to_bad_test_results[url] = test_to_build_ids

    if len(test_to_build_ids) > 0:
        print "URL: {}".format(url)
        print "{:>60}  {:10}  {:25}  {}".format(
            "Test Name", "Total Runs", "Bad Runs(failed/timeout/hanging)", "Flakyness")
        for bad_test in test_to_build_ids:
            failed = len(test_to_build_ids[bad_test]['failed'])
            timeout = len(test_to_build_ids[bad_test]['timeout'])
            hanging = len(test_to_build_ids[bad_test]['hanging'])
            total = len(test_to_build_ids[bad_test]['all'])
            print "{:>60}  {:10}  {:7} ( {:4} / {:5} / {:5} )  {:2.0f}%".format(
                bad_test, total, failed + timeout, failed, timeout, hanging,
                (failed + timeout) * 100.0 / total)
    else:
        print "No flaky tests founds."
        if len(build_ids) == len(build_ids_without_tests_run):
            print "None of the analyzed builds have test result."

    print "Builds analyzed: {}".format(build_ids)
    print "Builds without any test runs: {}".format(build_ids_without_tests_run)
    print ""


all_bad_tests = all_hanging_tests.union(all_failed_tests)
if args.mvn:
    includes = ",".join(all_bad_tests)
    with open("./includes", "w") as inc_file:
        inc_file.write(includes)

    excludes = ["**/{0}.java".format(bad_test) for bad_test in all_bad_tests]
    with open("./excludes", "w") as exc_file:
        exc_file.write(",".join(excludes))

    with open("./timeout", "w") as timeout_file:
        timeout_file.write(",".join(all_timeout_tests))

    with open("./failed", "w") as failed_file:
        failed_file.write(",".join(all_failed_tests))

dev_support_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(dev_support_dir, "flaky-dashboard-template.html"), "r") as f:
    template = Template(f.read())

with open("dashboard.html", "w") as f:
    f.write(template.render(results=url_to_bad_test_results))
