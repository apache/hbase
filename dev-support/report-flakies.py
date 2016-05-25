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

# This script uses Jenkins REST api to collect test result(s) of given build/builds and generates
# flakyness data about unittests.
# Print help: report-flakies.py -h
import argparse
import findHangingTests
import logging
import requests
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--urls", metavar="url[ max-builds]", action="append", required=True,
    help="Urls to analyze, which can refer to simple projects, multi-configuration projects or "
         "individual build run. Optionally, specify maximum builds to analyze for this url "
         "(if available on jenkins) using space as separator. By default, all available "
         "builds are analyzed.")
parser.add_argument("--mvn", action="store_true",
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


# Given url of an executed build, analyzes its console text, and returns
# [list of all tests, list of timeout tests, list of failed tests].
def get_bad_tests(build_url):
    logger.info("Getting test results for %s", build_url)
    console_url = build_url + "/consoleText"
    return findHangingTests.get_hanging_tests(console_url)


# If any url is of type multi-configuration project (i.e. has key 'activeConfigurations'),
# get urls for individual jobs.
def expand_multi_configuration_projects(urls_list):
    expanded_urls = []
    for url_max_build in urls_list:
        splits = url_max_build.split()
        url = splits[0]
        max_builds = 10000  # Some high value
        if len(splits) == 2:
            max_builds = int(splits[1])
        json_response = requests.get(url + "/api/json").json()
        if json_response.has_key("activeConfigurations"):
            for config in json_response["activeConfigurations"]:
                expanded_urls.append({'url':config["url"], 'max_builds': max_builds})
        else:
            expanded_urls.append({'url':url, 'max_builds': max_builds})
    return expanded_urls


# Set of timeout/failed tests across all given urls.
all_timeout_tests = set()
all_failed_tests = set()

# Iterates over each url, gets test results and prints flaky tests.
expanded_urls  = expand_multi_configuration_projects(args.urls)
for url_max_build in expanded_urls:
    url = url_max_build["url"]
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
        build_ids.append(build_id)
        result = get_bad_tests(build["url"])
        if len(result[0]) > 0:
            build_id_to_results[build_id] = result
        else:
            build_ids_without_tests_run.append(build_id)
        num_builds += 1
        if num_builds == url_max_build["max_builds"]:
            break

    # Collect list of bad tests.
    bad_tests = set()
    for build in build_id_to_results:
        [_, timeout_tests, failed_tests] = build_id_to_results[build]
        all_timeout_tests.update(timeout_tests)
        all_failed_tests.update(failed_tests)
        bad_tests.update(timeout_tests.union(failed_tests))

    # Get total and failed/timeout times for each bad test.
    build_counts = {key : {'total': 0, 'timeout': 0, 'fail': 0 } for key in bad_tests}
    for build in build_id_to_results:
        [all_tests, timeout_tests, failed_tests] = build_id_to_results[build]
        for bad_test in bad_tests:
            if all_tests.issuperset([bad_test]):
                build_counts[bad_test]["total"] += 1
            if timeout_tests.issuperset([bad_test]):
                build_counts[bad_test]['timeout'] += 1
            if failed_tests.issuperset([bad_test]):
                build_counts[bad_test]['fail'] += 1

    if len(bad_tests) > 0:
        print "URL: {}".format(url)
        print "{:>60}  {:25}  {:10}  {}".format(
            "Test Name", "Bad Runs(failed/timeout)", "Total Runs", "Flakyness")
        for bad_test in bad_tests:
            fail = build_counts[bad_test]['fail']
            timeout = build_counts[bad_test]['timeout']
            total = build_counts[bad_test]['total']
            print "{:>60}  {:10} ({:4} / {:4})  {:10}  {:2.0f}%".format(
                bad_test, fail + timeout, fail, timeout, total, (fail + timeout) * 100.0 / total)
    else:
        print "No flaky tests founds."
        if len(build_ids) == len(build_ids_without_tests_run):
            print "None of the analyzed builds have test result."

    print "Builds analyzed: {}".format(build_ids)
    print "Builds without any test runs: {}".format(build_ids_without_tests_run)
    print ""


all_bad_tests = all_timeout_tests.union(all_failed_tests)
if args.mvn:
    includes = ",".join(all_bad_tests)
    with open("./includes", "w") as inc_file:
        inc_file.write(includes)

    excludes = ["**/{0}.java".format(bad_test) for bad_test in all_bad_tests]
    with open("./excludes", "w") as exc_file:
        exc_file.write(",".join(excludes))

    with open("./timeout", "w") as file:
        file.write(",".join(all_timeout_tests))

    with open("./failed", "w") as file:
        file.write(",".join(all_failed_tests))
