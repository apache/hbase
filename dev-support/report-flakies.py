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

# This script uses Jenkins REST api to collect test results of given builds and generates flakyness
# data about unittests.
# Print help: ./report-flakies.py -h
import argparse
import logging
import re
import requests

parser = argparse.ArgumentParser()
parser.add_argument("--max-builds", type=int, metavar="n",
    help="Number of builds to analyze for each job (if available in jenkins). Default: all "
        + "available builds.")
parser.add_argument("--mvn", action="store_true",
    help="Writes two strings for including/excluding these flaky tests using maven flags. These "
        + "strings are written to files so they can be saved as artifacts and easily imported in "
        + "other projects.")
parser.add_argument("-v", "--verbose", help="Prints more logs.", action="store_true")
parser.add_argument(
    "urls", help="Space separated list of urls (single/multi-configuration project) to analyze")
args = parser.parse_args()

logging.basicConfig()
logger = logging.getLogger("org.apache.hadoop.hbase.report-flakies")
if args.verbose:
  logger.setLevel(logging.INFO)

# Given url of an executed build, fetches its test report, and returns dictionary from testname to
# pass/skip/fail status.
def get_build_results(build_url):
    logger.info("Getting test results for %s", build_url)
    url = build_url + "testReport/api/json?tree=suites[cases[className,name,status]]"
    response = requests.get(url)
    if response.status_code == 404:
        logger.info("No test results for %s", build_url)
        return {}
    json_response = response.json()

    tests = {}
    for test_cases in json_response["suites"]:
        for test in test_cases["cases"]:
            # Truncate initial "org.apache.hadoop.hbase." from all tests.
            test_name = (test["className"] + "#" + test["name"])[24:]
            tests[test_name] = test["status"]
    return tests

# If any url is of type multi-configuration project (i.e. has key 'activeConfigurations'),
# get urls for individual jobs.
jobs_list = []
for url in args.urls.split():
    json_response = requests.get(url + "/api/json").json()
    if json_response.has_key("activeConfigurations"):
        for config in json_response["activeConfigurations"]:
            jobs_list.append(config["url"])
    elif json_response.has_key("builds"):
        jobs_list.append(url)
    else:
        raise Exception("Bad url ({0}).".format(url))

global_bad_tests = set()
# Iterates over each job, gets its test results and prints flaky tests.
for job_url in jobs_list:
    logger.info("Analyzing job: %s", job_url)
    build_id_to_results = {}
    builds = requests.get(job_url + "/api/json").json()["builds"]
    num_builds = 0
    build_ids = []
    build_ids_without_result = []
    for build in builds:
        build_id = build["number"]
        build_ids.append(build_id)
        build_result = get_build_results(build["url"])
        if len(build_result) > 0:
            build_id_to_results[build_id] = build_result
        else:
            build_ids_without_result.append(build_id)
        num_builds += 1
        if num_builds == args.max_builds:
            break

    # Collect list of bad tests.
    bad_tests = set()
    for build in build_id_to_results:
        for test in build_id_to_results[build]:
            if (build_id_to_results[build][test] == "REGRESSION"
                or build_id_to_results[build][test] == "FAILED"):
                bad_tests.add(test)
                global_bad_tests.add(test)

    # Get total and failed build times for each bad test.
    build_counts = {key:dict([('total', 0), ('failed', 0)]) for key in bad_tests}
    for build in build_id_to_results:
        build_results = build_id_to_results[build]
        for bad_test in bad_tests:
            if build_results.has_key(bad_test):
                if build_results[bad_test] != "SKIPPED":  # Ignore the test if it's skipped.
                    build_counts[bad_test]['total'] += 1
                if build_results[bad_test] == "REGRESSION":
                    build_counts[bad_test]['failed'] += 1

    if len(bad_tests) > 0:
        print "Job: {}".format(job_url)
        print "{:>100}  {:6}  {:10}  {}".format("Test Name", "Failed", "Total Runs", "Flakyness")
        for bad_test in bad_tests:
            fail = build_counts[bad_test]['failed']
            total = build_counts[bad_test]['total']
            print "{:>100}  {:6}  {:10}  {:2.0f}%".format(bad_test, fail, total, fail*100.0/total)
    else:
        print "No flaky tests founds."
        if len(build_ids) == len(build_ids_without_result):
            print "None of the analyzed builds have test result."

    print "Builds analyzed: " + str(build_ids)
    print "Builds with no results: " + str(build_ids_without_result)
    print ""

if args.mvn:
    # There might be multiple tests failing within each TestCase, avoid duplication of TestCase names.
    test_cases = set()
    for test in global_bad_tests:
        test = re.sub(".*\.", "", test)  # Remove package name prefix.
        test = re.sub("#.*", "", test)  # Remove individual unittest's name
        test_cases.add(test)

    includes = ",".join(test_cases)
    with open("./includes", "w") as inc_file:
        inc_file.write(includes)
        inc_file.close()

    excludes = ""
    for test_case in test_cases:
        excludes += "**/" + test_case + ".java,"
    with open("./excludes", "w") as exc_file:
        exc_file.write(excludes)
        exc_file.close()
