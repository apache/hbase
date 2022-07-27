#!/usr/bin/env python2
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

# pylint: disable=invalid-name
# To disable 'invalid constant name' warnings.
# pylint: disable=import-error
# Testing environment may not have all dependencies.

"""
This script uses Jenkins REST api to collect test result(s) of given build/builds and generates
flakyness data about unittests.
Print help: report-flakies.py -h
"""

import argparse
import logging
import os
import time
from collections import OrderedDict
from jinja2 import Template

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
parser.add_argument('--is-yetus', metavar='True/False', action='append', choices=['True', 'False'],
                    help='True, if build is yetus style i.e. look for maven output in artifacts; '
                         'False, if maven output is in <url>/consoleText itself.')
parser.add_argument(
    "--mvn", action="store_true",
    help="Writes two strings for including/excluding these flaky tests using maven flags. These "
         "strings are written to files so they can be saved as artifacts and easily imported in "
         "other projects. Also writes timeout and failing tests in separate files for "
         "reference.")
parser.add_argument("-o", "--output", metavar='dir', action='store', required=False,
                    help="the output directory")
parser.add_argument("-v", "--verbose", help="Prints more logs.", action="store_true")
args = parser.parse_args()

logging.basicConfig()
logger = logging.getLogger(__name__)
if args.verbose:
    logger.setLevel(logging.INFO)

output_dir = '.'
if args.output is not None:
    output_dir = args.output
    if not os.path.exists(output_dir):
      os.makedirs(output_dir)

def get_bad_tests(build_url, is_yetus):
    """
    Given url of an executed build, analyzes its maven output, and returns
    [list of all tests, list of timeout tests, list of failed tests].
    Returns None if can't get maven output from the build or if there is any other error.
    """
    logger.info("Analyzing %s", build_url)
    needed_fields="_class,building"
    if is_yetus:
        needed_fields+=",artifacts[fileName,relativePath]"
    response = requests.get(build_url + "/api/json?tree=" + needed_fields).json()
    if response["building"]:
        logger.info("Skipping this build since it is in progress.")
        return {}
    console_url = None
    if is_yetus:
        for artifact in response["artifacts"]:
            if artifact["fileName"] == "patch-unit-root.txt":
                console_url = build_url + "/artifact/" + artifact["relativePath"]
                break
        if console_url is None:
            logger.info("Can't find 'patch-unit-root.txt' artifact for Yetus build %s\n. Ignoring "
                        "this build.", build_url)
            return
    else:
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
    is_yetus_arg = cli_args.is_yetus
    if excluded_builds_arg is not None and len(excluded_builds_arg) != len(job_urls):
        raise Exception("Number of --excluded-builds arguments should be same as that of --urls "
                        "since values are matched.")
    if max_builds_arg is not None and len(max_builds_arg) != len(job_urls):
        raise Exception("Number of --max-builds arguments should be same as that of --urls "
                        "since values are matched.")
    final_expanded_urls = []
    for (i, job_url) in enumerate(job_urls):
        max_builds = 10000  # Some high number
        is_yetus = False
        if is_yetus_arg is not None:
            is_yetus = is_yetus_arg[i] == "True"
        if max_builds_arg is not None and max_builds_arg[i] != 0:
            max_builds = int(max_builds_arg[i])
        excluded_builds = []
        if excluded_builds_arg is not None and excluded_builds_arg[i] != "None":
            excluded_builds = [int(x) for x in excluded_builds_arg[i].split(",")]
        request = requests.get(job_url + "/api/json?tree=_class,activeConfigurations%5Burl%5D")
        if request.status_code != 200:
            raise Exception("Failed to get job information from jenkins for url '" + job_url +
                            "'. Jenkins returned HTTP status " + str(request.status_code))
        response = request.json()
        if response.has_key("activeConfigurations"):
            for config in response["activeConfigurations"]:
                final_expanded_urls.append({'url':config["url"], 'max_builds': max_builds,
                                            'excludes': excluded_builds, 'is_yetus': is_yetus})
        else:
            final_expanded_urls.append({'url':job_url, 'max_builds': max_builds,
                                        'excludes': excluded_builds, 'is_yetus': is_yetus})
    return final_expanded_urls


# Set of timeout/failed tests across all given urls.
all_timeout_tests = set()
all_failed_tests = set()
all_hanging_tests = set()
# Contains { <url> : { <bad_test> : { 'all': [<build ids>], 'failed': [<build ids>],
#                                     'timeout': [<build ids>], 'hanging': [<builds ids>] } } }
url_to_bad_test_results = OrderedDict()
# Contains { <url> : [run_ids] }
# Used for common min/max build ids when generating sparklines.
url_to_build_ids = OrderedDict()

# Iterates over each url, gets test results and prints flaky tests.
expanded_urls = expand_multi_config_projects(args)
for url_max_build in expanded_urls:
    url = url_max_build["url"]
    excludes = url_max_build["excludes"]
    json_response = requests.get(url + "/api/json?tree=id,builds%5Bnumber,url%5D").json()
    if json_response.has_key("builds"):
        builds = json_response["builds"]
        logger.info("Analyzing job: %s", url)
    else:
        builds = [{'number': json_response["id"], 'url': url}]
        logger.info("Analyzing build : %s", url)
    build_id_to_results = {}
    num_builds = 0
    url_to_build_ids[url] = []
    build_ids_without_tests_run = []
    for build in builds:
        build_id = build["number"]
        if build_id in excludes:
            continue
        result = get_bad_tests(build["url"], url_max_build['is_yetus'])
        if not result:
            continue
        if len(result[0]) > 0:
            build_id_to_results[build_id] = result
        else:
            build_ids_without_tests_run.append(build_id)
        num_builds += 1
        url_to_build_ids[url].append(build_id)
        if num_builds == url_max_build["max_builds"]:
            break
    url_to_build_ids[url].sort()

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
    test_to_build_ids = {key : {'all' : set(), 'timeout': set(), 'failed': set(),
                                'hanging' : set(), 'bad_count' : 0}
                         for key in bad_tests}
    for build in build_id_to_results:
        [all_tests, failed_tests, timeout_tests, hanging_tests] = build_id_to_results[build]
        for bad_test in test_to_build_ids:
            is_bad = False
            if all_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]["all"].add(build)
            if timeout_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]['timeout'].add(build)
                is_bad = True
            if failed_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]['failed'].add(build)
                is_bad = True
            if hanging_tests.issuperset([bad_test]):
                test_to_build_ids[bad_test]['hanging'].add(build)
                is_bad = True
            if is_bad:
                test_to_build_ids[bad_test]['bad_count'] += 1

    # Calculate flakyness % and successful builds for each test. Also sort build ids.
    for bad_test in test_to_build_ids:
        test_result = test_to_build_ids[bad_test]
        test_result['flakyness'] = test_result['bad_count'] * 100.0 / len(test_result['all'])
        test_result['success'] = (test_result['all'].difference(
            test_result['failed'].union(test_result['hanging'])))
        for key in ['all', 'timeout', 'failed', 'hanging', 'success']:
            test_result[key] = sorted(test_result[key])


    # Sort tests in descending order by flakyness.
    sorted_test_to_build_ids = OrderedDict(
        sorted(test_to_build_ids.iteritems(), key=lambda x: x[1]['flakyness'], reverse=True))
    url_to_bad_test_results[url] = sorted_test_to_build_ids

    if len(sorted_test_to_build_ids) > 0:
        print "URL: {}".format(url)
        print "{:>60}  {:10}  {:25}  {}".format(
            "Test Name", "Total Runs", "Bad Runs(failed/timeout/hanging)", "Flakyness")
        for bad_test in sorted_test_to_build_ids:
            test_status = sorted_test_to_build_ids[bad_test]
            print "{:>60}  {:10}  {:7} ( {:4} / {:5} / {:5} )  {:2.0f}%".format(
                bad_test, len(test_status['all']), test_status['bad_count'],
                len(test_status['failed']), len(test_status['timeout']),
                len(test_status['hanging']), test_status['flakyness'])
    else:
        print "No flaky tests founds."
        if len(url_to_build_ids[url]) == len(build_ids_without_tests_run):
            print "None of the analyzed builds have test result."

    print "Builds analyzed: {}".format(url_to_build_ids[url])
    print "Builds without any test runs: {}".format(build_ids_without_tests_run)
    print ""


all_bad_tests = all_hanging_tests.union(all_failed_tests)
if args.mvn:
    includes = ",".join(all_bad_tests)
    with open(output_dir + "/includes", "w") as inc_file:
        inc_file.write(includes)

    excludes = ["**/{0}.java".format(bad_test) for bad_test in all_bad_tests]
    with open(output_dir + "/excludes", "w") as exc_file:
        exc_file.write(",".join(excludes))

    with open(output_dir + "/timeout", "w") as timeout_file:
        timeout_file.write(",".join(all_timeout_tests))

    with open(output_dir + "/failed", "w") as failed_file:
        failed_file.write(",".join(all_failed_tests))

dev_support_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(dev_support_dir, "flaky-dashboard-template.html"), "r") as f:
    template = Template(f.read())

with open(output_dir + "/dashboard.html", "w") as f:
    datetime = time.strftime("%m/%d/%Y %H:%M:%S")
    f.write(template.render(datetime=datetime, bad_tests_count=len(all_bad_tests),
                            results=url_to_bad_test_results, build_ids=url_to_build_ids))
