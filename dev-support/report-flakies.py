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
from jinja2 import Template
import logging
import requests

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
    logger.info("Analyzing %s", build_url)
    json_response = requests.get(build_url + "/api/json").json()
    if json_response["building"]:
        logger.info("Skipping this build since it is in progress.")
        return {}
    console_url = build_url + "/consoleText"
    return findHangingTests.get_bad_tests(console_url)


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
all_hanging_tests = set()
# Contains { <url> : { <bad_test> : { 'all': [<build ids>], 'failed': [<build ids>],
#                                     'timeout': [<build ids>], 'hanging': [<builds ids>] } } }
url_to_bad_test_results = {}

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
        if result == {}:
            continue
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


template = Template("""
    <!DOCTYPE html>
    <html>
        <head>
        <title>Apache HBase Flaky Dashboard</title>
        <style type="text/css">
            table {
                table-layout: fixed;
            }
            th {
                font-size: 15px;
            }
            td {
                font-size: 18px;
                vertical-align: text-top;
                overflow: hidden;
                white-space: nowrap;
            }
            .show_hide_button {
                font-size: 100%;
                padding: .5em 1em;
                border: 0 rgba(0,0,0,0);
                border-radius: 10px;
            }
        </style>
        </head>
        <body>
            <p>
              <img style="vertical-align:middle; display:inline-block;" height="80px"
                   src="https://hbase.apache.org/images/hbase_logo_with_orca_large.png">
              &nbsp;&nbsp;&nbsp;&nbsp;
              <span style="font-size:50px; vertical-align:middle; display:inline-block;">
                  Apache HBase Flaky Tests Dashboard
              </span>
            </p>
            <br><br>
            {% set counter = 0 %}
            {% for url in results %}
                {% set result = results[url] %}
                {# Dedup ids since test names may duplicate across urls #}
                {% set counter = counter + 1 %}
                <span style="font-size:20px; font-weight:bold;">Job : {{ url |e }}
                <a href="{{ url |e }}" style="text-decoration:none;">&#x1f517;</a></span>
                <br/><br/>
                <table>
                    <tr>
                        <th width="400px">Test Name</th>
                        <th width="150px">Flakyness</th>
                        <th width="200px">Failed/Timeout/Hanging</th>
                        <th>Run Ids</th>
                    </tr>
                    {% for test in result %}
                        {% set all = result[test]['all'] %}
                        {% set failed = result[test]['failed'] %}
                        {% set timeout = result[test]['timeout'] %}
                        {% set hanging = result[test]['hanging'] %}
                        {% set success = all.difference(failed).difference(hanging) %}
                        <tr>
                            <td>{{ test |e }}</td>
                            {% set flakyness =
                                (failed|length + hanging|length) * 100 / all|length %}
                            {% if flakyness == 100 %}
                                <td align="middle" style="background-color:#FF9999;">
                            {% else %}
                                <td align="middle">
                            {% endif %}
                                    {{ "{:.1f}% ({} / {})".format(
                                        flakyness, failed|length + hanging|length, all|length) }}
                                </td>
                            <td align="middle">
                                {{ failed|length }} / {{ timeout|length }} / {{ hanging|length }}
                            </td>
                            <td>
                                {% set id = "details_" ~ test ~ "_" ~ counter  %}
                                <button class="show_hide_button" onclick="toggle('{{ id }}')">
                                    show/hide</button>
                                <br/>
                                <div id="{{ id }}"
                                    style="display: none; width:500px; white-space: normal">
                                {% macro print_run_ids(url, run_ids) -%}
                                    {% for i in run_ids %}
                                        <a href="{{ url }}/{{ i }}">{{ i }}</a>&nbsp;
                                    {% endfor %}
                                {%- endmacro %}
                                    Failed : {{ print_run_ids(url, failed) }}<br/>
                                    Timed Out : {{ print_run_ids(url, timeout) }}<br/>
                                    Hanging : {{ print_run_ids(url, hanging) }}<br/>
                                    Succeeded : {{ print_run_ids(url, success) }}
                                </div>
                            </td>
                        </tr>
                    {% endfor %}
                </table>
                <br><br><br>
            {% endfor %}
            <script type="text/javascript">
                function toggle(id) {
                    if (document.getElementById(id).style["display"] == "none") {
                        document.getElementById(id).style["display"]  = "block";
                    } else {
                        document.getElementById(id).style["display"] = "none";
                    }
                }
            </script>
        </body>
    </html>
    """)

with open("dashboard.html", "w") as f:
    f.write(template.render(results=url_to_bad_test_results))
