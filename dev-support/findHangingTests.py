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

# pylint: disable=invalid-name
# To disable 'invalid constant name' warnings.

"""
# Script to find hanging test from Jenkins build output
# usage: ./findHangingTests.py <url of Jenkins build console>
"""

import re
import sys
import requests

# If any of these strings appear in the console output, it's a build one should probably ignore
# for analyzing failed/hanging tests.
BAD_RUN_STRINGS = [
    "Slave went offline during the build",  # Machine went down, can't do anything about it.
    "The forked VM terminated without properly saying goodbye",  # JVM crashed.
]


def get_bad_tests(console_url):
    """
    Returns [[all tests], [failed tests], [timeout tests], [hanging tests]] if successfully gets
    the build information.
    If there is error getting console text or if there are blacklisted strings in console text,
    then returns None.
    """
    response = requests.get(console_url)
    if response.status_code != 200:
        print "Error getting consoleText. Response = {} {}".format(
            response.status_code, response.reason)
        return

    # All tests: All testcases which were run.
    # Hanging test: A testcase which started but never finished.
    # Failed test: Testcase which encountered any kind of failure. It can be failing atomic tests,
    #   timed out tests, etc
    # Timeout test: A Testcase which encountered timeout. Naturally, all timeout tests will be
    #   included in failed tests.
    all_tests_set = set()
    hanging_tests_set = set()
    failed_tests_set = set()
    timeout_tests_set = set()
    for line in response.content.splitlines():
        result1 = re.match("^Running org.apache.hadoop.hbase.(\\w*\\.)*(\\w*)", line)
        if result1:
            test_case = result1.group(2)
            if test_case in all_tests_set:
                print ("ERROR! Multiple tests with same name '{}'. Might get wrong results "
                       "for this test.".format(test_case))
            else:
                hanging_tests_set.add(test_case)
                all_tests_set.add(test_case)
        result2 = re.match("^Tests run:.*- in org.apache.hadoop.hbase.(\\w*\\.)*(\\w*)", line)
        if result2:
            test_case = result2.group(2)
            if "FAILURE!" in line:
                failed_tests_set.add(test_case)
            if test_case not in hanging_tests_set:
                print  ("ERROR! No test '{}' found in hanging_tests. Might get wrong results "
                        "for this test.".format(test_case))
            else:
                hanging_tests_set.remove(test_case)
        result3 = re.match("^\\s+(\\w*).*\\sTestTimedOut", line)
        if result3:
            test_case = result3.group(1)
            timeout_tests_set.add(test_case)
        for bad_string in BAD_RUN_STRINGS:
            if re.match(".*" + bad_string + ".*", line):
                print "Bad string found in build:\n > {}".format(line)
                return
    print "Result > total tests: {:4}   failed : {:4}  timedout : {:4}  hanging : {:4}".format(
        len(all_tests_set), len(failed_tests_set), len(timeout_tests_set), len(hanging_tests_set))
    return [all_tests_set, failed_tests_set, timeout_tests_set, hanging_tests_set]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "ERROR : Provide the jenkins job console URL as the only argument."
        sys.exit(1)

    print "Fetching {}".format(sys.argv[1])
    result = get_bad_tests(sys.argv[1])
    if not result:
        sys.exit(1)
    [all_tests, failed_tests, timedout_tests, hanging_tests] = result

    print "Found {} hanging tests:".format(len(hanging_tests))
    for test in hanging_tests:
        print test
    print "\n"
    print "Found {} failed tests of which {} timed out:".format(
        len(failed_tests), len(timedout_tests))
    for test in failed_tests:
        print "{0} {1}".format(test, ("(Timed Out)" if test in timedout_tests else ""))

    print ("\nA test may have had 0 or more atomic test failures before it timed out. So a "
           "'Timed Out' test may have other errors too.")
