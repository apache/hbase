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
##
# script to find hanging test from Jenkins build output
# usage: ./findHangingTests.py <url of Jenkins build console>
#
import re
import requests
import sys

# Returns [[all tests], [failed tests], [timeout tests], [hanging tests]]
# Definitions:
# All tests: All testcases which were run.
# Hanging test: A testcase which started but never finished.
# Failed test: Testcase which encountered any kind of failure. It can be failing atomic tests,
#   timed out tests, etc
# Timeout test: A Testcase which encountered timeout. Naturally, all timeout tests will be
#   included in failed tests.
def get_bad_tests(console_url):
    response = requests.get(console_url)
    if response.status_code != 200:
        print "Error getting consoleText. Response = {} {}".format(
            response.status_code, response.reason)
        return {}

    all_tests = set()
    hanging_tests = set()
    failed_tests = set()
    timeout_tests = set()
    for line in response.content.splitlines():
        result1 = re.match("^Running org.apache.hadoop.hbase.(\w*\.)*(\w*)", line)
        if result1:
            test_case = result1.group(2)
            if test_case in all_tests:
                print  ("ERROR! Multiple tests with same name '{}'. Might get wrong results "
                       "for this test.".format(test_case))
            else:
                hanging_tests.add(test_case)
                all_tests.add(test_case)
        result2 = re.match("^Tests run:.*- in org.apache.hadoop.hbase.(\w*\.)*(\w*)", line)
        if result2:
            test_case = result2.group(2)
            if "FAILURE!" in line:
                failed_tests.add(test_case)
            if test_case not in hanging_tests:
                print  ("ERROR! No test '{}' found in hanging_tests. Might get wrong results "
                        "for this test.".format(test_case))
            else:
                hanging_tests.remove(test_case)
        result3 = re.match("^\s+(\w*).*\sTestTimedOut", line)
        if result3:
            test_case = result3.group(1)
            timeout_tests.add(test_case)
    print "Result > total tests: {:4}   failed : {:4}  timedout : {:4}  hanging : {:4}".format(
          len(all_tests), len(failed_tests), len(timeout_tests), len(hanging_tests))
    return [all_tests, failed_tests, timeout_tests, hanging_tests]

if __name__ == "__main__":
    if len(sys.argv) != 2 :
        print "ERROR : Provide the jenkins job console URL as the only argument."
        sys.exit(1)

    print "Fetching {}".format(sys.argv[1])
    [all_tests, failed_tests, timedout_tests, hanging_tests] = get_bad_tests(sys.argv[1])
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
