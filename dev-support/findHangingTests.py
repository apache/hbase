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

def get_hanging_tests(console_url):
    response = requests.get(console_url)
    if response.status_code != 200:
        print "Error getting consoleText. Response = {} {}".format(
            response.status_code, response.reason)
        return {}

    all_tests = set()
    hanging_tests = set()
    failed_tests = set()
    for line in response.content.splitlines():
        result1 = re.match("^Running org.apache.hadoop.hbase.(\w*\.)*(\w*)", line)
        if result1:
            test_case = result1.group(2)
            hanging_tests.add(test_case)
            all_tests.add(test_case)
        result2 = re.match("^Tests run:.*- in org.apache.hadoop.hbase.(\w*\.)*(\w*)", line)
        if result2:
            test_case = result2.group(2)
            hanging_tests.remove(test_case)
            if "FAILURE!" in line:
                failed_tests.add(test_case)
    print "Result > total tests: {:4}   hanging : {:4}   failed : {:4}".format(
        len(all_tests), len(hanging_tests), len(failed_tests))
    return [all_tests, hanging_tests, failed_tests]

if __name__ == "__main__":
    if len(sys.argv) != 2 :
        print "ERROR : Provide the jenkins job console URL as the only argument."
        sys.exit(1)

    print "Fetching {}".format(sys.argv[1])
    [all_tests, hanging_tests, failed_tests] = get_hanging_tests(sys.argv[1])
    print "Found {} hanging tests:".format(len(hanging_tests))
    for test in hanging_tests:
        print test
    print "\n"
    print "Found {} failed tests:".format(len(failed_tests))
    for test in failed_tests:
        print test
