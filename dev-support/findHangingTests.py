#!/usr/bin/python
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
import urllib2
import sys
import string
if len(sys.argv) != 2 :
  print "ERROR : Provide the jenkins job console URL as the only argument."
  exit(1)
print "Fetching " + sys.argv[1]
response = urllib2.urlopen(sys.argv[1])
i = 0;
tests = {}
failed_tests = {}
summary = 0
host = False
patch = False
branch = False
while True:
  n = response.readline()
  if n == "" :
    break
  if not host and n.find("Building remotely on") >= 0:
    host = True
    print n.strip()    
    continue
  if not patch and n.find("Testing patch for ") >= 0:
    patch = True
    print n.strip()    
    continue
  if not branch and n.find("Testing patch on branch ") >= 0:
    branch = True
    print n.strip()    
    continue
  if n.find("PATCH APPLICATION FAILED") >= 0:
    print "PATCH APPLICATION FAILED"
    sys.exit(1) 
  if summary == 0 and n.find("Running tests.") >= 0:
    summary = summary + 1
    continue
  if summary == 1 and n.find("[INFO] Reactor Summary:") >= 0:
    summary = summary + 1
    continue
  if summary == 2 and n.find("[INFO] Apache HBase ") >= 0:
    sys.stdout.write(n)
    continue
  if n.find("org.apache.hadoop.hbase") < 0:
    continue 
  test_name = string.strip(n[n.find("org.apache.hadoop.hbase"):len(n)])
  if n.find("Running org.apache.hadoop.hbase") > -1 :
    tests[test_name] = False
  if n.find("Tests run:") > -1 :
    if n.find("FAILURE") > -1 or n.find("ERROR") > -1:
      failed_tests[test_name] = True
    tests[test_name] = True
response.close()

print "Printing hanging tests"
for key, value in tests.iteritems():
  if value == False:
    print "Hanging test : " + key
print "Printing Failing tests"
for key, value in failed_tests.iteritems():
  print "Failing test : " + key
