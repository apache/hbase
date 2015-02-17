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
# script to diff results of checkstyle-result.xml output. Find the result files
# under $buildRoot/target
#
# usage: ./checkstyle_report.py <master-result.xml> <patch-result.xml>
#

import os
import sys
import xml.etree.ElementTree as etree
from collections import defaultdict

if len(sys.argv) != 3 :
  print "usage: %s checkstyle-result-master.xml checkstyle-result-patch.xml" % sys.argv[0]
  exit(1)

def path_key(x):
  path = x.attrib['name']
  return path[path.find('hbase-'):]

def print_row(path, master_errors, patch_errors):
    print '%s\t%s\t%s' % (k,master_dict[k],child_errors)

master = etree.parse(sys.argv[1])
patch = etree.parse(sys.argv[2])

master_dict = defaultdict(int)

for child in master.getroot().getchildren():
    if child.tag != 'file':
        continue
    child_errors = len(child.getchildren())
    if child_errors == 0:
        continue
    master_dict[path_key(child)] = child_errors

for child in patch.getroot().getchildren():
    if child.tag != 'file':
        continue
    child_errors = len(child.getchildren())
    if child_errors == 0:
        continue
    k = path_key(child)
    if child_errors > master_dict[k]:
        print_row(k, master_dict[k], child_errors)
