#!/usr/bin/python2
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

def error_name(x):
  error_class = x.attrib['source']
  return error_class[error_class.rfind(".") + 1:]

def print_row(path, error, master_errors, patch_errors):
    print '%s\t%s\t%s\t%s' % (path,error, master_errors,patch_errors)

master = etree.parse(sys.argv[1])
patch = etree.parse(sys.argv[2])

master_dict = defaultdict(int)
ret_value = 0

for child in master.getroot().getchildren():
    if child.tag != 'file':
        continue
    file = path_key(child)
    for error_tag in child.getchildren():
        error = error_name(error_tag)
        if (file, error) in master_dict:
            master_dict[(file, error)] += 1
        else:
            master_dict[(file, error)] = 1

for child in patch.getroot().getchildren():
    if child.tag != 'file':
        continue
    temp_dict = defaultdict(int)
    for error_tag in child.getchildren():
        error = error_name(error_tag)
        if error in temp_dict:
            temp_dict[error] += 1
        else:
            temp_dict[error] = 1

    file = path_key(child)
    for error, count in temp_dict.iteritems():
        if count > master_dict[(file, error)]:
            print_row(file, error, master_dict[(file, error)], count)
            ret_value = 1

sys.exit(ret_value)
