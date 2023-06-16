#!/usr/bin/env python3
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

import sys
import re
import os

if len(sys.argv) != 3:
  print("usage: %s <NEW_RELEASENOTES.md> <PREV_RELEASENOTES.md>" % sys.argv[0])
  sys.exit(1)

pattern = re.compile(r'^# .+ Release Notes$')
with open(sys.argv[1], 'r', errors = 'ignore') as new_r, open(sys.argv[2], 'r', errors = 'ignore') as prev_r, open(sys.argv[2] + '.tmp', 'w') as w:
  line = prev_r.readline()
  while line:
    if pattern.match(line):
      break
    line = prev_r.readline()
  w.writelines('# RELEASENOTES')
  for newline in new_r:
    w.writelines(newline)
  while line:
    w.writelines(line)
    line = prev_r.readline()
os.rename(sys.argv[2] + '.tmp', sys.argv[2])
