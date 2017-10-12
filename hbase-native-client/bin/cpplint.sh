#!/usr/bin/env bash
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
IFS=$'\n\t'
OUTPUT_DIR=$1
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CPPLINT_LOC=https://raw.githubusercontent.com/google/styleguide/gh-pages/cpplint/cpplint.py
OUTPUT=$OUTPUT_DIR/cpplint.py

declare -a MODULES=( client connection exceptions security serde utils test-util )

# Download if not already there
wget -nc $CPPLINT_LOC -O $OUTPUT

# Execute the script
# Exclude the following rules: build/header_guard (We use #pragma once instead)
#                              readability/todo (TODOs are generic)
#                              build/c++11 (We are building with c++14)
for m in ${MODULES[@]}; do
  if [ $m != "security" ]; then  #These are empty
    exec find ${SCRIPT_DIR}/../src/hbase/$m -name "*.h" -or -name "*.cc" | xargs -P8 python $OUTPUT --filter=-build/header_guard,-readability/todo,-build/c++11 --linelength=100
  fi
  if [ $m != "test-util" ]; then
    exec find ${SCRIPT_DIR}/../include/hbase/$m -name "*.h" -or -name "*.cc" | xargs -P8 python $OUTPUT --filter=-build/header_guard,-readability/todo,-build/c++11 --linelength=100
  fi
done
