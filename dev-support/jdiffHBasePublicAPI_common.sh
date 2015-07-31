#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
##########################################################################################################################
#
### Purpose: To describe whether the directory specified has the old directory format or the new directory format
### Usage: This function takes one argument: The directory in question
### It will set the temporary variable BRANCH_FORMAT. This variable can change with every call, so it is up to the user to
### store it into something else as soon as the function exists
### Example:
### > isNewFormat ./myDevDir/testing/branch/hbase
isNewFormat() {

  echo "Determining if directory $1 is of the 0.94 and before OR 0.95 and after versions";
  if [[ "$1" = "" ]]; then
    echo "Directory not specified. Exiting";
  fi
  echo "First, check that $1 exists";
  if [[ -d $1 ]]; then
    echo "Directory $1 exists"
  else
    echo "Directory $1 does not exist. Exiting";
    exit 1;
  fi

  if [[ -d "$1/hbase-server" ]]; then

    echo "The directory $1/hbase-server exists so this is of the new format";
    export BRANCH_FORMAT=new;

  else
    echo "The directory $1/hbase-server does not exist. Therefore, this is of the old format";
    export BRANCH_FORMAT=old;
  fi
}

### Purpose: To describe whether the argument specified is a git repo or a local directory
### Usage: This function takes one argument: The directory in question
### It will set the temporary variable INPUT_FORMAT. This variable can change with every call, so it is up to the user to
### store it into something else as soon as the function exists
### Example:
### > isGitRepo ./myDevDir/testing/branch/hbase

isGitRepo() {

  echo "Determining if this is a local directory or a git repo.";
  if [[ "$1" = "" ]]; then
    echo "No value specified for repo or directory. Exiting."
    exit 1;
  fi

  if [[ `echo $1 | grep 'http://'` || `echo $1 | grep 'https://'` || `echo $1 | grep 'git://'`  ]]; then
    echo "Looks like $1 is a git repo";
    export INPUT_FORMAT=git_repo
  else
    echo "$1 is a local directory";
    export INPUT_FORMAT=local_directory
  fi


}
