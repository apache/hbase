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

# This script assumes that your remote is called "origin" 
# and that your local master branch is called "master".
# I am sure it could be made more abstract but these are the defaults.

# Edit this line to point to your default directory, 
# or always pass a directory to the script.

DEFAULT_DIR="EDIT_ME"

function print_usage {
  cat << __EOF

$0: A script to manage your Apache HBase Git repository.

If run with no arguments, it reads the DEFAULT_DIR variable, which you
can specify by editing the script.

Usage: $0 [-d <dir>]
       $0 -h

  -h           Show this screen.
  -d <dir>     The absolute or relative directory of your Git repository.
__EOF
}

function get_all_branches {
  # Gets all git branches present locally
  all_branches=()
  for i in `git branch --list | sed -e "s/\*//g"`; do
    all_branches+=("$(echo $i | awk '{print($1)}')")
  done
}

function get_tracking_branches {
  # Gets all branches with a remote tracking branch
  tracking_branches=()
  for i in `git branch -lvv | grep "\[origin/" | sed -e 's/\*//g' | awk {'print $1'}`; do
    tracking_branches+=("$(echo $i | awk '{print($1)}')")
  done
}

function check_git_branch_status {
  # Checks the current Git branch to see if it's dirty
  # Returns 1 if the branch is dirty
  git_dirty=$(git diff --shortstat 2> /dev/null | wc -l|awk {'print $1'})
  if [ "$git_dirty" -ne 0 ]; then
    echo "Git status is dirty. Commit locally first." >&2
    exit 1
  fi
}

function get_jira_status {
  # This function expects as an argument the JIRA ID, 
  # and returns 99 if resolved and 1 if it couldn't
  # get the status.

  # The JIRA status looks like this in the HTML: 
  # span id="resolution-val" class="value resolved" >
  # The following is a bit brittle, but filters for lines with 
  # resolution-val returns 99 if it's resolved
  jira_url='https://issues.apache.org/jira/rest/api/2/issue'
  jira_id="$1"
  curl -s "$jira_url/$jira_id?fields=resolution" |grep -q '{"resolution":null}'
  status=$?
  if [ $status -ne 0 -a $status -ne 1 ]; then
    echo "Could not get JIRA status. Check your network." >&2
    exit 1
  fi
  if [ $status -ne 0 ]; then
    return 99
  fi
}

# Process the arguments
while getopts ":hd:" opt; do
  case $opt in
    d)
      # A directory was passed in
      dir="$OPTARG"
      if [ ! -d "$dir/.git/" ]; then
        echo "$dir does not exist or is not a Git repository." >&2
        exit 1
      fi
      ;;
    h)
      # Print usage instructions
      print_usage
      exit 0
      ;;
    *)  
      echo "Invalid argument: $OPTARG" >&2
      print_usage >&2
      exit 1
      ;;
  esac
done

if [ -z "$dir" ]; then
  # No directory was passed in
  dir="$DEFAULT_DIR"
  if [ "$dir" = "EDIT_ME" ]; then
    echo "You need to edit the DEFAULT_DIR in $0." >&2
    $0 -h
    exit 1
  elif [ ! -d "$DEFAULT_DIR/.git/" ]; then
    echo "Default directory $DEFAULT_DIR is not a Git repository." >&2
    exit 1
  fi
fi

cd "$dir"

# For each tracking branch, check it out and make sure it's fresh
# This function creates tracking_branches array and stores the tracking branches in it
get_tracking_branches
for i in "${tracking_branches[@]}"; do
  git checkout -q "$i"
  # Exit if git status is dirty
  check_git_branch_status 
  git pull -q --rebase
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "Unable to pull changes in $i: $status Exiting." >&2
    exit 1
  fi
  echo "Refreshed $i from remote."
done

# Run the function to get the list of all branches
# The function creates array all_branches and stores the branches in it
get_all_branches

# Declare array to hold deleted branch info
deleted_branches=()

for i in "${all_branches[@]}"; do
  # Check JIRA status to see if we still need this branch
  # JIRA expects uppercase
  jira_id="$(echo $i | awk '{print toupper($0)'})"
  if [[ "$jira_id" == HBASE-* ]]; then
    # Returns 1 if the JIRA is closed, 0 otherwise
    get_jira_status "$jira_id"
    jira_status=$?
    if [ $jira_status -eq 99 ]; then
        # the JIRA seems to be resolved or is at least not unresolved
        deleted_branches+=("$i")
    fi
  fi

  git checkout -q "$i"

  # Exit if git status is dirty
  check_git_branch_status 

  # If this branch has a remote, don't rebase it
  # If it has a remote, it has a log with at least one entry
  git log -n 1 origin/"$i" > /dev/null 2>&1
  status=$?
  if [ $status -eq 128 ]; then
    # Status 128 means there is no remote branch
    # Try to rebase against master
    echo "Rebasing $i on origin/master"
    git rebase -q origin/master > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Failed. Rolling back. Rebase $i manually."
      git rebase --abort
    fi
  elif [ $status -ne 0 ]; then 
  # If status is 0 it means there is a remote branch, we already took care of it
    echo "Unknown error: $?" >&2
    exit 1
  fi
done

# Offer to clean up all deleted branches
for i in "${deleted_branches[@]}"; do
  read -p "$i's JIRA is resolved. Delete? " yn
  case $yn in
    [Yy]) 
        git branch -D $i
        ;;
    *) 
        echo "To delete it manually, run git branch -D $deleted_branches"
        ;;
  esac
done
git checkout -q master
exit 0
