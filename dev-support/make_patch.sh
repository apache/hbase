#!/bin/bash
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Make a patch for the current branch based on its tracking branch

# Process args
while getopts "ahd:b:" opt; do
    case "$opt" in
        a)  addendum='-addendum'
            ;;
        d)
            patch_dir=$OPTARG
            ;;
        b)
            tracking_branch=$OPTARG
            ;;
        *)
            echo -e "Usage: $0 [-h] [-a] [-d] <directory> \n\
        Must be run from within the git branch to make the patch against.\n\
        -h - display these instructions.\n\
        -a - Add an 'addendum' prefix to the patch name.\n\
        -b - Specify the base branch to diff from. (defaults to the tracking branch or origin master)\n\
        -d - specify a patch directory (defaults to ~/patches/)"
            exit 0
            ;;
    esac
done

# Find what branch we are on
branch=$(git branch |grep '*' |awk '{print $2}')
if [ ! "$branch" ]; then
    echo "Can't determine the git branch. Exiting." >&2
    exit 1
fi

# Exit if git status is dirty
git_dirty=$(git diff --shortstat 2> /dev/null | wc -l|awk {'print $1'})
echo "git_dirty is $git_dirty"
if [ "$git_dirty" -ne 0 ]; then
    echo "Git status is dirty. Commit locally first.">&2
    exit 1
fi

# Determine the tracking branch if needed.
# If it was passed in from the command line
# with -b then use dthat no matter what.
if [ ! "$tracking_branch" ]; then
  git log -n 1 origin/$branch > /dev/null 2>&1
  status=$?
  if [ "$status" -eq 128 ]; then
      # Status 128 means there is no remote branch
      tracking_branch='origin/master'
  elif [ "$status" -eq 0 ]; then
      # Status 0 means there is a remote branch
      tracking_branch="origin/$branch"
  else
      echo "Unknown error: $?" >&2
      exit 1
  fi
fi


# Deal with invalid or missing $patch_dir
if [ ! "$patch_dir" ]; then
    echo -e "Patch directory not specified. Falling back to ~/patches/."
    patch_dir=~/patches
fi

if [ ! -d "$patch_dir" ]; then
    echo "$patch_dir does not exist. Creating it."
    mkdir $patch_dir
fi

# Determine what to call the patch
# Check to see if any patch exists that includes the branch name
status=$(ls $patch_dir/*$branch* 2>/dev/null|grep -v addendum|wc -l|awk {'print $1'})
if [ "$status" -eq 0 ]; then
    # This is the first patch we are making for this release
    prefix=''
elif  [ "$status" -ge 1 ]; then
    # At least one patch already exists -- add a version prefix
    for i in {1..99}; do
        # Check to see the maximum version of patch that exists
        if [ ! -f "$patch_dir/$branch-v$i.patch" ]; then
            version=$i
            if [ -n "$addendum" ]; then
                # Don't increment the patch # if it is an addendum
                echo "Creating an addendum"
                if [ "$version" -eq 1 ]; then
                    # We are creating an addendum to the first version of the patch
                    prefix=''
                else
                    # We are making an addendum to a different version of the patch
                    let version=$version-1
                    prefix="-v$version"
                fi
            else
                prefix="-v$version"
            fi
            break
        fi
    done
fi
# If this is against a tracking branch other than master
# include it in the patch name
tracking_suffix=""
if [[ $tracking_branch != "origin/master" \
    &&  $tracking_branch != "master" ]]; then
    tracking_suffix="-${tracking_branch#origin/}"
fi

patch_name="$branch$prefix$addendum$tracking_suffix.patch"

# Do we need to make a diff?
git diff --quiet $tracking_branch
status=$?
if [ "$status" -eq 0 ]; then
    echo "There is no difference between $branch and $tracking_branch."
    echo "No patch created."
    exit 0
fi

# Check whether we need to squash or not
local_commits=$(git log $tracking_branch..$branch|grep 'Author:'|wc -l|awk {'print $1'})
if [ "$local_commits" -gt 1 ]; then
    read -p "$local_commits commits exist only in your local branch. Interactive rebase?" yn
    case $yn in
        [Yy]* )
            git rebase -i $tracking_branch
                ;;
        [Nn]* )
          echo "Creating $patch_dir/$patch_name using git diff."
          git diff $tracking_branch > $patch_dir/$patch_name
          exit 0
        ;;
    esac
fi

echo "Creating patch $patch_dir/$patch_name using git format-patch"
git format-patch --stdout $tracking_branch > $patch_dir/$patch_name
