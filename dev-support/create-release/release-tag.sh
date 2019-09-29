#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Tags release. Updates releasenotes and changes.
SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

function exit_with_usage {
  local NAME=$(basename $0)
  cat << EOF
usage: $NAME
Tags an $PROJECT release on a particular branch.

Inputs are specified with the following environment variables:
ASF_USERNAME - Apache Username
ASF_PASSWORD - Apache Password
GIT_NAME - Name to use with git
GIT_EMAIL - E-mail address to use with git
GIT_BRANCH - Git branch on which to make release
RELEASE_VERSION - Version used in pom files for release
RELEASE_TAG - Name of release tag
NEXT_VERSION - Development version after release
EOF
  exit 1
}

set -e
set -o pipefail

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

for env in ASF_USERNAME ASF_PASSWORD RELEASE_VERSION RELEASE_TAG NEXT_VERSION GIT_EMAIL GIT_NAME GIT_BRANCH; do
  if [ -z "${!env}" ]; then
    echo "$env must be set to run this script"
    exit 1
  fi
done

init_java
init_mvn

rm -rf ${PROJECT}

ASF_REPO="gitbox.apache.org/repos/asf/${PROJECT}.git"
# Ugly!
encoded_username=$(python -c "import urllib; print urllib.quote('''$ASF_USERNAME''')")
encoded_password=$(python -c "import urllib; print urllib.quote('''$ASF_PASSWORD''')")
git clone "https://$encoded_username:$encoded_password@$ASF_REPO" -b $GIT_BRANCH
# NOTE: Here we are prepending project name on version for fetching
# changes from the HBASE JIRA. It has issues for hbase, hbase-conectors,
# hbase-operator-tools, etc.
shopt -s nocasematch
if [ "${PROJECT}" != "hbase" ]; then
  # Needs the '-' on the end.
  prefix="${PROJECT}-"
fi
shopt -u nocasematch
update_releasenotes `pwd`/${PROJECT} "${prefix}${RELEASE_VERSION}"

cd ${PROJECT}

git config user.name "$GIT_NAME"
git config user.email $GIT_EMAIL

# Create release version
$MVN versions:set -DnewVersion=$RELEASE_VERSION | grep -v "no value" # silence logs
git add RELEASENOTES.md CHANGES.md

git commit -a -m "Preparing ${PROJECT} release $RELEASE_TAG; tagging and updates to CHANGES.md and RELEASENOTES.md"
echo "Creating tag $RELEASE_TAG at the head of $GIT_BRANCH"
git tag $RELEASE_TAG

# Create next version
$MVN versions:set -DnewVersion=$NEXT_VERSION | grep -v "no value" # silence logs

git commit -a -m "Preparing development version $NEXT_VERSION"

if ! is_dry_run; then
  # Push changes
  git push origin $RELEASE_TAG
  git push origin HEAD:$GIT_BRANCH
  cd ..
  rm -rf ${PROJECT}
else
  cd ..
  mv ${PROJECT} ${PROJECT}.tag
  echo "Clone with version changes and tag available as ${PROJECT}.tag in the output directory."
fi

