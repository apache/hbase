#!/bin/bash
#
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

# This script is meant to run as part of a Jenkins job such as
# https://builds.apache.org/job/hbase_generate_website/
#
# It needs to be built on a Jenkins server with the label git-websites
#
# Allows specifying options for working directory, maven repo, and publishing to git
# run with --help for usage.
#
# If there is a build error, the Jenkins job is configured to send an email

declare CURRENT_HBASE_COMMIT
declare PUSHED
declare FILE
declare WEBSITE_COMMIT_MSG
declare -a FILES_TO_REMOVE

set -e
function usage {
  echo "Usage: ${0} [options] /path/to/hbase/checkout"
  echo ""
  echo "    --working-dir /path/to/use  Path for writing logs and a local checkout of hbase-site repo."
  echo "                                if given must exist."
  echo "                                defaults to making a directory via mktemp."
  echo "    --local-repo /path/for/maven/.m2  Path for putting local maven repo."
  echo "                                if given must exist."
  echo "                                defaults to making a clean directory in --working-dir."
  echo "    --publish                   if given, will attempt to push results back to the hbase-site repo."
  echo "    --help                      show this usage message."
  exit 1
}
# if no args specified, show usage
if [ $# -lt 1 ]; then
  usage
fi

# Get arguments
declare component_dir
declare working_dir
declare local_repo
declare publish
while [ $# -gt 0 ]
do
  case "$1" in
    --working-dir) shift; working_dir=$1; shift;;
    --local-repo) shift; local_repo=$1; shift;;
    --publish) shift; publish="true";;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

# should still have where component checkout is.
if [ $# -lt 1 ]; then
  usage
fi
component_dir="$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"

if [ -z "${working_dir}" ]; then
  echo "[DEBUG] defaulting to creating a directory via mktemp"
  if ! working_dir="$(mktemp -d -t hbase-generate-website)" ; then
    echo "Failed to create temporary working directory. Please specify via --working-dir"
    exit 1
  fi
else
  # absolutes please
  working_dir="$(cd "$(dirname "${working_dir}")"; pwd)/$(basename "${working_dir}")"
  if [ ! -d "${working_dir}" ]; then
    echo "passed working directory '${working_dir}' must already exist."
    exit 1
  fi
fi

echo "You'll find logs and temp files in ${working_dir}"

if [ -z "${local_repo}" ]; then
  echo "[DEBUG] defaulting to creating a local repo within '${working_dir}'"
  local_repo="${working_dir}/.m2/repo"
  # Nuke the local maven repo each time, to start with a known environment
  rm -Rf "${local_repo}"
  mkdir -p "${local_repo}"
else
  # absolutes please
  local_repo="$(cd "$(dirname "${local_repo}")"; pwd)/$(basename "${local_repo}")"
  if [ ! -d "${local_repo}" ]; then
    echo "passed directory for storing the maven repo '${local_repo}' must already exist."
    exit 1
  fi
fi

# Set up the environment
if [ -z "${JAVA_HOME}" ]; then
  JAVA_HOME="${JDK_1_8_LATEST__HOME}"
  export JAVA_HOME
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi
if [ -z "${MAVEN_HOME}" ]; then
  MAVEN_HOME="${MAVEN_3_3_3_HOME}"
  export MAVEN_HOME
  export PATH="${MAVEN_HOME}/bin:${PATH}"
fi
export MAVEN_OPTS="${MAVEN_OPTS} -Dmaven.repo.local=${local_repo}"

# Verify the Maven version
mvn -version
# Verify the git version
git --version

cd "${working_dir}"

# Clean any leftover files in case we are reusing the workspace
rm -Rf -- *.patch *.patch.zip target *.txt hbase-site

# Save and print the SHA we are building
CURRENT_HBASE_COMMIT="$(cd "${component_dir}" && git show-ref --hash --dereference --verify refs/remotes/origin/HEAD)"
# Fail if it's empty
if [ -z "${CURRENT_HBASE_COMMIT}" ]; then
  echo "Got back a blank answer for the current HEAD on the remote hbase repository. failing."
  exit 1
fi
echo "Current HBase commit: $CURRENT_HBASE_COMMIT"

# Clone the hbase-site repo manually so it doesn't trigger spurious
# commits in Jenkins.
git clone --depth 1 --branch asf-site https://gitbox.apache.org/repos/asf/hbase-site.git

# Figure out if the commit of the hbase repo has already been built and bail if so.
declare -i PUSHED
PUSHED=$(cd hbase-site && git rev-list --grep "${CURRENT_HBASE_COMMIT}" --fixed-strings --count HEAD)
echo "[DEBUG] hash was found in $PUSHED commits for hbase-site repository."

if [ "${PUSHED}" -ne 0 ]; then
  echo "$CURRENT_HBASE_COMMIT is already mentioned in the hbase-site commit log. Not building."
  exit 0
else
  echo "$CURRENT_HBASE_COMMIT is not yet mentioned in the hbase-site commit log. Assuming we don't have it yet."
fi

# Go to the hbase directory so we can build the site
cd "${component_dir}"

# This will only be set for builds that are triggered by SCM change, not manual builds
if [ -n "$CHANGE_ID" ]; then
  echo -n " ($CHANGE_ID - $CHANGE_TITLE)"
fi

# Build and install HBase, then build the site
echo "Building HBase"
# TODO we have to do a local install first because for whatever reason, the maven-javadoc-plugin's
# forked compile phase requires that test-scoped dependencies be available, which
# doesn't work since we will not have done a test-compile phase (MJAVADOC-490). the first place this
# breaks for me is hbase-server trying to find hbase-http:test and hbase-zookeeper:test.
# But! some sunshine: because we're doing a full install before running site, we can skip all the
# compiling in the forked executions. We have to do it awkwardly because MJAVADOC-444.
if mvn \
    --batch-mode \
    -Psite-install-step \
    --log-file="${working_dir}/hbase-install-log-${CURRENT_HBASE_COMMIT}.txt" \
    clean install \
  && mvn site \
    --batch-mode \
    -Psite-build-step \
    --log-file="${working_dir}/hbase-site-log-${CURRENT_HBASE_COMMIT}.txt"; then
  echo "Successfully built site."
else
  status=$?
  echo "Maven commands to build the site failed. check logs for details ${working_dir}/hbase-*-log-*.txt"
  exit $status
fi

# Stage the site
echo "Staging HBase site"
mvn \
  --batch-mode \
  --log-file="${working_dir}/hbase-stage-log-${CURRENT_HBASE_COMMIT}.txt" \
  site:stage
status=$?
if [ $status -ne 0 ] || [ ! -d target/staging ]; then
  echo "Failure: mvn site:stage"
  exit $status
fi

# Get ready to update the hbase-site repo with the new artifacts
cd "${working_dir}/hbase-site"

#Remove previously-generated files
FILES_TO_REMOVE=("hbase-*"
                 "apidocs"
                 "devapidocs"
                 "testapidocs"
                 "testdevapidocs"
                 "xref"
                 "xref-test"
                 "*book*"
                 "*.html"
                 "*.pdf*"
                 "css"
                 "js"
                 "images")

for FILE in "${FILES_TO_REMOVE[@]}"; do
  if [ -e "${FILE}" ]; then
    echo "Removing hbase-site/$FILE"
    rm -Rf "${FILE}"
  fi
done

# Copy in the newly-built artifacts
# TODO what do we do when the site build wants to remove something? Can't rsync because e.g. release-specific docs.
cp -pPR "${component_dir}"/target/staging/* .

# If the index.html is missing, bail because this is serious
if [ ! -f index.html ]; then
  echo "The index.html is missing. Aborting."
  exit 1
fi

echo "Adding all the files we know about"
git add .
# Create the commit message and commit the changes
WEBSITE_COMMIT_MSG="Published site at $CURRENT_HBASE_COMMIT."
echo "WEBSITE_COMMIT_MSG: $WEBSITE_COMMIT_MSG"
git commit -m "${WEBSITE_COMMIT_MSG}" -a
# Dump a little report
echo "This commit changed these files (excluding Modified files):"
git diff --name-status --diff-filter=ADCRTXUB origin/asf-site | tee "${working_dir}/hbase-file-diff-summary-${CURRENT_HBASE_COMMIT}.txt"
# Create a patch, which Jenkins can save as an artifact and can be examined for debugging
git format-patch --stdout origin/asf-site > "${working_dir}/${CURRENT_HBASE_COMMIT}.patch"
if [ ! -s "${working_dir}/${CURRENT_HBASE_COMMIT}.patch" ]; then
  echo "Something went wrong when creating the patch of our updated site."
  exit 1
fi
echo "Change set saved to patch ${working_dir}/${CURRENT_HBASE_COMMIT}.patch"

if [ -n "${publish}" ]; then
  echo "Publishing changes to remote repo..."
  if git push origin asf-site; then
    echo "changes pushed."
  else
    echo "Failed to push to asf-site. Website not updated."
    exit 1
  fi
  echo "Sending empty commit to work around INFRA-10751."
  git commit --allow-empty -m "INFRA-10751 Empty commit"
  # Push the empty commit
  if git push origin asf-site; then
    echo "empty commit pushed."
  else
    echo "Failed to push the empty commit to asf-site. Website may not update. Manually push an empty commit to fix this. (See INFRA-10751)"
    exit 1
  fi
  echo "Pushed the changes to branch asf-site. Refresh http://hbase.apache.org/ to see the changes within a few minutes."
fi

# Zip up the patch so Jenkins can save it
cd "${working_dir}"
zip website.patch.zip "${CURRENT_HBASE_COMMIT}.patch"
