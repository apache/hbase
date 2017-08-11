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
# It expects to have the hbase repo cloned to the directory hbase
#
# If there is a build error, the Jenkins job is configured to send an email

LOCAL_REPO=${WORKSPACE}/.m2/repo
# Nuke the local maven repo each time, to start with a known environment
rm -Rf "${LOCAL_REPO}"
mkdir -p "${LOCAL_REPO}"

# Clean any leftover files in case we are reusing the workspace
rm -Rf -- *.patch *.patch.zip hbase/target target *.txt hbase-site

# Set up the environment
export JAVA_HOME=$JDK_1_8_LATEST__HOME
export PATH=$JAVA_HOME/bin:$MAVEN_3_3_3_HOME/bin:$PATH
export MAVEN_OPTS="-XX:MaxPermSize=256m -Dmaven.repo.local=${LOCAL_REPO}"

# Verify the Maven version
mvn -version

# Save and print the SHA we are building
CURRENT_HBASE_COMMIT="$(git log --pretty=format:%H -n1)"
echo "Current HBase commit: $CURRENT_HBASE_COMMIT"

# Clone the hbase-site repo manually so it doesn't trigger spurious
# commits in Jenkins.
git clone --depth 1 --branch asf-site https://git-wip-us.apache.org/repos/asf/hbase-site.git

# Figure out the last commit we built the site from, and bail if the build
# still represents the SHA of HBase master
cd "${WORKSPACE}/hbase-site" || exit -1
git log --pretty=%s | grep ${CURRENT_HBASE_COMMIT}
PUSHED=$?
echo "PUSHED is $PUSHED"

if [ $PUSHED -eq 0 ]; then
  echo "$CURRENT_HBASE_COMMIT is already mentioned in the hbase-site commit log. Not building."
  exit 0
else
  echo "$CURRENT_HBASE_COMMIT is not yet mentioned in the hbase-site commit log. Assuming we don't have it yet. $PUSHED"
fi

# Go to the hbase directory so we can build the site
cd "${WORKSPACE}/hbase" || exit -1

# This will only be set for builds that are triggered by SCM change, not manual builds
if [ "$CHANGE_ID" ]; then
  echo -n " ($CHANGE_ID - $CHANGE_TITLE)"
fi

# Build and install HBase, then build the site
echo "Building HBase"
mvn \
  -DskipTests \
  -Dmaven.javadoc.skip=true \
  --batch-mode \
  -Dcheckstyle.skip=true \
  -Dfindbugs.skip=true \
  --log-file="${WORKSPACE}/hbase-build-log-${CURRENT_HBASE_COMMIT}.txt" \
  clean install \
&& mvn clean site \
  --batch-mode \
  -DskipTests \
  --log-file="${WORKSPACE}/hbase-install-log-${CURRENT_HBASE_COMMIT}.txt"

status=$?
if [ $status -ne 0 ]; then
  echo "Failure: mvn clean site"
  exit $status
fi

# Stage the site
echo "Staging HBase site"
mvn \
  --batch-mode \
  --log-file="${WORKSPACE}/hbase-stage-log-${CURRENT_HBASE_COMMIT}.txt" \
  site:stage
status=$?
if [ $status -ne 0 ] || [ ! -d target/staging ]; then
  echo "Failure: mvn site:stage"
  exit $status
fi

# Get ready to update the hbase-site repo with the new artifacts
cd "${WORKSPACE}/hbase-site" || exit -1

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
  echo "Removing ${WORKSPACE}/hbase-site/$FILE"
  rm -Rf "${FILE}"
done

# Copy in the newly-built artifacts
cp -au "${WORKSPACE}"/hbase/target/staging/* .

# If the index.html is missing, bail because this is serious
if [ ! -f index.html ]; then
  echo "The index.html is missing. Aborting."
  exit 1
else
  # Add all the changes
  echo "Adding all the files we know about"
  git add .
  # Create the commit message and commit the changes
  WEBSITE_COMMIT_MSG="Published site at $CURRENT_HBASE_COMMIT."
  echo "WEBSITE_COMMIT_MSG: $WEBSITE_COMMIT_MSG"
  git commit -m "${WEBSITE_COMMIT_MSG}" -a
  # Dump a little report
  echo "This commit changed these files (excluding Modified files):"
  git diff --name-status --diff-filter=ADCRTXUB origin/asf-site
  # Create a patch, which Jenkins can save as an artifact and can be examined for debugging
  git format-patch --stdout origin/asf-site > "${WORKSPACE}/${CURRENT_HBASE_COMMIT}.patch"
  echo "Change set saved to patch ${WORKSPACE}/${CURRENT_HBASE_COMMIT}.patch"
  # Push the real commit
  git push origin asf-site || (echo "Failed to push to asf-site. Website not updated." && exit -1)
  # Create an empty commit to work around INFRA-10751
  git commit --allow-empty -m "INFRA-10751 Empty commit"
  # Push the empty commit
  git push origin asf-site || (echo "Failed to push the empty commit to asf-site. Website may not update. Manually push an empty commit to fix this. (See INFRA-10751)" && exit -1)
  echo "Pushed the changes to branch asf-site. Refresh http://hbase.apache.org/ to see the changes within a few minutes."
  git fetch origin
  git reset --hard origin/asf-site

  # Zip up the patch so Jenkins can save it
  cd "${WORKSPACE}" || exit -1
  zip website.patch.zip "${CURRENT_HBASE_COMMIT}.patch"
fi

#echo "Dumping current environment:"
#env

