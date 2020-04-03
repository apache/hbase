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

trap cleanup EXIT

# Source in utils.
SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

# Print usage and exit.
function exit_with_usage {
  cat <<'EOF'
Usage: release-build.sh <tag|publish-dist|publish-snapshot|publish-release>
Creates release deliverables from a tag or commit.
Arguments:
 tag               Prepares for release on specified git branch: Set release version, create release tag,
                   increment version for ongoing dev, and publish to Apache git repo
 publish-dist      Build and publish distribution packages (tarballs) to Apache dist repo
 publish-snapshot  Build and publish maven artifacts snapshot release to Apache snapshots repo
 publish-release   Build and publish maven artifacts release to Apache release repo

All other inputs are environment variables:
Used for 'tag' and 'publish':
  PROJECT - The project to build. No default.
  ASF_USERNAME - Username of ASF committer account
  ASF_PASSWORD - Password of ASF committer account

Used only for 'tag':
  GIT_NAME - Name to use with git
  GIT_EMAIL - E-mail address to use with git
  GIT_BRANCH - Git branch on which to make release
  RELEASE_VERSION - Version used in pom files for release
  RELEASE_TAG - Name of release tag
  NEXT_VERSION - Development version after release

Used only for 'publish':
  GIT_REF - Release tag or commit to build from
  VERSION - Version of project to be built (e.g. 2.1.2).
    Optional for 'publish', as it defaults to the version in pom at GIT_REF.
  PACKAGE_VERSION - Release identifier in top level dist directory (e.g. 2.1.2RC1)
  GPG_KEY - GPG key id (usually email addr) used to sign release artifacts
  GPG_PASSPHRASE - Passphrase for GPG key
  REPO - Set to full path of a directory to use as maven local repo (dependencies cache)
    to avoid re-downloading dependencies for each stage.

For example:
 $ PROJECT="hbase-operator-tools" ASF_USERNAME=NAME ASF_PASSWORD=PASSWORD GPG_PASSPHRASE=PASSWORD GPG_KEY=stack@apache.org ./release-build.sh publish-dist
EOF
  exit 1
}

set -e

function cleanup {
  echo "Cleaning up temp settings file." >&2
  rm -f "${MAVEN_SETTINGS_FILE}" &> /dev/null || true
  # If REPO was set, then leave things be. Otherwise if we defined a repo clean it out.
  if [[ -z "${REPO}" ]] && [[ -n "${MAVEN_LOCAL_REPO}" ]]; then
    echo "Cleaning up temp repo in '${MAVEN_LOCAL_REPO}'. set REPO to reuse downloads." >&2
    rm -rf "${MAVEN_LOCAL_REPO}" &> /dev/null || true
  fi
}

if [ $# -ne 1 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
export GPG_TTY=$(tty)

init_java
init_mvn
init_python
# Print out subset of perl version (used in git hooks)
perl --version | grep 'This is'

rm -rf "${PROJECT}"

if [[ "$1" == "tag" ]]; then
  # for 'tag' stage
  set -o pipefail
  check_get_passwords ASF_PASSWORD
  check_needed_vars PROJECT ASF_USERNAME ASF_PASSWORD RELEASE_VERSION RELEASE_TAG NEXT_VERSION \
      GIT_EMAIL GIT_NAME GIT_BRANCH
  ASF_REPO="gitbox.apache.org/repos/asf/${PROJECT}.git"
  encoded_username=$(python -c "import urllib; print urllib.quote('''$ASF_USERNAME''')")
  encoded_password=$(python -c "import urllib; print urllib.quote('''$ASF_PASSWORD''')")
  git clone "https://$encoded_username:$encoded_password@$ASF_REPO" -b $GIT_BRANCH

  # 'update_releasenotes' searches the project's Jira for issues where 'Fix Version' matches specified
  # $jira_fix_version. For most projects this is same as ${RELEASE_VERSION}. However, all the 'hbase-*'
  # projects share the same HBASE jira name.  To make this work, by convention, the HBASE jira "Fix Version"
  # field values have the sub-project name pre-pended, as in "hbase-operator-tools-1.0.0".
  # So, here we prepend the project name to the version, but only for the hbase sub-projects.
  jira_fix_version="${RELEASE_VERSION}"
  shopt -s nocasematch
  if [[ "${PROJECT}" =~ ^hbase- ]]; then
    jira_fix_version="${PROJECT}-${RELEASE_VERSION}"
  fi
  shopt -u nocasematch
  update_releasenotes `pwd`/${PROJECT} "${jira_fix_version}"

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
  exit 0
fi

### Below is for 'publish-*' stages ###

check_get_passwords ASF_PASSWORD GPG_PASSPHRASE
check_needed_vars PROJECT ASF_USERNAME ASF_PASSWORD GPG_KEY GPG_PASSPHRASE

# Commit ref to checkout when building
BASE_DIR=$(pwd)
GIT_REF=${GIT_REF:-master}
if [[ "$PROJECT" =~ ^hbase ]]; then
  RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/hbase"
else
  RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/${PROJECT}"
fi

# in case of dry run, enable publish steps to chain from tag step
if is_dry_run && [[ "${TAG_SAME_DRY_RUN:-}" == "true" && -d "${PROJECT}.tag" ]]; then
  ln -s "${PROJECT}.tag" "${PROJECT}"
else
  ASF_REPO="${ASF_REPO:-https://gitbox.apache.org/repos/asf/${PROJECT}.git}"
  git clone "$ASF_REPO" "${PROJECT}"
fi
cd ${PROJECT}
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
echo "Checked out ${PROJECT} git hash $git_hash"

if [ -z "$VERSION" ]; then
  # Run $MVN in a separate command so that 'set -e' does the right thing.
  TMP=$(mktemp)
  $MVN help:evaluate -Dexpression=project.version > $TMP
  VERSION=$(cat $TMP | grep -v INFO | grep -v WARNING | grep -v Download)
  rm $TMP
fi

# This is a band-aid fix to avoid the failure of Maven nightly snapshot in some Jenkins
# machines by explicitly calling /usr/sbin/lsof. Please see SPARK-22377 and the discussion
# in its pull request.
LSOF=lsof
if ! hash $LSOF 2>/dev/null; then
  LSOF=/usr/sbin/lsof
fi

if [ -z "$PACKAGE_VERSION" ]; then
  PACKAGE_VERSION="${VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

git clean -d -f -x
cd ..

MAVEN_LOCAL_REPO="${REPO:-$(pwd)/$(mktemp -d hbase-repo-XXXXX)}"
MAVEN_SETTINGS_FILE="/${MAVEN_LOCAL_REPO}/tmp-settings.xml"
export MAVEN_SETTINGS_FILE MAVEN_LOCAL_REPO ASF_USERNAME ASF_PASSWORD
# reference passwords from env rather than storing in the settings.xml file.
cat <<'EOF' > "$MAVEN_SETTINGS_FILE"
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <localRepository>/${env.MAVEN_LOCAL_REPO}</localRepository>
  <servers>
    <server><id>apache.snapshots.https</id><username>${env.ASF_USERNAME}</username>
      <password>${env.ASF_PASSWORD}</password></server>
    <server><id>apache.releases.https</id><username>${env.ASF_USERNAME}</username>
      <password>${env.ASF_PASSWORD}</password></server>
  </servers>
</settings>
EOF

if [[ "$1" == "publish-dist" ]]; then
  # Source and binary tarballs
  echo "Packaging release source tarballs"
  make_src_release "${PROJECT}" "${VERSION}"

  # Add timestamps to mvn logs.
  MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss ${MAVEN_OPTS}"

  echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Building binary dist"
  make_binary_release "${PROJECT}" "${VERSION}"
  echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Done building binary distribution"

  if [[ "$PROJECT" =~ ^hbase- ]]; then
    DEST_DIR_NAME="${PROJECT}-${PACKAGE_VERSION}"
  else
    DEST_DIR_NAME="$PACKAGE_VERSION"
  fi
  svn_target="svn-${PROJECT}"
  svn co --depth=empty "$RELEASE_STAGING_LOCATION" "$svn_target"
  rm -rf "$svn_target/${DEST_DIR_NAME}"
  mkdir -p "$svn_target/${DEST_DIR_NAME}"

  echo "Copying release tarballs"
  cp ${PROJECT}-*.tar.* "$svn_target/${DEST_DIR_NAME}/"
  cp ${PROJECT}/CHANGES.md "$svn_target/${DEST_DIR_NAME}/"
  cp ${PROJECT}/RELEASENOTES.md "$svn_target/${DEST_DIR_NAME}/"
  shopt -s nocasematch
  # Generate api report only if project is hbase for now.
  if [ "${PROJECT}" == "hbase" ]; then
    # This script usually reports an errcode along w/ the report.
    generate_api_report ./${PROJECT} "${API_DIFF_TAG}" "${PACKAGE_VERSION}" || true
    cp api*.html "$svn_target/${DEST_DIR_NAME}/"
  fi
  shopt -u nocasematch

  svn add "$svn_target/${DEST_DIR_NAME}"

  if ! is_dry_run; then
    cd "$svn_target"
    svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Apache ${PROJECT} $PACKAGE_VERSION" --no-auth-cache
    cd ..
    rm -rf "$svn_target"
  else
    mv "$svn_target/${DEST_DIR_NAME}" "${svn_target}_${DEST_DIR_NAME}.dist"
    echo "svn-managed 'dist' directory with release tarballs, CHANGES.md and RELEASENOTES.md available as $(pwd)/${svn_target}_${DEST_DIR_NAME}.dist"
    rm -rf "$svn_target"
  fi

  exit 0
fi

if [[ "$1" == "publish-snapshot" ]]; then
  cd "${PROJECT}"
  # Publish ${PROJECT} to Maven snapshot repo
  echo "Deploying ${PROJECT} SNAPSHOT at '$GIT_REF' ($git_hash)"
  echo "Publish version is $VERSION"
  if [[ ! $VERSION == *"SNAPSHOT"* ]]; then
    echo "ERROR: Snapshots must have a version containing SNAPSHOT"
    echo "ERROR: You gave version '$VERSION'"
    exit 1
  fi
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$VERSION
  $MVN --settings $MAVEN_SETTINGS_FILE -DskipTests -P "${PUBLISH_PROFILES}" deploy
  cd ..
  exit 0
fi

if [[ "$1" == "publish-release" ]]; then
  (
  cd "${PROJECT}"
  # Publish ${PROJECT} to Maven release repo
  echo "Publishing ${PROJECT} checkout at '$GIT_REF' ($git_hash)"
  echo "Publish version is $VERSION"
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$VERSION
  declare -a mvn_goals=(clean install)
  declare staged_repo_id="dryrun-no-repo"
  if ! is_dry_run; then
    mvn_goals=("${mvn_goals[@]}" deploy)
  fi
  echo "Staging release in nexus"
  if ! MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} --settings "$MAVEN_SETTINGS_FILE" \
      -DskipTests -Dcheckstyle.skip=true -P "${PUBLISH_PROFILES}" \
      "${mvn_goals[@]}" > "${BASE_DIR}/mvn_deploy.log"; then
    echo "Staging build failed, see 'mvn_deploy.log' for details." >&2
    exit 1
  fi
  if ! is_dry_run; then
    staged_repo_id=$(grep -o "Closing staging repository with ID .*" "${BASE_DIR}/mvn_deploy.log" \
        | sed -e 's/Closing staging repository with ID "\([^"]*\)"./\1/')
    echo "Artifacts successfully staged to repo ${staged_repo_id}"
  else
    echo "Artifacts successfully built. not staged due to dry run."
  fi
  # Dump out email to send. Where we find vote.tmpl depends
  # on where this script is run from
  export PROJECT_TEXT=$(echo "${PROJECT}" | sed "s/-/ /g")
  eval "echo \"$(< ${SELF}/vote.tmpl)\"" |tee "${BASE_DIR}/vote.txt"
  )
  exit 0
fi

cd ..
rm -rf "${PROJECT}"
echo "ERROR: expects to be called with 'tag', 'publish-dist', 'publish-release', or 'publish-snapshot'" >&2
exit_with_usage
