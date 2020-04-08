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
  cat << EOF
Usage: release-build.sh <publish-dist|publish-snapshot|publish-release>
Creates release deliverables from a tag or commit.
Arguments:
 publish-dist      Build and publish distribution packages (tarballs) to Apache dist repo
 publish-snapshot  Build and publish maven artifacts snapshot release to Apache snapshots repo
 publish-release   Build and publish maven artifacts release to Apache release repo

All other inputs are environment variables:
 GIT_REF - Release tag or commit to build from
 PACKAGE_VERSION - Release identifier in top level package directory (e.g. 2.1.2RC1)
 VERSION - (optional) Version of project being built (e.g. 2.1.2)
 ASF_USERNAME - Username of ASF committer account
 ASF_PASSWORD - Password of ASF committer account
 GPG_KEY - GPG key used to sign release artifacts
 GPG_PASSPHRASE - Passphrase for GPG key
 PROJECT - The project to build. No default.

Set REPO environment to full path to repo to use
to avoid re-downloading dependencies on each run.

For example:
 $ PROJECT="hbase-operator-tools" ASF_USERNAME=NAME ASF_PASSWORD=PASSWORD GPG_PASSPHRASE=PASSWORD GPG_KEY=stack@apache.org ./release-build.sh publish-dist
EOF
  exit 1
}

set -e

function cleanup {
  echo "Cleaning up temp settings file." >&2
  rm "${tmp_settings}" &> /dev/null || true
  # If REPO was set, then leave things be. Otherwise if we defined a repo clean it out.
  if [[ -z "${REPO}" ]] && [[ -n "${tmp_repo}" ]]; then
    echo "Cleaning up temp repo in '${tmp_repo}'. set REPO to reuse downloads." >&2
    rm -rf "${tmp_repo}" &> /dev/null || true
  fi
}

if [ $# -eq 0 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

# Read in the ASF password.
if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

# Read in the GPG passphrase
if [[ -z "$GPG_PASSPHRASE" ]]; then
  echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
  echo 'unlock the GPG signing key that will be used to sign the release!'
  echo
  stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
  export GPG_PASSPHRASE
  export GPG_TTY=$(tty)
fi

for env in ASF_USERNAME GPG_PASSPHRASE GPG_KEY; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Commit ref to checkout when building
BASE_DIR=$(pwd)
GIT_REF=${GIT_REF:-master}
if [[ "$PROJECT" =~ ^hbase ]]; then
  RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/hbase"
else
  RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/${PROJECT}"
fi

init_java
init_mvn
init_python
# Print out subset of perl version.
perl --version | grep 'This is'

rm -rf "${PROJECT}"
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

tmp_repo="${REPO:-`pwd`/$(mktemp -d hbase-repo-XXXXX)}"
tmp_settings="/${tmp_repo}/tmp-settings.xml"
echo "<settings><servers>" > "$tmp_settings"
echo "<server><id>apache.snapshots.https</id><username>$ASF_USERNAME</username>" >> "$tmp_settings"
echo "<password>$ASF_PASSWORD</password></server>" >> "$tmp_settings"
echo "<server><id>apache.releases.https</id><username>$ASF_USERNAME</username>" >> "$tmp_settings"
echo "<password>$ASF_PASSWORD</password></server>" >> "$tmp_settings"
echo "</servers>" >> "$tmp_settings"
echo "</settings>" >> "$tmp_settings"
export tmp_settings

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
  $MVN --settings $tmp_settings -DskipTests -P "${PUBLISH_PROFILES}" deploy
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
  if ! MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} --settings "$tmp_settings" \
      -DskipTests -Dcheckstyle.skip=true -P "${PUBLISH_PROFILES}" \
      -Dmaven.repo.local="${tmp_repo}" \
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
echo "ERROR: expects to be called with 'install', 'publish-release' or 'publish-snapshot'"
