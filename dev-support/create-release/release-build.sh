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
Usage: release-build.sh <build|publish-snapshot|publish-release>
Creates build deliverables from a tag/commit.
Arguments:
 build             Create binary packages and commit to dist.apache.org/repos/dist/dev/hbase/
 publish-snapshot  Publish snapshot release to Apache snapshots
 publish-release   Publish a release to Apache release repo

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
 $ PROJECT="hbase-operator-tools" ASF_USERNAME=NAME ASF_PASSWORD=PASSWORD GPG_PASSPHRASE=PASSWORD GPG_KEY=stack@apache.org ./release-build.sh build
EOF
  exit 1
}

set -e

function cleanup {
  rm ${tmp_settings} &> /dev/null || true
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
GIT_REF=${GIT_REF:-master}

RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/hbase"

GPG="gpg --pinentry-mode loopback -u $GPG_KEY --no-tty --batch"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=8e226b97c0c82 # Profile for project staging uploads via INFRA-17900 Need nexus "staging profile id" for the hbase project
BASE_DIR=$(pwd)

init_java
init_mvn
init_python
# Print out subset of perl version.
perl --version | grep 'This is'

rm -rf ${PROJECT}
ASF_REPO="${ASF_REPO:-https://gitbox.apache.org/repos/asf/${PROJECT}.git}"
git clone "$ASF_REPO"
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

# Profiles for publishing snapshots and release to Maven Central
PUBLISH_PROFILES="-Papache-release -Prelease"

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

DEST_DIR_NAME="$PACKAGE_VERSION"

git clean -d -f -x
cd ..

tmp_repo="${REPO:-`pwd`/$(mktemp -d hbase-repo-XXXXX)}"
# Reexamine. Not sure this working. Pass as arg? That don't seem to work either!
tmp_settings="/${tmp_repo}/tmp-settings.xml"
echo "<settings><servers>" > $tmp_settings
echo "<server><id>apache.snapshots.https</id><username>$ASF_USERNAME</username>" >> $tmp_settings
echo "<password>$ASF_PASSWORD</password></server>" >> $tmp_settings
echo "<server><id>apache-release</id><username>$ASF_USERNAME</username>" >> $tmp_settings
echo "<password>$ASF_PASSWORD</password></server>" >> $tmp_settings
echo "</servers>" >> $tmp_settings
echo "</settings>" >> $tmp_settings
export tmp_settings

if [[ "$1" == "build" ]]; then
  # Source and binary tarballs
  echo "Packaging release source tarballs"
  make_src_release "${PROJECT}" "${VERSION}"

  # Add timestamps to mvn logs.
  MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss ${MAVEN_OPTS}"

  echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Building binary dist"
  make_binary_release "${PROJECT}" "${VERSION}"
  echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Done building binary distribution"

  if ! is_dry_run; then
    svn co --depth=empty $RELEASE_STAGING_LOCATION svn-hbase
    rm -rf "svn-hbase/${DEST_DIR_NAME}"
    mkdir -p "svn-hbase/${DEST_DIR_NAME}"

    echo "Copying release tarballs"
    cp ${PROJECT}-*.tar.* "svn-hbase/${DEST_DIR_NAME}/"
    cp ${PROJECT}/CHANGES.md "svn-hbase/${DEST_DIR_NAME}/"
    cp ${PROJECT}/RELEASENOTES.md "svn-hbase/${DEST_DIR_NAME}/"
    shopt -s nocasematch
    # Generate api report only if project is hbase for now.
    if [ "${PROJECT}" == "hbase" ]; then
      # This script usually reports an errcode along w/ the report.
      generate_api_report ./${PROJECT} "${API_DIFF_TAG}" "${PACKAGE_VERSION}" || true
      cp api*.html "svn-hbase/${DEST_DIR_NAME}/"
    fi
    shopt -u nocasematch

    svn add "svn-hbase/${DEST_DIR_NAME}"

    cd svn-hbase
    svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Apache ${PROJECT} $PACKAGE_VERSION" --no-auth-cache
    cd ..
    rm -rf svn-hbase
  fi

  exit 0
fi

if [[ "$1" == "publish-snapshot" ]]; then
  cd "${PROJECT}"
  # Publish ${PROJECT} to Maven release repo
  echo "Deploying ${PROJECT} SNAPSHOT at '$GIT_REF' ($git_hash)"
  echo "Publish version is $VERSION"
  if [[ ! $VERSION == *"SNAPSHOT"* ]]; then
    echo "ERROR: Snapshots must have a version containing SNAPSHOT"
    echo "ERROR: You gave version '$VERSION'"
    exit 1
  fi
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$VERSION
  $MVN --settings $tmp_settings -DskipTests $PUBLISH_PROFILES deploy
  cd ..
  exit 0
fi

if [[ "$1" == "publish-release" ]]; then
  cd "${PROJECT}"
  # Get list of modules from parent pom but filter out 'assembly' modules.
  # Used below in a few places.
  modules=`sed -n 's/<module>\(.*\)<.*$/\1/p' pom.xml | grep -v '-assembly' |  tr '\n' ' '`
  # Need to add the 'parent' module too. Its the SECOND artifactId instance in pom
  artifactid=`sed -n 's/<artifactId>\(.*\)<.*$/\1/p' pom.xml | tr '\n' ' '| awk '{print $2}'`
  modules="${artifactid} ${modules}"
  # Get the second groupId in the pom. This is the groupId for these artifacts.
  groupid=`sed -n 's/<groupId>\(.*\)<.*$/\1/p' pom.xml | tr '\n' ' '| awk '{print $2}'`
  # Convert groupid to a dir path for use below reaching into repo for jars.
  groupid_as_dir=`echo $groupid | sed -n 's/\./\//gp'`
  echo "pwd=`pwd`, groupid_as_dir=${groupid_as_dir}"
  # Publish ${PROJECT} to Maven release repo
  echo "Publishing ${PROJECT} checkout at '$GIT_REF' ($git_hash)"
  echo "Publish version is $VERSION"
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$VERSION
  MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} --settings $tmp_settings \
    clean install -DskipTests \
    -Dcheckstyle.skip=true ${PUBLISH_PROFILES} \
    -Dmaven.repo.local="${tmp_repo}"
  pushd "${tmp_repo}/${groupid_as_dir}"
  # Remove any extra files generated during install
  # Remove extaneous files from module subdirs
  find $modules -type f | grep -v \.jar | grep -v \.pom | xargs rm

  # Using Nexus API documented here:
  # https://support.sonatype.com/entries/39720203-Uploading-to-a-Staging-Repository-via-REST-API
  if ! is_dry_run; then
    echo "Creating Nexus staging repository"
    repo_request="<promoteRequest><data><description>Apache ${PROJECT} $VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
    staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachehbase-[0-9]\{4\}\).*/\1/")
    echo "Created Nexus staging repository: $staged_repo_id"
  fi

  # this must have .asc, and .sha1 - it really doesn't like anything else there
  for file in $(find $modules -type f)
  do
    if [[ "$file" == *.asc ]]; then
      continue
    fi
    if [ ! -f $file.asc ]; then
      echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --output "$file.asc" \
        --detach-sig --armour $file;
    fi
    if [ $(command -v md5)  ]; then
      # Available on OS X; -q to keep only hash
      md5 -q "$file" > "$file.md5"
    else
      # Available on Linux; cut to keep only hash
      md5sum "$file" | cut -f1 -d' ' > "$file.md5"
    fi
    if [ $(command -v sha1sum)  ]; then
      sha1sum "$file" | cut -f1 -d' ' > "$file.sha1"
    else
      shasum "$file" | cut -f1 -d' ' > "$file.sha1"
    fi
  done

  if ! is_dry_run; then
    nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
    echo "Uplading files to $nexus_upload"
    for file in $(find ${modules} -type f)
    do
      # strip leading ./
      file_short=$(echo $file | sed -e "s/\.\///")
      dest_url="$nexus_upload/$groupid_as_dir/$file_short"
      echo "  Uploading $file to $dest_url"
      curl -u "$ASF_USERNAME:$ASF_PASSWORD" --upload-file "${file_short}" "${dest_url}"
    done

    echo "Closing nexus staging repository"
    repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache ${PROJECT} $VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
    echo "Closed Nexus staging repository: $staged_repo_id"
  fi

  popd
  rm -rf "$tmp_repo"
  cd ..
  # Dump out email to send. Where we find vote.tmpl depends
  # on where this script is run from
  export PROJECT_TEXT=$(echo "${PROJECT}" | sed "s/-/ /g")
  eval "echo \"$(< ${SELF}/vote.tmpl)\"" |tee vote.txt
  exit 0
fi

cd ..
rm -rf "${PROJECT}"
echo "ERROR: expects to be called with 'install', 'publish-release' or 'publish-snapshot'"
