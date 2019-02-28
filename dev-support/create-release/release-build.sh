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
set -x

trap cleanup EXIT

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

function exit_with_usage {
  cat << EOF
usage: release-build.sh <package|docs|publish-snapshot|publish-release>
Creates build deliverables from an HBase commit.

Top level targets are
  build: Create binary packages and commit them to dist.apache.org/repos/dist/dev/hbase/
  publish-snapshot: Publish snapshot release to Apache snapshots
  publish-release: Publish a release to Apache release repo

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
HBASE_PACKAGE_VERSION - Release identifier in top level package directory (e.g. 2.1.2RC1)
HBASE_VERSION - (optional) Version of HBase being built (e.g. 2.1.2)

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account

GPG_KEY - GPG key used to sign release artifacts
GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

set -e

function cleanup {
  rm ${tmp_settings} &> /dev/null
}

if [ $# -eq 0 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

if [[ -z "$GPG_PASSPHRASE" ]]; then
  echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
  echo 'unlock the GPG signing key that will be used to sign the release!'
  echo
  stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
  export GPG_PASSPHRASE
fi
export GPG_TTY=$(tty)

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

GPG="gpg -u $GPG_KEY --no-tty --batch"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=8e226b97c0c82 # Profile for HBase staging uploads via INFRA-17900 Need nexus "staging profile id" for the hbase project
BASE_DIR=$(pwd)

init_java
init_mvn
init_python

rm -rf hbase
git clone "$ASF_REPO"
cd hbase
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
echo "Checked out HBase git hash $git_hash"

if [ -z "$HBASE_VERSION" ]; then
  # Run $MVN in a separate command so that 'set -e' does the right thing.
  TMP=$(mktemp)
  $MVN help:evaluate -Dexpression=project.version > $TMP
  HBASE_VERSION=$(cat $TMP | grep -v INFO | grep -v WARNING | grep -v Download)
  rm $TMP
fi

# Depending on the version being built, certain extra profiles need to be activated.
BASE_PROFILES="-Papache-release -Prelease"

# Profiles for publishing snapshots and release to Maven Central
PUBLISH_PROFILES="$BASE_PROFILES"

if [[ ! $HBASE_VERSION < "2.0." ]]; then
  if [[ $JAVA_VERSION < "1.8." ]]; then
    echo "Java version $JAVA_VERSION is less than required 1.8 for 2.0+"
    echo "Please set JAVA_HOME correctly."
    exit 1
  fi
else
  if ! [[ $JAVA_VERSION =~ 1\.7\..* ]]; then
    if [ -z "$JAVA_7_HOME" ]; then
      echo "Java version $JAVA_VERSION is higher than required 1.7 for pre-2.0"
      echo "Please set JAVA_HOME correctly."
      exit 1
    else
      export JAVA_HOME="$JAVA_7_HOME"
    fi
  fi
fi

# This is a band-aid fix to avoid the failure of Maven nightly snapshot in some Jenkins
# machines by explicitly calling /usr/sbin/lsof. Please see SPARK-22377 and the discussion
# in its pull request.
LSOF=lsof
if ! hash $LSOF 2>/dev/null; then
  LSOF=/usr/sbin/lsof
fi

if [ -z "$HBASE_PACKAGE_VERSION" ]; then
  HBASE_PACKAGE_VERSION="${HBASE_VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

DEST_DIR_NAME="$HBASE_PACKAGE_VERSION"

git clean -d -f -x
cd ..
tmp_repo=`pwd`/$(mktemp -d hbase-repo-XXXXX)
# Reexamine. Not sure this working. Pass as arg? That don't seem to work either!
tmp_settings="/tmp/tmp-settings.xml"
echo "<settings><servers>" > $tmp_settings
echo "<server><id>apache.snapshots.https</id><username>$ASF_USERNAME</username>" >> $tmp_settings
echo "<password>$ASF_PASSWORD</password></server>" >> $tmp_settings
echo "<server><id>apache-release</id><username>$ASF_USERNAME</username>" >> $tmp_settings
echo "<password>$ASF_PASSWORD</password></server>" >> $tmp_settings
echo "</servers>" >> $tmp_settings
echo "<profiles><profile><id>apache-release</id><properties>" >> $tmp_settings
echo "<gpg.keyname>$GPG_KEY</gpg.keyname>" >> $tmp_settings
echo "<gpg.passphrase>$GPG_PASSPHRASE</gpg.passphrase>" >> $tmp_settings
echo "</properties></profile></profiles>" >> $tmp_settings
echo "</settings>" >> $tmp_settings
export tmp_settings

if [[ "$1" == "build" ]]; then
  # Source and binary tarballs
  echo "Packaging release source tarballs"
  # Tar up the src and sign and hash it.
  rm -rf hbase-$HBASE_VERSION-src*
  ls
  cd hbase
  git archive --format=tar.gz --output=../hbase-$HBASE_VERSION-src.tar.gz --prefix=hbase-"${HBASE_VERSION}/" "${GIT_REF}"
  cd ..
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour --output hbase-$HBASE_VERSION-src.tar.gz.asc \
    --detach-sig hbase-$HBASE_VERSION-src.tar.gz
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
    SHA512 hbase-$HBASE_VERSION-src.tar.gz > hbase-$HBASE_VERSION-src.tar.gz.sha512

  # Updated for each binary build; spark did many. HBase does one bin only.
  make_binary_release() {
    FLAGS="$MVN_EXTRA_OPTS -B $BASE_RELEASE_PROFILES $2"

    echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Building binary dist"
    cp -R hbase hbase-$HBASE_VERSION-bin
    cd hbase-$HBASE_VERSION-bin

    echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Creating distribution: ($FLAGS)"

    # Get maven home set by MVN
    MVN_HOME=`$MVN -version 2>&1 | grep 'Maven home' | awk '{print $NF}'`

    MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} clean install -DskipTests \
        -Dmaven.repo.local=${tmp_repo}
    echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Building all (site, etc., with release profiile)"
    MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} -Darguments="-Dgpg.passphrase=$GPG_PASSPHRASE" --settings $tmp_settings install -DskipTests \
        -Dcheckstyle.skip=true site assembly:single \
        -Papache-release -Prelease \
        -Dmaven.repo.local=${tmp_repo}

    echo "`date -u +'%Y-%m-%dT%H:%M:%SZ'` Copying and signing regular binary distribution"
    cp ./hbase-assembly/target/hbase-$HBASE_VERSION-bin.tar.gz ..
    cd ..
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output hbase-$HBASE_VERSION-bin.tar.gz.asc \
      --detach-sig hbase-$HBASE_VERSION-bin.tar.gz
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --print-md \
      SHA512 hbase-$HBASE_VERSION-bin.tar.gz > \
      hbase-$HBASE_VERSION-bin.tar.gz.sha512
  }

  rm -rf hbase-$HBASE_VERSION-bin*/
  make_binary_release

  if ! is_dry_run; then
    svn co --depth=empty $RELEASE_STAGING_LOCATION svn-hbase
    rm -rf "svn-hbase/${DEST_DIR_NAME}"
    mkdir -p "svn-hbase/${DEST_DIR_NAME}"

    echo "Copying release tarballs"
    cp hbase-*.tar.* "svn-hbase/${DEST_DIR_NAME}/"
    cp hbase-$HBASE_VERSION-bin/CHANGES.md "svn-hbase/${DEST_DIR_NAME}/"
    cp hbase-$HBASE_VERSION-bin/RELEASENOTES.md "svn-hbase/${DEST_DIR_NAME}/"
    generate_api_report ./hbase ${PREV_REL_TAG} ${RELEASE_TAG}
    cp api*.html "svn-hbase/${DEST_DIR_NAME}/"

    svn add "svn-hbase/${DEST_DIR_NAME}"

    cd svn-hbase
    svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Apache HBase $HBASE_PACKAGE_VERSION" --no-auth-cache
    cd ..
    rm -rf svn-hbase
  fi

  exit 0
fi

if [[ "$1" == "publish-snapshot" ]]; then
  cd hbase
  # Publish HBase to Maven release repo
  echo "Deploying HBase SNAPSHOT at '$GIT_REF' ($git_hash)"
  echo "Publish version is $HBASE_VERSION"
  if [[ ! $HBASE_VERSION == *"SNAPSHOT"* ]]; then
    echo "ERROR: Snapshots must have a version containing SNAPSHOT"
    echo "ERROR: You gave version '$HBASE_VERSION'"
    exit 1
  fi
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$HBASE_VERSION
  $MVN --settings $tmp_settings -DskipTests $PUBLISH_PROFILES deploy
  cd ..
  exit 0
fi

if [[ "$1" == "publish-release" ]]; then
  cd hbase
  # Publish HBase to Maven release repo
  echo "Publishing HBase checkout at '$GIT_REF' ($git_hash)"
  echo "Publish version is $HBASE_VERSION"
  # Coerce the requested version
  $MVN versions:set -DnewVersion=$HBASE_VERSION
  MAVEN_OPTS="${MAVEN_OPTS}" $MVN --settings $tmp_settings -Dmaven.repo.local=$tmp_repo \
      -DskipTests $PUBLISH_PROFILES clean install
  pushd $tmp_repo/org/apache/hbase
  # Remove any extra files generated during install
  # Do find in hbase* because thirdparty is at same level!
  find hbase* -type f | grep -v \.jar | grep -v \.pom | xargs rm

  # Using Nexus API documented here:
  # https://support.sonatype.com/entries/39720203-Uploading-to-a-Staging-Repository-via-REST-API
  if ! is_dry_run; then
    echo "Creating Nexus staging repository"
    repo_request="<promoteRequest><data><description>Apache HBase $HBASE_VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
    staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachehbase-[0-9]\{4\}\).*/\1/")
    echo "Created Nexus staging repository: $staged_repo_id"
  fi

  # this must have .asc, and .sha1 - it really doesn't like anything else there
  for file in $(find hbase* -type f)
  do
    if [[ "$file" == *.asc ]]; then
      continue
    fi
    if [ ! -f $file.asc ]; then
      echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc \
        --detach-sig --armour $file;
    fi
    if [ $(command -v md5)  ]; then
      # Available on OS X; -q to keep only hash
      md5 -q $file > $file.md5
    else
      # Available on Linux; cut to keep only hash
      md5sum $file | cut -f1 -d' ' > $file.md5
    fi
    sha1sum $file | cut -f1 -d' ' > $file.sha1
  done

  if ! is_dry_run; then
    nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
    echo "Uplading files to $nexus_upload"
    for file in $(find hbase* -type f)
    do
      # strip leading ./
      file_short=$(echo $file | sed -e "s/\.\///")
      dest_url="$nexus_upload/org/apache/hbase/$file_short"
      echo "  Uploading $file to $dest_url"
      curl -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $file_short $dest_url
    done

    echo "Closing nexus staging repository"
    repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache HBase $HBASE_VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
    echo "Closed Nexus staging repository: $staged_repo_id"
  fi

  popd
  rm -rf $tmp_repo
  cd ..
  # Dump out email to send.
  eval "echo \"$(<vote.tmpl)\"" |tee vote.txt
  exit 0
fi

cd ..
rm -rf hbase
echo "ERROR: expects to be called with 'install', 'publish-release' or 'publish-snapshot'"
