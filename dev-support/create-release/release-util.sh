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
DRY_RUN=${DRY_RUN:-0}
GPG="gpg --pinentry-mode loopback --no-tty --batch"
YETUS_VERSION=0.9.0

function error {
  echo "$*"
  exit 1
}

function read_config {
  local PROMPT="$1"
  local DEFAULT="$2"
  local REPLY=

  read -p "$PROMPT [$DEFAULT]: " REPLY
  local RETVAL="${REPLY:-$DEFAULT}"
  if [ -z "$RETVAL" ]; then
    error "$PROMPT is must be provided."
  fi
  echo "$RETVAL"
}

function parse_version {
  grep -e '<version>.*</version>' | \
    head -n 2 | tail -n 1 | cut -d'>' -f2 | cut -d '<' -f1
}

function run_silent {
  local BANNER="$1"
  local LOG_FILE="$2"
  shift 2

  echo "========================"
  echo "= $BANNER"
  echo "Command: $*"
  echo "Log file: $LOG_FILE"

  "$@" 1>"$LOG_FILE" 2>&1

  local EC=$?
  if [ $EC != 0 ]; then
    echo "Command FAILED. Check full logs for details."
    tail "$LOG_FILE"
    exit $EC
  fi
}

function fcreate_secure {
  local FPATH="$1"
  rm -f "$FPATH"
  touch "$FPATH"
  chmod 600 "$FPATH"
}

function check_for_tag {
  curl -s --head --fail "$ASF_GITHUB_REPO/releases/tag/$1" > /dev/null
}

# API compare version.
function get_api_diff_version {
  local version=$1
  local rev=$(echo "$version" | cut -d . -f 3)
  local api_diff_tag
  if [ $rev != 0 ]; then
    local short_version=$(echo "$version" | cut -d . -f 1-2)
    api_diff_tag="rel/${short_version}.$((rev - 1))"
  else
    local major=$(echo "$version" | cut -d . -f 1)
    local minor=$(echo "$version" | cut -d . -f 2)
    if [ $minor != 0 ]; then
      api_diff_tag="rel/${major}.$((minor - 1)).0)"
    else
      api_diff_tag="rel/$((major - 1)).0.0)"
    fi
  fi
  api_diff_tag=$(read_config "api_diff_tag", "$api_diff_tag")
  echo $api_diff_tag
}

# Get all branches that begin with 'branch-', the hbase convention for
# release branches, sort them and then pop off the most recent.
function get_release_info {
  export PROJECT=$(read_config "PROJECT" "$PROJECT")

  if [[ -z "${ASF_REPO}" ]]; then
    ASF_REPO="https://gitbox.apache.org/repos/asf/${PROJECT}.git"
  fi
  if [[ -z "${ASF_REPO_WEBUI}" ]]; then
    ASF_REPO_WEBUI="https://gitbox.apache.org/repos/asf?p=${PROJECT}.git"
  fi
  if [[ -z "${ASF_GITHUB_REPO}" ]]; then
    ASF_GITHUB_REPO="https://github.com/apache/${PROJECT}"
  fi
  if [ -z "$GIT_BRANCH" ]; then
    # If no branch is specified, find out the latest branch from the repo.
    GIT_BRANCH=$(git ls-remote --heads "$ASF_REPO" |
      grep refs/heads/branch- |
      awk '{print $2}' |
      sort -r |
      head -n 1 |
      cut -d/ -f3)
  fi

  export GIT_BRANCH=$(read_config "GIT_BRANCH" "$GIT_BRANCH")

  # Find the current version for the branch.
  local VERSION=$(curl -s "$ASF_REPO_WEBUI;a=blob_plain;f=pom.xml;hb=refs/heads/$GIT_BRANCH" |
    parse_version)
  echo "Current branch VERSION is $VERSION."

  NEXT_VERSION="$VERSION"
  RELEASE_VERSION=""
  SHORT_VERSION=$(echo "$VERSION" | cut -d . -f 1-2)
   if [[ ! $VERSION =~ .*-SNAPSHOT ]]; then
    RELEASE_VERSION="$VERSION"
  else
    RELEASE_VERSION="${VERSION/-SNAPSHOT/}"
  fi

  local REV=$(echo "$VERSION" | cut -d . -f 3)

  # Find out what RC is being prepared.
  # - If the current version is "x.y.0", then this is RC0 of the "x.y.0" release.
  # - If not, need to check whether the previous version has been already released or not.
  #   - If it has, then we're building RC0 of the current version.
  #   - If it has not, we're building the next RC of the previous version.
  local RC_COUNT
  if [ $REV != 0 ]; then
    local PREV_REL_REV=$((REV - 1))
    PREV_REL_TAG="rel/${SHORT_VERSION}.${PREV_REL_REV}"
    if check_for_tag "$PREV_REL_TAG"; then
      RC_COUNT=0
      REV=$((REV + 1))
      NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    else
      RELEASE_VERSION="${SHORT_VERSION}.${PREV_REL_REV}"
      RC_COUNT=$(git ls-remote --tags "$ASF_REPO" "${RELEASE_VERSION}RC*" | wc -l)
      # This makes a 'number' of it.
      RC_COUNT=$((RC_COUNT))
    fi
  else
    REV=$((REV + 1))
    NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    RC_COUNT=0
  fi

  export RELEASE_VERSION=$(read_config "RELEASE_VERSION" "$RELEASE_VERSION")
  export NEXT_VERSION=$(read_config "NEXT_VERSION" "$NEXT_VERSION")


  RC_COUNT=$(read_config "RC_COUNT" "$RC_COUNT")

  # Check if the RC already exists, and if re-creating the RC, skip tag creation.
  RELEASE_TAG="${RELEASE_VERSION}RC${RC_COUNT}"
  SKIP_TAG=0
  if check_for_tag "$RELEASE_TAG"; then
    read -p "$RELEASE_TAG already exists. Continue anyway [y/n]? " ANSWER
    if [ "$ANSWER" != "y" ]; then
      error "Exiting."
    fi
    SKIP_TAG=1
  fi

  export RELEASE_TAG

  GIT_REF="$RELEASE_TAG"
  if is_dry_run; then
    echo "This is a dry run. Please confirm the ref that will be built for testing."
    GIT_REF=$(read_config "GIT_REF" "$GIT_REF")
  fi
  export GIT_REF
  export PACKAGE_VERSION="$RELEASE_TAG"

  export API_DIFF_TAG=$(get_api_diff_version $RELEASE_VERSION)

  # Gather some user information.
  export ASF_USERNAME=$(read_config "ASF_USERNAME" "$LOGNAME")

  GIT_NAME=$(git config user.name || echo "")
  export GIT_NAME=$(read_config "GIT_NAME" "$GIT_NAME")

  export GIT_EMAIL="$ASF_USERNAME@apache.org"
  export GPG_KEY=$(read_config "GPG_KEY" "$GIT_EMAIL")

  cat <<EOF
================
Release details:
GIT_BRANCH:      $GIT_BRANCH
RELEASE_VERSION: $RELEASE_VERSION
RELEASE_TAG:     $RELEASE_TAG
NEXT_VERSION:    $NEXT_VERSION
API_DIFF_TAG:    $API_DIFF_TAG
ASF_USERNAME:    $ASF_USERNAME
GPG_KEY:         $GPG_KEY
GIT_NAME:        $GIT_NAME
GIT-EMAIL:       $GIT_EMAIL
================
EOF

  read -p "Is this info correct [y/n]? " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi

  if ! is_dry_run; then
    if [ -z "$ASF_PASSWORD" ]; then
      stty -echo && printf "ASF_PASSWORD: " && read ASF_PASSWORD && printf '\n' && stty echo
    fi
  else
    ASF_PASSWORD="***INVALID***"
  fi

  if [ -z "$GPG_PASSPHRASE" ]; then
    stty -echo && printf "GPG_PASSPHRASE: " && read GPG_PASSPHRASE && printf '\n' && stty echo
    export GPG_TTY=$(tty)
  fi

  export ASF_PASSWORD
  export GPG_PASSPHRASE
}

function is_dry_run {
  [[ $DRY_RUN = 1 ]]
}

# Initializes JAVA_VERSION to the version of the JVM in use.
function init_java {
  if [ -z "$JAVA_HOME" ]; then
    error "JAVA_HOME is not set."
  fi
  JAVA_VERSION=$("${JAVA_HOME}"/bin/javac -version 2>&1 | cut -d " " -f 2)
  echo "java version: $JAVA_VERSION"
  export JAVA_VERSION
}

function init_python {
  if ! [ -x "$(command -v python2)"  ]; then
    echo 'Error: python2 needed by yetus. Install or add link? E.g: sudo ln -sf /usr/bin/python2.7 /usr/local/bin/python2' >&2
    exit 1
  fi
  echo "python version: `python2 --version`"
}

# Set MVN
function init_mvn {
  if [ -n "$MAVEN_HOME" ]; then
      MVN=${MAVEN_HOME}/bin/mvn
  elif [ $(type -P mvn) ]; then
      MVN=mvn
  else
    error "MAVEN_HOME is not set nor is mvn on the current path."
  fi
  echo "mvn version: `$MVN --version`"
  # Add timestamped logging.
  MVN="${MVN} -B"
  export MVN
}

# Writes report into cwd!
function generate_api_report {
  local project="$1"
  local previous_tag="$2"
  local release_tag="$3"
  # Generate api report.
  ${project}/dev-support/checkcompatibility.py --annotation \
    org.apache.yetus.audience.InterfaceAudience.Public  \
    $previous_tag $release_tag
  local previous_version=$(echo ${previous_tag} | sed -e 's/rel\///')
  cp ${project}/target/compat-check/report.html "./api_compare_${previous_version}_to_${release_tag}.html"
}

# Update the CHANGES.md
# DOES NOT DO COMMITS! Caller should do that.
# yetus requires python2 to be on the path.
function update_releasenotes {
  local project="$1"
  local release_version="$2"
  local yetus="apache-yetus-${YETUS_VERSION}"
  wget -qO- "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/yetus/${YETUS_VERSION}/${yetus}-bin.tar.gz" | \
    tar xvz -C .
  cd ./${yetus} || exit
  ./bin/releasedocmaker -p HBASE --fileversions -v ${release_version} -l --sortorder=newer --skip-credits
  # First clear out the changes written by previous RCs.
  pwd
  sed -i -e "/^## Release ${release_version}/,/^## Release/ {//!d; /^## Release ${release_version}/d;}" \
    ${project}/CHANGES.md || true
  sed -i -e "/^# HBASE  ${release_version} Release Notes/,/^# HBASE/{//!d; /^# HBASE  ${release_version} Release Notes/d;}" \
    ${project}/RELEASENOTES.md || true

  # The above generates RELEASENOTES.X.X.X.md and CHANGELOG.X.X.X.md.
  # To insert into project CHANGES.me...need to cut the top off the
  # CHANGELOG.X.X.X.md file removing license and first line and then
  # insert it after the license comment closing where we have a
  # DO NOT REMOVE marker text!
  sed -i -e '/## Release/,$!d' CHANGELOG.${release_version}.md
  sed -i -e "/DO NOT REMOVE/r CHANGELOG.${release_version}.md" ${project}/CHANGES.md
  # Similar for RELEASENOTES but slightly different.
  sed -i -e '/Release Notes/,$!d' RELEASENOTES.${release_version}.md
  sed -i -e "/DO NOT REMOVE/r RELEASENOTES.${release_version}.md" ${project}/RELEASENOTES.md
  cd .. || exit
}

# Make src release.
# Takes as arguments first the project name -- e.g. hbase or hbase-operator-tools
# -- and then the version string. Expects to find checkout adjacent to this script
# named for 'project', the first arg passed.
# Expects the following three defines in the environment:
# - GPG needs to be defined, with the path to GPG: defaults 'gpg'.
# - The passphrase in the GPG_PASSPHRASE variable: no default (we don't make .asc file).
# - GIT_REF which is the tag to create the tgz from: defaults to 'master'.
# For example:
# $ GPG_PASSPHRASE="XYZ" GIT_REF="master" make_src_release hbase-operator-tools 1.0.0
make_src_release() {
  # Tar up the src and sign and hash it.
  project="${1}"
  version="${2}"
  basename="${project}-${version}"
  rm -rf "${basename}-src*"
  tgz="${basename}-src.tar.gz"
  cd "${project}" || exit
  git clean -d -f -x
  git archive --format=tar.gz --output="../${tgz}" --prefix="${basename}/" "${GIT_REF:-master}"
  cd .. || exit
  echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --armour --output "${tgz}.asc" \
    --detach-sig "${tgz}"
  echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --print-md SHA512 "${tgz}" > "${tgz}.sha512"
}

# Make binary release.
# Takes as arguments first the project name -- e.g. hbase or hbase-operator-tools
# -- and then the version string. Expects to find checkout adjacent to this script
# named for 'project', the first arg passed.
# Expects the following three defines in the environment:
# - GPG needs to be defined, with the path to GPG: defaults 'gpg'.
# - The passphrase in the GPG_PASSPHRASE variable: no default (we don't make .asc file).
# - GIT_REF which is the tag to create the tgz from: defaults to 'master'.
# - MVN Default is 'mvn'.
# For example:
# $ GPG_PASSPHRASE="XYZ" GIT_REF="master" make_src_release hbase-operator-tools 1.0.0
make_binary_release() {
  project="${1}"
  version="${2}"
  basename="${project}-${version}"
  rm -rf "${basename}-bin*"
  cd $project || exit

  git clean -d -f -x
  # Three invocations of maven. This seems to work. One to
  # populate the repo, another to build the site, and then
  # a third to assemble the binary artifact. Trying to do
  # all in the one invocation fails; a problem in our
  # assembly spec to in maven. TODO. Meantime, three invocations.
  MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} --settings $tmp_settings clean install -DskipTests \
    -Dmaven.repo.local="${tmp_repo}"
  MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} --settings $tmp_settings site -DskipTests \
    -Dmaven.repo.local="${tmp_repo}"
  MAVEN_OPTS="${MAVEN_OPTS}" ${MVN} --settings $tmp_settings install assembly:single -DskipTests \
    -Dcheckstyle.skip=true "${PUBLISH_PROFILES}" -Dmaven.repo.local="${tmp_repo}"

  # Check there is a bin gz output. The build may not produce one: e.g. hbase-thirdparty.
  f_bin_tgz="./${PROJECT}-assembly/target/${basename}*-bin.tar.gz"
  if test -f "${f_bin_tgz}"; then
    cp "${f_bin_tgz}" ..
    cd .. || exit
    for i in "${basename}"*-bin.tar.gz; do
      echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --armour --output "$i.asc" --detach-sig "$i"
      echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --print-md SHA512 "${i}" > "$i.sha512"
    done
  else
    cd .. || exit
    echo "No ${f_bin_tgz} product; expected?"
  fi
}
