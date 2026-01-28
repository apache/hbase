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

# Turn on Bash command logging for debug mode
if [ "$DEBUG" = "1" ]; then
  set -x
fi

# Make a tmp dir into which we put files cleaned-up on exit.
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

set -e
# Use the adjacent do-release-docker.sh instead, if you can.
# Otherwise, this runs core of the release creation.
# Will ask you questions on what to build and for logins
# and passwords to use building.
export PROJECT="${PROJECT:-hbase}"

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=SCRIPTDIR/release-util.sh
. "$SELF/release-util.sh"

while getopts "b:fs:" opt; do
  case $opt in
    b) export GIT_BRANCH=$OPTARG ;;
    f) export DRY_RUN=0 ;;  # "force", ie actually publish this release (otherwise defaults to dry run)
    s) RELEASE_STEP="$OPTARG" ;;
    ?) error "Invalid option: $OPTARG" ;;
  esac
done
shift $((OPTIND-1))
if (( $# > 0 )); then
  error "Arguments can only be provided with option flags, invalid args: $*"
fi

function gpg_agent_help {
  cat <<EOF
Trying to sign a test file using your GPG setup failed.

Please make sure you have a local gpg-agent running with access to your secret keys prior to
starting a release build. If you are creating release artifacts on a remote machine please check
that you have set up ssh forwarding to the gpg-agent extra socket.

For help on how to do this please see the README file in the create-release directory.
EOF
  exit 1
}

# If running in docker, import and then cache keys.
if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  # when Docker Desktop for mac is running under load there is a delay before the mounted volume
  # becomes available. if we do not pause then we may try to use the gpg-agent socket before docker
  # has got it ready and we will not think there is a gpg-agent.
  if [ "${HOST_OS}" == "DARWIN" ]; then
    sleep 5
  fi
  # in docker our working dir is set to where all of our scripts are held
  # and we want default output to go into the "output" directory that should be in there.
  if [ -d "output" ]; then
    cd output
  fi
  echo "GPG Version: $("${GPG}" "${GPG_ARGS[@]}" --version)"
  # Inside docker, need to import the GPG key stored in the current directory.
  if ! $GPG "${GPG_ARGS[@]}" --import "$SELF/gpg.key.public" ; then
    gpg_agent_help
  fi

  # We may need to adjust the path since JAVA_HOME may be overridden by the driver script.
  if [ -n "$JAVA_HOME" ]; then
    echo "Using JAVA_HOME from host."
    export PATH="$JAVA_HOME/bin:$PATH"
  else
    # JAVA_HOME for the openjdk package.
    arch=`uname -m | sed -e s/aarch64/arm64/ | sed -e s/x86_64/amd64/`
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-${arch}/
  fi
else
  # Outside docker, need to ask for information about the release.
  get_release_info
fi

# Check GPG
gpg_test_file="${TMPDIR}/gpg_test.$$.txt"
echo "Testing gpg signing ${GPG} ${GPG_ARGS[@]} --detach --armor --sign ${gpg_test_file}"
echo "foo" > "${gpg_test_file}"
if ! "${GPG}" "${GPG_ARGS[@]}" --detach --armor --sign "${gpg_test_file}" ; then
  gpg_agent_help
fi
# In --batch mode we have to be explicit about what we are verifying
if ! "${GPG}" "${GPG_ARGS[@]}" --verify "${gpg_test_file}.asc" "${gpg_test_file}" ; then
  gpg_agent_help
fi
GPG_TTY="$(tty)"
export GPG_TTY

if [[ -z "$RELEASE_STEP" ]]; then
  # If doing all stages, leave out 'publish-snapshot'
  RELEASE_STEP="tag_publish-dist_publish-release"
  # and use shared maven local repo for efficiency
  export REPO="${REPO:-$(pwd)/$(mktemp -d hbase-repo-XXXXX)}"
fi

function should_build {
  local WHAT=$1
  if [[ -z "$RELEASE_STEP" ]]; then
    return 0
  elif [[ "$RELEASE_STEP" == *"$WHAT"* ]]; then
    return 0
  else
    return 1
  fi
}

if should_build "tag" && [ "$SKIP_TAG" = 0 ]; then
  if [ -z "${YETUS_HOME}" ] && [ "${RUNNING_IN_DOCKER}" != "1" ]; then
    declare local_yetus="/opt/apache-yetus/0.12.0/"
    if [ "$(get_host_os)" = "DARWIN" ]; then
      local_yetus="/usr/local/Cellar/yetus/0.12.0/"
    fi
    YETUS_HOME="$(read_config "YETUS_HOME not defined. Absolute path to local install of Apache Yetus" "${local_yetus}")"
    export YETUS_HOME
  fi
  run_silent "Creating release tag $RELEASE_TAG..." "tag.log" \
    "$SELF/release-build.sh" tag
  if is_dry_run; then
    export TAG_SAME_DRY_RUN="true";
  fi
else
  echo "Skipping tag creation for $RELEASE_TAG."
fi

if should_build "publish-dist"; then
  run_silent "Publishing distribution packages (tarballs)" "publish-dist.log" \
    "$SELF/release-build.sh" publish-dist
else
  echo "Skipping publish-dist step."
fi

if should_build "publish-snapshot"; then
  run_silent "Publishing snapshot" "publish-snapshot.log" \
    "$SELF/release-build.sh" publish-snapshot

elif should_build "publish-release"; then
  run_silent "Publishing release" "publish-release.log" \
    "$SELF/release-build.sh" publish-release
else
  echo "Skipping publish-release step."
fi
