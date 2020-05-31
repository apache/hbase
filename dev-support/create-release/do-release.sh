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

# If running in docker, import and then cache keys.
if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  # Run gpg agent.
  eval "$(gpg-agent --disable-scdaemon --daemon --no-grab  --allow-preset-passphrase \
          --default-cache-ttl=86400 --max-cache-ttl=86400)"
  echo "GPG Version: $(gpg --version)"
  # Inside docker, need to import the GPG keyfile stored in the current directory.
  # (On workstation, assume GPG has access to keychain/cache with key_id already imported.)
  echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --import "$SELF/gpg.key"

  # We may need to adjust the path since JAVA_HOME may be overridden by the driver script.
  if [ -n "$JAVA_HOME" ]; then
    export PATH="$JAVA_HOME/bin:$PATH"
  else
    # JAVA_HOME for the openjdk package.
    export JAVA_HOME=/usr
  fi
else
  # Outside docker, need to ask for information about the release.
  get_release_info
fi
GPG_TTY="$(tty)"
export GPG_TTY

if [[ -n "${REPO}" ]]; then
  echo "reusing persistent maven repo, testing write access"
  echo "foo" > "${REPO}/test.file"
  if [[ -d "${REPO}/org/apache/hbase/" ]]; then
    echo "reusing persistent maven repo, clearing out our project artifacts."
    rm -rf "${REPO}/org/apache/hbase/"
  fi
fi

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
    declare local_yetus="/opt/apache-yetus/0.11.1/"
    if [ "$(get_host_os)" = "DARWIN" ]; then
      local_yetus="/usr/local/Cellar/yetus/0.11.1/"
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
