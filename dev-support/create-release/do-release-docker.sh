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

#
# Creates a HBase release candidate. The script will update versions, tag the branch,
# build HBase binary packages and documentation, and upload maven artifacts to a staging
# repository. There is also a dry run mode where only local builds are performed, and
# nothing is uploaded to the ASF repos.
#
# Run with "-h" for options. For example, running below will do all
# steps above using the 'rm' dir under Downloads as workspace:
#
# $ ./do-release-docker.sh  -d ~/Downloads/rm
#
# The scripts in this directory came originally from spark [1]. They were then
# modified to suite the hbase context. These scripts supercedes the old
# ../make_rc.sh script for making release candidates because what is here is more
# comprehensive doing more steps of the RM process as well as running in a
# container so the RM build environment can be a constant.
#
# It:
#  * Tags release
#  * Sets version to the release version
#  * Sets version to next SNAPSHOT version.
#  * Builds, signs, and hashes all artifacts.
#  * Pushes release tgzs to the dev dir in a apache dist.
#  * Pushes to repository.apache.org staging.
#
# The entry point is here, in the do-release-docker.sh script.
#
# 1. https://github.com/apache/spark/tree/master/dev/create-release
#
set -e

# Set this to build other hbase repos: e.g. PROJECT=hbase-operator-tools
export PROJECT="${PROJECT:-hbase}"

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=SCRIPTDIR/release-util.sh
. "$SELF/release-util.sh"
ORIG_PWD="$(pwd)"

function usage {
  local NAME
  NAME="$(basename "${BASH_SOURCE[0]}")"
  cat <<EOF
Usage: $NAME [options]

This script runs the release scripts inside a docker image.

Options:

  -d [path]    required. working directory. output will be written to "output" in here.
  -f           "force" -- actually publish this release. Unless you specify '-f', it will
               default to dry run mode, which checks and does local builds, but does not upload anything.
  -t [tag]     tag for the hbase-rm docker image to use for building (default: "latest").
  -j [path]    path to local JDK installation to use building. By default the script will
               use openjdk8 installed in the docker image.
  -m [volume]  named volume to use for maven artifact caching
  -p [project] project to build, such as 'hbase' or 'hbase-thirdparty'; defaults to $PROJECT env var
  -r [repo]    git repo to use for remote git operations. defaults to ASF gitbox for project.
  -s [step]    runs a single step of the process; valid steps are: tag|publish-dist|publish-release.
               If none specified, runs tag, then publish-dist, and then publish-release.
               'publish-snapshot' is also an allowed, less used, option.
EOF
  exit 1
}

WORKDIR=
IMGTAG=latest
JAVA=
RELEASE_STEP=
GIT_REPO=
MAVEN_VOLUME=
while getopts "d:fhj:m:p:r:s:t:" opt; do
  case $opt in
    d) WORKDIR="$OPTARG" ;;
    f) DRY_RUN=0 ;;
    t) IMGTAG="$OPTARG" ;;
    j) JAVA="$OPTARG" ;;
    m) MAVEN_VOLUME="$OPTARG" ;;
    p) PROJECT="$OPTARG" ;;
    r) GIT_REPO="$OPTARG" ;;
    s) RELEASE_STEP="$OPTARG" ;;
    h) usage ;;
    ?) error "Invalid option. Run with -h for help." ;;
  esac
done
shift $((OPTIND-1))
if (( $# > 0 )); then
  error "Arguments can only be provided with option flags, invalid args: $*"
fi

if [ -z "$WORKDIR" ] || [ ! -d "$WORKDIR" ]; then
  error "Work directory (-d) must be defined and exist. Run with -h for help."
fi

if [ -d "$WORKDIR/output" ]; then
  read -r -p "Output directory already exists. Overwrite and continue? [y/n] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    error "Exiting."
  fi
fi

cd "$WORKDIR"
rm -rf "$WORKDIR/output"
mkdir "$WORKDIR/output"

get_release_info

# Place all RM scripts and necessary data in a local directory that must be defined in the command
# line. This directory is mounted into the image. Its WORKDIR, the arg passed with -d.
for f in "$SELF"/*; do
  if [ -f "$f" ]; then
    cp "$f" "$WORKDIR"
  fi
done

GPG_KEY_FILE="$WORKDIR/gpg.key"
fcreate_secure "$GPG_KEY_FILE"
$GPG --passphrase "$GPG_PASSPHRASE" --export-secret-key --armor "$GPG_KEY" > "$GPG_KEY_FILE"

run_silent "Building hbase-rm image with tag $IMGTAG..." "docker-build.log" \
  docker build -t "hbase-rm:$IMGTAG" --build-arg UID=$UID "$SELF/hbase-rm"

# Write the release information to a file with environment variables to be used when running the
# image.
ENVFILE="$WORKDIR/env.list"
fcreate_secure "$ENVFILE"

function cleanup {
  rm -f "$ENVFILE"
  rm -f "$GPG_KEY_FILE"
}

trap cleanup EXIT

cat > "$ENVFILE" <<EOF
PROJECT=$PROJECT
DRY_RUN=$DRY_RUN
SKIP_TAG=$SKIP_TAG
RUNNING_IN_DOCKER=1
GIT_BRANCH=$GIT_BRANCH
NEXT_VERSION=$NEXT_VERSION
RELEASE_VERSION=$RELEASE_VERSION
RELEASE_TAG=$RELEASE_TAG
GIT_REF=$GIT_REF
ASF_USERNAME=$ASF_USERNAME
GIT_NAME=$GIT_NAME
GIT_EMAIL=$GIT_EMAIL
GPG_KEY=$GPG_KEY
ASF_PASSWORD=$ASF_PASSWORD
GPG_PASSPHRASE=$GPG_PASSPHRASE
RELEASE_STEP=$RELEASE_STEP
API_DIFF_TAG=$API_DIFF_TAG
EOF

JAVA_VOL=()
if [ -n "$JAVA" ]; then
  echo "JAVA_HOME=/opt/hbase-java" >> "$ENVFILE"
  JAVA_VOL=(--volume "$JAVA:/opt/hbase-java")
fi

MAVEN_MOUNT=()
if [ -n "${MAVEN_VOLUME}" ]; then
  MAVEN_MOUNT=(--mount "type=volume,src=${MAVEN_VOLUME},dst=/home/hbase-rm/.m2-repository/")
  echo "REPO=/home/hbase-rm/.m2-repository" >> "${ENVFILE}"
fi

#TODO some debug output would be good here
GIT_REPO_MOUNT=()
if [ -n "${GIT_REPO}" ]; then
  case "${GIT_REPO}" in
    # skip the easy to identify remote protocols
    ssh://*|git://*|http://*|https://*|ftp://*|ftps://*) ;;
    # for sure local
    /*)
      GIT_REPO_MOUNT=(--mount "type=bind,src=${GIT_REPO},dst=/opt/hbase-repo,consistency=delegated")
      echo "HOST_GIT_REPO=${GIT_REPO}" >> "${ENVFILE}"
      GIT_REPO="/opt/hbase-repo"
      ;;
    # on the host but normally git wouldn't use the local optimization
    file://*)
      echo "[INFO] converted file:// git repo to a local path, which changes git to assume --local."
      GIT_REPO_MOUNT=(--mount "type=bind,src=${GIT_REPO#file://},dst=/opt/hbase-repo,consistency=delegated")
      echo "HOST_GIT_REPO=${GIT_REPO}" >> "${ENVFILE}"
      GIT_REPO="/opt/hbase-repo"
      ;;
    # have to decide if it's a local path or the "scp-ish" remote
    *)
      declare colon_remove_prefix;
      declare slash_remove_prefix;
      declare local_path;
      colon_remove_prefix="${GIT_REPO#*:}"
      slash_remove_prefix="${GIT_REPO#*/}"
      if [ "${GIT_REPO}" = "${colon_remove_prefix}" ]; then
        # if there was no colon at all, we assume this must be a local path
        local_path="no colon at all"
      elif [ "${GIT_REPO}" != "${slash_remove_prefix}" ]; then
        # if there was a colon and there is no slash, then we assume it must be scp-style host
        # and a relative path

        if [ "${#colon_remove_prefix}" -lt "${#slash_remove_prefix}" ]; then
          # Given the substrings made by removing everything up to the first colon and slash
          # we can determine which comes first based on the longer substring length.
          # if the slash is first, then we assume the colon is part of a path name and if the colon
          # is first then it is the seperator between a scp-style host name and the path.
          local_path="slash happened before a colon"
        fi
      fi
      if [ -n "${local_path}" ]; then
        # convert to an absolute path
        GIT_REPO="$(cd "$(dirname "${ORIG_PWD}/${GIT_REPO}")"; pwd)/$(basename "${ORIG_PWD}/${GIT_REPO}")"
        GIT_REPO_MOUNT=(--mount "type=bind,src=${GIT_REPO},dst=/opt/hbase-repo,consistency=delegated")
        echo "HOST_GIT_REPO=${GIT_REPO}" >> "${ENVFILE}"
        GIT_REPO="/opt/hbase-repo"
      fi
      ;;
  esac
  echo "GIT_REPO=${GIT_REPO}" >> "${ENVFILE}"
fi

echo "Building $RELEASE_TAG; output will be at $WORKDIR/output"
# Where possible we specifcy "consistency=delegated" when we do not need host access during the
# build run. On Mac OS X specifically this gets us a big perf improvement.
cmd=(docker run -ti \
  --env-file "$ENVFILE" \
  --mount "type=bind,src=${WORKDIR},dst=/opt/hbase-rm,consistency=delegated" \
  "${JAVA_VOL[@]}" \
  "${GIT_REPO_MOUNT[@]}" \
  "${MAVEN_MOUNT[@]}" \
  "hbase-rm:$IMGTAG")
echo "${cmd[*]}"
"${cmd[@]}"
