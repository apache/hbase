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
Usage: $NAME [OPTIONS]
Runs release scripts inside a docker image.
Options:
  -d [path]    Required. Working directory. Output will be written to "output" in here.
  -f           "force" -- actually publish this release. Unless you specify '-f', it will
               default to dry run mode, which checks and does local builds, but does not
               upload anything.
  -t [tag]     Tag for the hbase-rm docker image to use for building (default: "latest").
  -j [path]    Path to local JDK installation to use building. By default the script will
               use openjdk8 installed in the docker image.
  -p [project] Project to build: e.g. 'hbase' or 'hbase-thirdparty'; defaults to PROJECT env var
  -r [repo]    Git repo to use for remote git operations. defaults to ASF gitbox for project.
  -s [step]    Runs a single step of the process; valid steps: tag|publish-dist|publish-release.
               If none specified, runs tag, then publish-dist, and then publish-release.
               'publish-snapshot' is also an allowed, less used, option.
  -x           Debug. Does less clean up (env file, gpg forwarding on mac)
EOF
  exit 1
}

WORKDIR=
IMGTAG=latest
JAVA=
RELEASE_STEP=
GIT_REPO=
while getopts "d:fhj:p:r:s:t:x" opt; do
  case $opt in
    d) WORKDIR="$OPTARG" ;;
    f) DRY_RUN=0 ;;
    t) IMGTAG="$OPTARG" ;;
    j) JAVA="$OPTARG" ;;
    p) PROJECT="$OPTARG" ;;
    r) GIT_REPO="$OPTARG" ;;
    s) RELEASE_STEP="$OPTARG" ;;
    x) DEBUG=1 ;;
    h) usage ;;
    ?) error "Invalid option. Run with -h for help." ;;
  esac
done
shift $((OPTIND-1))
if (( $# > 0 )); then
  error "Arguments can only be provided with option flags, invalid args: $*"
fi

if [ "$DEBUG" = "1" ]; then
  set -x
fi
export DEBUG

if [ -z "$WORKDIR" ] || [ ! -d "$WORKDIR" ]; then
  error "Work directory (-d) must be defined and exist. Run with -h for help."
fi

if [ -d "$WORKDIR/output" ]; then
  read -r -p "Output directory already exists. Overwrite and continue? [y/n] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    error "Exiting."
  fi
fi

if [ -f "${WORKDIR}/gpg-proxy.ssh.pid" ] || \
   [ -f "${WORKDIR}/gpg-proxy.cid" ] || \
   [ -f "${WORKDIR}/release.cid" ]; then
  read -r -p "container/pid files from prior run exists. Overwrite and continue? [y/n] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    error "Exiting."
  fi
fi

cd "$WORKDIR"
rm -rf "$WORKDIR/output"
rm -rf "${WORKDIR}/gpg-proxy.ssh.pid" "${WORKDIR}/gpg-proxy.cid" "${WORKDIR}/release.cid"
mkdir "$WORKDIR/output"

banner "Gathering release details."
HOST_OS="$(get_host_os)"
get_release_info

banner "Setup"

# Place all RM scripts and necessary data in a local directory that must be defined in the command
# line. This directory is mounted into the image. Its WORKDIR, the arg passed with -d.
for f in "$SELF"/*; do
  if [ -f "$f" ]; then
    cp "$f" "$WORKDIR"
  fi
done

# We need to import that public key in the container in order to use the private key via the agent.
GPG_KEY_FILE="$WORKDIR/gpg.key.public"
log "Exporting public key for ${GPG_KEY}"
fcreate_secure "$GPG_KEY_FILE"
$GPG "${GPG_ARGS[@]}" --export "${GPG_KEY}" > "${GPG_KEY_FILE}"

function cleanup {
  local id
  banner "Release Cleanup"
  if is_debug; then
    log "skipping due to debug run"
    return 0
  fi
  log "details in cleanup.log"
  if [ -f "${ENVFILE}" ]; then
    rm -f "$ENVFILE"
  fi
  rm -f "$GPG_KEY_FILE"
  if [ -f "${WORKDIR}/gpg-proxy.ssh.pid" ]; then
    id=$(cat "${WORKDIR}/gpg-proxy.ssh.pid")
    echo "Stopping ssh tunnel for gpg-agent at PID ${id}" | tee -a cleanup.log
    kill -9 "${id}" >>cleanup.log 2>&1 || true
    rm -f "${WORKDIR}/gpg-proxy.ssh.pid" >>cleanup.log 2>&1
  fi
  if [ -f "${WORKDIR}/gpg-proxy.cid" ]; then
    id=$(cat "${WORKDIR}/gpg-proxy.cid")
    echo "Stopping gpg-proxy container with ID ${id}" | tee -a cleanup.log
    docker kill "${id}" >>cleanup.log 2>&1 || true
    rm -f "${WORKDIR}/gpg-proxy.cid" >>cleanup.log 2>&1
    # TODO we should remove the gpgagent volume?
  fi
  if [ -f "${WORKDIR}/release.cid" ]; then
    id=$(cat "${WORKDIR}/release.cid")
    echo "Stopping release container with ID ${id}" | tee -a cleanup.log
    docker kill "${id}" >>cleanup.log 2>&1 || true
    rm -f "${WORKDIR}/release.cid" >>cleanup.log 2>&1
  fi
}

trap cleanup EXIT

log "Host OS: ${HOST_OS}"
if [ "${HOST_OS}" == "DARWIN" ]; then
  run_silent "Building gpg-agent-proxy image with tag ${IMGTAG}..." "docker-proxy-build.log" \
    docker build --build-arg "UID=${UID}" --build-arg "RM_USER=${USER}" \
        --tag "org.apache.hbase/gpg-agent-proxy:${IMGTAG}" "${SELF}/mac-sshd-gpg-agent"
fi

run_silent "Building hbase-rm image with tag $IMGTAG..." "docker-build.log" \
  docker build --tag "org.apache.hbase/hbase-rm:$IMGTAG" --build-arg "UID=$UID" \
      --build-arg "RM_USER=${USER}" "$SELF/hbase-rm"

banner "Final prep for container launch."
log "Writing out environment for container."
# Write the release information to a file with environment variables to be used when running the
# image.
ENVFILE="$WORKDIR/env.list"
fcreate_secure "$ENVFILE"

cat > "$ENVFILE" <<EOF
PROJECT=$PROJECT
DRY_RUN=$DRY_RUN
SKIP_TAG=$SKIP_TAG
RUNNING_IN_DOCKER=1
GIT_BRANCH=$GIT_BRANCH
NEXT_VERSION=$NEXT_VERSION
PREV_VERSION=$PREV_VERSION
RELEASE_VERSION=$RELEASE_VERSION
RELEASE_TAG=$RELEASE_TAG
GIT_REF=$GIT_REF
ASF_USERNAME=$ASF_USERNAME
GIT_NAME=$GIT_NAME
GIT_EMAIL=$GIT_EMAIL
GPG_KEY=$GPG_KEY
ASF_PASSWORD=$ASF_PASSWORD
RELEASE_STEP=$RELEASE_STEP
API_DIFF_TAG=$API_DIFF_TAG
HOST_OS=$HOST_OS
DEBUG=$DEBUG
EOF

JAVA_MOUNT=()
if [ -n "$JAVA" ]; then
  echo "JAVA_HOME=/opt/hbase-java" >> "$ENVFILE"
  JAVA_MOUNT=(--mount "type=bind,src=${JAVA},dst=/opt/hbase-java,readonly")
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
      log "Converted file:// git repo to a local path, which changes git to assume --local."
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

GPG_PROXY_MOUNT=()
if [ "${HOST_OS}" == "DARWIN" ]; then
  GPG_PROXY_MOUNT=(--mount "type=volume,src=gpgagent,dst=/home/${USER}/.gnupg/")
  log "Setting up GPG agent proxy container needed on OS X."
  log "    we should clean this up for you. If that fails the container ID is below and in " \
      "gpg-proxy.cid"
  #TODO the key pair used should be configurable
  docker run --rm -p 62222:22 \
     --detach --cidfile "${WORKDIR}/gpg-proxy.cid" \
     --mount \
     "type=bind,src=${HOME}/.ssh/id_rsa.pub,dst=/home/${USER}/.ssh/authorized_keys,readonly" \
     "${GPG_PROXY_MOUNT[@]}" \
     "org.apache.hbase/gpg-agent-proxy:${IMGTAG}"

  KEYSCAN_OUTPUT_PATH="${WORKDIR}/gpg-agent-proxy.ssh-keyscan"
  if [ -f "${KEYSCAN_OUTPUT_PATH}" ]; then
    # cleanup first so that the below checks will work, in case this is a rerun.
    rm "${KEYSCAN_OUTPUT_PATH}"
  fi

  log "Waiting for port 62222 to be available"
  while [ ! -s "${KEYSCAN_OUTPUT_PATH}" ]; do
    sleep 5
    ssh-keyscan -p 62222 localhost 2>/dev/null | sort > "${KEYSCAN_OUTPUT_PATH}"
  done
  log "Done - port 62222 is ready"

  # gotta trust the container host
  sort "${HOME}/.ssh/known_hosts" | comm -1 -3 - "${KEYSCAN_OUTPUT_PATH}" \
      > "${WORKDIR}/gpg-agent-proxy.known_hosts"
  if [ -s "${WORKDIR}/gpg-agent-proxy.known_hosts" ]; then
    log "Your ssh known_hosts does not include the entries for the gpg-agent proxy container."
    log "The following entry(ies) are missing:"
    sed -e 's/^/    /' "${WORKDIR}/gpg-agent-proxy.known_hosts"
    read -r -p "Okay to add these entries to ${HOME}/.ssh/known_hosts? [y/n] " ANSWER
    if [ "$ANSWER" != "y" ]; then
      error "Exiting."
    fi
    cat "${WORKDIR}/gpg-agent-proxy.known_hosts" >> "${HOME}/.ssh/known_hosts"
  fi
  log "Launching ssh reverse tunnel from the container to gpg agent."
  log "    we should clean this up for you. If that fails the PID is in gpg-proxy.ssh.pid"
  ssh -p 62222 -R "/home/${USER}/.gnupg/S.gpg-agent:$(gpgconf --list-dir agent-socket)" \
      -i "${HOME}/.ssh/id_rsa" -N -n localhost >gpg-proxy.ssh.log 2>&1 &
  echo $! > "${WORKDIR}/gpg-proxy.ssh.pid"
else
  # Note that on linux we always directly mount the gpg agent's extra socket to limit what the
  # container can ask the gpg-agent to do.
  # When working on a remote linux machine you should be sure to forward both the remote machine's
  # agent socket and agent extra socket to your local gpg-agent's extra socket. See the README.txt
  # for an example.
  GPG_PROXY_MOUNT=(--mount \
      "type=bind,src=$(gpgconf --list-dir agent-socket),dst=/home/${USER}/.gnupg/S.gpg-agent")
fi

banner "Building $RELEASE_TAG; output will be at $WORKDIR/output"
log "We should clean the container up when we are done. If that fails then the container ID " \
    "is in release.cid"
echo
# Where possible we specify "consistency=delegated" when we do not need host access during the
# build run. On Mac OS X specifically this gets us a big perf improvement.
cmd=(docker run --rm -ti \
  --env-file "$ENVFILE" \
  --cidfile "${WORKDIR}/release.cid" \
  --mount "type=bind,src=${WORKDIR},dst=/home/${USER}/hbase-rm,consistency=delegated" \
  "${JAVA_MOUNT[@]}" \
  "${GIT_REPO_MOUNT[@]}" \
  "${GPG_PROXY_MOUNT[@]}" \
  "org.apache.hbase/hbase-rm:$IMGTAG")
echo "${cmd[*]}"
"${cmd[@]}"
