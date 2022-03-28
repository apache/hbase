#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
function usage {
  echo "Usage: ${0} [options] /path/to/download/file.tar.gz download/fragment/eg/project/subdir/some-artifact-version.tar.gz"
  echo ""
  echo "    --force                       for a redownload even if /path/to/download/file.tar.gz exists."
  echo "    --verify-tar-gz               Only use a cached file if it can be parsed as a gzipped tarball."
  echo "    --working-dir /path/to/use    Path for writing tempfiles. must exist."
  echo "                                  defaults to making a directory via mktemp that we clean."
  echo "    --keys url://to/project/KEYS  where to get KEYS. needed to check signature on download."
  echo ""
  exit 1
}
# if no args specified, show usage
if [ $# -lt 2 ]; then
  usage
fi


# Get arguments
declare done_if_cached="true"
declare verify_tar_gz="false"
declare working_dir
declare cleanup="true"
declare keys
while [ $# -gt 0 ]
do
  case "$1" in
    --force) shift; done_if_cached="false";;
    --verify-tar-gz) shift; verify_tar_gz="true";;
    --working-dir) shift; working_dir=$1; cleanup="false"; shift;;
    --keys) shift; keys=$1; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

# should still have required args
if [ $# -lt 2 ]; then
  usage
fi

target="$1"
artifact="$2"

if [ -f "${target}" ] && [ -s "${target}" ] && [ -r "${target}" ] && [ "true" = "${done_if_cached}" ]; then
  if [ "false" = "${verify_tar_gz}" ]; then
    echo "Reusing existing download of '${artifact}'."
    exit 0
  fi
  if ! tar tzf "${target}" > /dev/null 2>&1; then
    echo "Cached artifact is not a well formed gzipped tarball; clearing the cached file at '${target}'."
    rm -rf "${target}"
  else
    echo "Reusing existing download of '${artifact}', which is a well formed gzipped tarball."
    exit 0
  fi
fi

if [ -z "${working_dir}" ]; then
  if ! working_dir="$(mktemp -d -t hbase-download-apache-artifact)" ; then
    echo "Failed to create temporary working directory. Please specify via --working-dir" >&2
    exit 1
  fi
else
  # absolutes please
  working_dir="$(cd "$(dirname "${working_dir}")"; pwd)/$(basename "${working_dir}")"
  if [ ! -d "${working_dir}" ]; then
    echo "passed working directory '${working_dir}' must already exist." >&2
    exit 1
  fi
fi

function cleanup {
  if [ -n "${keys}" ]; then
    echo "Stopping gpg agent daemon"
    gpgconf --homedir "${working_dir}/.gpg" --kill gpg-agent
    echo "Stopped gpg agent daemon"
  fi

  if [ "true" = "${cleanup}" ]; then
    echo "cleaning up temp space."
    rm -rf "${working_dir}"
  fi
}
trap cleanup EXIT SIGQUIT

echo "New download of '${artifact}'"

# N.B. this comes first so that if gpg falls over we skip the expensive download.
if [ -n "${keys}" ]; then
  if [ ! -d "${working_dir}/.gpg" ]; then
    rm -rf "${working_dir}/.gpg"
    mkdir -p "${working_dir}/.gpg"
    chmod -R 700 "${working_dir}/.gpg"
  fi

  echo "installing project KEYS"
  curl -L --fail -o "${working_dir}/KEYS" "${keys}"
  if ! gpg --homedir "${working_dir}/.gpg" --import "${working_dir}/KEYS" ; then
    echo "ERROR importing the keys via gpg failed. If the output above mentions this error:" >&2
    echo "    gpg: can't connect to the agent: File name too long" >&2
    # we mean to give them the command to run, not to run it.
    #shellcheck disable=SC2016
    echo 'then you prolly need to create /var/run/user/$(id -u)' >&2
    echo "see this thread on gnupg-users: https://s.apache.org/uI7x" >&2
    exit 2
  fi

  echo "downloading signature"
  curl -L --fail -o "${working_dir}/artifact.asc" "https://archive.apache.org/dist/${artifact}.asc"
fi

echo "downloading artifact"
if ! curl --dump-header "${working_dir}/artifact_download_headers.txt" -L --fail -o "${working_dir}/artifact" "https://www.apache.org/dyn/closer.lua?filename=${artifact}&action=download" ; then
  echo "Artifact wasn't in mirror system. falling back to archive.a.o."
  curl --dump-header "${working_dir}/artifact_fallback_headers.txt" -L --fail -o "${working_dir}/artifact" "http://archive.apache.org/dist/${artifact}"
fi

if [ -n "${keys}" ]; then
  echo "verifying artifact signature"
  gpg --homedir "${working_dir}/.gpg" --verify "${working_dir}/artifact.asc"
  echo "signature good."
fi

echo "moving artifact into place at '${target}'"
# ensure we're on the same filesystem
mv "${working_dir}/artifact" "${target}.copying"
# attempt atomic move
mv "${target}.copying" "${target}"
echo "all done!"
