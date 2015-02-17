#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# hbase_docker.sh
# A driver script to build HBase master branch from source and start the HBase
# shell in a Docker container.
#
# Usage: This script automates the build process facilitated by the Dockerfile
#        in the hbase_docker folder. In particular, it is assumed that an
#        Oracle JDK .tar.gz and an Apache Maven .tar.gz file are both present in
#        the same directory as the Dockerfile; this script can download and place
#        those tarballs for you. Moreover, due to bugs in Docker, the docker build
#        command occasionally fails, but then succeeds upon rerunning; this script
#        will rerun the command and attempt to finish a build. Finally, this
#        script allows you to specify a desired name for the Docker image being
#        created, defaulting to "hbase_docker." For complete usage instructions,
#        run this script with the -h or --help option.
#
# Example: To build HBase and start an HBase shell using Oracle JDK 7u67 and
#          Apache Maven 3.2.3:
#
# # ./hbase_docker.sh -j http://download.oracle.com/otn-pub/java/jdk/7u67-b01/jdk-7u67-linux-x64.tar.gz \
#       -m http://apache.claz.org/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz


# Before doing anything else, make sure Docker is installed.
if ! which docker &> /dev/null; then
  cat >&2 << __EOF
Docker must be installed to run hbase_docker. Go to http://www.docker.io
for installation instructions. Exiting...
__EOF
  exit 1
elif ! docker ps &> /dev/null; then
  echo "Docker must be run as root or with sudo privileges. Exiting..." >&2
  exit 1
fi

# Usage message.
usage() {
  SCRIPT=$(basename "${BASH_SOURCE}")

  cat << __EOF

hbase_docker. A driver script for building HBase and starting the HBase shell
inside of a Docker container.

Usage: ${SCRIPT} [-j | --jdk <url>] [-m | --mvn <url>] [-n | --name <name>]
       ${SCRIPT} -h | --help

  -h | --help          Show this screen.
  -j | --jdk '<url>'   A URL pointing to an x64 .tar.gz file of Oracle's JDK.
                       Using this argument implies acceptance of the Oracle
                       Binary Code License Agreement for Java SE. See www.oracle.com
                       for more details.
  -m | --mvn '<url>'   A URL pointing to an x64 .tar.gz file of Apache Maven.
  -n | --name <name>   The name to give to the Docker image created by this script.
                       If left blank, "hbase_docker" will be used.
__EOF
}

if ! [ ${#} -le 6 ]; then
  usage >&2
  exit 1
fi

# Default Docker image name.
IMAGE_NAME=hbase_docker

while ((${#})); do
  case "${1}" in
    -h | --help )
      usage; exit 0              ;;
    -j | --jdk  )
      JDK_URL="${2}"; shift 2    ;;
    -m | --mvn  )
      MAVEN_URL="${2}"; shift 2  ;;
    -n | --name )
      IMAGE_NAME="${2}"; shift 2 ;;
    *           )
      usage >&2; exit 1          ;;
  esac
done

# The relative file path to the directory containing this script. This allows the
# script to be run from any working directory and still have it place downloaded
# files in the right locations.
SCRIPT_DIRECTORY=$(dirname ${BASH_SOURCE})

# If JDK_URL is set, download the JDK into the hbase_docker folder.
if [ -n "${JDK_URL}" ]; then
  echo "Downloading Oracle JDK..."
  ORACLE_HEADER="Cookie: oraclelicense=accept-securebackup-cookie"
  if ! wget -nv -N --header "${ORACLE_HEADER}" -P "${SCRIPT_DIRECTORY}/hbase_docker" \
      "${JDK_URL}"; then
    echo "Error downloading Oracle JDK. Exiting..." >&2
    exit 1
  fi
fi

# If MAVEN_URL is set, download Maven into the hbase_docker folder.
if [ -n "${MAVEN_URL}" ]; then
  echo "Downloading Apache Maven..."
  if ! wget -nv -N -P "${SCRIPT_DIRECTORY}/hbase_docker" "${MAVEN_URL}"; then
    echo "Error downloading Apache Maven. Exiting..." >&2
    exit 1
  fi
fi

# Before running docker build, confirm that the hbase_docker folder contains
# one JDK .tar.gz and one Maven .tar.gz.
FILE_CHECK_EXIT_CODE=0
for file in jdk maven; do
  NUM_FILES=$(ls -l "${SCRIPT_DIRECTORY}/hbase_docker/"*${file}*.tar.gz 2>/dev/null | \
      wc -l)
  if [ ${NUM_FILES} -eq 0 ]; then
    echo "Could not detect tarball containing \"${file}\" in hbase_docker folder." >&2
    FILE_CHECK_EXIT_CODE=1
  elif [ ${NUM_FILES} -gt 1 ]; then
    echo "There are too many files containing \"${file}\" in hbase_docker folder." >&2
    FILE_CHECK_EXIT_CODE=1
  fi
done
if [ ${FILE_CHECK_EXIT_CODE} -ne 0 ]; then
  echo "Required dependencies not satisfied. Exiting..." >&2
  exit 1
fi

# docker build can be buggy (e.g. see https://github.com/docker/docker/issues/4036).
# To get around this, this script will try doing up to ${MAX_BUILD_ATTEMPTS} builds.
BUILD_ATTEMPTS=0
MAX_BUILD_ATTEMPTS=10
echo "Beginning docker build..."
until docker build -t ${IMAGE_NAME} ${SCRIPT_DIRECTORY}/hbase_docker; do
  ((++BUILD_ATTEMPTS))
  if [ ${BUILD_ATTEMPTS} -ge ${MAX_BUILD_ATTEMPTS} ]; then
    echo "Build of ${IMAGE_NAME} failed after ${BUILD_ATTEMPTS} attempts. Exiting..." >&2
    exit 1
  else
    echo "Build attempt #${BUILD_ATTEMPTS} of ${IMAGE_NAME} was unsuccessful. Retrying..."
  fi
done

echo "Successfully built ${IMAGE_NAME}."

echo "Starting hbase shell..."
docker run -it ${IMAGE_NAME}
