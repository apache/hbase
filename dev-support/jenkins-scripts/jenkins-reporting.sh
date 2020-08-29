#!/bin/bash
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

set -e
function usage {
  echo "Usage: ${0} [options]"
  echo ""
  echo "    --working-dir /path/to/use     Path for writing logs and such."
  echo "                                   defaults to making a directory via mktemp."
  echo "    --base-dir /path/to/check      Path to summarize. defaults to current dir."
  echo "    --output /path/to/report.html  where to write results. defaults to 'report.html' in"
  echo "                                   the working-dir."
  echo "    --help                         show this usage message."
  exit 1
}

# Get arguments
declare working_dir
declare base_dir
declare output
while [ $# -gt 0 ]
do
  case "$1" in
    --working-dir) shift; working_dir=$1; shift;;
    --base-dir) shift; base_dir=$1; shift;;
    --output) shift; output=$1; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

if [ -z "${base_dir}" ]; then
  base_dir="$(pwd)"
else
  # absolutes please
  base_dir="$(cd "$(dirname "${base_dir}")"; pwd)/$(basename "${base_dir}")"
  if [ ! -d "${base_dir}" ]; then
    echo "passed base directory '${base_dir}' must already exist."
    exit 1
  fi
fi

if [ -z "${working_dir}" ]; then
  echo "[DEBUG] defaulting to creating a directory via mktemp"
  if ! working_dir="$(mktemp -d -t hbase-jenkins-reporting)" ; then
    echo "Failed to create temporary working directory. Please specify via --working-dir"
    exit 1
  fi
else
  # absolutes please
  working_dir="$(cd "$(dirname "${working_dir}")"; pwd)/$(basename "${working_dir}")"
  if [ ! -d "${working_dir}" ]; then
    echo "passed working directory '${working_dir}' must already exist."
    exit 1
  fi
fi

echo "You'll find logs and temp files in ${working_dir}"


if [ -z "${output}" ]; then
  echo "[DEBUG] defaulting to creating 'report.html' within '${working_dir}'"
  output="${working_dir}/report.html"
else
  if [ ! -d "$(dirname "${output}")" ]; then
    echo "passed output file must be in a directory that exists."
    exit 1
  fi
  # absolutes please
  output="$(cd "$(dirname "${output}")"; pwd)/$(basename "${output}")"
fi
cd "${working_dir}"
cat >"${output}" <<EOF
<html>
  <body>
  <pre>
$(ls -lR "${base_dir}")
  </pre>
  </body>
</html>
EOF
