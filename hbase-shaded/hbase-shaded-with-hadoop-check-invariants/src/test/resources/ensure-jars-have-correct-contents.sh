#!/usr/bin/env bash
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
  echo "Usage: ${0} [options] [/path/to/some/example.jar:/path/to/another/example/created.jar]"
  echo ""
  echo "  accepts a single command line argument with a colon separated list of"
  echo "  paths to jars to check. Iterates through each such passed jar and checks"
  echo "  all the contained paths to make sure they follow the below constructed"
  echo "  safe list."
  echo ""
  echo "    --allow-hadoop     Include stuff from the Apache Hadoop project in the list"
  echo "                       of allowed jar contents. default: false"
  echo "    --debug            print more info to stderr"
  exit 1
}
# if no args specified, show usage
if [ $# -lt 1 ]; then
  usage
fi

# Get arguments
declare allow_hadoop
declare debug
while [ $# -gt 0 ]
do
  case "$1" in
    --allow-hadoop) shift; allow_hadoop="true";;
    --debug) shift; debug="true";;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

if [ -z "${JAVA_HOME}" ]; then
  echo "[ERROR] Must have JAVA_HOME defined." 1>&2
  exit 1
fi
JAR="${JAVA_HOME}/bin/jar"

# should still have jars to check.
if [ $# -lt 1 ]; then
  usage
fi
if [ -n "${debug}" ]; then
  echo "[DEBUG] Checking on jars: $*" >&2
  echo "jar command is: $(which jar)" >&2
  echo "grep command is: $(which grep)" >&2
  grep -V >&2 || true
fi

IFS=: read -r -d '' -a artifact_list < <(printf '%s\0' "$1")

# we have to allow the directories that lead to the hbase dirs
allowed_expr="(^org/$|^org/apache/$|^org/apache/hadoop/$"
# We allow the following things to exist in our client artifacts:
#   * classes in packages that start with org.apache.hadoop.hbase, which by
#     convention should be in a path that looks like org/apache/hadoop/hbase
allowed_expr+="|^org/apache/hadoop/hbase"
#   * classes in packages that start with org.apache.hbase
allowed_expr+="|^org/apache/hbase/"
#   * whatever in the "META-INF" directory
allowed_expr+="|^META-INF/"
#   * the folding tables from jcodings
allowed_expr+="|^tables/"
#   * contents of hbase-webapps
allowed_expr+="|^hbase-webapps/"
#   * HBase's default configuration files, which have the form
#     "_module_-default.xml"
allowed_expr+="|^hbase-default.xml$"
# public suffix list used by httpcomponents
allowed_expr+="|^mozilla/$"
allowed_expr+="|^mozilla/public-suffix-list.txt$"
# Comes from commons-configuration, not sure if relocatable.
allowed_expr+="|^digesterRules.xml$"
allowed_expr+="|^properties.dtd$"
allowed_expr+="|^PropertyList-1.0.dtd$"
# Shaded jetty resources
allowed_expr+="|^about.html$"
allowed_expr+="|^jetty-dir.css$"


if [ -n "${allow_hadoop}" ]; then
  #   * classes in packages that start with org.apache.hadoop, which by
  #     convention should be in a path that looks like org/apache/hadoop
  allowed_expr+="|^org/apache/hadoop/"
  #   * Hadoop's default configuration files, which have the form
  #     "_module_-default.xml"
  allowed_expr+="|^[^-]*-default.xml$"
  #   * Hadoop's versioning properties files, which have the form
  #     "_module_-version-info.properties"
  allowed_expr+="|^[^-]*-version-info.properties$"
  #   * Hadoop's application classloader properties file.
  allowed_expr+="|^org.apache.hadoop.application-classloader.properties$"
else
  # We have some classes for integrating with the Hadoop Metrics2 system
  # that have to be in a particular package space due to access rules.
  allowed_expr+="|^org/apache/hadoop/metrics2"
fi


allowed_expr+=")"
declare -i bad_artifacts=0
declare -a bad_contents
for artifact in "${artifact_list[@]}"; do
  bad_contents=($("${JAR}" tf "${artifact}" | grep -v -E "${allowed_expr}" || true))
  class_count=$("${JAR}" tf "${artifact}" | grep -c -E '\.class$' || true)
  if [ ${#bad_contents[@]} -eq 0 ] && [ "${class_count}" -lt 1 ]; then
    bad_contents=("The artifact contains no java class files.")
  fi
  if [ ${#bad_contents[@]} -gt 0 ]; then
    echo "[ERROR] Found artifact with unexpected contents: '${artifact}'"
    echo "    Please check the following and either correct the build or update"
    echo "    the allowed list with reasoning."
    echo ""
    for bad_line in "${bad_contents[@]}"; do
      echo "    ${bad_line}"
    done
    bad_artifacts=${bad_artifacts}+1
  else
    echo "[INFO] Artifact looks correct: '$(basename "${artifact}")'"
  fi
done

# if there was atleast one bad artifact, exit with failure
if [ "${bad_artifacts}" -gt 0 ]; then
  exit 1
fi
