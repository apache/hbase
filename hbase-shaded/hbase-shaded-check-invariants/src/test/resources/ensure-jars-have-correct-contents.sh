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
  echo "    --debug            print more info to stderr"
  exit 1
}
# if no args specified, show usage
if [ $# -lt 1 ]; then
  usage
fi

# Get arguments
declare debug
while [ $# -gt 0 ]
do
  case "$1" in
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

# Set up the global allow list of class names. Every artifact is checked against
# this allow list. Classes that don't match the list will be printed out as errors
# and fail the build. This is meant to avoid including unexpected classes in our shaded jars.
#
# Since this list applies to every artifact, only add here if you want a pattern to be
# allowed in all artifacts. If you want a pattern to only be allowed in a particular artifact,
# check out the functions: check_packages_marker_classes and check_all_classes.
# The first one shows how to smoke test a jar for specific classes, and the second one
# shows how (for hadoop) to add classes to the allowlist conditionally based on the artifact.

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

# Generates an expected_packages string for the given artifact.
# The current possible expected packages are: mapreduce, server, hadoop, client
# What packages are expected for an artifact dictate which smoke tests we
# apply on the artifact's jar and possibly modify the allowlist in check_all_classes.
# This gives us extra confidence that our shading configs are producing artifacts with
# the correct sets of hbase classes, in addition to the standard dependencies.
function get_expected_packages() {
  # artifacts come to us with the path rooted in hbase-shaded, so go up two levels to get
  # the artifact name
  local target_dir artifact_dir artifact_name
  target_dir=$(dirname "${1}")
  artifact_dir=$(dirname "${target_dir}")
  # now get the basename to remove any further parent dirs
  artifact_name=$(basename "${artifact_dir}")

  case "$artifact_name" in
    hbase-shaded-mapreduce         ) echo "mapreduce,server";;
    hbase-shaded-client            ) echo "client,hadoop";;
    hbase-shaded-client-byo-hadoop ) echo "client";;
    *                              )
      echo "[ERROR] Unexpected artifact name $artifact_name. Should have matched one of:"
      echo "    hbase-shaded-mapreduce, hbase-shaded-client, hbase-shaded-client-byo-hadoop"
      echo "    If this is a new artifact, please add to the list."
      exit 1
  esac
}

# Checks that an artifact contains a specific class pattern. If it doesn't,
# prints an error and returns non-success
function ensure_matches() {
  local artifact=$1
  local package=$2
  local pattern=$3
  local -a matches
  matches=($("${JAR}" tf "${artifact}" | grep -E "${pattern}" || true))

  if [ ${#matches[@]} -eq 0 ]; then
    echo "[ERROR] Expected package '$package', but was missing matches for artifact:"
    echo "    '$artifact' "
    echo "    Expected pattern that was not found: '$pattern'."
    echo "    Please check the build to ensure the correct classes are being bundled."
    return 1
  fi
}

# Smoke test which verifies that certain expected classes
# either are or are not in the jar, based on the expected packages
# for that jar. Returns non-success if any check fails.
function check_packages_marker_classes() {
  set -e
  local artifact=$1
  local expected_packages=$2

  # for hadoop, we only check existence
  # we don't need the disallowed_expr check because it's already
  # handled by the global allowlist checking in check_all_classes.
  # the only reason we need this for hbase classes is because
  # org/apache/hbase is globally allowed and grep regex does
  # not allow negative lookaheads
  local hadoop_marker="org/apache/hadoop/fs/FileSystem.class$"
  if [[ "$expected_packages" =~ .*"hadoop".* ]]; then
    ensure_matches "$artifact" "hadoop" "$hadoop_marker"
  fi

  local disallowed_expr=""

  local client_marker="^org/apache/hadoop/hbase/client/Scan.class$"
  if [[ "$expected_packages" =~ .*"client".* ]]; then
    ensure_matches "$artifact" "client" "$client_marker"
  else
    disallowed_expr+="|${client_marker}"
  fi

  local server_marker="^org/apache/hadoop/hbase/regionserver/HRegion.class$"
  if [[ "$expected_packages" =~ .*"server".* ]]; then
    ensure_matches "$artifact" "server" "$server_marker"
  else
    disallowed_expr+="|${server_marker}"
  fi

  local mapreduce_marker="^org/apache/hadoop/hbase/mapred/Driver.class$"
  if [[ "$expected_packages" =~ .*"mapreduce".* ]]; then
    ensure_matches "$artifact" "mapreduce" "$mapreduce_marker"
  else
    disallowed_expr+="|${mapreduce_marker}"
  fi

  # remove the leading '|'
  disallowed_expr="${disallowed_expr:1}"

  if [[ -n "$disallowed_expr" ]]; then
    local -a matches
    matches=($("${JAR}" tf "${artifact}" | grep -E "${disallowed_expr}" || true))
    if [ ${#matches[@]} -gt 0 ]; then
      echo "[ERROR] Passed expected packages [ $expected_packages ] but had unexpected matches"
      echo "    for artifact: '$artifact'. "
      echo "    Please check the following and either add expected package or fix build."
      echo ""
      for bad_line in "${matches[@]}"; do
        echo "    ${bad_line}"
      done
      return 1
    fi
  fi
}

# Checks all classes against the passed in allowlist, returning
# any bad contents which don't match the allowlist.
# The allowlist is updated to include hadoop based on the expected_packages for
# the artifact in question.
function check_all_classes() {
  local artifact=$1
  local allowed_expr=$2
  local expected_packages=$3

  if [[ "$expected_packages" =~ .*"hadoop".* ]]; then
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

  # close the parenthesis that was originally opened at the
  # declaration of the original allowed_expr at the top of the file
  allowed_expr+=")"

  "${JAR}" tf "${artifact}" | grep -v -E "${allowed_expr}" || true
}

declare -i bad_artifacts=0
declare -a bad_contents
for artifact in "${artifact_list[@]}"; do
  expected_packages=$(get_expected_packages "$artifact")
  check_packages_marker_classes "$artifact" "$expected_packages"

  bad_contents=($(check_all_classes "$artifact" "$allowed_expr" "$expected_packages"))
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
