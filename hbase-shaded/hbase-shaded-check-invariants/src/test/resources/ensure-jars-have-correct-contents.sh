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

# Usage: $0 [/path/to/some/example.jar:/path/to/another/example/created.jar]
#
# accepts a single command line argument with a colon separated list of
# paths to jars to check. Iterates through each such passed jar and checks
# all the contained paths to make sure they follow the below constructed
# safe list.

# we have to allow the directories that lead to the org/apache/hadoop dir
allowed_expr="(^org/$|^org/apache/$"
# We allow the following things to exist in our client artifacts:
#   * classes in packages that start with org.apache.hadoop, which by
#     convention should be in a path that looks like org/apache/hadoop
allowed_expr+="|^org/apache/hadoop/"
#   * classes in packages that start with org.apache.hbase
allowed_expr+="|^org/apache/hbase/"
#   * whatever in the "META-INF" directory
allowed_expr+="|^META-INF/"
#   * the folding tables from jcodings
allowed_expr+="|^tables/"
#   * contents of hbase-webapps
allowed_expr+="|^hbase-webapps/"
#   * Hadoop's and HBase's default configuration files, which have the form
#     "_module_-default.xml"
allowed_expr+="|^[^-]*-default.xml$"
#   * Hadoop's versioning properties files, which have the form
#     "_module_-version-info.properties"
allowed_expr+="|^[^-]*-version-info.properties$"
#   * Hadoop's application classloader properties file.
allowed_expr+="|^org.apache.hadoop.application-classloader.properties$"
# public suffix list used by httpcomponents
allowed_expr+="|^mozilla/$"
allowed_expr+="|^mozilla/public-suffix-list.txt$"
# Comes from commons-configuration, not sure if relocatable.
allowed_expr+="|^digesterRules.xml$"
allowed_expr+="|^properties.dtd$"
allowed_expr+="|^PropertyList-1.0.dtd$"


allowed_expr+=")"
declare -i bad_artifacts=0
declare -a bad_contents
IFS=: read -r -d '' -a artifact_list < <(printf '%s\0' "$1")
for artifact in "${artifact_list[@]}"; do
  bad_contents=($(jar tf "${artifact}" | grep -v -E "${allowed_expr}"))
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
