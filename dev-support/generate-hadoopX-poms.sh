#!/bin/bash
##
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
##

# Generates a pom.xml with a haoop1 or a hadoop2 suffix which can
# then be used for generating hadoop1 or hadoop2 suitable hbase's
# fit for publishing to a maven repository. Use these poms also
# making tarballs to release.  The original pom.xml is untouched.
#
# This script exists because we cannot figure how to publish
# into the local (or remote) repositories artifacts that can be
# used by downstream projects (with poms describing necessary
# includes).  See HBASE-8224 and HBASE-8488 for background.
#
# Generation is done by replacing values in original pom and
# enabling appropriate profile using the '!' trick in the
# hbase.profile property (this is fragile!) so no need to specify
# profile on the command line.  The original pom.xml should be
# what we maintain adding in new depdencies, etc., as needed.
# This script should make it through most any change to the
# original.
#
# Here is how you would build an hbase against hadoop2 and publish
# the artifacts to your local repo:
#
# First run this script passing in current project version and what
# version you would like the generated artifacts to have.  Include
# either -hadoop1 if built against hadoop1 or -hadoop2 if build against
# hadoop2.  These substrings are expected as part of the new version.
#
#  $ bash -x ./dev-support/generate-hadoopX-poms.sh 0.95.2-SNAPSHOT 0.95.2-hadoop2-SNAPSHOT
#
# This will generate new poms beside current pom.xml made from the
# origin pom.xml but with a hadoop1 or hadoop2 suffix dependent on
# what you passed for a new version.  Now build passing generated
# pom name as the pom mvn should use.  For example, say we were building
# hbase against hadoop2:
#
#$ mvn clean install -DskipTests -f pom.xml.hadoop2
#

# TODO: Generate new poms into target dirs so doesn't pollute src tree.
# It is a little awkward to do since parent pom needs to be able to find
# the child modules and the child modules need to be able to get to the
# parent.

function usage {
  echo "Usage: $0 CURRENT_VERSION NEW_VERSION"
  echo "For example, $0 0.95.2-SNAPSHOT 0.95.2-hadoop1-SNAPSHOT"
  echo "Presumes VERSION has hadoop1 or hadoop2 in it."
  exit 1
}

if [[ "$#" -ne 2 ]]; then usage; fi
old_hbase_version="$1"
new_hbase_version="$2"
# Get hadoop version from the new hbase version
hadoop_version=`echo "$new_hbase_version" | sed -n 's/.*-\(hadoop[12]\).*/\1/p'`
if [[ -z $hadoop_version ]]; then usage ; fi

# Get dir to operate in
hbase_home="${HBASE_HOME}"
if [[ -z "$hbase_home" ]]; then
  here="`dirname \"$0\"`"              # relative
  here="`( cd \"$here\" && pwd )`"  # absolutized and normalized
  if [ -z "$here" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
  fi
  hbase_home="`dirname \"$here\"`"
fi

# Now figure which profile to activate.
h1=
h2=
default='<name>!hadoop.profile<\/name>'
notdefault='<name>hadoop.profile<\/name>'
case "${hadoop_version}" in
  hadoop1)
    h1="${default}"
    h2="${notdefault}<value>2.0<\/value>"
    ;;
  hadoop2)
    h1="${notdefault}<value>1.1<\/value>"
    h2="${default}"
    ;;
 *) echo "Unknown ${hadoop_version}"
    usage
    ;;
esac

pom=pom.xml
nupom="$pom.$hadoop_version"
poms=`find $hbase_home -name ${pom}`
for p in $poms; do
  nuname="`dirname $p`/${nupom}"
  # Now we do search and replace of explicit strings.  The best
  # way of seeing what the below does is by doing a diff between
  # the original pom and the generated pom (pom.xml.hadoop1 or
  # pom.xml.hadoop2). We replace the compat.module variable with
  # either hbase-hadoop1-compat or hbase-hadoop2-compat, we
  # replace the version string in all poms, we change modules
  # to include reference to the non-standard pom name, we
  # adjust relative paths so child modules can find the parent pom,
  # and we enable/disable hadoop 1 and hadoop 2 profiles as
  # appropriate removing a comment string too.  We output the
  # new pom beside the original.
  sed -e "s/\${compat.module}/hbase-${hadoop_version}-compat/" \
    -e "s/${old_hbase_version}/${new_hbase_version}/" \
    -e "s/\(<module>[^<]*\)/\1\/${nupom}/" \
    -e "s/\(relativePath>\.\.\)/\1\/${nupom}/" \
    -e "s/<!--h1-->.*name>.*/${h1}/" \
    -e "s/<!--h2-->.*<name>.*/${h2}/" \
    -e '/--Below formatting for .*poms\.sh--/d' \
    -e 's/\(<pomFileName>\)[^<]*/\1${nupom}/' \
  $p > "$nuname"
done
