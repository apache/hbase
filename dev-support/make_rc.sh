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

# Script that assembles all you need to make an RC. Does build of the tar.gzs
# which it stashes into a dir above $(pwd) named for the script with a
# timestamp suffix. Deploys builds to maven.
#
# To finish, check what was build.  If good copy to people.apache.org and
# close the maven repos.  Call a vote. 
#
# Presumes that dev-support/generate-hadoopX-poms.sh has already been run.
# Presumes your settings.xml all set up so can sign artifacts published to mvn, etc.

set -e -x

# Script checks out a tag, cleans the checkout and then builds src and bin
# tarballs. It then deploys to the apache maven repository.
# Presumes run from git dir.

# Need a git tag to build.
if [ "$1" = "" ]
then
  echo -n "Usage: $0 TAG_TO_PACKAGE"
  exit 1
fi
git_tag=$1

# Set mvn and mvnopts
mvn=mvn
if [ "$MAVEN" != "" ]; then
  mvn="${MAVEN}"
fi
mvnopts="-Xmx3g"
if [ "$MAVEN_OPTS" != "" ]; then
  mvnopts="${MAVEN_OPTS}"
fi

# Ensure we are inside a git repo before making progress
# The below will fail if outside git.
git -C . rev-parse

# Checkout git_tag
git checkout "${git_tag}"

# Get mvn protject version
#shellcheck disable=SC2016
version=$(${mvn} -q -N -Dexec.executable="echo" -Dexec.args='${project.version}' exec:exec)
hbase_name="hbase-${version}"

# Make a dir to save tgzs into.
d=`date -u +"%Y%m%dT%H%M%SZ"`
output_dir="/${TMPDIR}/$hbase_name.$d"
mkdir -p "${output_dir}"


# Build src tgz.
function build_src {
  git archive --format=tar.gz --output="${output_dir}/${hbase_name}-src.tar.gz" --prefix="${hbase_name}/" "${git_tag}"
}

# Build bin tgz
function build_bin {
  MAVEN_OPTS="${mvnopts}" ${mvn} clean install -DskipTests \
    -Papache-release -Prelease \
    -Dmaven.repo.local=${output_dir}/repository
  MAVEN_OPTS="${mvnopts}" ${mvn} install -DskipTests \
    -Dcheckstyle.skip=true site assembly:single \
    -Papache-release -Prelease \
    -Dmaven.repo.local=${output_dir}/repository
  mv ./hbase-assembly/target/hbase-*.tar.gz "${output_dir}"
}

# Make sure all clean.
git clean -f -x -d
MAVEN_OPTS="${mvnopts}" ${mvn} clean

# Now do the two builds,  one for hadoop1, then hadoop2
# Run a rat check.
${mvn} apache-rat:check

#Build src.
build_src

# Build bin product
build_bin

# Deploy to mvn repository
# Depends on build_bin having populated the local repository
# If the below upload fails, you will probably have to clean the partial
# upload from repository.apache.org by 'drop'ping it from the staging
# repository before restart.
MAVEN_OPTS="${mvnopts}" ${mvn} deploy -DskipTests -Papache-release -Prelease \
    -Dmaven.repo.local=${output_dir}/repository

# Do sha512 and md5
cd ${output_dir}
for i in *.tar.gz; do echo $i; gpg --print-md SHA512 $i > $i.sha512 ; done

echo "Check the content of ${output_dir}.  If good, sign and push to dist.apache.org"
echo " cd ${output_dir}"
echo ' for i in *.tar.gz; do echo $i; gpg --armor --output $i.asc --detach-sig $i  ; done'
echo " rsync -av ${output_dir}/*.gz ${output_dir}/*.sha512 ${output_dir}/*.asc ${APACHE_HBASE_DIST_DEV_DIR}/${hbase_name}/"
echo "Check the content deployed to maven.  If good, close the repo and record links of temporary staging repo"
