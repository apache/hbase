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

# Script that assembles all you need to make an RC.
# Does build of the tar.gzs which it stashes into a dir above $HBASE_HOME
# named for the script with a timestamp suffix.
# Deploys builds to maven.
#
# To finish, check what was build.  If good copy to people.apache.org and
# close the maven repos.  Call a vote. 
#
# Presumes that dev-support/generate-hadoopX-poms.sh has already been run.
# Presumes your settings.xml all set up so can sign artifacts published to mvn, etc.

set -e

devsupport=`dirname "$0"`
devsupport=`cd "$devsupport">/dev/null; pwd`
HBASE_HOME=`cd ${devsupport}/.. > /dev/null; pwd`

# Set mvn and mvnopts
mvn=mvn
if [ "$MAVEN" != "" ]; then
  mvn="${MAVEN}"
fi
mvnopts="-Xmx3g"
if [ "$MAVEN_OPTS" != "" ]; then
  mvnopts="${MAVEN_OPTS}"
fi

# Make a dir to save tgzs in.
d=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
archivedir="${HBASE_HOME}/../`basename $0`.$d"
echo "Archive dir ${archivedir}"
mkdir -p "${archivedir}" 

function tgz_mover {
  mv "${HBASE_HOME}"/hbase-assembly/target/hbase-*.tar.gz "${archivedir}"
}

function deploy {
  MAVEN_OPTS="${mvnopts}" ${mvn} -f pom.xml.$1 clean install -DskipTests -Prelease
  MAVEN_OPTS="${mvnopts}" ${mvn} -f pom.xml.$1 install -DskipTests site assembly:single -Prelease 
  tgz_mover
  MAVEN_OPTS="${mvnopts}" ${mvn} -f pom.xml.$1 deploy -DskipTests -Papache-release 
}

# Build src tarball
MAVEN_OPTS="${mvnopts}" ${mvn} clean install -DskipTests assembly:single -Dassembly.file="${HBASE_HOME}/hbase-assembly/src/main/assembly/src.xml" -Prelease
tgz_mover

# Now do the two builds,  one for hadoop1, then hadoop2
deploy "hadoop1"
deploy "hadoop2"

echo "DONE"
echo "Check the content of ${archivedir}.  If good, sign and push to people.apache.org"
echo " cd ${archivedir}"
echo ' for i in *.tar.gz; do echo $i; gpg --print-mds $i > $i.mds ; done'
echo ' for i in *.tar.gz; do echo $i; gpg --armor --output $i.asc --detach-sig $i  ; done'
echo ' rsync -av ${archivedir} people.apache.org:public_html/hbase-VERSION'
echo "Check the content deployed to maven.  If good, close the repo and record links of temporary staging repo"
echo "If all good tag the RC"
