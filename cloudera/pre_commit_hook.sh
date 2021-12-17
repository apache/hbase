#!/bin/bash

set -vx

export JAVA_HOME=$JAVA_1_8_HOME
export PATH=${JAVA_HOME}/bin:${MAVEN_3_5_0_HOME}/bin:$PATH

WORKSPACE=${WORKSPACE:-"."}

echo "GIT_COMMIT (hash of push under review):   $GIT_COMMIT"
echo "GERRIT_BRANCH (branch name to commit to): $GERRIT_BRANCH"
echo "GERRIT_REFSPEC (Gerrit review):           $GERRIT_REFSPEC"
echo "WORKSPACE (Location of local repo):       $WORKSPACE"

cd "${WORKSPACE}"
mvn --settings "${WORKSPACE}/cloudera/settings.xml" clean package -fae -nsu -B -Dhttps.protocols=TLSv1.2 -Dhadoop.profile=3.0 -T2 -PrunAllTests

RETURN=${?}

if [[ ${RETURN} -ne 0 ]]; then
  exit 1
fi

exit 0
