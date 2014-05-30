#!/usr/bin/env bash

set -e -x

MVNCMD=${MVNCMD:-mvn}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HBASE_HOME=${DIR}/../../

if [ -d "${HBASE_HOME}/.git" ]; then
	VERSION=`${DIR}/generate_version.py --pom ${HBASE_HOME}/pom.xml --git_svn`
else
	VERSION=`${DIR}/generate_version.py --pom ${HBASE_HOME}/pom.xml --svn`
fi

echo $VERSION

BASE_REPO=http://nexus.vip.facebook.com:8181/nexus/content/repositories

if [[ $VERSION == *-SNAPSHOT* ]]; then
    REPO_NAME=libs-snapshots-local
else
    REPO_NAME=libs-releases-local
fi

REPO_URL=${BASE_REPO}/${REPO_NAME}


$MVNCMD versions:set -DnewVersion=${VERSION}
$MVNCMD clean package assembly:single deploy -DskipTests -DaltDeploymentRepository=${REPO_NAME}::default::${REPO_URL}
