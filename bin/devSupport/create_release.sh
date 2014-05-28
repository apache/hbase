#!/usr/bin/env bash

set -e -x

MVNCMD=${MVNCMD:-mvn}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HBASE_HOME=${DIR}/../../

VERSION=`${DIR}/generate_version.py --pom ${HBASE_HOME}/pom.xml --svn`
echo $VERSION

$MVNCMD versions:set -DnewVersion=${VERSION}
$MVNCMD clean package assembly:single deploy -DskipTests