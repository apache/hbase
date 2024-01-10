#!/bin/bash
set -e
set -x

ROOT_DIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

for iv in HBASE_VERSION SET_VERSION PKG_RELEASE; do
    if [[ "X${!iv}" = "X" ]]; then
        echo "Must specifiy $iv"
        exit 1
    fi
done

# Setup build dir
BUILD_DIR="${ROOT_DIR}/build"
rm -rf $BUILD_DIR
mkdir -p ${BUILD_DIR}/{SOURCES,SPECS,RPMS}
cp -a $ROOT_DIR/sources/* ${BUILD_DIR}/SOURCES/
cp $ROOT_DIR/hbase.spec ${BUILD_DIR}/SPECS/

# Download bin tar built by hbase-assembly
SOURCES_DIR=$BUILD_DIR/SOURCES
mvn dependency:copy \
    -Dartifact=org.apache.hbase:hbase-assembly:${SET_VERSION}:tar.gz:bin \
    -DoutputDirectory=$SOURCES_DIR \
    -DlocalRepositoryDirectory=$SOURCES_DIR \
    -Dtransitive=false
INPUT_TAR=`ls -d $SOURCES_DIR/hbase-assembly-*.tar.gz`

if [[ $HBASE_VERSION == *"-SNAPSHOT" ]]; then
    # unreleased verion. do i want to denote that in the rpm release somehow?
    # it can't be in the version, so strip here
    HBASE_VERSION=${HBASE_VERSION//-SNAPSHOT/}
fi

rpmbuild \
    --define "_topdir $BUILD_DIR" \
    --define "input_tar $INPUT_TAR" \
    --define "hbase_version ${HBASE_VERSION}" \
    --define "maven_version ${SET_VERSION}" \
    --define "release ${PKG_RELEASE}%{?dist}" \
    -bb \
    $BUILD_DIR/SPECS/hbase.spec

if [[ -d $RPMS_OUTPUT_DIR ]]; then
    mkdir -p $RPMS_OUTPUT_DIR

    # Move rpms to output dir for upload

    find ${BUILD_DIR}/RPMS -name "*.rpm" -exec mv {} $RPMS_OUTPUT_DIR/ \;
fi
