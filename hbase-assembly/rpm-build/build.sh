#!/bin/bash
set -e
set -x

RPM_DIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# If not specified, extract the version.
if [[ "X$HBASE_VERSION" = "X" ]]; then
    echo "Must specifiy \$HBASE_VERSION"
    exit 1
fi

# Setup scratch dir
SCRATCH_DIR="${RPM_DIR}/scratch"

rm -rf $SCRATCH_DIR
mkdir -p ${SCRATCH_DIR}/{SOURCES,SPECS,RPMS,SRPMS}
cp -a $RPM_DIR/sources/* ${SCRATCH_DIR}/SOURCES/
cp $RPM_DIR/hbase.spec ${SCRATCH_DIR}/SPECS/

# Set up src dir
SRC_DIR="${RPM_DIR}/hbase-$HBASE_VERSION"
TAR_NAME=hbase-$HBASE_VERSION.tar.gz

rm -rf $SRC_DIR
rsync -a $RPM_DIR/../../ $SRC_DIR --exclude rpm-assembly --exclude .git

pushd $SRC_DIR/../
tar -czf ${SCRATCH_DIR}/SOURCES/${TAR_NAME} $(basename $SRC_DIR)
popd

# Build srpm

rpmbuild \
    --define "_topdir $SCRATCH_DIR" \
    --define "input_tar $TAR_NAME" \
    --define "mvn_target_dir $RPM_DIR/../target" \
    --define "hbase_version ${HBASE_VERSION}" \
    --define "release ${PKG_RELEASE}%{?dist}" \
    -bs --nodeps --buildroot="${SCRATCH_DIR}/INSTALL" \
    ${SCRATCH_DIR}/SPECS/hbase.spec

src_rpm=$(ls -1 ${SCRATCH_DIR}/SRPMS/hbase-*)

# build rpm

rpmbuild \
    --define "_topdir $SCRATCH_DIR" \
    --define "input_tar $TAR_NAME" \
    --define "mvn_target_dir $RPM_DIR/../target" \
    --define "hbase_version ${HBASE_VERSION}" \
    --define "release ${PKG_RELEASE}%{?dist}" \
    --rebuild $src_rpm

if [[ -d $RPMS_OUTPUT_DIR ]]; then
    mkdir -p $RPMS_OUTPUT_DIR

    # Move rpms to output dir for upload

    find ${SCRATCH_DIR}/{SRPMS,RPMS} -name "*.rpm" -exec mv {} $RPMS_OUTPUT_DIR/ \;
fi
