#!/bin/bash

LANG="java"
SOURCE="LegacyHBase.thrift"
THRIFT="thrift-apache"
OUTPUT="../../../../../../java"
PKG_PATH="org/apache/hadoop/hbase/thrift/generated"
LOCAL_DIR="gen-$LANG"
VERSION="Thrift version 0.9.1"

# check thrift version
if [ "`$THRIFT -version`" != "$VERSION" ]; then
  echo "Please use thrift with the version: $VERSION"
  exit 1
fi

rm -rf $LOCAL_DIR
$THRIFT --gen $LANG:hashcode $SOURCE
rm -rf $OUTPUT/$PKG_PATH/*
mv $LOCAL_DIR/$PKG_PATH/* $OUTPUT/$PKG_PATH/
rm -rf $LOCAL_DIR
