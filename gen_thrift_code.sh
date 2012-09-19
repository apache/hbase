#!/bin/bash

set -e -u -o pipefail -x

if [ ! -f pom.xml ]; then
  echo "Have to run from HBase directory" >&2
  exit 1
fi

THRIFT=thrift

IF=src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift

set +e
THRIFT_VERSION=`$THRIFT -version`
set -e

EXPECTED_THRIFT_VERSION="Thrift version 0.8.0"
if [ "$THRIFT_VERSION" != "$EXPECTED_THRIFT_VERSION" ]; then
  echo "Expected $EXPECTED_THRIFT_VERSION, got $THRIFT_VERSION" >&2
  exit 1
fi

THRIFT_VERSION=`echo $THRIFT_VERSION`

$THRIFT --gen java -out src/main/java $IF

echo "Automatically changing IOError's superclass"
F=src/main/java/org/apache/hadoop/hbase/thrift/generated/IOError.java 
awk '{ sub(/extends Exception/, "extends java.io.IOException"); print }' <$F \
  >$F.new
set +e
diff $F $F.new
if [ $? -eq 0 ]; then
  echo "Failed to replace 'extends Exception'" >&2
  exit 1
fi
set -e
mv -f $F.new $F

