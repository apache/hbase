#!/usr/bin/env bash
#
#/**
# * Copyright The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
#


MVNCMD=mvn
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TEST=
ITERATIONS=25
KEEP=0

function usage {
echo "${0} [options]"
echo ""
echo "OPTIONS:"
echo "          -h      Help"
echo "          -k      Keep test output even if it all passes"
echo "          -t      Specify test to run"
echo "          -n      Specify number of tests to run.  Default 25"
}

while getopts “hkt:n:” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         k)
             KEEP=1
             ;;
         t)
             TEST=$OPTARG
             ;;
         n)
             ITERATIONS=$OPTARG
             ;;
         ?)
             usage
             exit
             ;;
     esac
done

pushd $DIR/../../

CMD="$MVNCMD test"
if [ $TEST ]; then
    CMD="$CMD -Dtest=$TEST"
fi

TEMPDIR=`mktemp --directory -t hbase-tests.XXXXXXX 2>/dev/null || mktemp -d -t 'hbase-tests'`
echo "Logs will be here: ${TEMPDIR}"

$MVNCMD clean package -DskipTests 1>> ${TEMPDIR}/mvn.log.0 2>&1
rm -rf target/surefire-reports/
FAILED=0



for i in `seq 1 $ITERATIONS`;
do
    $CMD 1>> ${TEMPDIR}/mvn.log.$i 2>&1
    RESULT=$?
    TARGET="${TEMPDIR}/${i}"
    if [ $RESULT -ne 0 ]; then
        TARGET="${TARGET}-FAILED"
        FAILED=$((FAILED+1))
    fi

    if [ -d target/surefire-reports ]; then
        mv target/surefire-reports "${TARGET}"
    fi
done
echo "Failed $FAILED of $ITERATIONS"

if [ $FAILED -ne 0 ] || [ $KEEP -ne 0 ]; then
    echo "Kept logs here: $TEMPDIR"
else
    echo "Everything passed deleting logs."
    rm -rf ${TEMPDIR}
fi

popd