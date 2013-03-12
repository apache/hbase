#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
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

usage()
{
cat << EOF
usage: $0 [options] [test-name...]

Run a set of hbase tests. Individual tests may be specified on the
command line or in a file using -f, with one test per line.  Runs all
tests by default.  Each specified tests should include the fully
qualified package name.

options:
   -h          Show this message
   -c          Run 'mvn clean' before running the tests
   -f FILE     Run the additional tests listed in the FILE
   -u          Only run unit tests. Default is to run 
               unit and integration tests
   -n N        Run each test N times. Default = 1.
   -s N        Print N slowest tests
   -H          Print which tests are hanging (if any)
   -e          Echo the maven call before running. Default: not enabled
   -r          Runs remotely, on the build server. Default: not enabled
EOF
}

echoUsage=0
server=0
testFile=
doClean=""
testType=verify
numIters=1
showSlowest=
showHanging=

# normalize path refs for surefire
if [[ "$0" != /* ]]; then
    # relative path
    scriptDir=`pwd`/$(dirname $0)
else
    # absolute path
    scriptDir=$(dirname $0)
fi
testDir=$scriptDir/../../../target/surefire-reports

while getopts "hcerHun:s:f:" OPTION
do
     case $OPTION in
         h)
             usage
             exit 0
             ;;
	 c)
	     doClean="clean"
	     ;;
         H)
             showHanging=1
             ;;
         u)
	     testType=test
             ;;
         n)
             numIters=$OPTARG
             ;;
         s)
             showSlowest=$OPTARG
             ;;
         f)
             testFile=$OPTARG
             ;;
        e)
             echoUsage=1
             ;;
        r)
            server=1
            ;;
	 ?) 
	     usage
	     exit 1
     esac
done

testIdx=0

# add tests specified in a file
if [ ! -z $testFile ]; then
    exec 3<$testFile

    while read <&3 line ; do
	if [ ! -z "$line" ]; then
	    test[$testIdx]="$line"
	fi
	testIdx=$(($testIdx+1))
    done
fi

# add tests specified on cmd line
if [ ! -z $BASH_ARGC ]; then
    shift $(($OPTIND - 1))
    for (( i = $OPTIND; i <= $BASH_ARGC; i++ ))
    do
	test[$testIdx]=$1;
	testIdx=$(($testIdx+1))
	shift
    done
fi

echo "Running tests..."
numTests=${#test[@]}

for ((  i = 1 ;  i <= $numIters; i++  ))
do
    if [[ $numTests > 0 ]]; then
        #Now loop through each test
	for (( j = 0; j < $numTests; j++ ))
	do
        # Create the general command
        cmd="nice -10 mvn $doClean $testType -Dtest=${test[$j]}"

        # Add that is should run locally, if not on the server
        if [ ${server} -eq 0 ]; then
            cmd="${cmd} -P localTests"
        fi

        # Print the command, if we should
        if [ ${echoUsage} -eq 1 ]; then
            echo "${cmd}"
        fi

        # Run the command
        $cmd

        if [ $? -ne 0 ]; then
		echo "${test[$j]} failed, iteration: $i"
		exit 1
	    fi
	done
    else
	echo "EXECUTING ALL TESTS"
    # Create the general command
    cmd="nice -10 mvn $doClean $testType"

    # Add that is should run locally, if not on the server
    if [ ${server} -eq 0 ]; then
       cmd="${cmd} -P localTests"
    fi

    # Print the command, if we should
    if [ ${echoUsage} -eq 1 ]; then
        echo "${cmd}"
    fi

    #now run the command
    $cmd
    fi
done

# Print a report of the slowest running tests
if [ ! -z $showSlowest ]; then
    
    testNameIdx=0
    for (( i = 0; i < ${#test[@]}; i++ ))
    do
	testNames[$i]=$testDir/'TEST-'${test[$i]}'.xml'
    done

    echo "Slowest $showSlowest tests:"

    awk '/<testsuite/ { \
              gsub(/\"/,""); \
              gsub(/>/,""); \
              split($7,testname,"="); \
              split($3,time,"="); \
              print  testname[2]"\t"time[2]}' \
	${testNames[@]} \
	| sort -k2,2rn \
	| head -n $showSlowest
fi

# Print a report of tests that hung
if [ ! -z $showHanging ]; then
    echo "Hanging tests:"
    find $testDir -type f -name *.xml -size 0k \
	| xargs basename \
	| cut -d"." -f -1
fi
