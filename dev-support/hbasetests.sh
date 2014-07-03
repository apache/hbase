#!/usr/bin/env bash
##
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
##
#
# This script:
#   - analyse the content of the .java test file to split them between
#         small/medium/large
#   - launch the small tests in a single maven, with surefire
#          parallelisation activated
#   - launch the medium & large in two maven, parallelized
#   - the flaky tests are run at the end, not parallelized
#   - present a small report of the global results
#   - copy the failed test reports with prefix 'fail_' and a timestamp
#         to protect them from a later deletion by maven
#   - if configured for, relaunch the tests in errors
#
#
# Caveats:
#   - multiple maven are launch, hence there can be recompilation
#  between the tests if a file is modified. For non flaky tests and
#  parallelization, the frame is the time to execute the small tests,
#  so it's around 4 minutes.
#   - Note that surefire is buggy, and the results presented while
#  running may be wrong. For example, it can says that a class tests
#  have 5 errors. When you look at the file it wrote, it says that the
#  2 tests are ok, and in the class there are actually two tests
#  methods, not five. If you generate the report at the end with
#  surefire-report it's fine however.
#
######################################### parameters

#mvn test -Dtest=org.apache.hadoop.hbase.regionserver.TestScanWithBloomError $*

#exit

#set to 0 to run only developpers tests (small & medium categories)
runAllTests=0

#set to 1 to replay the failed tests. Previous reports are kept in
# fail_ files
replayFailed=0 

#set to 0 to run all medium & large tests in a single maven operation
# instead of two
parallelMaven=1

#harcoded list of tests that often fail. We don't want to add any
# complexity around then so there are not run in parallel but after
# the others
#The ',' at the end is mandatory
flakyTests=
#org.apache.hadoop.hbase.mapreduce.TestTableInputFormatScan,org.apache.hadoop.hbase.catalog.TestMetaTableAccessorNoCluster,org.apache.hadoop.hbase.catalog.TestMetaTableAccessor,org.apache.hadoop.hbase.mapreduce.TestHFileOutputFormat,org.apache.hadoop.hbase.mapred.TestTableMapReduce,org.apache.hadoop.hbase.coprocessor.TestMasterCoprocessorExceptionWithAbort,org.apache.hadoop.hbase.coprocessor.TestMasterCoprocessorExceptionWithRemove,org.apache.hadoop.hbase.client.TestAdmin,org.apache.hadoop.hbase.master.TestMasterFailover,org.apache.hadoop.hbase.regionserver.wal.TestLogRolling,org.apache.hadoop.hbase.master.TestDistributedLogSplitting,org.apache.hadoop.hbase.master.TestMasterRestartAfterDisablingTable,org.apache.hadoop.hbase.TestGlobalMemStoreSize,

######################################### Internal parameters
#directory used for surefire & the source code.
#They should not need to be modified
#The final / is mandatory
rootTestClassDirectory="./src/test/java/"
surefireReportDirectory="./target/surefire-reports/"

#variable to use to debug the script without launching the tests
mvnCommand="mvn "
#mvnCommand="echo $mvnCommand"

######################################### Functions
#get the list of the process considered as dead
# i.e.: in the same group as the script and with a ppid of 1
# We do this because surefire can leave some dead process, so
# we will jstack them and kill them
function createListDeadProcess {
  id=$$
  listDeadProcess=""
  
  #list of the process with a ppid of 1
  sonProcess=`ps -o pid= --ppid 1`
  
  #then the process with a pgid of the script
  for pId in $sonProcess
  do
    pgid=`ps -o pgid= --pid $pId | sed 's/ //g'`
    if [ "$pgid" == "$id" ]
    then
      listDeadProcess="$pId $listDeadProcess"
    fi
  done
}

#kill the java sub process, if any, with a kill and a kill -9
#When maven/surefire fails, it lefts some process with a ppid==1
#we're going to find them with the pgid, print the stack and kill them.
function cleanProcess {
  id=$$

  createListDeadProcess
  for pId in $listDeadProcess
  do
    echo "$pId survived, I will kill if it's a java process. 'ps' says:"
    ps -fj --pid $pId
    name=`ps -o comm= --pid $pId`
    if [ "$name" == "java" ]
    then
      echo "$pId, java sub process of $id, is still running, killing it with a standard kill"
      echo "Stack for $pId before kill:"
      jstack -F -l $pId
      kill $pId
      echo "kill sent, waiting for 30 seconds"
      sleep 30      
      son=`ps -o pid= --pid $pId | wc -l`
      if (test $son -gt 0)
      then 
        echo "$pId, java sub process of $id, is still running after a standard kill, using kill -9 now"
        echo "Stack for $pId before kill -9:"
        jstack -F -l $pId
        kill -9 $pId
        echo "kill sent, waiting for 2 seconds"
        sleep 2           
        echo "Process $pId killed by kill -9" 
      else
        echo "Process $pId killed by standard kill -15" 
      fi
    else
      echo "$pId is not a java process (it's $name), I don't kill it."
    fi
  done
  
  createListDeadProcess
  if (test ${#listDeadProcess} -gt 0)
  then
    echo "There are still $sonProcess for process $id left."
  else
    echo "Process $id clean, no son process left"      
  fi  
}

#count the number of ',' in a string
# used to calculate the number of class
#write $count
function countClasses {
  cars=`echo $1 | sed 's/[^,]//g' | wc -c `
  count=$((cars - 1))
}

  
######################################### script
echo "Starting Script. Possible parameters are: runAllTests, replayFailed, nonParallelMaven"
echo "Other parameters are sent to maven"

#We will use this value at the end to calculate the execution time
startTime=`date +%s`

#look in the arguments if we override default values
for arg in "$@"
do
  if [ $arg == "runAllTests" ]
  then
    runAllTests=1
  else
    if [ $arg == "replayFailed" ]
    then
      replayFailed=1
    else
      if [ $arg == "nonParallelMaven" ]
      then
        parallelMaven=0
      else  
         args=$args" $arg"        
      fi
    fi
  fi   
done



testsList=$(find $rootTestClassDirectory -name "Test*.java")


#for all java test files, let see if they contain the pattern
# to recognize the category
for testFile in $testsList
do
  lenPath=$((${#rootTestClassDirectory}))
  len=$((${#testFile} - $lenPath - 5))  # len(".java") == 5
  
  shortTestFile=${testFile:lenPath:$len}  
  testName=$(echo $shortTestFile | sed 's/\//\./g')
  
  #The ',' is used in the grep pattern as we don't want to catch
  # partial name
  isFlaky=$((`echo $flakyTests | grep "$testName," | wc -l`))
  
  if (test $isFlaky -eq 0)
  then    
    isSmall=0
    isMedium=0
    isLarge=0
  
    # determine the category of the test by greping into the source code 
    isMedium=`grep "@Category" $testFile | grep "MediumTests.class" | wc -l`
    if (test $isMedium -eq 0) 
    then 
      isLarge=`grep "@Category" $testFile | grep "LargeTests.class" | wc -l`
      if (test $isLarge -eq 0)
      then
        isSmall=`grep "@Category" $testFile | grep "SmallTests.class" | wc -l`
        if (test $isSmall -eq 0)
        then
          echo "$testName is not categorized, so it won't be tested"
        else
          #sanity check on small tests
          isStrange=`grep "\.startMini" $testFile | wc -l`
          if (test $isStrange -gt 0)
          then
            echo "$testFile is categorized as 'small' but contains a .startMini string. Keep it as small anyway, but it's strange."
          fi
        fi
      fi
    fi
    
    #put the test in the right list
    if (test $isSmall -gt 0) 
    then 
      smallList="$smallList,$testName"
    fi    
    if (test $isMedium -gt 0) 
    then 
      mediumList="$mediumList,$testName"
    fi    
    if (test $isLarge -gt 0) 
    then 
      largeList="$largeList,$testName"
    fi        
    
  fi   
done

#remove the ',' at the beginning
smallList=${smallList:1:${#smallList}}
mediumList=${mediumList:1:${#mediumList}}
largeList=${largeList:1:${#largeList}}

countClasses $smallList
echo "There are $count small tests"

countClasses $mediumList
echo "There are $count medium tests"

countClasses $largeList
echo "There are $count large tests"




#do we launch only dev or all tests?
if (test $runAllTests -eq 1)
then
  echo "Running all tests, small, medium and large"
  longList="$mediumList,$largeList"
else
  echo "Running developper tests only, small and medium categories"
  longList=$mediumList
fi

#medium and large test can be run in //, so we're
#going to create two lists
nextList=1
for testClass in `echo $longList | sed 's/,/ /g'`
do
  if (test $nextList -eq 1)
  then
    nextList=2
    runList1=$runList1,$testClass
  else
    nextList=1 
    runList2=$runList2,$testClass
  fi
done

#remove the ',' at the beginning
runList1=${runList1:1:${#runList1}}
runList2=${runList2:1:${#runList2}}

#now we can run the tests, at last!

echo "Running small tests with one maven instance, in parallel"
#echo Small tests are $smallList 
$mvnCommand -P singleJVMTests test -Dtest=$smallList  $args 
cleanProcess

exeTime=$(((`date +%s` - $startTime)/60))
echo "Small tests executed after $exeTime minutes"

if (test $parallelMaven -gt 0)
then 
  echo "Running tests with two maven instances in parallel"
  $mvnCommand -P localTests test -Dtest=$runList1  $args &
  
  #give some time  to the fist process if there is anything to compile
  sleep 30
  $mvnCommand -P localTests test -Dtest=$runList2  $args

  #wait for forked process to finish
  wait
  
  cleanProcess
  
  exeTime=$(((`date +%s` - $startTime)/60))
  echo "Medium and large (if selected) tests executed after $exeTime minutes"

  #now the flaky tests, alone, if the list is not empty
  # we test on size greater then 5 to remove any "," effect
  if (test $runAllTests -eq 1 && test ${#flakyTests} -gt 5)
  then
    echo "Running flaky tests"
    $mvnCommand -P localTests test -Dtest=$flakyTests $args
    cleanProcess
    exeTime=$(((`date +%s` - $startTime)/60))
    echo "Flaky tests executed after $exeTime minutes"    
  fi
else
  echo "Running tests with a single maven instance, no parallelization"
  $mvnCommand -P localTests test -Dtest=$runList1,$runList2,$flakyTests $args
  cleanProcess  
  exeTime=$(((`date +%s` - $startTime)/60))
  echo "Single maven instance tests executed after $exeTime minutes"     
fi

#let's analyze the results
fullRunList="$smallList,$longList"

if (test $runAllTests -eq 1)
then
  fullRunList="$fullRunList,$flakyTests"
fi

#single timestamp to ensure files uniquess.
timestamp=`date +%s`

#some counters, initialized because they may not be touched
# in the loop
errorCounter=0
sucessCounter=0
notFinishedCounter=0

for testClass in `echo $fullRunList | sed 's/,/ /g'`
do
  reportFile=$surefireReportDirectory/$testClass.txt
  outputReportFile=$surefireReportDirectory/$testClass-output.txt
  
  if [ -s $reportFile ];
  then
    isError=`grep FAILURE $reportFile | wc -l`
    if (test $isError -gt 0)
    then
      errorList="$errorList,$testClass"
      errorCounter=$(($errorCounter + 1))
      
      #let's copy the files if we want to use it later      
      cp $reportFile "$surefireReportDirectory/fail_$timestamp.$testClass.txt"
      if [ -s $reportFile ];
      then
        cp $outputReportFile "$surefireReportDirectory/fail_$timestamp.$testClass"-output.txt""
      fi
    else
     
      sucessCounter=$(($sucessCounter +1))
    fi  
  else
     #report file does not exist or is empty => the test didn't finish
     notFinishedCounter=$(($notFinishedCounter + 1))
     notFinishedList="$notFinishedList,$testClass"
  fi  
done

#list of all tests that failed
replayList="$notFinishedList""$errorList"

#remove the ',' at the beginning
notFinishedList=${notFinishedList:1:${#notFinishedList}}
errorList=${errorList:1:${#errorList}}
replayList=${replayList:1:${#replayList}}

#make it simpler to read by removing the org.* stuff from the name
notFinishedPresList=`echo $notFinishedList | sed 's/org.apache.hadoop.hbase.//g' | sed 's/,/, /g'`
errorPresList=`echo $errorList | sed 's/org.apache.hadoop.hbase.//g' | sed 's/,/, /g'`


#calculate the execution time
curTime=`date +%s`
exeTime=$((($curTime - $startTime)/60))

echo "##########################"
echo "$sucessCounter tests executed successfully"
echo "$errorCounter tests are in error"
echo "$notFinishedCounter tests didn't finish"
echo
echo "Tests in error are: $errorPresList"
echo "Tests that didn't finish are: $notFinishedPresList"
echo
echo "Execution time in minutes: $exeTime" 
echo "##########################"


if (test ${#replayList} -gt 0)
then
  if (test $replayFailed -gt 0)
  then
    echo "Replaying all tests that failed"
    $mvnCommand -P localTests test -Dtest=$replayList  $args
    echo "Replaying done"
  fi
fi

exit
