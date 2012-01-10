#!/bin/bash
# script to find hanging test from Jenkins build output
# usage: ./findHangingTest.sh <url of Jenkins build console>
#
`curl -k -o jenkins.out "$1"`
expecting=Running
cat jenkins.out | while read line; do
 if [[ "$line" =~ "Running org.apache.hadoop" ]]; then
  if [[ "$expecting" =~ "Running" ]]; then 
   expecting=Tests
  else
   echo "Hanging test: $prevLine"
  fi
 fi
 if [[ "$line" =~ "Tests run" ]]; then
  expecting=Running
 fi
 if [[ "$line" =~ "Forking command line" ]]; then
  a=$line
 else
  prevLine=$line
 fi
done
rm jenkins.out
