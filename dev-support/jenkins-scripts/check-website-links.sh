#!/bin/bash

# This script is designed to run as a Jenkins job, such as at
# https://builds.apache.org/view/All/job/HBase%20Website%20Link%20Checker/
#
# It generates artifacts which the Jenkins job then can mail out and/or archive.
#
# We download a specific version of linklint because the original has bugs and
# is not well maintained.
#
# See http://www.linklint.org/doc/inputs.html for linklint options

# Clean up the workspace
rm -rf *.zip > /dev/null
rm -rf linklint > /dev/null
rm -Rf link_report

# This version of linklint fixes some bugs in the now-unmaintained 2.3.5 version
wget http://ingo-karkat.de/downloads/tools/linklint/linklint-2.3.5_ingo_020.zip
unzip linklint-2.3.5_ingo_020.zip
chmod +x linklint/linklint.pl

# Run the checker
echo "Checking http://hbase.apache.org and saving report to link_report/"
echo "Excluding /testapidocs/ because some tests use private classes not published in /apidocs/."
# Check internal structure
linklint/linklint.pl -http \
                     -host hbase.apache.org \
                     /@ \
                     -skip /testapidocs/@ \
                     -skip /testdevapidocs/@ \
                     -net \
                     -redirect \
                     -no_query_string \
                     -htmlonly \
                     -timeout 30 \
                     -delay 1 \
                     -limit 100000 \
                     -doc link_report

# Detect whether we had errors and act accordingly
if ! grep -q 'ERROR' link_report/index.html; then
  echo "Errors found. Sending email."
  exit 1
else
  echo "No errors found. Warnings might be present."
fi