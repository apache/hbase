#!/bin/bash
set -x

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

################################################ ABOUT JDIFF #######################################################
#
#   What is JDiff? JDiff is a tool for comparing the public APIs of two separate Java codebases. Like diff, it will
#   give additions, changes, and removals. It will output an HTML report with the information.
#   To learn more, visit http://javadiff.sourceforge.net/.
#   JDiff is licensed under LGPL.

############################################# QUICK-START EXAMPLE ##################################################
#
#   Suppose we wanted to see the API diffs between HBase 0.92 and HBase 0.94. We could use this tool like so:
#   > ./jdiffHBaseFromGitRepos.sh https://github.com/apache/hbase.git 0.92 https://github.com/apache/hbase.git 0.94
#
#   This would generate a report in the local folder /tmp/jdiff/hbase_jdiff_report-p-0.92-c-0.94/
#   To view the report, simply examine /tmp/jdiff/hbase_jdiff_report-p-0.92-c-0.94/changes.html in your choice of
#   browser.
#
#   Note that this works because 0.92 and 0.94 have the source directory structure that is specified in the
#   hbase_jdiff_template.xml file. To compare 0.95 to 0.96, both of which have a different structure than 0.94, it is
#   necessary to modify hbase_jdiff_template.xml.
#
#   On a local machine, JDiff reports have taken ~10-12 minutes to run. On Jenkins, it has taken over 35 minutes
#   in some cases. Your mileage may vary.

############################################### EXAMPLE USE CASES ##################################################
#
#   Example 1: Generate a report to check if potential change doesn't break API compatibility with Apache HBase 0.94
#
#   In this case, you could compare the version you are using against a repo branch where your changes are.
#   >  ./jdiffHBaseFromGitRepos.sh https://github.com/apache/hbase.git 0.94 https://github.com/MY_REPO/hbase.git 0.94
#
#   Example 2: Generate a report to check if two branches of the same repo have any public API incompatibilities
#   >  ./jdiffHBaseFromGitRepos.sh https://github.com/MY_REPO/hbase.git $BRANCH_1  \
#   >   https://github.com/MY_REPO/hbase.git $BRANCH_2
#
#   Example 3: Have Example 1 done in a special directory in the user's home folder
#
#   >  export JDIFF_WORKING_DIRECTORY=~/jdiff_reports
#   >  ./jdiffHBaseFromGitRepos.sh https://github.com/apache/hbase.git 0.94 https://github.com/MY_REPO/hbase.git 0.94
#

############################################# READING A JDIFF REPORT ###############################################
#
#   The purpose of the JDiff report is show things that have changed between two versions of the public API. A user
#   would use this report to determine if committing a change would cause existing API clients to break. To do so,
#   there are specific things that one should look for in the report.
#
#   1. Identify the classes that constitute the public API. An example in 0.94 might be all classes in
#      org.apache.hadoop.hbase.client.*
#   2. After identifying those classes, go through each one and look for offending changes.
#      Those may include, but are not limited to:
#      1. Removed methods
#      2. Changed methods (including changes in return type and exception types)
#      3. Methods added to interfaces
#      4. Changed class inheritence information (may in innocuous but definitely worth validating)
#      5. Removed or renamed public static member variables and constants
#      6. Removed or renamed packages
#      7. Class moved to a different package

########################################### SETTING THE JDIFF WORKING DIRECTORY ####################################
#
#   By default, the working environment of jdiff is /tmp/jdiff. However, sometimes it is nice to have it place reports
#   and temp files elsewhere. In that case, please export JDIFF_WORKING_DIRECTORY into the bash environment and this
#   script will pick that up and use it.
#
#   WARNING: We do not perform validation on this directory, so please make sure it a valid, non-sensitive location.
#

### At this point, CURRENT_BRANCH, PREVIOUS_BRANCH, CURRENT_REPO, PREVIOUS_REPO need to all be specified ###
set -e
#check number of arguments. Should be 4
echo "Starting jdiff"

EXPECTED_ARGS=4

if [[ $# -ne $EXPECTED_ARGS ]]; then
  echo "Incorrect number of arguments. Expected $EXPECTED_ARGS";
  echo "Usage: $0 PREVIOUS_REPO PREVIOUS_BRANCH CURRENT_REPO CURRENT_BRANCH";
  exit 1;
fi

PREVIOUS_REPO=$1
PREVIOUS_BRANCH=$2
CURRENT_REPO=$3
CURRENT_BRANCH=$4

echo "About to validate branches for valid names."
echo "Specifics here: https://www.kernel.org/pub/software/scm/git/docs/git-check-ref-format.html"

git check-ref-format --branch $PREVIOUS_BRANCH
git check-ref-format --branch $CURRENT_BRANCH

set +e

if [[ "$JDIFF_WORKING_DIRECTORY" = "" ]]; then

  echo "JDIFF_WORKING_DIRECTORY not set. That's not an issue. We will default it to ./jidff"
  JDIFF_WORKING_DIRECTORY=/tmp/jdiff
else
  echo "JDIFF_WORKING_DIRECTORY set to $JDIFF_WORKING_DIRECTORY";
fi

mkdir -p $JDIFF_WORKING_DIRECTORY


scenario_template_name=hbase_jdiff_p-$PREVIOUS_BRANCH-c-$CURRENT_BRANCH.xml
cp ./hbase_jdiff_template.xml $JDIFF_WORKING_DIRECTORY/$scenario_template_name

cd $JDIFF_WORKING_DIRECTORY

echo "Beginning jdiff between the following two git repos"
echo "Starting with $PREVIOUS_REPO:$PREVIOUS_BRANCH to $CURRENT_REPO:$CURRENT_BRANCH"

### Pull down the jdiff tool and unpack it

if [ ! -d jdiff-1.1.1-with-incompatible-option ]; then
  curl -O http://cloud.github.com/downloads/tomwhite/jdiff/jdiff-1.1.1-with-incompatible-option.zip
  unzip jdiff-1.1.1-with-incompatible-option.zip
fi

JDIFF_HOME=`pwd`/jdiff-1.1.1-with-incompatible-option

#Copy the template_template into the current working directory

### Configure the jdiff script

echo "Configuring the jdiff script"
sed -i "s]hbase_jdiff_report]hbase_jdiff_report-p-$PREVIOUS_BRANCH-c-$CURRENT_BRANCH]g" ./$scenario_template_name
sed -i "s]JDIFF_HOME_NAME]$JDIFF_HOME]g" ./$scenario_template_name
sed -i "s]OLD_BRANCH_NAME]$PREVIOUS_BRANCH]g" ./$scenario_template_name
sed -i "s]NEW_BRANCH_NAME]$CURRENT_BRANCH]g" ./$scenario_template_name

sed -i "s]V1]$PREVIOUS_BRANCH]g" ./$scenario_template_name
sed -i "s]V2]$CURRENT_BRANCH]g" ./$scenario_template_name

sed -i "s]JDIFF_FOLDER]$JDIFF_WORKING_DIRECTORY]g" ./$scenario_template_name

### Pull down the branches
echo "Pulling down the branches"
cat ./$scenario_template_name


### Previous Branch ###

echo "Pulling down previous branch"
rm -rf p-$PREVIOUS_BRANCH
mkdir -p p-$PREVIOUS_BRANCH
cd p-$PREVIOUS_BRANCH
git clone $PREVIOUS_REPO && cd hbase && git checkout origin/$PREVIOUS_BRANCH && cd ..


### Current Branch ###

echo "Pulling down current branch"
rm -rf ../c-$CURRENT_BRANCH
mkdir -p ../c-$CURRENT_BRANCH
cd ../c-$CURRENT_BRANCH
git clone $CURRENT_REPO && cd hbase && git checkout origin/$CURRENT_BRANCH && cd ..
cd ..


### Run the jdiff command

echo "Running jdiff"
echo "About to issue command to run jdiff";
ant -f ./$scenario_template_name

echo "jdiff operation complete. Report placed into $JDIFF_WORKING_DIRECTORY/hbase_jdiff_report-p-$PREVIOUS_BRANCH-c-$CURRENT_BRANCH/changes.html"
