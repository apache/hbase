#!/bin/bash
set -e

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

################################################ ABOUT JDIFF #######################################################
#
#   What is JDiff? JDiff is a tool for comparing the public APIs of two separate Java codebases. Like diff, it will
#   give additions, changes, and removals. It will output an HTML report with the information.
#   To learn more, visit http://javadiff.sourceforge.net/.
#   JDiff is licensed under LGPL.

############################################# QUICK-START EXAMPLE ##################################################
#
#   Suppose we wanted to see the API diffs between HBase 0.92 and HBase 0.94. We could use this tool like so:
#   > ./jdiffHBasePublicAPI.sh https://github.com/apache/hbase.git 0.92 https://github.com/apache/hbase.git 0.94
#
#   This would generate a report in the local folder /tmp/jdiff/hbase_jdiff_report-p-0.92-c-0.94/
#   To view the report, simply examine /tmp/jdiff/hbase_jdiff_report-p-0.92-c-0.94/changes.html in your choice of
#   browser.
#
#   Note that this works because 0.92 and 0.94 have the source directory structure that is specified in the
#   hbase_jdiff_template.xml file. To compare 0.95 to 0.96, which have the post-singularity structure,  two other
#   template files (included) are used. The formats are autoated and is all taken care of automatically by the script.
#
#   On a local machine, JDiff reports have taken ~20-30 minutes to run. On Jenkins, it has taken over 35 minutes
#   in some cases. Your mileage may vary. Trunk and 0.95 take more time than 0.92 and 0.94.
#
#
############################################ SPECIFYING A LOCAL REPO ###############################################
#
#   The JDiff tool also works with local code. Instead of specifying a repo and a branch, you can specifying the
#   absolute path of the ./hbase folder and a name for code (e.g. experimental_94).
#
#   A local repo can be specified for none, one, or both of the sources.
#
############################################### EXAMPLE USE CASES ##################################################
#
#   Example 1: Generate a report to check if potential change doesn't break API compatibility with Apache HBase 0.94
#
#   In this case, you could compare the version you are using against a repo branch where your changes are.
#   >  ./jdiffHBasePublicAPI.sh https://github.com/apache/hbase.git 0.94 https://github.com/MY_REPO/hbase.git 0.94
#
#   Example 2: Generate a report to check if two branches of the same repo have any public API incompatibilities
#   >  ./jdiffHBasePublicAPI.sh https://github.com/MY_REPO/hbase.git $BRANCH_1  \
#   >   https://github.com/MY_REPO/hbase.git $BRANCH_2
#
#   Example 3: Have Example 1 done in a special directory in the user's home folder
#
#   >  export JDIFF_WORKING_DIRECTORY=~/jdiff_reports
#   >  ./jdiffHBasePublicAPI.sh https://github.com/apache/hbase.git 0.94 https://github.com/MY_REPO/hbase.git 0.94
#
#   Example 4: Check the API diff of a local change against an existing repo branch.
#   >  ./jdiffHBasePublicAPI.sh https://github.com/apache/hbase.git 0.95 /home/aleks/exp_hbase/hbase experiment_95
#
#   Example 5: Compare two local repos for public API changes
#   >  ./jdiffHBasePublicAPI.sh /home/aleks/stable_hbase/hbase stable_95 /home/aleks/exp_hbase/hbase experiment_95
#
#
################################################## NOTE ON USAGE ###################################################
#
#   1. When using this tool, please specify the initial version first and the current version second. The semantics
#      do not make sense otherwise. For example: jdiff 94 95 is good. jdiff 95 94 is bad
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

scriptDirectory=$(dirname ${BASH_SOURCE[0]})
x=`echo $scriptDirectory | sed "s{\.{{g"`
DEV_SUPPORT_HOME="`pwd`$x"
. $scriptDirectory/jdiffHBasePublicAPI_common.sh

EXPECTED_ARGS=4

if [[ "$#" -ne "$EXPECTED_ARGS" ]]; then
  echo "This tool expects $EXPECTED_ARGS arguments, but received $#. Please check your command and try again.";
  echo "Usage: $0 <repoUrl or local source directory 1> <branch or source name 1> <repoUrl or local source directory 2> <branch or source name 2>"
  exit 1;
fi

echo "JDiff evaluation beginning:";
isGitRepo $1
FIRST_SOURCE_TYPE=$INPUT_FORMAT;
isGitRepo $3
SECOND_SOURCE_TYPE=$INPUT_FORMAT;

PREVIOUS_BRANCH=$2  ## We will still call it a branch even if it's not from a git repo.
CURRENT_BRANCH=$4

echo "We are going to compare source 1 which is a $FIRST_SOURCE_TYPE and source 2, which is a $SECOND_SOURCE_TYPE"


# Check that if either source is from a git repo, that the name is reasonable.
if [[ "$FIRST_SOURCE_TYPE" = "git_repo" ]]; then

  git check-ref-format --branch $2
fi

if [[ "$SECOND_SOURCE_TYPE" = "git_repo" ]]; then

  git check-ref-format --branch $4
fi

#If the JDIFF_WORKING_DIRECTORY is set, then we will output the report there. Otherwise, to the default location
if [[ "$JDIFF_WORKING_DIRECTORY" = "" ]]; then

  JDIFF_WORKING_DIRECTORY=/tmp/jdiff
  echo "JDIFF_WORKING_DIRECTORY not set. That's not an issue. We will default it to $JDIFF_WORKING_DIRECTORY."
else
  echo "JDIFF_WORKING_DIRECTORY set to $JDIFF_WORKING_DIRECTORY";
fi
mkdir -p $JDIFF_WORKING_DIRECTORY

# We will need this to reference the template we want to use
cd $JDIFF_WORKING_DIRECTORY
scenario_template_name=hbase_jdiff_p-$PREVIOUS_BRANCH-c-$CURRENT_BRANCH.xml


# Pull down JDiff tool and unpack it
if [ ! -d jdiff-1.1.1-with-incompatible-option ]; then
  curl -OL http://cloud.github.com/downloads/tomwhite/jdiff/jdiff-1.1.1-with-incompatible-option.zip
  unzip jdiff-1.1.1-with-incompatible-option.zip
fi

JDIFF_HOME=`pwd`/jdiff-1.1.1-with-incompatible-option
cd $JDIFF_WORKING_DIRECTORY

# Pull down sources if necessary. Note that references to previous change are prefaced with p- in order to avoid collission of branch names
if [[ "$FIRST_SOURCE_TYPE" = "git_repo" ]]; then

  PREVIOUS_REPO=$1
  rm -rf p-$PREVIOUS_BRANCH
  mkdir -p p-$PREVIOUS_BRANCH
  cd p-$PREVIOUS_BRANCH
  git clone --depth 1 --branch $PREVIOUS_BRANCH $PREVIOUS_REPO
  cd $JDIFF_WORKING_DIRECTORY
  HBASE_1_HOME=`pwd`/p-$PREVIOUS_BRANCH/hbase
else
  HBASE_1_HOME=$1
fi

echo "HBASE_1_HOME set to $HBASE_1_HOME"
echo "In HBASE_1_HOME, we have"
ls -la $HBASE_1_HOME

if [[ "$SECOND_SOURCE_TYPE" = "git_repo" ]]; then
  CURRENT_REPO=$3
  rm -rf $JDIFF_WORKING_DIRECTORY/c-$CURRENT_BRANCH
  mkdir -p $JDIFF_WORKING_DIRECTORY/c-$CURRENT_BRANCH
  cd $JDIFF_WORKING_DIRECTORY/c-$CURRENT_BRANCH
  git clone --depth 1 --branch $CURRENT_BRANCH $CURRENT_REPO
  cd $JDIFF_WORKING_DIRECTORY
  HBASE_2_HOME=`pwd`/c-$CURRENT_BRANCH/hbase
else
  HBASE_2_HOME=$3
fi

echo "HBASE_2_HOME set to $HBASE_2_HOME"
echo "In HBASE_2_HOME, we have"
ls -la $HBASE_2_HOME

# Next step is to pull down the proper template based on the directory structure
isNewFormat $HBASE_1_HOME
export P_FORMAT=$BRANCH_FORMAT

isNewFormat $HBASE_2_HOME
export C_FORMAT=$BRANCH_FORMAT

if  [[ "$C_FORMAT" = "new" ]]; then

  if [[ "$P_FORMAT" = "new" ]]; then
    templateFile=$DEV_SUPPORT_HOME/hbase_jdiff_afterSingularityTemplate.xml
    echo "Previous format is of the new style. We'll be using template $templateFile";
  else
    templateFile=$DEV_SUPPORT_HOME/hbase_jdiff_acrossSingularityTemplate.xml
    echo "Previous format is of the old style. We'll be using template $templateFile";
  fi

else

  if [[ "P_FORMAT" != "old" ]]; then
    echo "When using this tool, please specify the initial version first and the current version second. They should be in ascending chronological order.
          The semantics do not make sense otherwise. For example: jdiff 94 95 is good. jdiff 95 94 is bad."
    echo "Exiting the script."
    exit 5;
  fi
  templateFile=$DEV_SUPPORT_HOME/hbase_jdiff_template.xml
  echo "Both formats are using the 94 and earlier style directory format. We'll be using template $templateFile"
fi

cp $templateFile $JDIFF_WORKING_DIRECTORY/$scenario_template_name

### Configure the jdiff script

### Note that PREVIOUS_BRANCH and CURRENT_BRANCH will be the absolute locations of the source.
echo "Configuring the jdiff script"

# Extension to -i is done to support in-place editing on GNU sed and BSD sed.
sed -i.tmp "s]hbase_jdiff_report]hbase_jdiff_report-p-$PREVIOUS_BRANCH-c-$CURRENT_BRANCH]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name
sed -i.tmp "s]JDIFF_HOME_NAME]$JDIFF_HOME]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name
sed -i.tmp "s]OLD_BRANCH_NAME]$HBASE_1_HOME]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name
sed -i.tmp "s]NEW_BRANCH_NAME]$HBASE_2_HOME]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name

sed -i.tmp "s]V1]$PREVIOUS_BRANCH]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name
sed -i.tmp "s]V2]$CURRENT_BRANCH]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name

sed -i.tmp "s]JDIFF_FOLDER]$JDIFF_WORKING_DIRECTORY]g" \
    $JDIFF_WORKING_DIRECTORY/$scenario_template_name

echo "Running jdiff";
ls -la $JDIFF_WORKING_DIRECTORY;
ant -f $JDIFF_WORKING_DIRECTORY/$scenario_template_name;

echo "jdiff operation complete. Report placed into $JDIFF_WORKING_DIRECTORY/hbase_jdiff_report-p-$PREVIOUS_BRANCH-c-$CURRENT_BRANCH/changes.html";

