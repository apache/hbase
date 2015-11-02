#!/bin/bash

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

USAGE="Usage: $0 [-i | -a] [-g <dir>] [-s <dir>]\n\
-h          Show this message\n\
-i          Prompts the user for input\n\
-a          Does not prompt the user. Potentially dangerous.\n\
-g          The local location of the HBase git repository\n\
-s          The local location of the HBase svn checkout\n\
Either -i or -a is required.\n\
Edit the script to set default Git and SVN directories."

if [ "$#" == "0" ]; then
  echo -e "$USAGE"
  exit 1
fi

# Process args
INTERACTIVE=0
AUTO=0
GIT_DIR=
SVN_DIR=
while getopts "hiag:s:" OPTION
do
  case $OPTION in
    h)
      echo -e "$USAGE"
      exit
      ;;
    i)
      if [ $AUTO -eq 1 ]; then
        echo "Cannot specify both -i and -a."
        echo -e "$USAGE"
        exit 1
      else
        INTERACTIVE=1
        AUTO=0
      fi
      ;;
    a)
      # We don't actually use this variable but require it to be
      # set explicitly because it will commit changes without asking
      if [ $INTERACTIVE -eq 1 ]; then
        echo "Cannot specify both -i and -a."
        echo -e "$USAGE"
        exit 1
      else
        AUTO=1
        INTERACTIVE=0
      fi
      ;;
    g)
      GIT_DIR=$OPTARG
      if [ ! -d "$GIT_DIR/.git" ]; then
        echo "Git directory $GIT_DIR does not exist or is not a Git repository."
        exit 1
      fi
      ;;
    s)
      SVN_DIR=$OPTARG
      if [ ! -d "$SVN_DIR/.svn" ]; then
        echo "SVN directory $SVN_DIR does not exist or is not a Subversion repository."
        exit 1
      fi
      ;;
  esac
done

# Set GIT_DIR and SVN_DIR to defaults if not given
if [ -z "$GIT_DIR" ]; then
  GIT_DIR="$HOME/git/hbase"
fi
if [ -z "$SVN_DIR" ]; then
  SVN_DIR="$HOME/svn/hbase.apache.org/trunk"
fi

export MAVEN_OPTS=${MAVEN_OPTS:-"-Xmx3g"}

cd $GIT_DIR

# Get the latest
echo "Updating Git"
git checkout -q master
git pull -q
status=$?
if [ $status -ne 0 ]; then
  echo "git pull command failed."
  echo " Please examine and run this script again after you have corrected the problem."
fi

GIT_SHA=$(git log --pretty=format:%H -n1)

# Generate the site to ~/git/hbase/target/site
if [ $INTERACTIVE -eq 1 ]; then
  read -p "Build the site? (y/n)" yn
  case $yn in
    [Yy]* )
      echo "Building $GIT_SHA"
      mvn clean site post-site site:stage
      status=$?
      if [ $status -ne 0 ]; then
        echo "The site does not build. Aborting."
        exit $status
      fi
      ;;
    [Nn]* )
      echo "Not building the site."
      if [ -d target/staging ]; then
        echo "Using site in $GIT_DIR/target/staging directory."
      else
        echo "Not building the site but $GIT_DIR/target/staging does not exist. Giving up."
        exit 1
      fi
      ;;
  esac
else
  echo "Building the site in auto mode."
  mvn clean site post-site site:stage
  status=$?
  if [ $status != 0 ]; then
    echo "The site does not build. Aborting."
    exit $status
  fi
fi


# Refresh the local copy of the live website
echo "Updating Subversion."
cd $SVN_DIR
# Be aware that this will restore all the files deleted a few lines down
# if you are debugging this script
# and need to run it multiple times without svn committing
svn update -q

# Make a list of things that are in SVN but not the generated site
# and not auto-generated
# -- we might need to delete these
# But we never want to delete images or logos

stale_dirs_exist=0
echo "The following DIRECTORIES (if any) might be stale:" |tee /tmp/out.txt
find . -type d  ! -path '*0.94*' ! -path '*apidocs*' ! -path '*xref*' ! -path '*book*' ! -path '*svn*' | \
  sed 's/\.\///g' | grep -v images | grep -v logos | grep -v img | grep -v css | \
  while read i; do
  if [ ! -d "$GIT_DIR/target/staging/$i" ]; then
    echo $i
    stale_dirs_exist=1
  fi
done

stale_files_exist=0
echo "The following FILES (if any) might be stale:" |tee -a /tmp/out.txt
find . -type f  ! -path '*0.94*' ! -path '*apidocs*' ! -path '*xref*' ! -path '*book*' ! -path '*svn*' | \
  sed 's/\.\///g' | grep -v images | grep -v logos | grep -v img | grep -v css | grep -v hbase-default.xml | \
  while read i; do
  if [ ! -f "$GIT_DIR/target/staging/$i" ]; then
    echo $i
    stale_files_exist=1
  fi
done

if [ $INTERACTIVE -eq 1 ]; then
  if [ $stale_dirs_exist -eq 1 -o $stale_files_exist -eq 1 ]; then
    read -p "Exit to take care of them? (y/n)" yn
    case $yn in
      [Yy]* )
        exit
        ;;
      [Nn]* )
        ;;
      * ) echo "Please answer yes or no.";;
    esac
  fi
else
  if [ $stale_dirs_exist -eq 1 -o $stale_files_exist -eq 1 ]; then
    echo "Stale files or directories exist, but not taking care of stale directories and files in auto mode." |tee -a /tmp/out.txt
    echo "If necessary, handle them in a manual commit."|tee -a /tmp/out.txt
  fi
fi

# Delete known auto-generated  content from trunk
echo "Deleting known auto-generated content from SVN"
rm -rf apidocs devapidocs xref xref-test book book.html java.html apache_hbase_reference_guide.pdf*

# Copy generated site to svn -- cp takes different options on Darwin and GNU
echo "Copying the generated site to SVN"
if [ `uname` == "Darwin" ]; then
  COPYOPTS='-r'
elif [ `uname` == "Linux" ]; then
  COPYOPTS='-au'
fi

cp $COPYOPTS $GIT_DIR/target/staging/. .

# Look for things we need to fix up in svn

echo "Untracked files: svn add"
svn status |grep '?' |sed -e "s/[[:space:]]//g"|cut -d '?' -f 2|while read i; do
  svn add $i
done

echo "Locally deleted files: svn del"
svn status |grep '!' |sed -e "s/[[:space:]]//g"|cut -d '!' -f 2|while read i; do
  svn del $i
done

svn_added=$(svn status | grep ^A|wc -l|awk {'print $1'})
svn_removed=$(svn status | grep ^D|wc -l|awk {'print $1'})
svn_modified=$(svn status |grep ^M|wc -l|awk {'print $1'})

echo -e "Published site at $GIT_SHA. \n\
\n\
Added $svn_added files. \n\
Removed $svn_removed files. \n\
Modified $svn_modified files." |tee /tmp/commit.txt

if [ $INTERACTIVE -eq 1 ]; then
  read -p "Commit changes? This will publish the website. (y/n)" yn
  case $yn in
  [Yy]* )
    echo "Commit message:"
    cat /tmp/commit.txt
    svn commit -F /tmp/commit.txt
    exit
    ;;
  [Nn]* )
    read -p "Revert SVN changes? (y/n)" revert
    case $revert in
    [Yy]* )
      svn revert -R .
      svn update
      exit
      ;;
    [Nn]* )
      exit
      ;;
    esac
    ;;
  esac
else
  echo "Commit message:"
  cat /tmp/commit.txt
  svn commit -q -F /tmp/commit.txt
fi
