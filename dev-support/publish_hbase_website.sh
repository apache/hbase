#!/bin/bash

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
INTERACTIVE=
AUTO=
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
      INTERACTIVE=1
      ;;
    a)
      # We don't actually use this variable but require it to be 
      # set explicitly because it will commit changes without asking
      AUTO=1
      ;;
    g)
      GIT_DIR=$OPTARG
      ;;
    s)
      SVN_DIR=$OPTARG
      ;;
  esac
done

if [ $INTERACTIVE ] && [ $AUTO ]; then
    echo "Only one of -i or -a can be used."
    echo -e $USAGE
    exit 1
fi

# Set GIT_DIR and SVN_DIR to defaults if not given
if [ ! $GIT_DIR ]; then
  GIT_DIR=~/git/hbase
fi
if [ ! $SVN_DIR ]; then
  SVN_DIR=~/svn/hbase.apache.org/trunk
fi

# Check that GIT_DIR and SVN_DIR exist
if [ ! -d $GIT_DIR -o ! -d $SVN_DIR ]; then
  echo "Both the GIT and SVN directories must exist."
  echo -e $USAGE
  exit 1
fi

cd $GIT_DIR

# Get the latest
echo "Updating Git"
git checkout master
git pull

# Generate the site to ~/git/hbase/target/stage
if [ $INTERACTIVE ]; then
    read -p "Build the site? (y/n)" yn
    case $yn in
        [Yy]* ) 
    			mvn clean package javadoc:aggregate site site:stage -DskipTests
          status=$?
          if [ $status -ne 0 ]; then
            echo "The website does not build. Aborting."
            exit $status
          fi
    			;;
        [Nn]* ) 
          echo "Not building the site."
        ;;
    esac
else
  echo "Building the site in auto mode."
  mvn clean package javadoc:aggregate site site:stage -DskipTests
  status=$?
  if [ $status != 0 ]; then
    echo "The website does not build. Aborting."
    exit $status
  fi
fi


# Refresh the local copy of the live website
echo "Updating Subversion..."
cd $SVN_DIR
# Be aware that this will restore all the files deleted a few lines down 
# if you are debugging this script 
# and need to run it multiple times without svn committing
svn update > /dev/null

# Get current size of svn directory and # files, for sanity checking before commit
SVN_OLD_SIZE=`du -sm . |awk '{print $1}'`
SVN_OLD_NUMFILES=`find . -type f |wc -l |awk '{print $1}'`

# Make a list of things that are in SVN but not the generated site 
# and not auto-generated 
# -- we might need to delete these
echo "The following directories (if any) might be stale:" |tee /tmp/out.txt
find . -type d  ! -path '*0.94*' ! -path '*apidocs*' \
 ! -path '*xref*' ! -path '*book*' \
 -exec ls $GIT_DIR/target/staging/{} \; |grep 'No such' |tee -a /tmp/out.txt
echo "The following files (if any) might be stale:" |tee -a /tmp/out.txt
find . -type f  ! -path '*0.94*' ! -path '*apidocs*' \
 ! -path '*xref*' ! -path '*book*' ! -name '*.svg' \
 -exec ls $GIT_DIR/target/staging/{} \; |grep 'No such' |tee -a /tmp/out.txt

if [ $INTERACTIVE ]; then
  read -p "Exit to take care of them? (y/n)" yn
  case $yn in
      [Yy]* ) 
        exit
        ;;
      [Nn]* )
        ;;
      * ) echo "Please answer yes or no.";;
  esac
else
  echo "Not taking care of stale directories and files in auto mode." |tee -a /tmp/out.txt
  echo "If necessary, handle them in a manual commit."|tee -a /tmp/out.txt
fi

# Delete known auto-generated  content from trunk
echo "Deleting known auto-generated content from SVN"
rm -rf apidocs devapidocs xref xref-test book book.html java.html

# Copy generated site to svn -- cp takes different options on Darwin and GNU
echo "Copying the generated site to SVN"
if [ `uname` == "Darwin" ]; then
  COPYOPTS='-r'
elif [ `uname` == "Linux" ]; then
  COPYOPTS='-au'
fi

cp $COPYOPTS $GIT_DIR/target/site/* .

# Look for things we need to fix up in svn

echo "Untracked files: svn add"
svn status |grep '?' |sed -e "s/[[:space:]]//g"|cut -d '?' -f 2|while read i
  do svn add $i 
done

echo "Locally deleted files: svn del"
svn status |grep '!' |sed -e "s/[[:space:]]//g"|cut -d '!' -f 2|while read i
  do svn del $i 
done

# Display the proposed changes. I filtered out 
# modified because there are so many.
if [ $INTERACTIVE ]; then
  svn status |grep -v '^M'|less -P "Enter 'q' to exit the list."
else
  echo "The following changes will be made to SVN."
  svn status
fi

# Get current size of svn directory, for sanity checking before commit
SVN_NEW_SIZE=`du -sm . |awk '{print $1}'`
SVN_NEW_NUMFILES=`find . -type f |wc -l |awk '{print $1}'`

# Get difference between new and old size and number of files
# We don't care about negatives so remove the sign
SVN_SIZE_DIFF=`expr $SVN_NEW_SIZE - $SVN_OLD_SIZE|sed 's/-//g'`
SVN_NUM_DIFF=`expr $SVN_NEW_NUMFILES - $SVN_OLD_NUMFILES|sed 's/-//g'`

# The whole site is only 500 MB so a difference of 10 MB is huge
# In this case, we should abort because something is wrong
# Leaving this commented out for now until we get some benchmarks
#if [ $SVN_SIZE_DIFF > 10 -o $SVN_NUM_DIFF > 50 ]; then
#  echo "This commit would cause the website to change sizes by \
#  $SVN_DIFF MB and $SVN_NUM_DIFF files. There is likely a problem. 
#  Aborting."
#  exit 1
#fi


if [ $INTERACTIVE ]; then
  read -p "Commit changes? This will publish the website. (y/n)" yn
  case $yn in
  [Yy]* ) 
    echo "Published website using script in interactive mode. \
This commit changed the size of the website by $SVN_SIZE_DIFF MB \
and the number of files by $SVN_NUM_DIFF files." |tee commit.txt
    cat /tmp/out.txt >> /tmp/commit.txt
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
  echo "Published website using script in auto mode. This commit \
changed the size of the website by $SVN_SIZE_DIFF MB and the number of files \
by $SVN_NUM_DIFF files." |tee /tmp/commit.txt
  cat /tmp/out.txt >> /tmp/commit.txt
  svn commit -q -F /tmp/commit.txt
fi

