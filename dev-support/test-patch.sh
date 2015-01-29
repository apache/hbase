#!/usr/bin/env bash
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


#set -x

### Setup some variables.  
### GIT_COMMIT and BUILD_URL are set by Hudson if it is run by patch process
### Read variables from properties file
bindir=$(dirname $0)

# Defaults
if [ -z "$MAVEN_HOME" ]; then
  MVN=mvn
else
  MVN=$MAVEN_HOME/bin/mvn
fi

NEWLINE=$'\n'

PROJECT_NAME=HBase
JENKINS=false
PATCH_DIR=/tmp
BASEDIR=$(pwd)
BRANCH_NAME="master"

PS=${PS:-ps}
AWK=${AWK:-awk}
WGET=${WGET:-wget}
GREP=${GREP:-grep}
EGREP=${EGREP:-egrep}
PATCH=${PATCH:-patch}
JIRACLI=${JIRA:-jira}
FINDBUGS_HOME=${FINDBUGS_HOME}
FORREST_HOME=${FORREST_HOME}
ECLIPSE_HOME=${ECLIPSE_HOME}
GIT=${GIT:-git}

###############################################################################
printUsage() {
  echo "Usage: $0 [options] patch-file | defect-number"
  echo
  echo "Where:"
  echo "  patch-file is a local patch file containing the changes to test"
  echo "  defect-number is a JIRA defect number (e.g. 'HADOOP-1234') to test (Jenkins only)"
  echo
  echo "Options:"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp')"
  echo "--basedir=<dir>        The directory to apply the patch to (default current directory)"
  echo "--mvn-cmd=<cmd>        The 'mvn' command to use (default \$MAVEN_HOME/bin/mvn, or 'mvn')"
  echo "--ps-cmd=<cmd>         The 'ps' command to use (default 'ps')"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--patch-cmd=<cmd>      The 'patch' command to use (default 'patch')"
  echo "--findbugs-home=<path> Findbugs home directory (default FINDBUGS_HOME environment variable)"
  echo "--forrest-home=<path>  Forrest home directory (default FORREST_HOME environment variable)"
  echo "--dirty-workspace      Allow the local workspace to have uncommitted changes"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--wget-cmd=<cmd>       The 'wget' command to use (default 'wget')"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default 'jira')"
  echo "--jira-password=<pw>   The password for the 'jira' command"
  echo "--eclipse-home=<path>  Eclipse home directory (default ECLIPSE_HOME environment variable)"
}

###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --jenkins)
      JENKINS=true
      ;;
    --patch-dir=*)
      PATCH_DIR=${i#*=}
      ;;
    --basedir=*)
      BASEDIR=${i#*=}
      ;;
    --mvn-cmd=*)
      MVN=${i#*=}
      ;;
    --ps-cmd=*)
      PS=${i#*=}
      ;;
    --awk-cmd=*)
      AWK=${i#*=}
      ;;
    --wget-cmd=*)
      WGET=${i#*=}
      ;;
    --grep-cmd=*)
      GREP=${i#*=}
      ;;
    --patch-cmd=*)
      PATCH=${i#*=}
      ;;
    --jira-cmd=*)
      JIRACLI=${i#*=}
      ;;
    --jira-password=*)
      JIRA_PASSWD=${i#*=}
      ;;
    --findbugs-home=*)
      FINDBUGS_HOME=${i#*=}
      ;;
    --forrest-home=*)
      FORREST_HOME=${i#*=}
      ;;
    --eclipse-home=*)
      ECLIPSE_HOME=${i#*=}
      ;;
    --dirty-workspace)
      DIRTY_WORKSPACE=true
      ;;
    --git-cmd=*)
      GIT=${i#*=}
      ;;
    *)
      PATCH_OR_DEFECT=$i
      ;;
    esac
  done
  if [ -z "$PATCH_OR_DEFECT" ]; then
    printUsage
    exit 1
  fi
  if [[ $JENKINS == "true" ]] ; then
    echo "Running in Jenkins mode"
    defect=$PATCH_OR_DEFECT
    ECLIPSE_PROPERTY="-Declipse.home=$ECLIPSE_HOME"
  else
    echo "Running in developer mode"
    JENKINS=false
    ### PATCH_FILE contains the location of the patchfile
    PATCH_FILE=$PATCH_OR_DEFECT
    if [[ ! -e "$PATCH_FILE" ]] ; then
      echo "Unable to locate the patch file $PATCH_FILE"
      cleanupAndExit 0
    fi
    ### Check if $PATCH_DIR exists. If it does not exist, create a new directory
    if [[ ! -e "$PATCH_DIR" ]] ; then
      mkdir "$PATCH_DIR"
      if [[ $? == 0 ]] ; then 
        echo "$PATCH_DIR has been created"
      else
        echo "Unable to create $PATCH_DIR"
        cleanupAndExit 0
      fi
    fi
    ### Obtain the patch filename to append it to the version number
    defect=`basename $PATCH_FILE`
  fi
}

###############################################################################
checkout () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Testing patch for ${defect}."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  ### When run by a developer, if the workspace contains modifications, do not continue
  ### unless the --dirty-workspace option was set
  if [[ $JENKINS == "false" ]] ; then
    if [[ -z $DIRTY_WORKSPACE ]] ; then
      # Ref http://stackoverflow.com/a/2659808 for details on checking dirty status
      ${GIT} diff-index --quiet HEAD
      if [[ $? -ne 0 ]] ; then
        uncommitted=`${GIT} diff --name-only HEAD`
        uncommitted="You have the following files with uncommitted changes:${NEWLINE}${uncommitted}"
      fi
      untracked="$(${GIT} ls-files --exclude-standard --others)" && test -z "${untracked}"
      if [[ $? -ne 0 ]] ; then
        untracked="You have untracked and unignored files:${NEWLINE}${untracked}"
      fi
      if [[ $uncommitted || $untracked ]] ; then
        echo "ERROR: can't run in a workspace that contains modifications."
        echo "Pass the '--dirty-workspace' flag to bypass."
        echo ""
        echo "${uncommitted}"
        echo ""
        echo "${untracked}"
        cleanupAndExit 1
      fi
    fi
    echo
  else
    if [[ $BRANCH_NAME -ne "master" ]]; then
      echo "${GIT} checkout ${BRANCH_NAME}"
      ${GIT} checkout ${BRANCH_NAME}
      echo "${GIT} status"
      ${GIT} status
    fi
  fi
  return $?
}

findBranchNameFromPatchName() {
  local patchName=$1
  for LOCAL_BRANCH_NAME in $BRANCH_NAMES; do
    if [[ $patchName =~ .*$LOCAL_BRANCH_NAME.* ]]; then
      BRANCH_NAME=$LOCAL_BRANCH_NAME
      break
    fi
  done
  return 0
}

###############################################################################
setup () {
  ### Download latest patch file (ignoring .htm and .html) when run from patch process
  if [[ $JENKINS == "true" ]] ; then
    $WGET -q -O $PATCH_DIR/jira http://issues.apache.org/jira/browse/$defect
    if [[ `$GREP -c 'Patch Available' $PATCH_DIR/jira` == 0 ]] ; then
      echo "$defect is not \"Patch Available\".  Exiting."
      cleanupAndExit 0
    fi
    relativePatchURL=`$GREP -o '"/jira/secure/attachment/[0-9]*/[^"]*' $PATCH_DIR/jira | $EGREP '(\.txt$|\.patch$|\.diff$)' | sort | tail -1 | $GREP -o '/jira/secure/attachment/[0-9]*/[^"]*'`
    patchURL="http://issues.apache.org${relativePatchURL}"
    patchNum=`echo $patchURL | $GREP -o '[0-9]*/' | $GREP -o '[0-9]*'`
    # ensure attachment has not already been tested
    ATTACHMENT_ID=$(basename $(dirname $patchURL))
    if grep -q "ATTACHMENT ID: $ATTACHMENT_ID" $PATCH_DIR/jira
    then
      echo "Attachment $ATTACHMENT_ID is already tested for $defect"
      exit 1
    fi
    echo "$defect patch is being downloaded at `date` from"
    echo "$patchURL"
    $WGET -q -O $PATCH_DIR/patch $patchURL
    VERSION=${GIT_COMMIT}_${defect}_PATCH-${patchNum}
    findBranchNameFromPatchName ${relativePatchURL}
    JIRA_COMMENT="Here are the results of testing the latest attachment 
  $patchURL
  against ${BRANCH_NAME} branch at commit ${GIT_COMMIT}.
  ATTACHMENT ID: ${ATTACHMENT_ID}"

  ### Copy the patch file to $PATCH_DIR
  else
    VERSION=PATCH-${defect}
    cp $PATCH_FILE $PATCH_DIR/patch
    if [[ $? == 0 ]] ; then
      echo "Patch file $PATCH_FILE copied to $PATCH_DIR"
    else
      echo "Could not copy $PATCH_FILE to $PATCH_DIR"
      cleanupAndExit 0
    fi
  fi
  . $BASEDIR/dev-support/test-patch.properties
  ### exit if warnings are NOT defined in the properties file
  if [ -z "$OK_FINDBUGS_WARNINGS" ] || [[ -z "$OK_JAVADOC_WARNINGS" ]] || [[ -z $OK_RELEASEAUDIT_WARNINGS ]] ; then
    echo "Please define the following properties in test-patch.properties file"
	 echo  "OK_FINDBUGS_WARNINGS"
	 echo  "OK_RELEASEAUDIT_WARNINGS"
	 echo  "OK_JAVADOC_WARNINGS"
    cleanupAndExit 1
  fi
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo " Pre-build master to verify stability and javac warnings"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean package checkstyle:checkstyle-aggregate -DskipTests -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/trunkJavacWarnings.txt 2>&1"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  # build core and tests
  $MVN clean package checkstyle:checkstyle-aggregate -DskipTests -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/trunkJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    ERR=`$GREP -A 5 'Compilation failure' $PATCH_DIR/trunkJavacWarnings.txt`
    echo "Trunk compilation is broken?
    {code}$ERR{code}"
    cleanupAndExit 1
  fi
  mv target/checkstyle-result.xml $PATCH_DIR/trunkCheckstyle.xml
}

###############################################################################
### Check for @author tags in the patch
checkAuthor () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are no @author tags in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  authorTags=`$GREP -c -i '@author' $PATCH_DIR/patch`
  echo "There appear to be $authorTags @author tags in the patch."
  if [[ $authorTags != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 @author{color}.  The patch appears to contain $authorTags @author tags which the Hadoop community has agreed to not allow in code contributions."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 @author{color}.  The patch does not contain any @author tags."
  return 0
}

###############################################################################
### Check for tests in the patch
checkTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are new or changed tests in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  testReferences=`$GREP -c -i '/test' $PATCH_DIR/patch`
  echo "There appear to be $testReferences test files referenced in the patch."
  if [[ $testReferences == 0 ]] ; then
    if [[ $JENKINS == "true" ]] ; then
      patchIsDoc=`$GREP -c -i 'title="documentation' $PATCH_DIR/jira`
      if [[ $patchIsDoc != 0 ]] ; then
        echo "The patch appears to be a documentation patch that doesn't require tests."
        JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+0 tests included{color}.  The patch appears to be a documentation patch that doesn't require tests."
        return 0
      fi
    fi
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 tests included{color}.  The patch doesn't appear to include any new or modified tests.
                        Please justify why no new tests are needed for this patch.
                        Also please list what manual steps were performed to verify this patch."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 tests included{color}.  The patch appears to include $testReferences new or modified tests."
  return 0
}

###############################################################################
### Check there are no compilation errors, passing a file to be parsed.
checkCompilationErrors() {
  local file=$1
  COMPILATION_ERROR=false
  eval $(awk '/ERROR/ {print "COMPILATION_ERROR=true"}' $file)
  if $COMPILATION_ERROR ; then
    ERRORS=$($AWK '/ERROR/ { print $0 }' $file)
    echo "======================================================================"
    echo "There are compilation errors."
    echo "======================================================================"
    echo "$ERRORS"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javac{color}.  The patch appears to cause mvn compile goal to fail.

    Compilation errors resume:
    $ERRORS
    "
    submitJiraComment 1
    cleanupAndExit 1
  fi
}

###############################################################################
### Check there are no protoc compilation errors, passing a file to be parsed.
checkProtocCompilationErrors() {
  local file=$1
  COMPILATION_ERROR=false
  eval $(awk '/\[ERROR/ {print "COMPILATION_ERROR=true"}' $file)
  if $COMPILATION_ERROR ; then
    ERRORS=$($AWK '/\[ERROR/ { print $0 }' $file)
    echo "======================================================================"
    echo "There are Protoc compilation errors."
    echo "======================================================================"
    echo "$ERRORS"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javac{color}.  The patch appears to cause mvn compile-protobuf profile to fail.

    Protoc Compilation errors resume:
    $ERRORS
    "
    cleanupAndExit 1
  fi
}

###############################################################################
### Attempt to apply the patch
applyPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Applying patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
 
  export PATCH
  $BASEDIR/dev-support/smart-apply-patch.sh $PATCH_DIR/patch
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 patch{color}.  The patch command could not apply the patch."
    return 1
  fi
  return 0
}

###############################################################################
### Check against known anti-patterns
checkAntiPatterns () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking against known anti-patterns."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  warnings=`$GREP 'new TreeMap<byte.*()' $PATCH_DIR/patch`
  if [[ $warnings != "" ]]; then
    JIRA_COMMENT="$JIRA_COMMENT

		    {color:red}-1 Anti-pattern{color}.  The patch appears to have anti-pattern where BYTES_COMPARATOR was omitted:
             $warnings."
	  return 1
  fi
  return 0
}

###############################################################################
### Check there are no javadoc warnings
checkJavadocWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javadoc warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean package javadoc:javadoc -DskipTests -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/patchJavadocWarnings.txt 2>&1"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  $MVN clean package javadoc:javadoc -DskipTests -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/patchJavadocWarnings.txt 2>&1
  javadocWarnings=`$GREP '\[WARNING\]' $PATCH_DIR/patchJavadocWarnings.txt | $AWK '/Javadoc Warnings/,EOF' | $GREP warning | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
  echo ""
  echo ""
  echo "There appear to be $javadocWarnings javadoc warnings generated by the patched build."

  ### if current warnings greater than OK_JAVADOC_WARNINGS
  if [[ $javadocWarnings -gt $OK_JAVADOC_WARNINGS ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javadoc{color}.  The javadoc tool appears to have generated `expr $(($javadocWarnings-$OK_JAVADOC_WARNINGS))` warning messages."
    # Add javadoc output url
    JIRA_COMMENT_FOOTER="Javadoc warnings: $BUILD_URL/artifact/patchprocess/patchJavadocWarnings.txt
$JIRA_COMMENT_FOOTER"
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javadoc{color}.  The javadoc tool did not generate any warning messages."
  return 0
}

###############################################################################
### Check there are no changes in the number of Javac warnings
checkJavacWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javac warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean package -DskipTests -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/patchJavacWarnings.txt 2>&1"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  $MVN clean package -DskipTests -D${PROJECT_NAME}PatchProcess  > $PATCH_DIR/patchJavacWarnings.txt 2>&1
  checkCompilationErrors $PATCH_DIR/patchJavacWarnings.txt
  ### Compare trunk and patch javac warning numbers
  if [[ -f $PATCH_DIR/patchJavacWarnings.txt ]] ; then
    trunkJavacWarnings=`$GREP '\[WARNING\]' $PATCH_DIR/trunkJavacWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    patchJavacWarnings=`$GREP '\[WARNING\]' $PATCH_DIR/patchJavacWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    echo "There appear to be $trunkJavacWarnings javac compiler warnings before the patch and $patchJavacWarnings javac compiler warnings after applying the patch."
    if [[ $patchJavacWarnings != "" && $trunkJavacWarnings != "" ]] ; then
      if [[ $patchJavacWarnings -gt $trunkJavacWarnings ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javac{color}.  The applied patch generated $patchJavacWarnings javac compiler warnings (more than the master's current $trunkJavacWarnings warnings)."
        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javac{color}.  The applied patch does not increase the total number of javac compiler warnings."
  return 0
}

checkCheckstyleErrors() {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched Checkstyle errors."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  if [[ -f $PATCH_DIR/trunkCheckstyle.xml ]] ; then
    $MVN package -DskipTests checkstyle:checkstyle-aggregate > /dev/null 2>&1
    mv target/checkstyle-result.xml $PATCH_DIR/patchCheckstyle.xml
    mv target/site/checkstyle-aggregate.html $PATCH_DIR
    mv target/site/checkstyle.css $PATCH_DIR
    trunkCheckstyleErrors=`$GREP '<error' $PATCH_DIR/trunkCheckstyle.xml | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    patchCheckstyleErrors=`$GREP '<error' $PATCH_DIR/patchCheckstyle.xml | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    if [[ $patchCheckstyleErrors -gt $trunkCheckstyleErrors ]] ; then
                JIRA_COMMENT_FOOTER="Checkstyle Errors: $BUILD_URL/artifact/patchprocess/checkstyle-aggregate.html

                $JIRA_COMMENT_FOOTER"

                JIRA_COMMENT="$JIRA_COMMENT

                {color:red}-1 checkstyle{color}.  The applied patch generated $patchCheckstyleErrors checkstyle errors (more than the master's current $trunkCheckstyleErrors errors)."
        return 1
    fi
    echo "There were $patchCheckstyleErrors checkstyle errors in this patch compared to $trunkCheckstyleErrors on master."
  fi
  JIRA_COMMENT_FOOTER="Checkstyle Errors: $BUILD_URL/artifact/patchprocess/checkstyle-aggregate.html

  $JIRA_COMMENT_FOOTER"

  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 checkstyle{color}.  The applied patch does not increase the total number of checkstyle errors"
  return 0

}
###############################################################################
checkProtocErrors () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining whether there is patched protoc error."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean install -DskipTests -Pcompile-protobuf -X -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/patchProtocErrors.txt 2>&1"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  $MVN clean install -DskipTests -Pcompile-protobuf -X -D${PROJECT_NAME}PatchProcess  > $PATCH_DIR/patchProtocErrors.txt 2>&1
  checkProtocCompilationErrors $PATCH_DIR/patchProtocErrors.txt
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javac{color}.  The applied patch does not increase the total number of javac compiler warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of release audit (RAT) warnings
checkReleaseAuditWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched release audit warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN apache-rat:check -D${PROJECT_NAME}PatchProcess 2>&1"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  $MVN apache-rat:check -D${PROJECT_NAME}PatchProcess 2>&1
  find $BASEDIR -name rat.txt | xargs cat > $PATCH_DIR/patchReleaseAuditWarnings.txt

  ### Compare trunk and patch release audit warning numbers
  if [[ -f $PATCH_DIR/patchReleaseAuditWarnings.txt ]] ; then
    patchReleaseAuditWarnings=`$GREP -c '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt`
    echo ""
    echo ""
    echo "There appear to be $OK_RELEASEAUDIT_WARNINGS release audit warnings before the patch and $patchReleaseAuditWarnings release audit warnings after applying the patch."
    if [[ $patchReleaseAuditWarnings != "" && $OK_RELEASEAUDIT_WARNINGS != "" ]] ; then
      if [[ $patchReleaseAuditWarnings -gt $OK_RELEASEAUDIT_WARNINGS ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 release audit{color}.  The applied patch generated $patchReleaseAuditWarnings release audit warnings (more than the master's current $OK_RELEASEAUDIT_WARNINGS warnings)."
        $GREP '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt > $PATCH_DIR/patchReleaseAuditProblems.txt
        echo "Lines that start with ????? in the release audit report indicate files that do not have an Apache license header." >> $PATCH_DIR/patchReleaseAuditProblems.txt
        JIRA_COMMENT_FOOTER="Release audit warnings: $BUILD_URL/artifact/patchprocess/patchReleaseAuditWarnings.txt
$JIRA_COMMENT_FOOTER"
        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 release audit{color}.  The applied patch does not increase the total number of release audit warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of Findbugs warnings
checkFindbugsWarnings () {
  findbugs_version=`${FINDBUGS_HOME}/bin/findbugs -version`
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched Findbugs warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean package findbugs:findbugs -D${PROJECT_NAME}PatchProcess" 
  export MAVEN_OPTS="${MAVEN_OPTS}"
  $MVN clean package findbugs:findbugs -D${PROJECT_NAME}PatchProcess -DskipTests < /dev/null

  if [ $? != 0 ] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 findbugs{color}.  The patch appears to cause Findbugs (version ${findbugs_version}) to fail."
    return 1
  fi
    
  findbugsWarnings=0
  for file in $(find $BASEDIR -name findbugsXml.xml)
  do
    relative_file=${file#$BASEDIR/} # strip leading $BASEDIR prefix
    if [ ! $relative_file == "target/findbugsXml.xml" ]; then
      module_suffix=${relative_file%/target/findbugsXml.xml} # strip trailing path
      module_suffix=`basename ${module_suffix}`
    fi
    
    cp $file $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml
    $FINDBUGS_HOME/bin/setBugDatabaseInfo -timestamp "01/01/2000" \
      $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml
    newFindbugsWarnings=`$FINDBUGS_HOME/bin/filterBugs -first "01/01/2000" $PATCH_DIR/patchFindbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.xml | $AWK '{print $1}'`
    echo "Found $newFindbugsWarnings Findbugs warnings ($file)"
    findbugsWarnings=$((findbugsWarnings+newFindbugsWarnings))
    echo "$FINDBUGS_HOME/bin/convertXmlToText -html  $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.xml $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.html"
    $FINDBUGS_HOME/bin/convertXmlToText -html  $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.xml $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.html
    file $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.xml $PATCH_DIR/newPatchFindbugsWarnings${module_suffix}.html
    JIRA_COMMENT_FOOTER="Findbugs warnings: $BUILD_URL/artifact/patchprocess/newPatchFindbugsWarnings${module_suffix}.html
$JIRA_COMMENT_FOOTER"
  done

  ### if current warnings greater than OK_FINDBUGS_WARNINGS
  if [[ $findbugsWarnings -gt $OK_FINDBUGS_WARNINGS ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 findbugs{color}.  The patch appears to introduce `expr $(($findbugsWarnings-$OK_FINDBUGS_WARNINGS))` new Findbugs (version ${findbugs_version}) warnings."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 findbugs{color}.  The patch does not introduce any new Findbugs (version ${findbugs_version}) warnings."
  return 0
}

###############################################################################
### Check line lengths
checkLineLengths () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking that no line have length > $MAX_LINE_LENGTH"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  #see http://en.wikipedia.org/wiki/Diff#Unified_format

  MAX_LINE_LENGTH_PATCH=`expr $MAX_LINE_LENGTH + 1`
  lines=`cat $PATCH_DIR/patch | grep "^+" | grep -v "^@@" | grep -v "^+++" | grep -v "import" | grep -v "org.apache.thrift." | grep -v "com.google.protobuf." | grep -v "hbase.protobuf.generated" | awk -v len="$MAX_LINE_LENGTH_PATCH" 'length ($0) > len' | head -n 10`
  ll=`echo "$lines" | wc -l`
  if [[ "$ll" -gt "1" ]]; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 lineLengths{color}.  The patch introduces the following lines longer than $MAX_LINE_LENGTH:
    $lines"

    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 lineLengths{color}.  The patch does not introduce lines longer than $MAX_LINE_LENGTH"
  return 0
}

###############################################################################
### Run the tests
runTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Running tests."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""


  ### kill any process remaining from another test, maybe even another project
  jps | grep surefirebooter | cut -d ' ' -f 1 | xargs kill -9 2>/dev/null
  
  failed_tests=""
  ### Kill any rogue build processes from the last attempt
  condemnedCount=`$PS auxwww | $GREP ${PROJECT_NAME}PatchProcess | $AWK '{print $2}' | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
  echo "WARNING: $condemnedCount rogue build processes detected, terminating."
  $PS auxwww | $GREP ${PROJECT_NAME}PatchProcess | $AWK '{print $2}' | /usr/bin/xargs -t -I {} /bin/kill -9 {} > /dev/null
  echo "$MVN clean test -Dsurefire.rerunFailingTestsCount=2 -P runAllTests -D${PROJECT_NAME}PatchProcess"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  ulimit -a
  $MVN clean test -Dsurefire.rerunFailingTestsCount=2 -P runAllTests -D${PROJECT_NAME}PatchProcess
  if [[ $? != 0 ]] ; then
     ### Find and format names of failed tests
     failed_tests=`find . -name 'TEST*.xml' | xargs $GREP  -l -E "<failure|<error" | sed -e "s|.*target/surefire-reports/TEST-|                  |g" | sed -e "s|\.xml||g"`
 
     JIRA_COMMENT="$JIRA_COMMENT

     {color:red}-1 core tests{color}.  The patch failed these unit tests:
     $failed_tests"
     BAD=1
  else
    JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 core tests{color}.  The patch passed unit tests in $modules."
    BAD=0
  fi
  ZOMBIE_TESTS_COUNT=`jps | grep surefirebooter | wc -l`
  if [[ $ZOMBIE_TESTS_COUNT != 0 ]] ; then
    #It seems sometimes the tests are not dying immediately. Let's give them 30s
    echo "Suspicious java process found - waiting 30s to see if there are just slow to stop"
    sleep 30
    ZOMBIE_TESTS_COUNT=`jps | grep surefirebooter | wc -l`
    if [[ $ZOMBIE_TESTS_COUNT != 0 ]] ; then
      echo "There are $ZOMBIE_TESTS_COUNT zombie tests, they should have been killed by surefire but survived"
      echo "************ BEGIN zombies jstack extract"
      ZB_STACK=`jps | grep surefirebooter | cut -d ' ' -f 1 | xargs -n 1 jstack | grep ".test" | grep "\.java"`
      jps | grep surefirebooter | cut -d ' ' -f 1 | xargs -n 1 jstack
      echo "************ END  zombies jstack extract"
      JIRA_COMMENT="$JIRA_COMMENT

     {color:red}-1 core zombie tests{color}.  There are ${ZOMBIE_TESTS_COUNT} zombie test(s): ${ZB_STACK}"
      BAD=1
      jps | grep surefirebooter | cut -d ' ' -f 1 | xargs kill -9
    else
      echo "We're ok: there is no zombie test, but some tests took some time to stop"
    fi
  else
    echo "We're ok: there is no zombie test"
  fi
  return $BAD
}

###############################################################################
### Check docbook site xml
checkSiteXml () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking Site generation"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""

  echo "$MVN package site -DskipTests -D${PROJECT_NAME}PatchProcess > $PATCH_DIR/patchSiteOutput.txt 2>&1"
  export MAVEN_OPTS="${MAVEN_OPTS}"
  $MVN package site -DskipTests -D${PROJECT_NAME}PatchProcess  > $PATCH_DIR/patchSiteOutput.txt 2>&1
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 site{color}.  The patch appears to cause mvn site goal to fail."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

  {color:green}+1 site{color}.  The mvn site goal succeeds with this patch."
  return 0
}

###############################################################################
### Run the inject-system-faults target
checkInjectSystemFaults () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking the integrity of system test framework code."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  
  ### Kill any rogue build processes from the last attempt
  $PS auxwww | $GREP ${PROJECT_NAME}PatchProcess | $AWK '{print $2}' | /usr/bin/xargs -t -I {} /bin/kill -9 {} > /dev/null

  #echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=no -Dcompile.c++=yes -Dforrest.home=$FORREST_HOME inject-system-faults"
  #$ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=no -Dcompile.c++=yes -Dforrest.home=$FORREST_HOME inject-system-faults
  echo "NOP"
  return 0
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 system test framework{color}.  The patch failed system test framework compile."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 system test framework{color}.  The patch passed system test framework compile."
  return 0
}

###############################################################################
### Submit a comment to the defect's Jira
submitJiraComment () {
  local result=$1
  ### Do not output the value of JIRA_COMMENT_FOOTER when run by a developer
  if [[  $JENKINS == "false" ]] ; then
    JIRA_COMMENT_FOOTER=""
  fi
  if [[ $result == 0 ]] ; then
    comment="{color:green}+1 overall{color}.  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  else
    comment="{color:red}-1 overall{color}.  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  fi
  ### Output the test result to the console
  echo "



$comment"  

  if [[ $JENKINS == "true" ]] ; then
    echo ""
    echo ""
    echo "======================================================================"
    echo "======================================================================"
    echo "    Adding comment to Jira."
    echo "======================================================================"
    echo "======================================================================"
    echo ""
    echo ""
    ### Update Jira with a comment
    export USER=hudson
    $JIRACLI -s https://issues.apache.org/jira -a addcomment -u hadoopqa -p $JIRA_PASSWD --comment "$comment" --issue $defect
    $JIRACLI -s https://issues.apache.org/jira -a logout -u hadoopqa -p $JIRA_PASSWD
  fi
}

###############################################################################
### Cleanup files
cleanupAndExit () {
  local result=$1
  if [[ $JENKINS == "true" ]] ; then
    if [ -e "$PATCH_DIR" ] ; then
      mv $PATCH_DIR $BASEDIR
    fi
  fi
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Finished build."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  exit $result
}

###############################################################################
###############################################################################
###############################################################################

JIRA_COMMENT=""
JIRA_COMMENT_FOOTER="Console output: $BUILD_URL/console

This message is automatically generated."

### Check if arguments to the script have been specified properly or not
parseArgs $@
cd $BASEDIR

checkout
RESULT=$?
if [[ $JENKINS == "true" ]] ; then
  if [[ $RESULT != 0 ]] ; then
    exit 100
  fi
fi
setup
checkAuthor
RESULT=$?
checkTests
(( RESULT = RESULT + $? ))
applyPatch
if [[ $? != 0 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi

checkAntiPatterns
(( RESULT = RESULT + $? ))
checkJavacWarnings
(( RESULT = RESULT + $? ))
checkProtocErrors
(( RESULT = RESULT + $? ))
checkJavadocWarnings
(( RESULT = RESULT + $? ))
checkCheckstyleErrors
(( RESULT = RESULT + $? ))
checkFindbugsWarnings
(( RESULT = RESULT + $? ))
checkReleaseAuditWarnings
(( RESULT = RESULT + $? ))
checkLineLengths
(( RESULT = RESULT + $? ))
checkSiteXml
(( RESULT = RESULT + $?))
### Do not call these when run by a developer 
if [[ $JENKINS == "true" ]] ; then
  runTests
  (( RESULT = RESULT + $? ))
JIRA_COMMENT_FOOTER="Test results: $BUILD_URL/testReport/
$JIRA_COMMENT_FOOTER"
fi
submitJiraComment $RESULT
cleanupAndExit $RESULT
