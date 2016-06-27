#!/usr/bin/env bash
#
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
#
#
# check_compatibility.sh
# A script that uses the Java API Compliance Checker (Java ACC) to gauge the binary and source
# compatibility of two arbitrary versions of Apache HBase.
#
# Special thanks to Andrey Ponomarenko, the leader of the Java ACC project, for introducing
# support for class annotation filtering into the tool at our request.
#
# Usage: This script checks out two versions of HBase (via a tag, branch, or commit hash in Git),
#        builds the releases, and generates XML descriptors of relevant JARs (i.e. excluding
#        test JARs, as well as external HBase dependencies). Next, the Java API Compliance
#        Checker (http://ispras.linuxbase.org/index.php/Java_API_Compliance_Checker) is
#        downloaded and run using these XML descriptor files to generate a report
#        of the degree of binary and source compatibility of the two HBase versions. Finally,
#        the resulting report is scraped and some of its results output to stdout.
#
#        Note that GNU getopt is required for this script to work properly. If you're running
#        a different variant (e.g. OS X ships with BSD getopt), you need to get the GNU variant
#        and either put it on the PATH or set GETOPT to the location of GNU getopt before
#        executing this script.
#
# Example: To compare the binary and source compatibility of the 0.98.6 release and the
#          tip of the master branch:
#          $ ./check_compatibility.sh 0.98.6
#          (i.e. if -b is omitted, a check is implicitly run against master).
#
#          To compare the binary and source compatibility of the HBase 0.98.5 and 0.98.6
#          releases:
#          $ ./check_compatibility.sh 0.98.5 0.98.6

SCRIPT_DIRECTORY=$(dirname ${BASH_SOURCE[0]})

# Usage message.
usage () {
  SCRIPT=$(basename "${BASH_SOURCE}")

  cat << __EOF

check_compatibility.sh
A script that uses the Java API Compliance Checker to gauge the binary and source
compatibility of two arbitrary versions of Apache HBase.

Usage: [<options>] <ref1> [<ref2>]

The positional arguments are Git references; this can be a tag (e.g. 0.98.6),
a branch (e.g. 0.98), or a particular commit hash. If ref2 is omitted, master
will be used.

Options:
  -a, --all                     Do not filter by interface annotations.
  -b, --binary-only             Only run the check for binary compatibility.
  -f, --force-download          Download dependencies (i.e. Java ACC), even if they are
                                already present.
  -h, --help                    Show this screen.
  -j, --java-acc                Specify which version of Java ACC to use to run the analysis. This
                                can be a tag, branch, or commit hash. Defaults to master.
  -n, --no-checkout             Run the tool without first using Git to checkout the two
                                HBase versions. If this option is selected,
                                dev-support/target/compatibility/1 and
                                dev-support/target compatibility/2 must each be Git repositories.
                                Also note that the references must still be specified as these are
                                used when naming the compatibility report.
  -o <opts>, --options=<opts>   A comma-separated list of options to pass directly to Java ACC.
  -q, --quick                   Runs Java ACC in quick analysis mode, which disables a
                                number of checks for things that may break compatibility.
  -r <url>, --repo=<url>        URL of the HBase Git repository to use. Defaults to Apache
                                HBase's GitHub (https://github.com/apache/hbase.git).
  -s, --source-only             Only run the check for source compatibility.
__EOF
}

# Allow a user to override which GETOPT to use, as described in the header.
GETOPT=${GETOPT:-/usr/bin/env getopt}

# Parse command line arguments and check for proper syntax.
if ! ARG_LIST=$(${GETOPT} -q -o abfhj:no:qr:s \
    -l all,binary-only,force-download,help,java-acc:,no-checkout,options:,quick,repo:,source-only \
    -- "${@}"); then
  usage >&2
  exit 2
fi
eval set -- "${ARG_LIST[@]}"

# Set defaults for options in case they're not specified on the command line.
JAVA_ACC_COMMIT="master"
REPO_URL="https://github.com/apache/hbase.git"

while ((${#})); do
  case "${1}" in
    -a | --all            )
      ALL=true
      shift 1 ;;
    -b | --binary-only    )
      JAVA_ACC_COMMAND+=(-binary)
      shift 1 ;;
    -f | --force-download )
      FORCE_DOWNLOAD=true
      shift 1 ;;
    -h | --help           )
      usage
      exit 0 ;;
    -j | --java-acc       )
      JAVA_ACC_COMMIT="${2}"
      shift 2 ;;
    -n | --no-checkout    )
      NO_CHECKOUT=true
      shift 1 ;;
    -q | --quick          )
      JAVA_ACC_COMMAND+=(-quick)
      shift 1 ;;
    -o | --options        )
      # Process and append the comma-separated list of options into the command array.
      JAVA_ACC_COMMAND+=($(tr "," "\n" <<< "${2}"))
      shift 2 ;;
    -r | --repo           )
      REPO_URL="${2}"
      shift 2 ;;
    -s | --source-only    )
      JAVA_ACC_COMMAND+=(-source)
      shift 1 ;;
    # getopt inserts -- to separate options and positional arguments.
    --                    )
      # First, shift past the -- to get to the positional arguments.
      shift 1
      # If there is one positional argument, only <ref1> was specified.
      if [ ${#} -eq 1 ]; then
        COMMIT[1]="${1}"
        COMMIT[2]=master
        shift 1
      # If there are two positional arguments, <ref1> and <ref2> were both specified.
      elif [ ${#} -eq 2 ]; then
        COMMIT[1]="${1}"
        COMMIT[2]="${2}"
        shift 2
      # If there are no positional arguments or too many, someone needs to reread the usage
      # message.
      else
        usage >&2
        exit 2
      fi
      ;;
  esac
done

# Do identical operations for both HBase versions in a for loop to save some lines of code.
for ref in 1 2; do
  if ! [ "${NO_CHECKOUT}" ]; then
    # Create empty directories for both versions in question.
    echo "Creating empty ${SCRIPT_DIRECTORY}/target/compatibility/${ref} directory..."
    rm -rf ${SCRIPT_DIRECTORY}/target/compatibility/${ref}
    mkdir -p ${SCRIPT_DIRECTORY}/target/compatibility/${ref}

    if [ "${ref}" = "1" ]; then
      echo "Cloning ${REPO_URL} into ${SCRIPT_DIRECTORY}/target/compatibility/${ref}..."
      if ! git clone ${REPO_URL} ${SCRIPT_DIRECTORY}/target/compatibility/${ref}; then
        echo "Error while cloning ${REPO_URL}. Exiting..." >&2
        exit 2
      fi
    elif [ "${ref}" = "2" ]; then
      # Avoid cloning from Git twice by copying first repo into different folder.
      echo "Copying Git repository into ${SCRIPT_DIRECTORY}/target/compatibility/${ref}..."
      cp -a ${SCRIPT_DIRECTORY}/target/compatibility/1/.git \
          ${SCRIPT_DIRECTORY}/target/compatibility/2
    fi

    # Use pushd and popd to keep track of directories while navigating around (and hide
    # printing of the stack).
    pushd ${SCRIPT_DIRECTORY}/target/compatibility/${ref} > /dev/null
    echo "Checking out ${COMMIT[${ref}]} into ${ref}/..."
    if ! git checkout -f ${COMMIT[${ref}]}; then
      echo "Error while checking out ${COMMIT[${ref}]}. Exiting..." >&2
      exit 2
    fi
    echo "Building ${COMMIT[${ref}]}..."
    if ! mvn clean package --batch-mode -DskipTests; then
      echo "Maven could not successfully package ${COMMIT[${ref}]}. Exiting..." >&2
      exit 2
    fi
    # grab sha for future reference
    SHA[${ref}]=$(git rev-parse --short HEAD)
    popd > /dev/null
  fi

  JAR_FIND_EXPRESSION=(-name "hbase*.jar" ! -name "*tests*" ! -name "*sources*" ! -name "*shade*")
  # Create an array of all the HBase JARs matching the find expression.
  JARS=$(find ${SCRIPT_DIRECTORY}/target/compatibility/${ref} "${JAR_FIND_EXPRESSION[@]}")

  if [ ${#JARS[@]} -eq 0 ]; then
    # If --no-checkout was specified and no JARs were found, try running mvn package
    # for the user before failing.
    if [ ${NO_CHECKOUT} ]; then
      for ref in 1 2; do
        pushd ${SCRIPT_DIRECTORY}/target/compatibility/${ref} > /dev/null
        echo "The --no-checkout option was specified, but no JARs were found." \
            "Attempting to build ${COMMIT[${ref}]}..."
        if ! mvn clean package --batch-mode -DskipTests; then
          echo "Maven could not successfully package ${COMMIT[${ref}]}. Exiting..." >&2
          exit 2
        fi
        SHA[${ref}]=$(git rev-parse --short HEAD)
        popd > /dev/null
      done

      JARS=$(find ${SCRIPT_DIRECTORY}/target/compatibility/${ref} "${JAR_FIND_EXPRESSION[@]}")
      if [ ${#JARS[@]} -eq 0 ]; then
        echo "Unable to find any JARs matching the find expression. Exiting..." >&2
        exit 2
      fi

    # If no JARs were found and --no-checkout was not specified, fail immediately.
    else
      echo "Unable to find any JARs matching the find expression. Exiting..." >&2
    fi
  fi

  # Create an XML descriptor containing paths to the JARs for Java ACC to use (support for
  # comma-separated lists of JARs was removed, as described on their issue tracker:
  # https://github.com/lvc/japi-compliance-checker/issues/27).
  DESCRIPTOR_PATH="${SCRIPT_DIRECTORY}/target/compatibility/${ref}.xml"
  echo "<version>${COMMIT[${ref}]}${SHA[${ref}]+"/${SHA[${ref}]}"}</version>" > "${DESCRIPTOR_PATH}"
  echo "<archives>" >> "${DESCRIPTOR_PATH}"

  echo "The JARs to be analyzed from ${COMMIT[${ref}]} are:"
  for jar in ${JARS}; do
    echo "  ${jar}" | tee -a "${DESCRIPTOR_PATH}"
  done
  echo "</archives>" >> "${DESCRIPTOR_PATH}"
done

# Download the Java API Compliance Checker (Java ACC) into /dev-support/target/compatibility.
# Note: Java API Compliance Checker (Java ACC) is licensed under the GNU GPL or LGPL. For more
#       information, visit http://ispras.linuxbase.org/index.php/Java_API_Compliance_Checker .

# Only clone Java ACC if it's missing or if option to force dependency download is present.
if [ ! -d ${SCRIPT_DIRECTORY}/target/compatibility/javaACC ] || [ -n "${FORCE_DOWNLOAD}" ]; then
  echo "Downloading Java API Compliance Checker..."
  rm -rf ${SCRIPT_DIRECTORY}/target/compatibility/javaACC
  if ! git clone https://github.com/lvc/japi-compliance-checker.git -b "${JAVA_ACC_COMMIT}" \
      ${SCRIPT_DIRECTORY}/target/compatibility/javaACC; then
    echo "Failed to download Java API Compliance Checker. Exiting..." >&2
    exit 2
  fi
fi

# Generate annotation list dynamically; this way, there's no chance the file
# gets stale and you have better visiblity into what classes are actually analyzed.
declare -a ANNOTATION_LIST
ANNOTATION_LIST+=(org.apache.hadoop.hbase.classification.InterfaceAudience.Public)
ANNOTATION_LIST+=(org.apache.hadoop.hbase.classification.InterfaceAudience.LimitedPrivate)
if ! [ -f ${SCRIPT_DIRECTORY}/target/compatibility/annotations ]; then
  cat > ${SCRIPT_DIRECTORY}/target/compatibility/annotations << __EOF
$(tr " " "\n" <<< "${ANNOTATION_LIST[@]}")
__EOF
fi

# Generate command line arguments for Java ACC.
JAVA_ACC_COMMAND+=(-l HBase)
JAVA_ACC_COMMAND+=(-old "${SCRIPT_DIRECTORY}/target/compatibility/1.xml")
JAVA_ACC_COMMAND+=(-new "${SCRIPT_DIRECTORY}/target/compatibility/2.xml")
JAVA_ACC_COMMAND+=(-report-path \
    ${SCRIPT_DIRECTORY}/target/compatibility/report/${COMMIT[1]}_${COMMIT[2]}_compat_report.html)
if [ "${ALL}" != "true" ] ; then
  JAVA_ACC_COMMAND+=(-annotations-list ${SCRIPT_DIRECTORY}/target/compatibility/annotations)
fi

# Delete any existing report folder under /dev-support/target/compatibility.
rm -rf ${SCRIPT_DIRECTORY}/target/compatibility/report

# Run the tool. Note that Java ACC returns an exit code of 0 if the two versions are
# compatible, an exit code of 1 if the two versions are not, and several other codes
# for various errors. See the tool's website for details.
echo "Running the Java API Compliance Checker..."
perl ${SCRIPT_DIRECTORY}/target/compatibility/javaACC/japi-compliance-checker.pl ${JAVA_ACC_COMMAND[@]}
