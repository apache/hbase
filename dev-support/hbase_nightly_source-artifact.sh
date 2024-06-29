#!/usr/bin/env bash
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

function usage {
  echo "Usage: ${0} [options] /path/to/component/checkout"
  echo ""
  echo "    --intermediate-file-dir /path/to/use  Path for writing listings and diffs. must exist."
  echo "                                          defaults to making a directory via mktemp."
  echo "    --unpack-temp-dir /path/to/use        Path for unpacking tarball. default to"
  echo "                                          'unpacked_src_tarball' in intermediate directory."
  echo "    --maven-m2-initial /path/to/use       Path for maven artifacts while building in"
  echo "                                          component-dir."
  echo "    --maven-m2-src-build /path/to/use     Path for maven artifacts while building from the"
  echo "                                          unpacked source tarball."
  echo "    --clean-source-checkout               Destructively clean component checkout before"
  echo "                                          comparing to source tarball. N.B. will delete"
  echo "                                          anything in the checkout dir that isn't from"
  echo "                                          a git checkout, including ignored files."
  exit 1
}

MVN="mvn"
if ! command -v mvn &>/dev/null; then
  MVN=$MAVEN_HOME/bin/mvn
fi
# if no args specified, show usage
if [ $# -lt 1 ]; then
  usage
fi

# Get arguments
declare component_dir
declare unpack_dir
declare m2_initial
declare m2_tarbuild
declare working_dir
declare source_clean
while [ $# -gt 0 ]
do
  case "$1" in
    --unpack-temp-dir) shift; unpack_dir=$1; shift;;
    --maven-m2-initial) shift; m2_initial=$1; shift;;
    --maven-m2-src-build) shift; m2_tarbuild=$1; shift;;
    --intermediate-file-dir) shift; working_dir=$1; shift;;
    --clean-source-checkout) shift; source_clean="true";;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

# should still have where component checkout is.
if [ $# -lt 1 ]; then
  usage
fi
component_dir="$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"

if [ -z "${working_dir}" ]; then
  if ! working_dir="$(mktemp -d -t hbase-srctarball-test)" ; then
    echo "Failed to create temporary working directory. Please specify via --unpack-temp-dir"
    exit 1
  fi
else
  # absolutes please
  working_dir="$(cd "$(dirname "${working_dir}")"; pwd)/$(basename "${working_dir}")"
  if [ ! -d "${working_dir}" ]; then
    echo "passed working directory '${working_dir}' must already exist."
    exit 1
  fi
fi

echo "You'll find logs and temp files in ${working_dir}"

if [ -z "${unpack_dir}" ]; then
  unpack_dir="${working_dir}/unpacked_src_tarball"
  mkdir "${unpack_dir}"
else
  # absolutes please
  unpack_dir="$(cd "$(dirname "${unpack_dir}")"; pwd)/$(basename "${unpack_dir}")"
  if [ ! -d "${unpack_dir}" ]; then
    echo "passed directory for unpacking the source tarball '${unpack_dir}' must already exist."
    exit 1
  fi
  rm -rf "${unpack_dir:?}/*"
fi

if [ -z "${m2_initial}" ]; then
  m2_initial="${working_dir}/.m2-initial"
  mkdir "${m2_initial}"
else
  # absolutes please
  m2_initial="$(cd "$(dirname "${m2_initial}")"; pwd)/$(basename "${m2_initial}")"
  if [ ! -d "${m2_initial}" ]; then
    echo "passed directory for storing the initial build's maven repo  '${m2_initial}' " \
        "must already exist."
    exit 1
  fi
fi

if [ -z "${m2_tarbuild}" ]; then
  m2_tarbuild="${working_dir}/.m2-tarbuild"
  mkdir "${m2_tarbuild}"
else
  # absolutes please
  m2_tarbuild="$(cd "$(dirname "${m2_tarbuild}")"; pwd)/$(basename "${m2_tarbuild}")"
  if [ ! -d "${m2_tarbuild}" ]; then
    echo "passed directory for storing the build from src tarball's maven repo  '${m2_tarbuild}' " \
        "must already exist."
    exit 1
  fi
fi

# This is meant to mimic what a release manager will do to create RCs.
# See http://hbase.apache.org/book.html#maven.release

echo "Maven details, in case our JDK doesn't match expectations:"
${MVN} --version --offline | tee "${working_dir}/maven_version"

echo "Do a clean building of the source artifact using code in ${component_dir}"
cd "${component_dir}"
if [ -n "${source_clean}" ]; then
  echo "Clean..."
  git clean -xdfff >"${working_dir}/component_git_clean.log" 2>&1
fi
echo "Follow the ref guide section on making a RC: Step 6 Build the source tarball"
git archive --format=tar.gz --output="${working_dir}/hbase-src.tar.gz" \
    --prefix="hbase-SOMEVERSION/" HEAD \
    >"${working_dir}/component_build_src_tarball.log" 2>&1

cd "${unpack_dir}"
echo "Unpack the source tarball"
tar --strip-components=1 -xzf "${working_dir}/hbase-src.tar.gz" \
    >"${working_dir}/srctarball_unpack.log" 2>&1

cd "${component_dir}"
echo "Diff against source tree"
diff --binary --recursive . "${unpack_dir}" >"${working_dir}/diff_output" || true

cd "${working_dir}"
# expectation check largely based on HBASE-14952
echo "Checking against things we don't expect to include in the source tarball (git related, etc.)"
# Add in lines to show differences between the source tarball and this branch, in the same format diff would give.
# e.g. prior to HBASE-19152 we'd have the following lines (ignoring the bash comment marker):
#Only in .: .gitattributes
#Only in .: .gitignore
cat >known_excluded <<END
Only in .: .git
END
if ! diff known_excluded diff_output >"${working_dir}/unexpected.diff" ; then
  echo "Any output here are unexpected differences between the source artifact we'd make for an RC and the current branch."
  echo "One potential source of differences is if you have an unclean working directory; you should expect to see"
  echo "such extraneous files below."
  echo ""
  echo "The expected differences are on the < side and the current differences are on the > side."
  echo "In a given set of differences, '.' refers to the branch in the repo and 'unpacked_src_tarball' refers to what we pulled out of the tarball."
  diff known_excluded diff_output
else
  echo "Everything looks as expected."
fi

function get_hadoop3_version {
  local version="$1"
  if [[ "${version}" =~ -SNAPSHOT$ ]]; then
    echo "${version/-SNAPSHOT/-hadoop3-SNAPSHOT}"
  else
    echo "${version}-hadoop3"
  fi
}

function build_tarball {
  local build_hadoop3=$1
  local mvn_extra_args=""
  local build_log="srctarball_install.log"
  local tarball_glob="hbase-*-bin.tar.gz"
  if [ $build_hadoop3 -ne 0 ]; then
    local version=$(${MVN} -Dmaven.repo.local="${m2_tarbuild}" help:evaluate -Dexpression=project.version -q -DforceStdout)
    local hadoop3_version=$(get_hadoop3_version $version)
    mvn_extra_args="-Drevision=${hadoop3_version} -Dhadoop.profile=3.0"
    build_log="hadoop3_srctarball_install.log"
    tarball_glob="hbase-*-hadoop3-*-bin.tar.gz"
    echo "Follow the ref guide section on making a RC: Step 8 Build the hadoop3 binary tarball."
  else
    echo "Follow the ref guide section on making a RC: Step 7 Build the binary tarball."
  fi
  if ${MVN} --threads=2 -DskipTests -Prelease --batch-mode -Dmaven.repo.local="${m2_tarbuild}" ${mvn_extra_args} clean install \
    assembly:single >"${working_dir}/${build_log}" 2>&1; then
    for artifact in "${unpack_dir}"/hbase-assembly/target/${tarball_glob}; do
      if [ -f "${artifact}" ]; then
        # TODO check the layout of the binary artifact we just made.
        echo "Building a binary tarball from the source tarball succeeded."
        return 0
      fi
    done
  fi

  echo "Building a binary tarball from the source tarball failed. see ${working_dir}/${build_log} for details."
  # Copy up the rat.txt to the working dir so available in build archive in case rat complaints.
  # rat.txt can be under any module target dir... copy them all up renaming them to include parent dir as we go.
  find ${unpack_dir} -name rat.txt -type f | while IFS= read -r NAME; do cp -v "$NAME" "${working_dir}/${NAME//\//_}"; done
  return 1
}

cd "${unpack_dir}"

${MVN} -Dmaven.repo.local="${m2_tarbuild}" help:active-profiles | grep -q hadoop-3.0
if [ $? -ne 0 ]; then
  echo "The hadoop-3.0 profile is not activated by default, build a default tarball first."
  # use java 8 to build with hadoop2
  JAVA_HOME="/usr/lib/jvm/java-8" build_tarball 0
  if [ $? -ne 0 ]; then
    exit 1
  fi

  # move the previous tarballs out, so it will not be cleaned while building against hadoop3
  mv "${unpack_dir}"/hbase-assembly/target/hbase-*-bin.tar.gz "${unpack_dir}"/
  echo "build a hadoop3 tarball."
  build_tarball 1
  if [ $? -ne 0 ]; then
    exit 1
  fi
  # move tarballs back
  mv "${unpack_dir}"/hbase-*-bin.tar.gz "${unpack_dir}"/hbase-assembly/target/
else
  echo "The hadoop-3.0 profile is activated by default, build a default tarball."
  build_tarball 0
fi
