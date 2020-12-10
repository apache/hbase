#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
DRY_RUN=${DRY_RUN:-1} #default to dry run
DEBUG=${DEBUG:-0}
GPG=${GPG:-gpg}
GPG_ARGS=(--no-autostart --batch)
if [ -n "${GPG_KEY}" ]; then
  GPG_ARGS=("${GPG_ARGS[@]}" --local-user "${GPG_KEY}")
fi
# Maven Profiles for publishing snapshots and release to Maven Central and Dist
PUBLISH_PROFILES=("-P" "apache-release,release")

set -e

function error {
  log "Error: $*" >&2
  exit 1
}

function read_config {
  local PROMPT="$1"
  local DEFAULT="$2"
  local REPLY=

  read -r -p "$PROMPT [$DEFAULT]: " REPLY
  local RETVAL="${REPLY:-$DEFAULT}"
  if [ -z "$RETVAL" ]; then
    error "$PROMPT must be provided."
  fi
  echo "$RETVAL"
}

function parse_version {
  grep -e '<version>.*</version>' | \
    head -n 2 | tail -n 1 | cut -d'>' -f2 | cut -d '<' -f1
}

function banner {
  local msg="$1"
  echo "========================"
  log "${msg}"
  echo
}

function log {
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ${1}"
}

# current number of seconds since epoch
function get_ctime {
  date +"%s"
}

function run_silent {
  local BANNER="$1"
  local LOG_FILE="$2"
  shift 2
  local -i start_time
  local -i stop_time

  banner "${BANNER}"
  log "Command: $*"
  log "Log file: $LOG_FILE"
  start_time="$(get_ctime)"

  if ! "$@" 1>"$LOG_FILE" 2>&1; then
    log "Command FAILED. Check full logs for details."
    tail "$LOG_FILE"
    exit 1
  fi
  stop_time="$(get_ctime)"
  log "SUCCESS ($((stop_time - start_time)) seconds)"
}

function fcreate_secure {
  local FPATH="$1"
  rm -f "$FPATH"
  touch "$FPATH"
  chmod 600 "$FPATH"
}

# API compare version.
function get_api_diff_version {
  local version="$1"
  local rev
  local api_diff_tag
  rev=$(echo "$version" | cut -d . -f 3)
  if [ "$rev" != 0 ]; then
    local short_version
    short_version="$(echo "$version" | cut -d . -f 1-2)"
    api_diff_tag="rel/${short_version}.$((rev - 1))"
  else
    local major minor
    major="$(echo "$version" | cut -d . -f 1)"
    minor="$(echo "$version" | cut -d . -f 2)"
    if [ "$minor" != 0 ]; then
      api_diff_tag="rel/${major}.$((minor - 1)).0"
    else
      api_diff_tag="rel/$((major - 1)).0.0"
    fi
  fi
  api_diff_tag="$(read_config "api_diff_tag" "$api_diff_tag")"
  echo "$api_diff_tag"
}

# Get all branches that begin with 'branch-', the hbase convention for
# release branches, sort them and then pop off the most recent.
function get_release_info {
  PROJECT="$(read_config "PROJECT" "$PROJECT")"
  export PROJECT

  if [[ -z "${ASF_REPO}" ]]; then
    ASF_REPO="https://gitbox.apache.org/repos/asf/${PROJECT}.git"
  fi
  if [[ -z "${ASF_REPO_WEBUI}" ]]; then
    ASF_REPO_WEBUI="https://gitbox.apache.org/repos/asf?p=${PROJECT}.git"
  fi
  if [[ -z "${ASF_GITHUB_REPO}" ]]; then
    ASF_GITHUB_REPO="https://github.com/apache/${PROJECT}"
  fi
  if [ -z "$GIT_BRANCH" ]; then
    # If no branch is specified, find out the latest branch from the repo.
    GIT_BRANCH="$(git ls-remote --heads "$ASF_REPO" |
      grep refs/heads/branch- |
      awk '{print $2}' |
      sort -r |
      head -n 1 |
      cut -d/ -f3)"
  fi

  GIT_BRANCH="$(read_config "GIT_BRANCH" "$GIT_BRANCH")"
  export GIT_BRANCH

  # Find the current version for the branch.
  local version
  version="$(curl -s "$ASF_REPO_WEBUI;a=blob_plain;f=pom.xml;hb=refs/heads/$GIT_BRANCH" |
    parse_version)"
  log "Current branch VERSION is $version."

  NEXT_VERSION="$version"
  RELEASE_VERSION=""
  SHORT_VERSION="$(echo "$version" | cut -d . -f 1-2)"
  if [[ ! "$version" =~ .*-SNAPSHOT ]]; then
    RELEASE_VERSION="$version"
  else
    RELEASE_VERSION="${version/-SNAPSHOT/}"
  fi

  local REV
  REV="$(echo "${RELEASE_VERSION}" | cut -d . -f 3)"

  # Find out what RC is being prepared.
  # - If the current version is "x.y.0", then this is RC0 of the "x.y.0" release.
  # - If not, need to check whether the previous version has been already released or not.
  #   - If it has, then we're building RC0 of the current version.
  #   - If it has not, we're building the next RC of the previous version.
  local RC_COUNT
  if [ "$REV" != 0 ]; then
    local PREV_REL_REV=$((REV - 1))
    PREV_REL_TAG="rel/${SHORT_VERSION}.${PREV_REL_REV}"
    if git ls-remote --tags "$ASF_REPO" "$PREV_REL_TAG" | grep -q "refs/tags/${PREV_REL_TAG}$" ; then
      RC_COUNT=0
      REV=$((REV + 1))
      NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    else
      RELEASE_VERSION="${SHORT_VERSION}.${PREV_REL_REV}"
      RC_COUNT="$(git ls-remote --tags "$ASF_REPO" "${RELEASE_VERSION}RC*" | wc -l)"
      # This makes a 'number' of it.
      RC_COUNT=$((RC_COUNT))
    fi
  else
    REV=$((REV + 1))
    NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    RC_COUNT=0
  fi

  RELEASE_VERSION="$(read_config "RELEASE_VERSION" "$RELEASE_VERSION")"
  NEXT_VERSION="$(read_config "NEXT_VERSION" "$NEXT_VERSION")"
  export RELEASE_VERSION NEXT_VERSION

  RC_COUNT="$(read_config "RC_COUNT" "$RC_COUNT")"
  RELEASE_TAG="${RELEASE_VERSION}RC${RC_COUNT}"
  RELEASE_TAG="$(read_config "RELEASE_TAG" "$RELEASE_TAG")"

  # Check if the RC already exists, and if re-creating the RC, skip tag creation.
  SKIP_TAG=0
  if git ls-remote --tags "$ASF_REPO" "$RELEASE_TAG" | grep -q "refs/tags/${RELEASE_TAG}$" ; then
    read -r -p "$RELEASE_TAG already exists. Continue anyway [y/n]? " ANSWER
    if [ "$ANSWER" != "y" ]; then
      log "Exiting."
      exit 1
    fi
    SKIP_TAG=1
  fi

  export RELEASE_TAG SKIP_TAG

  GIT_REF="$RELEASE_TAG"
  if is_dry_run; then
    log "This is a dry run. If tag does not actually exist, please confirm the ref that will be built for testing."
    GIT_REF="$(read_config "GIT_REF" "$GIT_REF")"
  fi
  export GIT_REF

  API_DIFF_TAG="$(get_api_diff_version "$RELEASE_VERSION")"

  # Gather some user information.
  ASF_USERNAME="$(read_config "ASF_USERNAME" "$LOGNAME")"

  GIT_NAME="$(git config user.name || echo "")"
  GIT_NAME="$(read_config "GIT_NAME" "$GIT_NAME")"

  GIT_EMAIL="$ASF_USERNAME@apache.org"
  GPG_KEY="$(read_config "GPG_KEY" "$GIT_EMAIL")"
  if ! GPG_KEY_ID=$("${GPG}" "${GPG_ARGS[@]}" --keyid-format 0xshort --list-public-key "${GPG_KEY}" | grep "\[S\]" | grep -o "0x[0-9A-F]*") ||
      [ -z "${GPG_KEY_ID}" ] ; then
    GPG_KEY_ID=$("${GPG}" "${GPG_ARGS[@]}" --keyid-format 0xshort --list-public-key "${GPG_KEY}" | head -n 1 | grep -o "0x[0-9A-F]*" || true)
  fi
  read -r -p "We think the key '${GPG_KEY}' corresponds to the key id '${GPG_KEY_ID}'. Is this correct [y/n]? " ANSWER
  if [ "$ANSWER" = "y" ]; then
    GPG_KEY="${GPG_KEY_ID}"
  fi
  export API_DIFF_TAG ASF_USERNAME GIT_NAME GIT_EMAIL GPG_KEY

  cat <<EOF
================
Release details:
GIT_BRANCH:      $GIT_BRANCH
RELEASE_VERSION: $RELEASE_VERSION
NEXT_VERSION:    $NEXT_VERSION
RELEASE_TAG:     $RELEASE_TAG $([[ "$GIT_REF" != "$RELEASE_TAG" ]] && printf "\n%s\n" "GIT_REF:         $GIT_REF")
API_DIFF_TAG:    $API_DIFF_TAG
ASF_USERNAME:    $ASF_USERNAME
GPG_KEY:         $GPG_KEY
GIT_NAME:        $GIT_NAME
GIT_EMAIL:       $GIT_EMAIL
DRY_RUN:         $(is_dry_run && echo "yes" || echo "NO, THIS BUILD WILL BE PUBLISHED!")
================
EOF

  read -r -p "Is this info correct [y/n]? " ANSWER
  if [ "$ANSWER" != "y" ]; then
    log "Exiting."
    exit 1
  fi
  GPG_ARGS=("${GPG_ARGS[@]}" --local-user "${GPG_KEY}")

  if ! is_dry_run; then
    if [ -z "$ASF_PASSWORD" ]; then
      stty -echo && printf "ASF_PASSWORD: " && read -r ASF_PASSWORD && printf '\n' && stty echo
    fi
  else
    ASF_PASSWORD="***INVALID***"
  fi

  export ASF_PASSWORD
}

function is_dry_run {
  [[ "$DRY_RUN" = 1 ]]
}

function is_debug {
  [[ "${DEBUG}" = 1 ]]
}

function check_get_passwords {
  for env in "$@"; do
    if [ -z "${!env}" ]; then
      log "The environment variable $env is not set. Please enter the password or passphrase."
      echo
      # shellcheck disable=SC2229
      stty -echo && printf "%s : " "$env" && read -r "$env" && printf '\n' && stty echo
    fi
    # shellcheck disable=SC2163
    export "$env"
  done
}

function check_needed_vars {
  local missing=0
  for env in "$@"; do
    if [ -z "${!env}" ]; then
      log "$env must be set to run this script"
      (( missing++ ))
    else
      # shellcheck disable=SC2163
      export "$env"
    fi
  done
  (( missing > 0 )) && exit_with_usage
  return 0
}

function init_locale {
  local locale_value
  OS="$(uname -s)"
  case "${OS}" in
    Darwin*)    locale_value="en_US.UTF-8";;
    Linux*)     locale_value="C.UTF-8";;
    *)          error "unknown OS";;
  esac
  export LC_ALL="$locale_value"
  export LANG="$locale_value"
}

# Initializes JAVA_VERSION to the version of the JVM in use.
function init_java {
  if [ -z "$JAVA_HOME" ]; then
    error "JAVA_HOME is not set."
  fi
  JAVA_VERSION=$("${JAVA_HOME}"/bin/javac -version 2>&1 | cut -d " " -f 2)
  log "java version: $JAVA_VERSION"
  export JAVA_VERSION
}

function init_python {
  if ! [ -x "$(command -v python2)"  ]; then
    error 'python2 needed by yetus. Install or add link? E.g: sudo ln -sf /usr/bin/python2.7 /usr/local/bin/python2'
  fi
  log "python version: $(python2 --version)"
}

# Set MVN
function init_mvn {
  if [ -n "$MAVEN_HOME" ]; then
      MVN=("${MAVEN_HOME}/bin/mvn")
  elif [ "$(type -P mvn)" ]; then
      MVN=(mvn)
  else
    error "MAVEN_HOME is not set nor is mvn on the current path."
  fi
  # Add batch mode.
  MVN=("${MVN[@]}" -B)
  export MVN
  echo -n "mvn version: "
  "${MVN[@]}" --version
  configure_maven
}

function init_yetus {
  declare YETUS_VERSION
  if [ -z "${YETUS_HOME}" ]; then
    error "Missing Apache Yetus."
  fi
  # Work around yetus bug by asking test-patch for the version instead of rdm.
  YETUS_VERSION=$("${YETUS_HOME}/bin/test-patch" --version)
  log "Apache Yetus version ${YETUS_VERSION}"
}

function configure_maven {
  # Add timestamps to mvn logs.
  MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss ${MAVEN_OPTS}"
  # Suppress gobs of "Download from central:" messages
  MAVEN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn ${MAVEN_OPTS}"
  MAVEN_LOCAL_REPO="${REPO:-$(pwd)/$(mktemp -d hbase-repo-XXXXX)}"
  [[ -d "$MAVEN_LOCAL_REPO" ]] || mkdir -p "$MAVEN_LOCAL_REPO"
  MAVEN_SETTINGS_FILE="${MAVEN_LOCAL_REPO}/tmp-settings.xml"
  MVN=("${MVN[@]}" --settings "${MAVEN_SETTINGS_FILE}")
  export MVN MAVEN_OPTS MAVEN_SETTINGS_FILE MAVEN_LOCAL_REPO
  export ASF_USERNAME ASF_PASSWORD
  # reference passwords from env rather than storing in the settings.xml file.
  cat <<'EOF' > "$MAVEN_SETTINGS_FILE"
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <localRepository>/${env.MAVEN_LOCAL_REPO}</localRepository>
  <servers>
    <server><id>apache.snapshots.https</id><username>${env.ASF_USERNAME}</username>
      <password>${env.ASF_PASSWORD}</password></server>
    <server><id>apache.releases.https</id><username>${env.ASF_USERNAME}</username>
      <password>${env.ASF_PASSWORD}</password></server>
  </servers>
  <profiles>
    <profile>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <properties>
        <gpg.keyname>${env.GPG_KEY}</gpg.keyname>
      </properties>
    </profile>
  </profiles>
</settings>
EOF
}

# clone of the repo, deleting anything that exists in the working directory named after the project.
# optionally with auth details for pushing.
function git_clone_overwrite {
  local asf_repo
  if [ -z "${PROJECT}" ] || [ "${PROJECT}" != "${PROJECT#/}" ]; then
    error "Project name must be defined and not start with a '/'. PROJECT='${PROJECT}'"
  fi
  rm -rf "${PROJECT}"

  if [[ -z "${GIT_REPO}" ]]; then
    asf_repo="gitbox.apache.org/repos/asf/${PROJECT}.git"
    log "Clone will be of the gitbox repo for ${PROJECT}."
    if [ -n "${ASF_USERNAME}" ] && [ -n "${ASF_PASSWORD}" ]; then
      # Ugly!
      encoded_username=$(python -c "import urllib; print urllib.quote('''$ASF_USERNAME''', '')")
      encoded_password=$(python -c "import urllib; print urllib.quote('''$ASF_PASSWORD''', '')")
      GIT_REPO="https://$encoded_username:$encoded_password@${asf_repo}"
    else
      GIT_REPO="https://${asf_repo}"
    fi
  else
    log "Clone will be of provided git repo."
  fi
  # N.B. we use the shared flag because the clone is short lived and if a local repo repo was
  #      given this will let us refer to objects there directly instead of hardlinks or copying.
  #      The option is silently ignored for non-local repositories. see the note on git help clone
  #      for the --shared option for details.
  git clone --shared -b "${GIT_BRANCH}" -- "${GIT_REPO}" "${PROJECT}"
  # If this was a host local git repo then add in an alternates and remote that will
  # work back on the host if the RM needs to do any post-processing steps, i.e. pushing the git tag
  # for more info see 'git help remote' and 'git help repository-layout'.
  if [ -n "$HOST_GIT_REPO" ]; then
    echo "${HOST_GIT_REPO}/objects" >> "${PROJECT}/.git/objects/info/alternates"
    (cd "${PROJECT}"; git remote add host "${HOST_GIT_REPO}")
  fi
}

function start_step {
  local name=$1
  if [ -z "${name}" ]; then
    name="${FUNCNAME[1]}"
  fi
  log "${name} start" >&2
  get_ctime
}

function stop_step {
  local name=$2
  local start_time=$1
  local stop_time
  if [ -z "${name}" ]; then
    name="${FUNCNAME[1]}"
  fi
  stop_time="$(get_ctime)"
  log "${name} stop ($((stop_time - start_time)) seconds)"
}

# Writes report into cwd!
# TODO should have option for maintenance release that include LimitedPrivate in report
function generate_api_report {
  local project="$1"
  local previous_tag="$2"
  local release_tag="$3"
  local previous_version
  local timing_token
  timing_token="$(start_step)"
  # Generate api report.
  "${project}"/dev-support/checkcompatibility.py --annotation \
    org.apache.yetus.audience.InterfaceAudience.Public  \
    "$previous_tag" "$release_tag"
  previous_version="$(echo "${previous_tag}" | sed -e 's/rel\///')"
  cp "${project}/target/compat-check/report.html" "./api_compare_${previous_version}_to_${release_tag}.html"
  stop_step "${timing_token}"
}

# Look up the Jira name associated with project.
# Currently all the 'hbase-*' projects share the same HBASE jira name.  This works because,
# by convention, the HBASE jira "Fix Version" field values have the sub-project name pre-pended,
# as in "hbase-operator-tools-1.0.0".
# TODO: For non-hbase-related projects, enhance this to use Jira API query instead of text lookup.
function get_jira_name {
  local project="$1"
  local jira_name
  case "${project}" in
    hbase*) jira_name="HBASE";;
    *)      jira_name="";;
  esac
  if [[ -z "$jira_name" ]]; then
    error "Sorry, can't determine the Jira name for project $project"
  fi
  log "$jira_name"
}

# Update the CHANGES.md
# DOES NOT DO COMMITS! Caller should do that.
# requires yetus to have a defined home already.
# yetus requires python2 to be on the path.
function update_releasenotes {
  local project_dir="$1"
  local jira_fix_version="$2"
  local jira_project
  local timing_token
  timing_token="$(start_step)"
  changelog="CHANGELOG.${jira_fix_version}.md"
  releasenotes="RELEASENOTES.${jira_fix_version}.md"
  if [ -f ${changelog} ]; then
    rm ${changelog}
  fi
  if [ -f ${releasenotes} ]; then
    rm ${releasenotes}
  fi
  jira_project="$(get_jira_name "$(basename "$project_dir")")"
  "${YETUS_HOME}/bin/releasedocmaker" -p "${jira_project}" --fileversions -v "${jira_fix_version}" \
      -l --sortorder=newer --skip-credits || true
  # First clear out the changes written by previous RCs.
  if [ -f "${project_dir}/CHANGES.md" ]; then
    sed -i -e \
        "/^## Release ${jira_fix_version}/,/^## Release/ {//!d; /^## Release ${jira_fix_version}/d;}" \
        "${project_dir}/CHANGES.md" || true
  fi
  if [ -f "${project_dir}/RELEASENOTES.md" ]; then
    sed -i -e \
        "/^# ${jira_project}  ${jira_fix_version} Release Notes/,/^# ${jira_project}/{//!d; /^# ${jira_project}  ${jira_fix_version} Release Notes/d;}" \
        "${project_dir}/RELEASENOTES.md" || true
  fi

  # Yetus will not generate CHANGES if no JIRAs fixed against the release version
  # (Could happen if a release were bungled such that we had to make a new one
  # without changes)
  if [ ! -f "${changelog}" ]; then
    echo -e "## Release ${jira_fix_version} - Unreleased (as of `date`)\nNo changes\n" > "${changelog}"
  fi
  if [ ! -f "${releasenotes}" ]; then
    echo -e "# hbase ${jira_fix_version} Release Notes\nNo changes\n" > "${releasenotes}"
  fi

  # The releasedocmaker call above generates RELEASENOTES.X.X.X.md and CHANGELOG.X.X.X.md.
  if [ -f "${project_dir}/CHANGES.md" ]; then
    # To insert into project's CHANGES.md...need to cut the top off the
    # CHANGELOG.X.X.X.md file removing license and first line and then
    # insert it after the license comment closing where we have a
    # DO NOT REMOVE marker text!
    sed -i -e '/## Release/,$!d' "${changelog}"
    sed -i -e '2,${/^# HBASE Changelog/d;}' "${project_dir}/CHANGES.md"
    sed -i -e "/DO NOT REMOVE/r ${changelog}" "${project_dir}/CHANGES.md"
  else
    mv "${changelog}" "${project_dir}/CHANGES.md"
  fi
  if [ -f "${project_dir}/RELEASENOTES.md" ]; then
    # Similar for RELEASENOTES but slightly different.
    sed -i -e '/Release Notes/,$!d' "${releasenotes}"
    sed -i -e '2,${/^# RELEASENOTES/d;}' "${project_dir}/RELEASENOTES.md"
    sed -i -e "/DO NOT REMOVE/r ${releasenotes}" "${project_dir}/RELEASENOTES.md"
  else
    mv "${releasenotes}" "${project_dir}/RELEASENOTES.md"
  fi
  stop_step "${timing_token}"
}

# Make src release.
# Takes as arguments first the project name -- e.g. hbase or hbase-operator-tools
# -- and then the version string. Expects to find checkout adjacent to this script
# named for 'project', the first arg passed.
# Expects the following three defines in the environment:
# - GPG needs to be defined, with the path to GPG: defaults 'gpg'.
# - GIT_REF which is the tag to create the tgz from: defaults to 'master'.
# For example:
# $ GIT_REF="master" make_src_release hbase-operator-tools 1.0.0
make_src_release() {
  # Tar up the src and sign and hash it.
  local project="${1}"
  local version="${2}"
  local base_name="${project}-${version}"
  local timing_token
  timing_token="$(start_step)"
  rm -rf "${base_name}"-src*
  tgz="${base_name}-src.tar.gz"
  cd "${project}" || exit
  git clean -d -f -x
  git archive --format=tar.gz --output="../${tgz}" --prefix="${base_name}/" "${GIT_REF:-master}"
  cd .. || exit
  $GPG "${GPG_ARGS[@]}" --armor --output "${tgz}.asc" --detach-sig "${tgz}"
  $GPG "${GPG_ARGS[@]}" --print-md SHA512 "${tgz}" > "${tgz}.sha512"
  stop_step "${timing_token}"
}

# Make binary release.
# Takes as arguments first the project name -- e.g. hbase or hbase-operator-tools
# -- and then the version string. Expects to find checkout adjacent to this script
# named for 'project', the first arg passed.
# Expects the following three defines in the environment:
# - GPG needs to be defined, with the path to GPG: defaults 'gpg'.
# - GIT_REF which is the tag to create the tgz from: defaults to 'master'.
# - MVN Default is "mvn -B --settings $MAVEN_SETTINGS_FILE".
# For example:
# $ GIT_REF="master" make_src_release hbase-operator-tools 1.0.0
make_binary_release() {
  local project="${1}"
  local version="${2}"
  local base_name="${project}-${version}"
  local timing_token
  timing_token="$(start_step)"
  rm -rf "${base_name}"-bin*
  cd "$project" || exit

  git clean -d -f -x
  # Three invocations of maven. This seems to work. One to
  # populate the repo, another to build the site, and then
  # a third to assemble the binary artifact. Trying to do
  # all in the one invocation fails; a problem in our
  # assembly spec to in maven. TODO. Meantime, three invocations.
  "${MVN[@]}" clean install -DskipTests
  "${MVN[@]}" site -DskipTests
  kick_gpg_agent
  "${MVN[@]}" install assembly:single -DskipTests -Dcheckstyle.skip=true "${PUBLISH_PROFILES[@]}"

  # Check there is a bin gz output. The build may not produce one: e.g. hbase-thirdparty.
  local f_bin_prefix="./${PROJECT}-assembly/target/${base_name}"
  if ls "${f_bin_prefix}"*-bin.tar.gz &>/dev/null; then
    cp "${f_bin_prefix}"*-bin.tar.gz ..
    cd .. || exit
    for i in "${base_name}"*-bin.tar.gz; do
      "${GPG}" "${GPG_ARGS[@]}" --armour --output "${i}.asc" --detach-sig "${i}"
      "${GPG}" "${GPG_ARGS[@]}" --print-md SHA512 "${i}" > "${i}.sha512"
    done
  else
    cd .. || exit
    log "No ${f_bin_prefix}*-bin.tar.gz product; expected?"
  fi

  stop_step "${timing_token}"
}

# "Wake up" the gpg agent so it responds properly to maven-gpg-plugin, and doesn't cause timeout.
# Specifically this is done between invocation of 'mvn site' and 'mvn assembly:single', because
# the 'site' build takes long enough that the gpg-agent does become unresponsive and the following
# 'assembly' build (where gpg signing occurs) experiences timeout, without this "kick".
function kick_gpg_agent {
  # All that's needed is to run gpg on a random file
  # TODO could we just call gpg-connect-agent /bye
  local i
  i="$(mktemp)"
  echo "This is a test file" > "$i"
  "${GPG}" "${GPG_ARGS[@]}" --armour --output "${i}.asc" --detach-sig "${i}"
  rm "$i" "$i.asc"
}

# Do maven command to set version into local pom
function maven_set_version { #input: <version_to_set>
  local this_version="$1"
  log "${MVN[@]}" versions:set -DnewVersion="$this_version"
  "${MVN[@]}" versions:set -DnewVersion="$this_version" | grep -v "no value" # silence logs
}

# Do maven command to read version from local pom
function maven_get_version {
  # shellcheck disable=SC2016
  "${MVN[@]}" -q -N -Dexec.executable="echo" -Dexec.args='${project.version}' exec:exec
}

# Do maven deploy to snapshot or release artifact repository, with checks.
function maven_deploy { #inputs: <snapshot|release> <log_file_path>
  local timing_token
  # Invoke with cwd=$PROJECT
  local deploy_type="$1"
  local mvn_log_file="$2" #secondary log file used later to extract staged_repo_id
  if [[ "$deploy_type" != "snapshot" && "$deploy_type" != "release" ]]; then
    error "unrecognized deploy type, must be 'snapshot'|'release'"
  fi
  if [[ -z "$mvn_log_file" ]] || ! touch "$mvn_log_file"; then
    error "must provide writable maven log output filepath"
  fi
  timing_token=$(start_step)
  # shellcheck disable=SC2153
  if [[ "$deploy_type" == "snapshot" ]] && ! [[ "$RELEASE_VERSION" =~ -SNAPSHOT$ ]]; then
    error "Snapshots must have a version with suffix '-SNAPSHOT'; you gave version '$RELEASE_VERSION'"
  elif [[ "$deploy_type" == "release" ]] && [[ "$RELEASE_VERSION" =~ SNAPSHOT ]]; then
    error "Non-snapshot release version must not include the word 'SNAPSHOT'; you gave version '$RELEASE_VERSION'"
  fi
  # Publish ${PROJECT} to Maven repo
  # shellcheck disable=SC2154
  log "Publishing ${PROJECT} checkout at '$GIT_REF' ($git_hash)"
  log "Publish version is $RELEASE_VERSION"
  # Coerce the requested version
  maven_set_version "$RELEASE_VERSION"
  # Prepare for signing
  kick_gpg_agent
  declare -a mvn_goals=(clean)
  if ! is_dry_run; then
    mvn_goals=("${mvn_goals[@]}" deploy)
  fi
  log "${MVN[@]}" -DskipTests -Dcheckstyle.skip=true "${PUBLISH_PROFILES[@]}" "${mvn_goals[@]}"
  log "Logging to ${mvn_log_file}.  This will take a while..."
  rm -f "$mvn_log_file"
  # The tortuous redirect in the next command allows mvn's stdout and stderr to go to mvn_log_file,
  # while also sending stderr back to the caller.
  # shellcheck disable=SC2094
  if ! "${MVN[@]}" -DskipTests -Dcheckstyle.skip=true "${PUBLISH_PROFILES[@]}" \
      "${mvn_goals[@]}" 1>> "$mvn_log_file" 2> >( tee -a "$mvn_log_file" >&2 ); then
    error "Deploy build failed, for details see log at '$mvn_log_file'."
  fi
  log "BUILD SUCCESS."
  stop_step "${timing_token}"
  return 0
}

# guess the host os
# * DARWIN
# * LINUX
function get_host_os() {
  uname -s | tr '[:lower:]' '[:upper:]'
}
