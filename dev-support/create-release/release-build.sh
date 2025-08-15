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

trap cleanup EXIT

# Source in utils.
SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=SCRIPTDIR/release-util.sh
. "$SELF/release-util.sh"

# Print usage and exit.
function exit_with_usage {
  cat <<'EOF'
Usage: release-build.sh <tag|publish-dist|publish-snapshot|publish-release>
Creates release deliverables from a tag or commit.
Argument: one of 'tag', 'publish-dist', 'publish-snapshot', or 'publish-release'
  tag               Prepares for release on specified git branch: Set release version,
                    update CHANGES and RELEASENOTES, create release tag,
                    increment version for ongoing dev, and publish to Apache git repo.
  publish-dist      Build and publish distribution packages (tarballs) to Apache dist repo
  publish-snapshot  Build and publish maven artifacts snapshot release to Apache snapshots repo
  publish-release   Build and publish maven artifacts release to Apache release repo, and
                    construct vote email from template

All other inputs are environment variables.  Please use do-release-docker.sh or
do-release.sh to set up the needed environment variables.  This script, release-build.sh,
is not intended to be called stand-alone, and such use is untested.  The env variables used are:

Used for 'tag' and 'publish' stages:
  PROJECT - The project to build. No default.
  RELEASE_VERSION - Version used in pom files for release (e.g. 2.1.2)
    Required for 'tag'; defaults for 'publish' to the version in pom at GIT_REF
  RELEASE_TAG - Name of release tag (e.g. 2.1.2RC0), also used by
    publish-dist as package version name in dist directory path
  ASF_USERNAME - Username of ASF committer account
  ASF_PASSWORD - Password of ASF committer account
  DRY_RUN - 1:true (default), 0:false. If "1", does almost all the work, but doesn't actually
    publish anything to upstream source or object repositories. It defaults to "1", so if you want
    to actually publish you have to set '-f' (force) flag in do-release.sh or do-release-docker.sh.

Used only for 'tag':
  YETUS_HOME - installation location for Apache Yetus
  GIT_NAME - Name to use with git
  GIT_EMAIL - E-mail address to use with git
  GIT_BRANCH - Git branch on which to make release. Tag is always placed at HEAD of this branch.
  NEXT_VERSION - Development version after release (e.g. 2.1.3-SNAPSHOT)

Used only for 'publish':
  GIT_REF - Release tag or commit to build from (defaults to $RELEASE_TAG; only need to
    separately define GIT_REF if RELEASE_TAG is not actually present as a tag at publish time)
    If both RELEASE_TAG and GIT_REF are undefined it will default to HEAD of master.
  GPG_KEY - GPG key id (usually email addr) used to sign release artifacts
  REPO - Set to full path of a directory to use as maven local repo (dependencies cache)
    to avoid re-downloading dependencies for each stage.  It is automatically set if you
    request full sequence of stages (tag, publish-dist, publish-release) in do-release.sh.

For example:
 $ PROJECT="hbase-operator-tools" ASF_USERNAME=NAME ASF_PASSWORD=PASSWORD GPG_KEY=stack@apache.org ./release-build.sh publish-dist
EOF
  exit 1
}

set -e

function cleanup {
  # If REPO was set, then leave things be. Otherwise if we defined a repo clean it out.
  if [[ -z "${REPO}" ]] && [[ -n "${MAVEN_LOCAL_REPO}" ]]; then
    log "Cleaning up temp repo in '${MAVEN_LOCAL_REPO}'. Set REPO to reuse downloads." >&2
    rm -f "${MAVEN_SETTINGS_FILE}" &> /dev/null || true
    rm -rf "${MAVEN_LOCAL_REPO}" &> /dev/null || true
  fi
}

if [ $# -ne 1 ]; then
  exit_with_usage
fi

if [[ "$1" == "-h" ]]; then
  exit_with_usage
fi

if [[ "$*" == *"help"* ]]; then
  exit_with_usage
fi

init_locale
init_java
init_mvn
init_python
# Print out subset of perl version (used in git hooks and japi-compliance-checker)
perl --version | grep 'This is'

rm -rf "${PROJECT}"

if is_debug; then
  set -x  # detailed logging during action
fi

if [[ "$1" == "tag" ]]; then
  init_yetus
  # for 'tag' stage
  set -o pipefail
  check_get_passwords ASF_PASSWORD
  check_needed_vars PROJECT RELEASE_VERSION RELEASE_TAG NEXT_VERSION GIT_EMAIL GIT_NAME GIT_BRANCH
  if [ -z "${GIT_REPO}" ]; then
    check_needed_vars ASF_USERNAME ASF_PASSWORD
  fi
  git_clone_overwrite

  # 'update_releasenotes' searches the project's Jira for issues where 'Fix Version' matches specified
  # $jira_fix_version. For most projects this is same as ${RELEASE_VERSION}. However, all the 'hbase-*'
  # projects share the same HBASE jira name.  To make this work, by convention, the HBASE jira "Fix Version"
  # field values have the sub-project name pre-pended, as in "hbase-operator-tools-1.0.0".
  # So, here we prepend the project name to the version, but only for the hbase sub-projects.
  jira_fix_version="${RELEASE_VERSION}"
  shopt -s nocasematch
  if [[ "${PROJECT}" == "hbase-thirdparty" ]]; then
    jira_fix_version="thirdparty-${RELEASE_VERSION}"
  elif [[ "${PROJECT}" =~ ^hbase- ]]; then
    jira_fix_version="${PROJECT}-${RELEASE_VERSION}"
  fi
  shopt -u nocasematch
  update_releasenotes "$(pwd)/${PROJECT}" "${jira_fix_version}"

  cd "${PROJECT}"

  git config user.name "$GIT_NAME"
  git config user.email "$GIT_EMAIL"
  git config user.signingkey "${GPG_KEY}"

  # Create release version
  maven_set_version "$RELEASE_VERSION"
  maven_spotless_apply
  git_add_poms
  # Always put CHANGES.md and RELEASENOTES.md to parent directory, so later we do not need to
  # check their position when generating release data. We can not put them under the source code
  # directory because for 3.x+, CHANGES.md and RELEASENOTES.md are not tracked so later when
  # generating src release tarball, we will reset the git repo
  if is_tracked "CHANGES.md"; then
    git add RELEASENOTES.md CHANGES.md
    git commit -s -m "Preparing ${PROJECT} release $RELEASE_TAG; tagging and updates to CHANGES.md and RELEASENOTES.md"
    cp CHANGES.md ../
    cp RELEASENOTES.md ../
  else
    # CHANGES.md is not tracked, should 3.x+
    git commit -s -m "Preparing ${PROJECT} release $RELEASE_TAG"
    mv CHANGES.md ../
    mv RELEASENOTES.md ../
  fi

  log "Creating tag $RELEASE_TAG at the head of $GIT_BRANCH"
  git tag -s -m "Via create-release" "$RELEASE_TAG"

  # Create next version
  maven_set_version "$NEXT_VERSION"
  maven_spotless_apply
  git_add_poms
  git commit -s -m "Preparing development version $NEXT_VERSION"

  if ! is_dry_run; then
    # Push changes
    git push origin "$RELEASE_TAG"
    git push origin "HEAD:$GIT_BRANCH"
    cd ..
    rm -rf "${PROJECT}"
  else
    cd ..
    mv "${PROJECT}" "${PROJECT}.tag"
    log "Dry run: Clone with version changes and tag available as ${PROJECT}.tag in the output directory."
  fi
  exit 0
fi

### Below is for 'publish-*' stages ###
check_get_passwords ASF_PASSWORD
check_needed_vars PROJECT ASF_USERNAME ASF_PASSWORD GPG_KEY

# Commit ref to checkout when building
BASE_DIR=$(pwd)
GIT_REF=${GIT_REF:-master}
if [[ "$PROJECT" =~ ^hbase ]]; then
  RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/hbase"
else
  RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/${PROJECT}"
fi

# in case of dry run, enable publish steps to chain from tag step
if is_dry_run && [[ "${TAG_SAME_DRY_RUN:-}" == "true" && -d "${PROJECT}.tag" ]]; then
  ln -s "${PROJECT}.tag" "${PROJECT}"
else
  git_clone_overwrite
fi
cd "${PROJECT}"
git checkout "$GIT_REF"
git_hash="$(git rev-parse --short HEAD)"
GIT_LONG_HASH="$(git rev-parse HEAD)"
log "Checked out ${PROJECT} at ${GIT_REF} commit $git_hash"

if [ -z "${RELEASE_VERSION}" ]; then
  RELEASE_VERSION="$(maven_get_version)"
fi
init_java17

# We need to do following as hbase-thirdparty requires toolchains setup
if [[ "${PROJECT}" == "hbase-thirdparty" ]]; then
  log "Setting up toolchains and JDK for hbase-thirdparty"
  set_java17_as_default_java
  init_toolchains
fi

# This is a band-aid fix to avoid the failure of Maven nightly snapshot in some Jenkins
# machines by explicitly calling /usr/sbin/lsof. Please see SPARK-22377 and the discussion
# in its pull request.
LSOF=lsof
if ! hash $LSOF 2>/dev/null; then
  LSOF=/usr/sbin/lsof
fi

package_version_name="$RELEASE_TAG"
if [ -z "$package_version_name" ]; then
  package_version_name="${RELEASE_VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

git clean -d -f -x
cd ..
if [[ "$PROJECT" =~ ^hbase- ]]; then
  DEST_DIR_NAME="${PROJECT}-${package_version_name}"
else
  DEST_DIR_NAME="$package_version_name"
fi

if [[ "$1" == "publish-dist" ]]; then
  # Source and binary tarballs
  log "Packaging release source tarballs"
  make_src_release "${PROJECT}" "${RELEASE_VERSION}"

  # we do not have binary tarballs for hbase-thirdparty
  if [[ "${PROJECT}" != "hbase-thirdparty" ]]; then
    make_binary_release "${PROJECT}" "${RELEASE_VERSION}"
  fi

  svn_target="svn-${PROJECT}"
  svn co --depth=empty "$RELEASE_STAGING_LOCATION" "$svn_target"
  rm -rf "${svn_target:?}/${DEST_DIR_NAME}"
  mkdir -p "$svn_target/${DEST_DIR_NAME}"

  log "Copying release tarballs"
  cp "${PROJECT}"-*.tar.* "$svn_target/${DEST_DIR_NAME}/"
  cp "CHANGES.md" "$svn_target/${DEST_DIR_NAME}/"
  cp "RELEASENOTES.md" "$svn_target/${DEST_DIR_NAME}/"

  shopt -s nocasematch
  # Generate api report only if project is hbase for now.
  if [ "${PROJECT}" == "hbase" ]; then
    # This script usually reports an errcode along w/ the report.
    generate_api_report "./${PROJECT}" "${API_DIFF_TAG}" "${GIT_REF}" || true
    cp api*.html "$svn_target/${DEST_DIR_NAME}/"
  fi
  shopt -u nocasematch

  log "svn add"
  svn add "$svn_target/${DEST_DIR_NAME}"

  if ! is_dry_run; then
    cd "$svn_target"
    svn ci --username "$ASF_USERNAME" --password "$ASF_PASSWORD" -m"Apache ${PROJECT} $package_version_name" --no-auth-cache
    cd ..
    rm -rf "$svn_target"
  else
    mv "$svn_target/${DEST_DIR_NAME}" "${svn_target}_${DEST_DIR_NAME}.dist"
    log "Dry run: svn-managed 'dist' directory with release tarballs, CHANGES.md and RELEASENOTES.md available as $(pwd)/${svn_target}_${DEST_DIR_NAME}.dist"
    rm -rf "$svn_target"
  fi
  log "svn ci done"

  exit 0
fi

if [[ "$1" == "publish-snapshot" ]]; then
  (
  cd "${PROJECT}"
  mvn_log="${BASE_DIR}/mvn_deploy_snapshot.log"
  log "Publishing snapshot to nexus"
  maven_deploy snapshot "$mvn_log"
  if ! is_dry_run; then
    log "Snapshot artifacts successfully published to repo."
    rm "$mvn_log"
  else
    log "Dry run: Snapshot artifacts successfully built, but not published due to dry run."
  fi
  )
  exit $?
fi

if [[ "$1" == "publish-release" ]]; then
  (
  cd "${PROJECT}"
  mvn_log="${BASE_DIR}/mvn_deploy_release.log"
  log "Staging release in nexus"
  maven_deploy release "$mvn_log"
  declare staged_repo_id
  declare hadoop3_staged_repo_id
  if ! is_dry_run; then
    mapfile -t staged_repo_ids < <(grep -o "Closing staging repository with ID .*" "$mvn_log" \
        | sed -e 's/Closing staging repository with ID "\([^"]*\)"./\1/')
    log "Release artifacts successfully published to repo: " "${staged_repo_ids[@]}"
    repo_count="${#staged_repo_ids[@]}"
    if [[ "${repo_count}" == "2" ]]; then
      staged_repo_id=${staged_repo_ids[0]}
      hadoop3_staged_repo_id=${staged_repo_ids[1]}
    elif [[ "${repo_count}" == "1" ]]; then
      staged_repo_id=${staged_repo_ids[0]}
      hadoop3_staged_repo_id="not-applicable"
    else
      staged_repo_id="not-applicable"
      hadoop3_staged_repo_id="not-applicable"
    fi
    rm "$mvn_log"
  else
    log "Dry run: Release artifacts successfully built, but not published due to dry run."
    staged_repo_id="dryrun-no-repo"
    hadoop3_staged_repo_id="dryrun-no-repo"
  fi
  export staged_repo_id
  export hadoop3_staged_repo_id
  # Dump out email to send. Where we find vote.tmpl depends
  # on where this script is run from
  PROJECT_TEXT="${PROJECT//-/ }" #substitute like 's/-/ /g'
  export PROJECT_TEXT
  eval "echo \"$(< "${SELF}/vote.tmpl")\"" |tee "${BASE_DIR}/vote.txt"
  )
  exit $?
fi

set +x  # done with detailed logging
cd ..
rm -rf "${PROJECT}"
log "ERROR: expects to be called with 'tag', 'publish-dist', 'publish-release', or 'publish-snapshot'" >&2
exit_with_usage
