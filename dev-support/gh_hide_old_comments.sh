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

set -eEuo pipefail
trap 'exit 1' ERR

declare DEBUG
if [ "${DEBUG}" = 'true' ] ; then
    set -x
fi

declare GITHUB_TOKEN
declare -a GITHUB_AUTH
GITHUB_AUTH=('--header' "Authorization: token ${GITHUB_TOKEN}")
declare GITHUB_API_URL="https://api.github.com"

# the user_id of the buildbot that leaves the comments we want to hide.
declare BUILD_BOT_USER_ID="${BUILD_BOT_USER_ID:-49303554}"
declare REPO="${REPO:-apache/hbase}"
declare JOB_NAME="${JOB_NAME:-HBase-PreCommit-GitHub-PR}"
JOB_NAME="$(echo "${JOB_NAME}" | cut -d/ -f1)"
declare CURL="${CURL:-curl}"

function fetch_comments {
    local pr="$1"
    local comments_file
    local page_file
    local headers_file
    local -a curl_args
    local page=1
    local next_url

    comments_file="$(mktemp "comments_${pr}" 2>/dev/null || mktemp -t "comments_${pr}.XXXXXXXXXX")" || \
        { >&2 echo 'cannot create temp file';  exit 1 ;}
    page_file="$(mktemp "page_${pr}" 2>/dev/null || mktemp -t "page_${pr}.XXXXXXXXXX")" || \
        { >&2 echo 'cannot create temp file';  exit 1 ;}
    headers_file="$(mktemp "headers_${pr}" 2>/dev/null || mktemp -t "headers_${pr}.XXXXXXXXXX")" || \
        { >&2 echo 'cannot create temp file';  exit 1 ;}

    # cleanup temp files on error
    trap 'rm -f "${page_file}" "${headers_file}"; exit 1' ERR

    next_url="${GITHUB_API_URL}/repos/${REPO}/issues/${pr}/comments?per_page=100"

    # start with empty JSON array
    echo '[]' > "${comments_file}"

    while [ -n "${next_url}" ] ; do
        curl_args=(
            --fail
            --max-time 30
            "${GITHUB_AUTH[@]}"
            --header 'Accept: application/vnd.github+json'
            --header 'X-GitHub-Api-Version: 2022-11-28'
            --dump-header "${headers_file}"
            --request GET
            --url "${next_url}"
        )
        if [ "${DEBUG}" = true ] ; then
            curl_args+=(--verbose)
            >&2 echo "Fetching page ${page}: ${next_url}"
        else
            curl_args+=(--silent)
        fi

        if ! "${CURL}" "${curl_args[@]}" > "${page_file}"; then
            >&2 echo "Failed to fetch page ${page}: ${next_url}"
            rm -f "${page_file}" "${headers_file}"
            exit 1
        fi

        if [ "${DEBUG}" = 'true' ] ; then
            >&2 echo "Page ${page} returned $(jq length "${page_file}") comments"
        fi

        # merge this page into the accumulated results
        if ! jq -s '.[0] + .[1]' "${comments_file}" "${page_file}" > "${comments_file}.tmp"; then
            >&2 echo "Failed to merge comments from page ${page}"
            rm -f "${page_file}" "${headers_file}" "${comments_file}.tmp"
            exit 1
        fi
        mv "${comments_file}.tmp" "${comments_file}"

        # check for next page in Link header
        # Link header format: <url>; rel="next", <url>; rel="last"
        # Extract URL associated with rel="next" regardless of position
        next_url=""
        if grep -qi '^link:' "${headers_file}" ; then
            next_url=$(grep -i '^link:' "${headers_file}" | tr ',' '\n' | grep 'rel="next"' | sed -n 's/.*<\([^>]*\)>.*/\1/p' || true)
        fi

        page=$((page + 1))
    done

    rm -f "${page_file}" "${headers_file}"
    trap - ERR

    if [ "${DEBUG}" = 'true' ] ; then
        >&2 echo "Total comments fetched: $(jq length "${comments_file}")"
        >&2 cat "${comments_file}"
    fi
    echo "${comments_file}"
}

function hide_old_comment {
    local pr="$1"
    local comment_id="$2"
    local -a curl_args
    local query
    read -r -d '' query << EOF || :
mutation {
  minimizeComment(input: { subjectId: \"$comment_id\", classifier: OUTDATED }) {
    minimizedComment {
      isMinimized,
      minimizedReason
    }
  }
}
EOF
    # remove newlines and whitespace rather than escaping them
    query="${query//[$'\r\n\t ']/}"

    curl_args=(
        --fail
        "${GITHUB_AUTH[@]}"
        --header 'Accept: application/vnd.github+json'
        --header 'Content-Type: application/json'
        --header 'X-GitHub-Api-Version: 2022-11-28'
        --request POST
        --url "${GITHUB_API_URL}/graphql"
    )
    if [ "${DEBUG}" = true ] ; then
        curl_args+=(--verbose)
    else
        curl_args+=(--silent)
    fi

    "${CURL}" "${curl_args[@]}" --data "{ \"query\": \"${query}\" }"
}

function identify_most_recent_build_number {
    local pr="$1"
    local comments_file="$2"
    local jq_filter
    local url_pattern="${JOB_NAME}/job/PR-${pr}/(?<buildnum>[0-9]+)/"
    # GitHub Actions URLs don't have /job/ in them
    if [[ "${JOB_NAME}" == *"GH-Actions"* ]]; then
        url_pattern="${JOB_NAME}/PR-${pr}/(?<buildnum>[0-9]+)/"
    fi
    read -r -d '' jq_filter << EOF || :
.[] \
| select(.user.id == ${BUILD_BOT_USER_ID}) \
| .body \
| capture("${url_pattern}") \
| .buildnum
EOF

    jq -r "${jq_filter}" "${comments_file}" \
        | sort -nu \
        | tail -n1
}

function identify_old_comment_ids {
    local pr="$1"
    local comments_file="$2"
    local most_recent_build_number="$3"
    local jq_filter
    local url_pattern="${JOB_NAME}/job/PR-${pr}/(?<buildnum>[0-9]+)/"
    # GitHub Actions URLs don't have /job/ in them
    if [[ "${JOB_NAME}" == *"GH-Actions"* ]]; then
        url_pattern="${JOB_NAME}/PR-${pr}/(?<buildnum>[0-9]+)/"
    fi
    read -r -d '' jq_filter << EOF || :
.[] \
| select(.user.id == ${BUILD_BOT_USER_ID}) \
| { node_id, buildnum: (.body | capture("${url_pattern}") | .buildnum | tonumber) } \
| select(.buildnum < (${most_recent_build_number} | tonumber)) \
| .node_id
EOF

    jq -r "${jq_filter}" "${comments_file}"
}

function main {
    local pr="$1"
    local comments_file
    local most_recent_build_number

    comments_file="$(fetch_comments "$pr")"
    most_recent_build_number="$(identify_most_recent_build_number "${pr}" "${comments_file}")"
    for comment_id in $(identify_old_comment_ids "${pr}" "${comments_file}" "${most_recent_build_number}") ; do
        hide_old_comment "${pr}" "${comment_id}"
        sleep 1
    done
}

main "$@"
