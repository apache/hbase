#!/bin/bash
set -e
set -x

MINOR_VERSION="2.5"
MAIN_BRANCH="hubspot-${MINOR_VERSION}"

if [[ "$GIT_BRANCH" = "$MAIN_BRANCH" ]]; then
   repos=( "8_hs-hbase" "aarch64_8_hs-hbase" )
else
    repos=( "8_hs-hbase-develop" "aarch64_8_hs-hbase-develop" )
fi

for repo in "${repos[@]}"; do
  rpm-upload --rpms-dir=$WORKSPACE/generated_rpms/ --repo-name $repo
done
