#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BIN_DIR=$(dirname "$0")
PB_SOURCE_DIR="${BIN_DIR}/../../hbase-protocol/src/main/protobuf/"
PB_DEST_DIR="${BIN_DIR}/../if/"
rsync -r --delete --exclude BUCK ${PB_SOURCE_DIR} ${PB_DEST_DIR}
