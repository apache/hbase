#!/bin/bash

set -e -u -o pipefail

SCRIPT_NAME=${0##*/}
SCRIPT_DIR=$(cd `dirname $0` && pwd )

print_usage() {
  cat >&2 <<EOT
Usage: $SCRIPT_NAME <options>
Options:
  --kill
    Kill local process-based HBase cluster using pid files.
  --show
    Show HBase processes running on this machine
EOT
  exit 1
}

show_processes() {
  ps -ef | grep -P "(HRegionServer|HMaster|HQuorumPeer) start" | grep -v grep
}

cmd_specified() {
  if [ "$CMD_SPECIFIED" ]; then
    echo "Only one command can be specified" >&2
    exit 1
  fi
  CMD_SPECIFIED=1
}

list_pid_files() {
  LOCAL_CLUSTER_DIR=$SCRIPT_DIR/../../target/local_cluster
  LOCAL_CLUSTER_DIR=$( cd $LOCAL_CLUSTER_DIR && pwd )
  find $LOCAL_CLUSTER_DIR -name "*.pid"
}

if [ $# -eq 0 ]; then
  print_usage
fi

IS_KILL=""
IS_SHOW=""
CMD_SPECIFIED=""

while [ $# -ne 0 ]; do
  case "$1" in
    -h|--help)
      print_usage ;;
    --kill)
      IS_KILL=1
      cmd_specified ;;
    --show)
      IS_SHOW=1
      cmd_specified ;;
    *)
      echo "Invalid option: $1" >&2
      exit 1
  esac
  shift
done

if [ "$IS_KILL" ]; then
  list_pid_files | \
    while read F; do
      PID=`cat $F`
      echo "Killing pid $PID from file $F"
      # Kill may fail but that's OK, so turn off error handling for a moment.
      set +e
      kill -9 $PID
      set -e
    done
elif [ "$IS_SHOW" ]; then
  PIDS=""
  for F in `list_pid_files`; do
    PID=`cat $F`
    if [ -n "$PID" ]; then
      if [ -n "$PIDS" ]; then
        PIDS="$PIDS,"
      fi
      PIDS="$PIDS$PID"
    fi
  done
  ps -p $PIDS
else
  echo "No command specified" >&2
  exit 1
fi
