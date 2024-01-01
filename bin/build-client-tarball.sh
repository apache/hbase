#!/usr/bin/env bash
#
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#
# Builds the HBase client tarball from an existing installation
#
# Environment Variables
#
#   HBASE_HOME   The HBase installation

# Exit on an error or an undefined variable being used
set -eu
shopt -s extglob

usage="Usage: build-client-tarball.sh"

# if no args specified, show usage
if [ $# -ne 0 ]; then
  echo $usage
  exit 1
fi

# Duplicating hbase-config.sh to avoiding checking for
# a Java installation (e.g. JAVA_HOME) configured to run
# this script.
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

# NB. set -u will fail this -z check
set +u
# the root of the hbase installation
if [ -z "$HBASE_HOME" ]; then
  export HBASE_HOME=$(readlink -f "$bin/..")
fi
# Restore the set -u
set -u
cd $HBASE_HOME

# The directory we'll create the tarball in
workDir="hbase-webapps/static/maven"

# Ensure directory being created exists
mkdir -p "$workDir"
if [ $? -ne 0 ]; then
  echo "Could not create $workDir"
  exit 1
fi

tmpDir="$workDir/.tmp"
mkdir -p "$tmpDir"
clientTarballDir="$(mktemp -d $tmpDir/hbase-client-tarball.XXX)" || exit 2

cleanup() {
  echo "Cleanup hook called"
  if [ -d "$clientTarballDir" ]; then
    echo "Removing $clientTarballDir"
    rm -rf "$clientTarballDir"
  fi
}

# Register a trap to clean up anything transient before we exit
trap cleanup SIGHUP SIGINT SIGTERM EXIT

copy_hbase_artifacts() {
  # Files/directories in bin/ that clients don't need
  local SCRIPTS_TO_EXCLUDE=("build-client-tarball.sh" "considerAsDead.sh" "draining_servers.rb" "get-active-master.rb"
    "graceful_stop.sh" "hbase-cleanup.sh" "hbase-daemon.sh" "hbase-daemons.sh" "local-master-backup.sh"
    "local-regionservers.sh" "master-backup.sh" "region_mover.rb" "region_status.rb" "regionservers.sh"
    "rolling-restart.sh" "shutdown_regionserver.rb" "start-hbase.cmd" "start-hbase.sh" "stop-hbase.cmd" "stop-hbase.sh"
    "zookeepers.sh" "test" "replication")

  local LIBS_TO_EXCLUDE=("zkcli" "junit*.jar" "spymemcached*.jar" "hbase-rsgroup*-tests.jar" "hbase-testing-util*.jar"
    "javax.inject*.jar" "hbase-annotations*.jar" "hbase-hadoop*-compat*-tests.jar" "hbase-it*.jar"
    "phoenix-server*.jar" "ranger*.jar" "ranger-hbase-plugin-impl" "atlas-plugin-classloader*.jar"
    "atlas-hbase-plugin-impl" "hbase-bridge-shim*.jar")

  # Copy the "guts" of the hbase installation
  cp -r "$HBASE_HOME/bin" "$clientTarballDir/"
  cp -L -r "$HBASE_HOME/lib" "$clientTarballDir/"

  # Exclude scripts which clients don't need. CDH parcel is filtering some of these down already..
  for f in "${SCRIPTS_TO_EXCLUDE[@]}"; do
    if [ -f "$clientTarballDir/bin/$f" ] || [ -d "$clientTarballDir/bin/$f" ]; then
      rm -r "$clientTarballDir/bin/$f"
    fi
  done

  # Exclude libs which clients don't need
  for pattern in "${LIBS_TO_EXCLUDE[@]}"; do
    rm -r "$clientTarballDir/lib/"$pattern || true
  done

  # Supplemental text files
  cp -r $HBASE_HOME/*.txt "$clientTarballDir/"
  # Relocate libraries we bundled to a place where they'll be put on the classpath
  #mv $clientTarballDir/lib/cod/* "$clientTarballDir/lib/client-facing-thirdparty/"
  #rmdir "$clientTarballDir/lib/cod"
}

copy_hadoop_artifacts() {
  local HADOOP_HOME=$(hadoop envvars | fgrep HADOOP_COMMON_HOME | cut -d= -f2 | tr -d "'")
  local JARS_TO_INCLUDE=("gcs-connector-shaded.jar" "hadoop-annotations.jar" "hadoop-auth.jar" "hadoop-aws.jar"
    "hadoop-azure.jar" "hadoop-azure-datalake.jar" "hadoop-common.jar" "lib/commons-logging*.jar" "lib/asm-*.jar"
    "lib/azure-data-lake-store-sdk-*.jar" "lib/wildfly-openssl-*.jar"
    "lib/jersey-core-*.jar" "client/jersey-client-*.jar")
  for jar in "${JARS_TO_INCLUDE[@]}"; do
    cp -L "$HADOOP_HOME/"$jar "$clientTarballDir/lib" || true
  done
}

copy_hdfs_artifacts() {
  local HDFS_HOME=$(hdfs envvars | fgrep HADOOP_HDFS_HOME | cut -d= -f2 | tr -d "'")
  local JARS_TO_INCLUDE=("hadoop-hdfs.jar" "hadoop-hdfs-client.jar")
  for jar in "${JARS_TO_INCLUDE[@]}"; do
    cp -L "$HDFS_HOME/"$jar "$clientTarballDir/lib" || true
  done
}

copy_yarn_artifacts() {
  YARN_HOME=$(yarn envvars | fgrep HADOOP_YARN_HOME | cut -d= -f2 | tr -d "'")
  local JARS_TO_INCLUDE=("hadoop-yarn-api.jar" "hadoop-yarn-client.jar" "hadoop-yarn-common.jar"
  "hadoop-yarn-registry.jar" "hadoop-yarn-server-common.jar")
  for jar in "${JARS_TO_INCLUDE[@]}"; do
    cp -L "$YARN_HOME/$jar" "$clientTarballDir/lib" || true
  done
}

copy_mapreduce_artifacts() {
  MAPREDUCE_HOME=$(mapred envvars | fgrep HADOOP_MAPRED_HOME | cut -d= -f2 | tr -d "'")
  local JARS_TO_INCLUDE=("azure-keyvault-core-*.jar" "azure-storage-*.jar" "bundle-*.jar" "commons-configuration-*.jar" "flogger*.jar"
    "forbiddenapis-*.jar" "gateway-cloud-bindings.jar" "gateway-i18n.jar" "gateway-shell.jar" "gateway-util-common.jar"
    "google-extensions-*.jar" "hadoop-distcp.jar" "hadoop-mapreduce-client-app.jar" "hadoop-mapreduce-client-common.jar"
    "hadoop-mapreduce-client-core.jar" "hadoop-mapreduce-client-jobclient.jar" "hadoop-mapreduce-client-nativetask.jar"
    "hadoop-mapreduce-client-shuffle.jar" "hadoop-mapreduce-examples.jar" "hadoop-streaming.jar" "lz4-*.jar"
    "ranger-raz-*.jar")
  for jar in "${JARS_TO_INCLUDE[@]}"; do
    cp -L "$MAPREDUCE_HOME/"$jar "$clientTarballDir/lib" || true
  done
}

# Don't create tarball if there is already a previous one
clientTarball="$HBASE_HOME/$workDir/hbase-client-tarball.tar.gz"
if [ -f "$clientTarball" ]; then
  echo "Tarball already exists at $clientTarball not creating a new"
  exit 0
fi

copy_hbase_artifacts
copy_hadoop_artifacts
copy_hdfs_artifacts
copy_yarn_artifacts
copy_mapreduce_artifacts

# Rename the work directory so we have a good directory name in the tarball
mv "$clientTarballDir" "$tmpDir/hbase-client-tarball"
clientTarballDir="$tmpDir/hbase-client-tarball"


pushd "$tmpDir"
# Create the client tarball from the same directory (avoid --strip options)
tar cvzf "$clientTarball" hbase-client-tarball
popd

chmod 644 "$clientTarball"

echo "Client tarball created at $clientTarball"
