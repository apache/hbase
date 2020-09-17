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

set -e
function usage {
  echo "Usage: ${0} [options] /path/to/component/bin-install /path/to/hadoop/executable /path/to/share/hadoop/yarn/timelineservice /path/to/hadoop/hadoop-yarn-server-tests-tests.jar /path/to/hadoop/hadoop-mapreduce-client-jobclient-tests.jar /path/to/mapred/executable"
  echo ""
  echo "    --zookeeper-data /path/to/use                                     Where the embedded zookeeper instance should write its data."
  echo "                                                                      defaults to 'zk-data' in the working-dir."
  echo "    --working-dir /path/to/use                                        Path for writing configs and logs. must exist."
  echo "                                                                      defaults to making a directory via mktemp."
  echo "    --hadoop-client-classpath /path/to/some.jar:/path/to/another.jar  classpath for hadoop jars."
  echo "                                                                      defaults to 'hadoop classpath'"
  echo "    --hbase-client-install /path/to/unpacked/client/tarball           if given we'll look here for hbase client jars instead of the bin-install"
  echo "    --force-data-clean                                                Delete all data in HDFS and ZK prior to starting up hbase"
  echo "    --single-process                                                  Run as single process instead of pseudo-distributed"
  echo ""
  exit 1
}
# if no args specified, show usage
if [ $# -lt 5 ]; then
  usage
fi

# Get arguments
declare component_install
declare hadoop_exec
declare working_dir
declare zk_data_dir
declare clean
declare distributed="true"
declare hadoop_jars
declare hbase_client
while [ $# -gt 0 ]
do
  case "$1" in
    --working-dir) shift; working_dir=$1; shift;;
    --force-data-clean) shift; clean="true";;
    --zookeeper-data) shift; zk_data_dir=$1; shift;;
    --single-process) shift; distributed="false";;
    --hadoop-client-classpath) shift; hadoop_jars="$1"; shift;;
    --hbase-client-install) shift; hbase_client="$1"; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;  # terminate while loop
  esac
done

# should still have where component checkout is.
if [ $# -lt 5 ]; then
  usage
fi
component_install="$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
hadoop_exec="$(cd "$(dirname "$2")"; pwd)/$(basename "$2")"
timeline_service_dir="$(cd "$(dirname "$3")"; pwd)/$(basename "$3")"
yarn_server_tests_test_jar="$(cd "$(dirname "$4")"; pwd)/$(basename "$4")"
mapred_jobclient_test_jar="$(cd "$(dirname "$5")"; pwd)/$(basename "$5")"
mapred_exec="$(cd "$(dirname "$6")"; pwd)/$(basename "$6")"

if [ ! -x "${hadoop_exec}" ]; then
  echo "hadoop cli does not appear to be executable." >&2
  exit 1
fi

if [ ! -x "${mapred_exec}" ]; then
  echo "mapred cli does not appear to be executable." >&2
  exit 1
fi

if [ ! -d "${component_install}" ]; then
  echo "Path to HBase binary install should be a directory." >&2
  exit 1
fi

if [ ! -f "${yarn_server_tests_test_jar}" ]; then
  echo "Specified YARN server tests test jar is not a file." >&2
  exit 1
fi

if [ ! -f "${mapred_jobclient_test_jar}" ]; then
  echo "Specified MapReduce jobclient test jar is not a file." >&2
  exit 1
fi

if [ -z "${working_dir}" ]; then
  if ! working_dir="$(mktemp -d -t hbase-pseudo-dist-test)" ; then
    echo "Failed to create temporary working directory. Please specify via --working-dir" >&2
    exit 1
  fi
else
  # absolutes please
  working_dir="$(cd "$(dirname "${working_dir}")"; pwd)/$(basename "${working_dir}")"
  if [ ! -d "${working_dir}" ]; then
    echo "passed working directory '${working_dir}' must already exist." >&2
    exit 1
  fi
fi

if [ -z "${zk_data_dir}" ]; then
  zk_data_dir="${working_dir}/zk-data"
  mkdir "${zk_data_dir}"
else
  # absolutes please
  zk_data_dir="$(cd "$(dirname "${zk_data_dir}")"; pwd)/$(basename "${zk_data_dir}")"
  if [ ! -d "${zk_data_dir}" ]; then
    echo "passed directory for unpacking the source tarball '${zk_data_dir}' must already exist."
    exit 1
  fi
fi

if [ -z "${hbase_client}" ]; then
  hbase_client="${component_install}"
else
  echo "Using HBase client-side artifact"
  # absolutes please
  hbase_client="$(cd "$(dirname "${hbase_client}")"; pwd)/$(basename "${hbase_client}")"
  if [ ! -d "${hbase_client}" ]; then
    echo "If given hbase client install should be a directory with contents of the client tarball." >&2
    exit 1
  fi
fi

if [ -n "${hadoop_jars}" ]; then
  declare -a tmp_jars
  for entry in $(echo "${hadoop_jars}" | tr ':' '\n'); do
    tmp_jars=("${tmp_jars[@]}" "$(cd "$(dirname "${entry}")"; pwd)/$(basename "${entry}")")
  done
  hadoop_jars="$(IFS=:; echo "${tmp_jars[*]}")"
fi


echo "You'll find logs and temp files in ${working_dir}"

function redirect_and_run {
  log_base=$1
  shift
  echo "$*" >"${log_base}.err"
  "$@" >"${log_base}.out" 2>>"${log_base}.err"
}

(cd "${working_dir}"

echo "Hadoop version information:"
"${hadoop_exec}" version
hadoop_version=$("${hadoop_exec}" version | head -n 1)
hadoop_version="${hadoop_version#Hadoop }"
if [ "${hadoop_version%.*.*}" -gt 2 ]; then
  "${hadoop_exec}" envvars
else
  echo "JAVA_HOME: ${JAVA_HOME}"
fi

# Ensure that if some other Hadoop install happens to be present in the environment we ignore it.
HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP="true"
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP

if [ -n "${clean}" ]; then
  echo "Cleaning out ZooKeeper..."
  rm -rf "${zk_data_dir:?}/*"
fi

echo "HBase version information:"
"${component_install}/bin/hbase" version 2>/dev/null
hbase_version=$("${component_install}/bin/hbase" version 2>&1 | grep ^HBase | head -n 1)
hbase_version="${hbase_version#HBase }"

if [ ! -s "${hbase_client}/lib/shaded-clients/hbase-shaded-mapreduce-${hbase_version}.jar" ]; then
  echo "HBase binary install doesn't appear to include a shaded mapreduce artifact." >&2
  exit 1
fi

if [ ! -s "${hbase_client}/lib/shaded-clients/hbase-shaded-client-${hbase_version}.jar" ]; then
  echo "HBase binary install doesn't appear to include a shaded client artifact." >&2
  exit 1
fi

if [ ! -s "${hbase_client}/lib/shaded-clients/hbase-shaded-client-byo-hadoop-${hbase_version}.jar" ]; then
  echo "HBase binary install doesn't appear to include a shaded client artifact." >&2
  exit 1
fi

echo "Writing out configuration for HBase."
rm -rf "${working_dir}/hbase-conf"
mkdir "${working_dir}/hbase-conf"

if [ -f "${component_install}/conf/log4j.properties" ]; then
  cp "${component_install}/conf/log4j.properties" "${working_dir}/hbase-conf/log4j.properties"
else
  cat >"${working_dir}/hbase-conf/log4j.properties" <<EOF
# Define some default values that can be overridden by system properties
hbase.root.logger=INFO,console

# Define the root logger to the system property "hbase.root.logger".
log4j.rootLogger=${hbase.root.logger}

# Logging Threshold
log4j.threshold=ALL
# console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{ISO8601} %-5p [%t] %c{2}: %.1000m%n
EOF
fi

cat >"${working_dir}/hbase-conf/hbase-site.xml" <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-->
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <!-- We rely on the defaultFS being set in our hadoop confs -->
    <value>/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>${zk_data_dir}</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>${distributed}</value>
  </property>
</configuration>
EOF

if [ "true" = "${distributed}" ]; then
  cat >"${working_dir}/hbase-conf/regionservers" <<EOF
localhost
EOF
fi

function cleanup {

  echo "Shutting down HBase"
  HBASE_CONF_DIR="${working_dir}/hbase-conf/" "${component_install}/bin/stop-hbase.sh"

  if [ -f "${working_dir}/hadoop.pid" ]; then
    echo "Shutdown: listing HDFS contents"
    redirect_and_run "${working_dir}/hadoop_listing_at_end" \
    "${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -ls -R /

    echo "Shutting down Hadoop"
    kill -6 "$(cat "${working_dir}/hadoop.pid")"
  fi
}

trap cleanup EXIT SIGQUIT

echo "Starting up Hadoop"

if [ "${hadoop_version%.*.*}" -gt 2 ]; then
  "${mapred_exec}" minicluster -format -writeConfig "${working_dir}/hbase-conf/core-site.xml" -writeDetails "${working_dir}/hadoop_cluster_info.json" >"${working_dir}/hadoop_cluster_command.out" 2>"${working_dir}/hadoop_cluster_command.err" &
else
  HADOOP_CLASSPATH="${timeline_service_dir}/*:${timeline_service_dir}/lib/*:${yarn_server_tests_test_jar}" "${hadoop_exec}" jar "${mapred_jobclient_test_jar}" minicluster -format -writeConfig "${working_dir}/hbase-conf/core-site.xml" -writeDetails "${working_dir}/hadoop_cluster_info.json" >"${working_dir}/hadoop_cluster_command.out" 2>"${working_dir}/hadoop_cluster_command.err" &
fi

echo "$!" > "${working_dir}/hadoop.pid"

# 2 + 4 + 8 + .. + 256 ~= 8.5 minutes.
max_sleep_time=512
sleep_time=2
until [[ -s "${working_dir}/hbase-conf/core-site.xml" || "${sleep_time}" -ge "${max_sleep_time}" ]]; do
  printf '\twaiting for Hadoop to finish starting up.\n'
  sleep "${sleep_time}"
  sleep_time="$((sleep_time*2))"
done

if [ "${sleep_time}" -ge "${max_sleep_time}" ] ; then
  echo "time out waiting for Hadoop to startup" >&2
  exit 1
fi

if [ "${hadoop_version%.*.*}" -gt 2 ]; then
  echo "Verifying configs"
  "${hadoop_exec}" --config "${working_dir}/hbase-conf/" conftest
fi

if [ -n "${clean}" ]; then
  echo "Cleaning out HDFS..."
  "${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -rm -r /hbase
  "${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -rm -r example/
  "${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -rm -r example-region-listing.data
fi

echo "Listing HDFS contents"
redirect_and_run "${working_dir}/hadoop_cluster_smoke" \
    "${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -ls -R /

echo "Starting up HBase"
HBASE_CONF_DIR="${working_dir}/hbase-conf/" "${component_install}/bin/start-hbase.sh"

sleep_time=2
until "${component_install}/bin/hbase" --config "${working_dir}/hbase-conf/" shell --noninteractive >"${working_dir}/waiting_hbase_startup.log" 2>&1 <<EOF
  count 'hbase:meta'
EOF
do
  printf '\tretry waiting for hbase to come up.\n'
  sleep "${sleep_time}"
  sleep_time="$((sleep_time*2))"
done

echo "Setting up table 'test:example' with 1,000 regions"
"${hbase_client}/bin/hbase" --config "${working_dir}/hbase-conf/" shell --noninteractive >"${working_dir}/table_create.log" 2>&1 <<EOF
  create_namespace 'test'
  create 'test:example', 'family1', 'family2', {NUMREGIONS => 1000, SPLITALGO => 'UniformSplit'}
EOF

echo "writing out example TSV to example.tsv"
cat >"${working_dir}/example.tsv" <<EOF
row1	value8	value8	
row3			value2
row2	value9		
row10		value1	
pow1	value8		value8
pow3		value2	
pow2			value9
pow10	value1		
paw1		value8	value8
paw3	value2		
paw2		value9	
paw10			value1
raw1	value8	value8	
raw3			value2
raw2	value9		
raw10		value1	
aow1	value8		value8
aow3		value2	
aow2			value9
aow10	value1		
aaw1		value8	value8
aaw3	value2		
aaw2		value9	
aaw10			value1
how1	value8	value8	
how3			value2
how2	value9		
how10		value1	
zow1	value8		value8
zow3		value2	
zow2			value9
zow10	value1		
zaw1		value8	value8
zaw3	value2		
zaw2		value9	
zaw10			value1
haw1	value8	value8	
haw3			value2
haw2	value9		
haw10		value1	
low1	value8		value8
low3		value2	
low2			value9
low10	value1		
law1		value8	value8
law3	value2		
law2		value9	
law10			value1
EOF

echo "uploading example.tsv to HDFS"
"${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -mkdir example
"${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -copyFromLocal "${working_dir}/example.tsv" "example/"

echo "Importing TSV via shaded client artifact for HBase - MapReduce integration."
# hbase_thirdparty_jars=("${component_install}"/lib/htrace-core4*.jar \
#     "${component_install}"/lib/slf4j-api-*.jar \
#     "${component_install}"/lib/commons-logging-*.jar \
#     "${component_install}"/lib/slf4j-log4j12-*.jar \
#     "${component_install}"/lib/log4j-1.2.*.jar \
#     "${working_dir}/hbase-conf/log4j.properties")
# hbase_dep_classpath=$(IFS=:; echo "${hbase_thirdparty_jars[*]}")
hbase_dep_classpath="$("${hbase_client}/bin/hbase" --config "${working_dir}/hbase-conf/" mapredcp)"
HADOOP_CLASSPATH="${hbase_dep_classpath}" redirect_and_run "${working_dir}/mr-importtsv" \
    "${hadoop_exec}" --config "${working_dir}/hbase-conf/" jar "${hbase_client}/lib/shaded-clients/hbase-shaded-mapreduce-${hbase_version}.jar" importtsv -Dimporttsv.columns=HBASE_ROW_KEY,family1:column1,family1:column4,family1:column3 test:example example/ -libjars "${hbase_dep_classpath}"
"${hbase_client}/bin/hbase" --config "${working_dir}/hbase-conf/" shell --noninteractive >"${working_dir}/scan_import.out" 2>"${working_dir}/scan_import.err" <<EOF
  scan 'test:example'
EOF

echo "Verifying row count from import."
import_rowcount=$(echo 'count "test:example"' | "${hbase_client}/bin/hbase" --config "${working_dir}/hbase-conf/" shell --noninteractive 2>/dev/null | tail -n 1)
if [ ! "${import_rowcount}" -eq 48 ]; then
  echo "ERROR: Instead of finding 48 rows, we found ${import_rowcount}."
  exit 2
fi

if [ -z "${hadoop_jars}" ]; then
  echo "Hadoop client jars not given; getting them from 'hadoop classpath' for the example."
  hadoop_jars=$("${hadoop_exec}" --config "${working_dir}/hbase-conf/" classpath)
fi

echo "Building shaded client example."
cat >"${working_dir}/HBaseClientReadWriteExample.java" <<EOF
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;


public class HBaseClientReadWriteExample {
  private static final byte[] FAMILY_BYTES = Bytes.toBytes("family2");

  public static void main(String[] args) throws Exception {
    Configuration hbase = HBaseConfiguration.create();
    Configuration hadoop = new Configuration();
    try (Connection connection = ConnectionFactory.createConnection(hbase)) {
      System.out.println("Generating list of regions");
      final List<String> regions = new LinkedList<>();
      try (Admin admin = connection.getAdmin()) {
        final ClusterMetrics cluster = admin.getClusterMetrics();
        System.out.println(String.format("\tCluster reports version %s, ave load %f, region count %d", cluster.getHBaseVersion(), cluster.getAverageLoad(), cluster.getRegionCount()));
        for (ServerMetrics server : cluster.getLiveServerMetrics().values()) {
          for (RegionMetrics region : server.getRegionMetrics().values()) {
            regions.add(region.getNameAsString());
          }
        }
      }
      final Path listing = new Path("example-region-listing.data");
      System.out.println("Writing list to HDFS");
      try (FileSystem fs = FileSystem.newInstance(hadoop)) {
        final Path path = fs.makeQualified(listing);
        try (FSDataOutputStream out = fs.create(path)) {
          out.writeInt(regions.size());
          for (String region : regions) {
            out.writeUTF(region);
          }
          out.hsync();
        }
      }
      final List<Put> puts = new LinkedList<>();
      final Put marker = new Put(new byte[] { (byte)0 });
      System.out.println("Reading list from HDFS");
      try (FileSystem fs = FileSystem.newInstance(hadoop)) {
        final Path path = fs.makeQualified(listing);
        final CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
        try (FSDataInputStream in = fs.open(path)) {
          final int count = in.readInt();
          marker.addColumn(FAMILY_BYTES, Bytes.toBytes("count"), Bytes.toBytes(count));
          for(int i = 0; i < count; i++) {
            builder.clear();
            final byte[] row = Bytes.toBytes(in.readUTF());
            final Put put = new Put(row);
            builder.setRow(row);
            builder.setFamily(FAMILY_BYTES);
            builder.setType(Cell.Type.Put);
            put.add(builder.build());
            puts.add(put);
          }
        }
      }
      System.out.println("Writing list into HBase table");
      try (Table table = connection.getTable(TableName.valueOf("test:example"))) {
        table.put(marker);
        table.put(puts);
      }
    }
  }
}
EOF
redirect_and_run "${working_dir}/hbase-shaded-client-compile" \
    javac -cp "${hbase_client}/lib/shaded-clients/hbase-shaded-client-byo-hadoop-${hbase_version}.jar:${hadoop_jars}" "${working_dir}/HBaseClientReadWriteExample.java"
echo "Running shaded client example. It'll fetch the set of regions, round-trip them to a file in HDFS, then write them one-per-row into the test table."
# The order of classpath entries here is important. if we're using non-shaded Hadoop 3 / 2.9.0 jars, we have to work around YARN-2190.
redirect_and_run "${working_dir}/hbase-shaded-client-example" \
    java -cp "${working_dir}/hbase-conf/:${hbase_client}/lib/shaded-clients/hbase-shaded-client-byo-hadoop-${hbase_version}.jar:${hbase_dep_classpath}:${working_dir}:${hadoop_jars}" HBaseClientReadWriteExample

echo "Checking on results of example program."
"${hadoop_exec}" --config "${working_dir}/hbase-conf/" fs -copyToLocal "example-region-listing.data" "${working_dir}/example-region-listing.data"

"${hbase_client}/bin/hbase" --config "${working_dir}/hbase-conf/" shell --noninteractive >"${working_dir}/scan_example.out" 2>"${working_dir}/scan_example.err" <<EOF
  scan 'test:example'
EOF

echo "Verifying row count from example."
example_rowcount=$(echo 'count "test:example"' | "${hbase_client}/bin/hbase" --config "${working_dir}/hbase-conf/" shell --noninteractive 2>/dev/null | tail -n 1)
if [ "${example_rowcount}" -gt "1050" ]; then
  echo "Found ${example_rowcount} rows, which is enough to cover 48 for import, 1000 example's use of user table regions, 2 for example's use of meta/namespace regions, and 1 for example's count record"
else
  echo "ERROR: Only found ${example_rowcount} rows."
fi

)
