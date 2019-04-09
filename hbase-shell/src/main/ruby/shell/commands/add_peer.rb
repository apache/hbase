#
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

module Shell
  module Commands
    class AddPeer < Command
      def help
        <<-EOF
A peer can either be another HBase cluster or a custom replication endpoint. In either case an id
must be specified to identify the peer.

For a HBase cluster peer, a cluster key must be provided and is composed like this:
hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
This gives a full path for HBase to connect to another HBase cluster.
An optional parameter for state identifies the replication peer's state is enabled or disabled.
And the default state is enabled.
An optional parameter for namespaces identifies which namespace's tables will be replicated
to the peer cluster.
An optional parameter for table column families identifies which tables and/or column families
will be replicated to the peer cluster.
An optional parameter for serial flag identifies whether or not the replication peer is a serial
replication peer. The default serial flag is false.

Note: Set a namespace in the peer config means that all tables in this namespace
will be replicated to the peer cluster. So if you already have set a namespace in peer config,
then you can't set this namespace's tables in the peer config again.

Examples:

  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase"
  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase", STATE => "ENABLED"
  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase", STATE => "DISABLED"
  hbase> add_peer '2', CLUSTER_KEY => "zk1,zk2,zk3:2182:/hbase-prod",
    TABLE_CFS => { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }
  hbase> add_peer '2', CLUSTER_KEY => "zk1,zk2,zk3:2182:/hbase-prod",
    NAMESPACES => ["ns1", "ns2", "ns3"]
  hbase> add_peer '2', CLUSTER_KEY => "zk1,zk2,zk3:2182:/hbase-prod",
    NAMESPACES => ["ns1", "ns2"], TABLE_CFS => { "ns3:table1" => [], "ns3:table2" => ["cf1"] }
  hbase> add_peer '3', CLUSTER_KEY => "zk1,zk2,zk3:2182:/hbase-prod",
    NAMESPACES => ["ns1", "ns2", "ns3"], SERIAL => true

For a custom replication endpoint, the ENDPOINT_CLASSNAME can be provided. Two optional arguments
are DATA and CONFIG which can be specified to set different either the peer_data or configuration
for the custom replication endpoint. Table column families is optional and can be specified with
the key TABLE_CFS.

  hbase> add_peer '6', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint'
  hbase> add_peer '7', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    DATA => { "key1" => 1 }
  hbase> add_peer '8', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    CONFIG => { "config1" => "value1", "config2" => "value2" }
  hbase> add_peer '9', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    DATA => { "key1" => 1 }, CONFIG => { "config1" => "value1", "config2" => "value2" },
  hbase> add_peer '10', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    TABLE_CFS => { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
  hbase> add_peer '11', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
    DATA => { "key1" => 1 }, CONFIG => { "config1" => "value1", "config2" => "value2" },
    TABLE_CFS => { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
  hbase> add_peer '12', ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint',
        CLUSTER_KEY => "server2.cie.com:2181:/hbase"

Note: Either CLUSTER_KEY or ENDPOINT_CLASSNAME must be specified. If ENDPOINT_CLASSNAME is specified, CLUSTER_KEY is
optional and should only be specified if a particular custom endpoint requires it.

The default replication peer is asynchronous. You can also add a synchronous replication peer
with REMOTE_WAL_DIR parameter. Meanwhile, synchronous replication peer also support other optional
config for asynchronous replication peer.

Examples:

  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase",
    REMOTE_WAL_DIR => "hdfs://srv1:9999/hbase"
  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase",
    STATE => "ENABLED", REMOTE_WAL_DIR => "hdfs://srv1:9999/hbase"
  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase",
    STATE => "DISABLED", REMOTE_WAL_DIR => "hdfs://srv1:9999/hbase"
  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase",
    REMOTE_WAL_DIR => "hdfs://srv1:9999/hbase", NAMESPACES => ["ns1", "ns2"]
  hbase> add_peer '1', CLUSTER_KEY => "server1.cie.com:2181:/hbase",
    REMOTE_WAL_DIR => "hdfs://srv1:9999/hbase", TABLE_CFS => { "table1" => [] }

Note: The REMOTE_WAL_DIR is not allowed to change.

EOF
      end

      def command(id, args = {}, peer_tableCFs = nil)
        replication_admin.add_peer(id, args, peer_tableCFs)
      end
    end
  end
end
