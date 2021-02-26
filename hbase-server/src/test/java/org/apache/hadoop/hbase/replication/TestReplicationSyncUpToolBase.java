/**
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
package org.apache.hadoop.hbase.replication;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_GLOBAL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

public abstract class TestReplicationSyncUpToolBase {

  protected static final HBaseTestingUtility UTIL1 = new HBaseTestingUtility();
  protected static final HBaseTestingUtility UTIL2 = new HBaseTestingUtility();

  protected static final TableName TN1 = TableName.valueOf("t1_syncup");
  protected static final TableName TN2 = TableName.valueOf("t2_syncup");

  protected static final byte[] FAMILY = Bytes.toBytes("cf1");
  protected static final byte[] QUALIFIER = Bytes.toBytes("q1");

  protected static final byte[] NO_REP_FAMILY = Bytes.toBytes("norep");

  protected TableDescriptor t1SyncupSource;
  protected TableDescriptor t1SyncupTarget;
  protected TableDescriptor t2SyncupSource;
  protected TableDescriptor t2SyncupTarget;

  protected Connection conn1;
  protected Connection conn2;

  protected Table ht1Source;
  protected Table ht2Source;
  protected Table ht1TargetAtPeer1;
  protected Table ht2TargetAtPeer1;

  protected void customizeClusterConf(Configuration conf) {
  }

  @Before
  public void setUp() throws Exception {
    customizeClusterConf(UTIL1.getConfiguration());
    customizeClusterConf(UTIL2.getConfiguration());
    TestReplicationBase.configureClusters(UTIL1, UTIL2);
    UTIL1.startMiniZKCluster();
    UTIL2.setZkCluster(UTIL1.getZkCluster());

    UTIL1.startMiniCluster(2);
    UTIL2.startMiniCluster(4);

    t1SyncupSource = TableDescriptorBuilder.newBuilder(TN1)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setScope(REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t1SyncupTarget = TableDescriptorBuilder.newBuilder(TN1)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t2SyncupSource = TableDescriptorBuilder.newBuilder(TN2)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setScope(REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t2SyncupTarget = TableDescriptorBuilder.newBuilder(TN2)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(ht1Source, true);
    Closeables.close(ht2Source, true);
    Closeables.close(ht1TargetAtPeer1, true);
    Closeables.close(ht2TargetAtPeer1, true);
    Closeables.close(conn1, true);
    Closeables.close(conn2, true);
    UTIL2.shutdownMiniCluster();
    UTIL1.shutdownMiniCluster();
  }

  final void setupReplication() throws Exception {
    Admin admin1 = UTIL1.getAdmin();
    admin1.createTable(t1SyncupSource);
    admin1.createTable(t2SyncupSource);

    Admin admin2 = UTIL2.getAdmin();
    admin2.createTable(t1SyncupTarget);
    admin2.createTable(t2SyncupTarget);

    // Get HTable from Master
    Connection conn1 = ConnectionFactory.createConnection(UTIL1.getConfiguration());
    ht1Source = conn1.getTable(TN1);
    ht2Source = conn1.getTable(TN2);

    // Get HTable from Peer1
    Connection conn2 = ConnectionFactory.createConnection(UTIL2.getConfiguration());
    ht1TargetAtPeer1 = conn2.getTable(TN1);
    ht2TargetAtPeer1 = conn2.getTable(TN2);

    /**
     * set M-S : Master: utility1 Slave1: utility2
     */
    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL2.getClusterKey()).build();
    admin1.addReplicationPeer("1", rpc);
  }

  final void syncUp(HBaseTestingUtility util) throws Exception {
    ToolRunner.run(util.getConfiguration(), new ReplicationSyncUp(), new String[0]);
  }

  // Utilities that manager shutdown / restart of source / sink clusters. They take care of
  // invalidating stale connections after shutdown / restarts.
  final void shutDownSourceHBaseCluster() throws Exception {
    Closeables.close(ht1Source, true);
    Closeables.close(ht2Source, true);
    UTIL1.shutdownMiniHBaseCluster();
  }

  final void shutDownTargetHBaseCluster() throws Exception {
    Closeables.close(ht1TargetAtPeer1, true);
    Closeables.close(ht2TargetAtPeer1, true);
    UTIL2.shutdownMiniHBaseCluster();
  }

  final void restartSourceHBaseCluster(int numServers) throws Exception {
    Closeables.close(ht1Source, true);
    Closeables.close(ht2Source, true);
    UTIL1.restartHBaseCluster(numServers);
    ht1Source = UTIL1.getConnection().getTable(TN1);
    ht2Source = UTIL1.getConnection().getTable(TN2);
  }

  final void restartTargetHBaseCluster(int numServers) throws Exception {
    Closeables.close(ht1TargetAtPeer1, true);
    Closeables.close(ht2TargetAtPeer1, true);
    UTIL2.restartHBaseCluster(numServers);
    ht1TargetAtPeer1 = UTIL2.getConnection().getTable(TN1);
    ht2TargetAtPeer1 = UTIL2.getConnection().getTable(TN2);
  }
}
