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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.TestBulkLoadReplication;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testcase for HBASE-23098
 */
// LargeTest because spins up four clusters.
@Category({ ReplicationTests.class, LargeTests.class })
public final class TestNamespaceReplicationWithBulkLoadedData extends TestBulkLoadReplication {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespaceReplicationWithBulkLoadedData.class);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNamespaceReplicationWithBulkLoadedData.class);

  private static final HBaseTestingUtil UTIL4 = new HBaseTestingUtil();
  private static final String PEER4_CLUSTER_ID = "peer4";
  private static final String PEER4_NS = "ns_peer1";
  private static final String PEER4_NS_TABLE = "ns_peer2";

  private static final Configuration CONF4 = UTIL4.getConfiguration();

  private static final String NS1 = "ns1";
  private static final String NS2 = "ns2";

  private static final TableName NS1_TABLE = TableName.valueOf(NS1 + ":t1_syncup");
  private static final TableName NS2_TABLE = TableName.valueOf(NS2 + ":t2_syncup");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBulkLoadConfigsForCluster(CONF4, PEER4_CLUSTER_ID);
    setupConfig(UTIL4, "/4");
    TestBulkLoadReplication.setUpBeforeClass();
    startFourthCluster();
  }

  private static void startFourthCluster() throws Exception {
    LOG.info("Setup Zk to same one from UTIL1 and UTIL2 and UTIL3");
    UTIL4.setZkCluster(UTIL1.getZkCluster());
    UTIL4.startMiniCluster(NUM_SLAVES1);

    TableDescriptor table = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName).setMaxVersions(100)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    Connection connection4 = ConnectionFactory.createConnection(CONF4);
    try (Admin admin4 = connection4.getAdmin()) {
      admin4.createTable(table, HBaseTestingUtil.KEYS_FOR_HBA_CREATE_TABLE);
    }
    UTIL4.waitUntilAllRegionsAssigned(tableName);
  }

  @Before
  @Override
  public void setUpBase() throws Exception {
    /** "super.setUpBase()" already sets peer1 from 1 <-> 2 <-> 3
     * and this test add the fourth cluster.
     * So we have following topology:
     *      1
     *     / \
     *    2   4
     *   /
     *  3
     *
     *  The 1 -> 4 has two peers,
     *  ns_peer1:  ns1 -> ns1 (validate this peer hfile-refs)
     *             ns_peer1 configuration is NAMESPACES => ["ns1"]
     *
     *  ns_peer2:  ns2:t2_syncup -> ns2:t2_syncup, this peers is
     *             ns_peer2 configuration is NAMESPACES => ["ns2"],
     *                       TABLE_CFS => { "ns2:t2_syncup" => []}
     *
     *  The 1 -> 2 has one peer, this peer configuration is
     *             add_peer '2', CLUSTER_KEY => "server1.cie.com:2181:/hbase"
     *
     */
    super.setUpBase();

    // Create tables
    TableDescriptor table1 = TableDescriptorBuilder.newBuilder(NS1_TABLE)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(famName)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    TableDescriptor table2 = TableDescriptorBuilder.newBuilder(NS2_TABLE)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(famName)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    Admin admin1 = UTIL1.getAdmin();
    admin1.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin1.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin1.createTable(table1);
    admin1.createTable(table2);

    Admin admin2 = UTIL2.getAdmin();
    admin2.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin2.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin2.createTable(table1);
    admin2.createTable(table2);

    Admin admin3 = UTIL3.getAdmin();
    admin3.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin3.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin3.createTable(table1);
    admin3.createTable(table2);

    Admin admin4 = UTIL4.getAdmin();
    admin4.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin4.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin4.createTable(table1);
    admin4.createTable(table2);

    /**
     *  Set ns_peer1 1: ns1 -> 2: ns1
     *
     *  add_peer 'ns_peer1', CLUSTER_KEY => "zk1,zk2,zk3:2182:/hbase-prod",
     *     NAMESPACES => ["ns1"]
     */
    Set<String> namespaces = new HashSet<>();
    namespaces.add(NS1);
    ReplicationPeerConfig rpc4_ns =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL4.getClusterKey())
            .setReplicateAllUserTables(false).setNamespaces(namespaces).build();
    admin1.addReplicationPeer(PEER4_NS, rpc4_ns);

    /**
     * Set ns_peer2 1: ns2:t2_syncup -> 4: ns2:t2_syncup
     *
     * add_peer 'ns_peer2', CLUSTER_KEY => "zk1,zk2,zk3:2182:/hbase-prod",
     *          NAMESPACES => ["ns2"], TABLE_CFS => { "ns2:t2_syncup" => [] }
     */
    Map<TableName, List<String>> tableCFsMap = new HashMap<>();
    tableCFsMap.put(NS2_TABLE, null);
    ReplicationPeerConfig rpc4_ns_table =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL4.getClusterKey())
            .setReplicateAllUserTables(false).setTableCFsMap(tableCFsMap).build();
    admin1.addReplicationPeer(PEER4_NS_TABLE, rpc4_ns_table);
  }

  @After
  @Override
  public void tearDownBase() throws Exception {
    super.tearDownBase();
    TableDescriptor table1 = TableDescriptorBuilder.newBuilder(NS1_TABLE)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(famName)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    TableDescriptor table2 = TableDescriptorBuilder.newBuilder(NS2_TABLE)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(famName)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();
    Admin admin1 = UTIL1.getAdmin();
    admin1.disableTable(table1.getTableName());
    admin1.deleteTable(table1.getTableName());
    admin1.disableTable(table2.getTableName());
    admin1.deleteTable(table2.getTableName());
    admin1.deleteNamespace(NS1);
    admin1.deleteNamespace(NS2);

    Admin admin2 = UTIL2.getAdmin();
    admin2.disableTable(table1.getTableName());
    admin2.deleteTable(table1.getTableName());
    admin2.disableTable(table2.getTableName());
    admin2.deleteTable(table2.getTableName());
    admin2.deleteNamespace(NS1);
    admin2.deleteNamespace(NS2);

    Admin admin3 = UTIL3.getAdmin();
    admin3.disableTable(table1.getTableName());
    admin3.deleteTable(table1.getTableName());
    admin3.disableTable(table2.getTableName());
    admin3.deleteTable(table2.getTableName());
    admin3.deleteNamespace(NS1);
    admin3.deleteNamespace(NS2);

    Admin admin4 = UTIL4.getAdmin();
    admin4.disableTable(table1.getTableName());
    admin4.deleteTable(table1.getTableName());
    admin4.disableTable(table2.getTableName());
    admin4.deleteTable(table2.getTableName());
    admin4.deleteNamespace(NS1);
    admin4.deleteNamespace(NS2);
    UTIL1.getAdmin().removeReplicationPeer(PEER4_NS);
    UTIL1.getAdmin().removeReplicationPeer(PEER4_NS_TABLE);
  }

  @Test
  @Override
  public void testBulkLoadReplicationActiveActive() throws Exception {
    Table peer1TestTable = UTIL1.getConnection().getTable(TestReplicationBase.tableName);
    Table peer2TestTable = UTIL2.getConnection().getTable(TestReplicationBase.tableName);
    Table peer3TestTable = UTIL3.getConnection().getTable(TestReplicationBase.tableName);
    Table notPeerTable = UTIL4.getConnection().getTable(TestReplicationBase.tableName);
    Table ns1Table = UTIL4.getConnection().getTable(NS1_TABLE);
    Table ns2Table = UTIL4.getConnection().getTable(NS2_TABLE);

    // case1: The ns1 tables will be replicate to cluster4
    byte[] row = Bytes.toBytes("002_ns_peer");
    byte[] value = Bytes.toBytes("v2");
    bulkLoadOnCluster(ns1Table.getName(), row, value, UTIL1);
    waitForReplication(ns1Table, 1, NB_RETRIES);
    assertTableHasValue(ns1Table, row, value);

    // case2: The ns2:t2_syncup will be replicate to cluster4
    // If it's not fix HBASE-23098 the ns_peer1's hfile-refs(zk) will be backlog
    row = Bytes.toBytes("003_ns_table_peer");
    value = Bytes.toBytes("v2");
    bulkLoadOnCluster(ns2Table.getName(), row, value, UTIL1);
    waitForReplication(ns2Table, 1, NB_RETRIES);
    assertTableHasValue(ns2Table, row, value);

    // case3: The table test will be replicate to cluster1,cluster2,cluster3
    //        not replicate to cluster4, because we not set other peer for that tables.
    row = Bytes.toBytes("001_nopeer");
    value = Bytes.toBytes("v1");
    assertBulkLoadConditions(tableName, row, value, UTIL1, peer1TestTable,
        peer2TestTable, peer3TestTable);
    assertTableNoValue(notPeerTable, row, value); // 1 -> 4, table is empty

    // Verify hfile-refs for 1:ns_peer1, expect is empty
    MiniZooKeeperCluster zkCluster = UTIL1.getZkCluster();
    ZKWatcher watcher = new ZKWatcher(UTIL1.getConfiguration(), "TestZnodeHFiles-refs", null);
    RecoverableZooKeeper zk = RecoverableZooKeeper.connect(UTIL1.getConfiguration(), watcher);
    ZKReplicationQueueStorage replicationQueueStorage =
        new ZKReplicationQueueStorage(watcher, UTIL1.getConfiguration());
    Set<String> hfiles = replicationQueueStorage.getAllHFileRefs();
    assertTrue(hfiles.isEmpty());
  }
}
