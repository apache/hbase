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

import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class })
public class TestReplicationEditsDroppedWithDroppedTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationEditsDroppedWithDroppedTable.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationEditsDroppedWithDroppedTable.class);

  private static Configuration conf1 = HBaseConfiguration.create();
  private static Configuration conf2 = HBaseConfiguration.create();

  protected static HBaseTestingUtility utility1;
  protected static HBaseTestingUtility utility2;

  private static Admin admin1;
  private static Admin admin2;

  private static final String namespace = "NS";
  private static final TableName NORMAL_TABLE = TableName.valueOf("normal-table");
  private static final TableName DROPPED_TABLE = TableName.valueOf("dropped-table");
  private static final TableName DROPPED_NS_TABLE = TableName.valueOf("NS:dropped-table");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final String PEER_ID = "1";
  private static final long SLEEP_TIME = 1000;
  private static final int NB_RETRIES = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set true to filter replication edits for dropped table
    conf1.setBoolean(HBaseInterClusterReplicationEndpoint.REPLICATION_DROP_ON_DELETED_TABLE_KEY,
        true);
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt("replication.source.nb.capacity", 1);
    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    conf1 = utility1.getConfiguration();

    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    utility1.startMiniCluster(1);
    utility2.startMiniCluster(1);

    admin1 = utility1.getAdmin();
    admin2 = utility2.getAdmin();

    NamespaceDescriptor nsDesc = NamespaceDescriptor.create(namespace).build();
    admin1.createNamespace(nsDesc);
    admin2.createNamespace(nsDesc);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    // Roll log
    for (JVMClusterUtil.RegionServerThread r : utility1.getHBaseCluster()
        .getRegionServerThreads()) {
      utility1.getAdmin().rollWALWriter(r.getRegionServer().getServerName());
    }
    // add peer
    ReplicationPeerConfig rpc = ReplicationPeerConfig.newBuilder()
        .setClusterKey(utility2.getClusterKey())
        .setReplicateAllUserTables(true).build();
    admin1.addReplicationPeer(PEER_ID, rpc);
    // create table
    createTable(NORMAL_TABLE);
  }

  @After
  public void tearDown() throws Exception {
    // Remove peer
    admin1.removeReplicationPeer(PEER_ID);
    // Drop table
    admin1.disableTable(NORMAL_TABLE);
    admin1.deleteTable(NORMAL_TABLE);
    admin2.disableTable(NORMAL_TABLE);
    admin2.deleteTable(NORMAL_TABLE);
  }

  private void createTable(TableName tableName) throws Exception {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()
    ).build();
    admin1.createTable(desc);
    admin2.createTable(desc);
    utility1.waitUntilAllRegionsAssigned(tableName);
    utility2.waitUntilAllRegionsAssigned(tableName);
  }

  @Test
  public void testEditsDroppedWithDroppedTable() throws Exception {
    testWithDroppedTable(DROPPED_TABLE);
  }

  @Test
  public void testEditsDroppedWithDroppedTableNS() throws Exception {
    testWithDroppedTable(DROPPED_NS_TABLE);
  }

  private void testWithDroppedTable(TableName droppedTableName) throws Exception {
    createTable(droppedTableName);
    admin1.disableReplicationPeer(PEER_ID);

    try (Table droppedTable = utility1.getConnection().getTable(droppedTableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      droppedTable.put(put);
    }

    admin1.disableTable(droppedTableName);
    admin1.deleteTable(droppedTableName);
    admin2.disableTable(droppedTableName);
    admin2.deleteTable(droppedTableName);

    admin1.enableReplicationPeer(PEER_ID);

    verifyReplicationProceeded();
  }

  @Test
  public void testEditsBehindDroppedTableTiming() throws Exception {
    createTable(DROPPED_TABLE);
    admin1.disableReplicationPeer(PEER_ID);

    try (Table droppedTable = utility1.getConnection().getTable(DROPPED_TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      droppedTable.put(put);
    }

    // Only delete table from peer cluster
    admin2.disableTable(DROPPED_TABLE);
    admin2.deleteTable(DROPPED_TABLE);

    admin1.enableReplicationPeer(PEER_ID);

    // the source table still exists, replication should be stalled
    verifyReplicationStuck();
    admin1.disableTable(DROPPED_TABLE);
    // still stuck, source table still exists
    verifyReplicationStuck();
    admin1.deleteTable(DROPPED_TABLE);
    // now the source table is gone, replication should proceed, the
    // offending edits be dropped
    verifyReplicationProceeded();
  }

  private void verifyReplicationProceeded() throws Exception {
    try (Table normalTable = utility1.getConnection().getTable(NORMAL_TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      normalTable.put(put);
    }
    utility2.waitFor(NB_RETRIES * SLEEP_TIME, (Predicate<Exception>) () -> {
      try (Table normalTable = utility2.getConnection().getTable(NORMAL_TABLE)) {
        Result result = normalTable.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        return result != null && !result.isEmpty()
            && Bytes.equals(VALUE, result.getValue(FAMILY, QUALIFIER));
      }
    });
  }

  private void verifyReplicationStuck() throws Exception {
    try (Table normalTable = utility1.getConnection().getTable(NORMAL_TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      normalTable.put(put);
    }
    try (Table normalTable = utility2.getConnection().getTable(NORMAL_TABLE)) {
      for (int i = 0; i < NB_RETRIES; i++) {
        Result result = normalTable.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        if (result != null && !result.isEmpty()) {
          fail("Edit should have been stuck behind dropped tables, but value is " + Bytes
              .toString(result.getValue(FAMILY, QUALIFIER)));
        } else {
          LOG.info("Row not replicated, let's wait a bit more...");
          Thread.sleep(SLEEP_TIME);
        }
      }
    }
  }
}