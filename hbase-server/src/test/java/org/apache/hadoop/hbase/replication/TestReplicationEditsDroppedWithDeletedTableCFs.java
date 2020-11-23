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
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint.REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
public class TestReplicationEditsDroppedWithDeletedTableCFs {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationEditsDroppedWithDeletedTableCFs.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationEditsDroppedWithDeletedTableCFs.class);

  private static Configuration conf1 = HBaseConfiguration.create();
  private static Configuration conf2 = HBaseConfiguration.create();

  protected static HBaseTestingUtility utility1;
  protected static HBaseTestingUtility utility2;

  private static Admin admin1;
  private static Admin admin2;

  private static final TableName TABLE = TableName.valueOf("table");
  private static final byte[] NORMAL_CF = Bytes.toBytes("normal_cf");
  private static final byte[] DROPPED_CF = Bytes.toBytes("dropped_cf");

  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final String PEER_ID = "1";
  private static final long SLEEP_TIME = 1000;
  private static final int NB_RETRIES = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set true to filter replication edits for dropped table
    conf1.setBoolean(REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY, true);
    conf1.set(ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt("replication.source.nb.capacity", 1);
    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    conf1 = utility1.getConfiguration();

    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(ZOOKEEPER_ZNODE_PARENT, "/2");
    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    utility1.startMiniCluster(1);
    utility2.startMiniCluster(1);

    admin1 = utility1.getAdmin();
    admin2 = utility2.getAdmin();
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
    createTable();
  }

  @After
  public void tearDown() throws Exception {
    // Remove peer
    admin1.removeReplicationPeer(PEER_ID);
    // Drop table
    admin1.disableTable(TABLE);
    admin1.deleteTable(TABLE);
    admin2.disableTable(TABLE);
    admin2.deleteTable(TABLE);
  }

  private void createTable() throws Exception {
    TableDescriptor desc = createTableDescriptor(NORMAL_CF, DROPPED_CF);
    admin1.createTable(desc);
    admin2.createTable(desc);
    utility1.waitUntilAllRegionsAssigned(desc.getTableName());
    utility2.waitUntilAllRegionsAssigned(desc.getTableName());
  }

  @Test
  public void testEditsDroppedWithDeleteCF() throws Exception {
    admin1.disableReplicationPeer(PEER_ID);

    try (Table table = utility1.getConnection().getTable(TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(DROPPED_CF, QUALIFIER, VALUE);
      table.put(put);
    }

    deleteCf(admin1);
    deleteCf(admin2);

    admin1.enableReplicationPeer(PEER_ID);

    verifyReplicationProceeded();
  }

  @Test
  public void testEditsBehindDeleteCFTiming() throws Exception {
    admin1.disableReplicationPeer(PEER_ID);

    try (Table table = utility1.getConnection().getTable(TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(DROPPED_CF, QUALIFIER, VALUE);
      table.put(put);
    }

    // Only delete cf from peer cluster
    deleteCf(admin2);

    admin1.enableReplicationPeer(PEER_ID);

    // the source table's cf still exists, replication should be stalled
    verifyReplicationStuck();
    deleteCf(admin1);
    // now the source table's cf is gone, replication should proceed, the
    // offending edits be dropped
    verifyReplicationProceeded();
  }

  private void verifyReplicationProceeded() throws Exception {
    try (Table table = utility1.getConnection().getTable(TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(NORMAL_CF, QUALIFIER, VALUE);
      table.put(put);
    }
    utility2.waitFor(NB_RETRIES * SLEEP_TIME, (Predicate<Exception>) () -> {
      try (Table peerTable = utility2.getConnection().getTable(TABLE)) {
        Result result = peerTable.get(new Get(ROW).addColumn(NORMAL_CF, QUALIFIER));
        return result != null && !result.isEmpty()
            && Bytes.equals(VALUE, result.getValue(NORMAL_CF, QUALIFIER));
      }
    });
  }

  private void verifyReplicationStuck() throws Exception {
    try (Table table = utility1.getConnection().getTable(TABLE)) {
      Put put = new Put(ROW);
      put.addColumn(NORMAL_CF, QUALIFIER, VALUE);
      table.put(put);
    }
    try (Table peerTable = utility2.getConnection().getTable(TABLE)) {
      for (int i = 0; i < NB_RETRIES; i++) {
        Result result = peerTable.get(new Get(ROW).addColumn(NORMAL_CF, QUALIFIER));
        if (result != null && !result.isEmpty()) {
          fail("Edit should have been stuck behind dropped tables, but value is " + Bytes
              .toString(result.getValue(NORMAL_CF, QUALIFIER)));
        } else {
          LOG.info("Row not replicated, let's wait a bit more...");
          Thread.sleep(SLEEP_TIME);
        }
      }
    }
  }

  private TableDescriptor createTableDescriptor(byte[]... cfs) {
    return TableDescriptorBuilder.newBuilder(TABLE)
        .setColumnFamilies(Arrays.stream(cfs).map(cf ->
            ColumnFamilyDescriptorBuilder.newBuilder(cf).setScope(REPLICATION_SCOPE_GLOBAL).build())
            .collect(Collectors.toList())
        ).build();
  }

  private void deleteCf(Admin admin) throws IOException {
    TableDescriptor desc = createTableDescriptor(NORMAL_CF);
    admin.modifyTable(desc);
  }
}