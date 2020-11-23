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

import static org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint.REPLICATION_DROP_ON_DELETED_TABLE_KEY;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationDroppedTables extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationDroppedTables.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationDroppedTables.class);
  private static final int ROWS_COUNT = 1000;

  @Before
  public void setUpBase() throws Exception {
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for (JVMClusterUtil.RegionServerThread r : UTIL1.getHBaseCluster()
        .getRegionServerThreads()) {
      UTIL1.getAdmin().rollWALWriter(r.getRegionServer().getServerName());
    }
    // Initialize the peer after wal rolling, so that we will abandon the stuck WALs.
    super.setUpBase();
    int rowCount = UTIL1.countRows(tableName);
    UTIL1.deleteTableData(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call deleteTableData on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for truncate");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(rowCount);
      scanner.close();
      if (res.length != 0) {
        if (res.length < lastCount) {
          i--; // Don't increment timeout if we make progress
        }
        lastCount = res.length;
        LOG.info("Still got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
    // Set the max request size to a tiny 10K for dividing the replication WAL entries into multiple
    // batches. the default max request size is 256M, so all replication entries are in a batch, but
    // when replicate at sink side, it'll apply to rs group by table name, so the WAL of test table
    // may apply first, and then test_dropped table, and we will believe that the replication is not
    // got stuck (HBASE-20475).
    CONF1.setInt(RpcServer.MAX_REQUEST_SIZE, 10 * 1024);
  }

  @Test
  public void testEditsStuckBehindDroppedTable() throws Exception {
    // Sanity check Make sure by default edits for dropped tables stall the replication queue, even
    // when the table(s) in question have been deleted on both ends.
    testEditsBehindDroppedTable(false, "test_dropped");
  }

  @Test
  public void testEditsDroppedWithDroppedTable() throws Exception {
    // Make sure by default edits for dropped tables are themselves dropped when the
    // table(s) in question have been deleted on both ends.
    testEditsBehindDroppedTable(true, "test_dropped");
  }

  @Test
  public void testEditsDroppedWithDroppedTableNS() throws Exception {
    // also try with a namespace
    UTIL1.getAdmin().createNamespace(NamespaceDescriptor.create("NS").build());
    UTIL2.getAdmin().createNamespace(NamespaceDescriptor.create("NS").build());
    try {
      testEditsBehindDroppedTable(true, "NS:test_dropped");
    } finally {
      UTIL1.getAdmin().deleteNamespace("NS");
      UTIL2.getAdmin().deleteNamespace("NS");
    }
  }

  private byte[] generateRowKey(int id) {
    return Bytes.toBytes(String.format("NormalPut%03d", id));
  }

  private void testEditsBehindDroppedTable(boolean allowProceeding, String tName) throws Exception {
    CONF1.setBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, allowProceeding);
    CONF1.setInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY, 1);

    // make sure we have a single region server only, so that all
    // edits for all tables go there
    restartSourceCluster(1);

    TableName tablename = TableName.valueOf(tName);
    byte[] familyName = Bytes.toBytes("fam");
    byte[] row = Bytes.toBytes("row");

    TableDescriptor table =
        TableDescriptorBuilder
            .newBuilder(tablename).setColumnFamily(ColumnFamilyDescriptorBuilder
                .newBuilder(familyName).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
            .build();

    Connection connection1 = ConnectionFactory.createConnection(UTIL1.getConfiguration());
    Connection connection2 = ConnectionFactory.createConnection(UTIL2.getConfiguration());
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.createTable(table);
    }
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.createTable(table);
    }
    UTIL1.waitUntilAllRegionsAssigned(tablename);
    UTIL2.waitUntilAllRegionsAssigned(tablename);

    // now suspend replication
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.disableReplicationPeer(PEER_ID2);
    }

    // put some data (lead with 0 so the edit gets sorted before the other table's edits
    // in the replication batch) write a bunch of edits, making sure we fill a batch
    try (Table droppedTable = connection1.getTable(tablename)) {
      byte[] rowKey = Bytes.toBytes(0 + " put on table to be dropped");
      Put put = new Put(rowKey);
      put.addColumn(familyName, row, row);
      droppedTable.put(put);
    }

    try (Table table1 = connection1.getTable(tableName)) {
      for (int i = 0; i < ROWS_COUNT; i++) {
        Put put = new Put(generateRowKey(i)).addColumn(famName, row, row);
        table1.put(put);
      }
    }

    try (Admin admin1 = connection1.getAdmin()) {
      admin1.disableTable(tablename);
      admin1.deleteTable(tablename);
    }
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.disableTable(tablename);
      admin2.deleteTable(tablename);
    }

    try (Admin admin1 = connection1.getAdmin()) {
      admin1.enableReplicationPeer(PEER_ID2);
    }

    if (allowProceeding) {
      // in this we'd expect the key to make it over
      verifyReplicationProceeded();
    } else {
      verifyReplicationStuck();
    }
    // just to be safe
    CONF1.setBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
  }

  @Test
  public void testEditsBehindDroppedTableTiming() throws Exception {
    CONF1.setBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, true);
    CONF1.setInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY, 1);

    // make sure we have a single region server only, so that all
    // edits for all tables go there
    restartSourceCluster(1);

    TableName tablename = TableName.valueOf("testdroppedtimed");
    byte[] familyName = Bytes.toBytes("fam");
    byte[] row = Bytes.toBytes("row");

    TableDescriptor table =
        TableDescriptorBuilder
            .newBuilder(tablename).setColumnFamily(ColumnFamilyDescriptorBuilder
                .newBuilder(familyName).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
            .build();

    Connection connection1 = ConnectionFactory.createConnection(CONF1);
    Connection connection2 = ConnectionFactory.createConnection(CONF2);
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.createTable(table);
    }
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.createTable(table);
    }
    UTIL1.waitUntilAllRegionsAssigned(tablename);
    UTIL2.waitUntilAllRegionsAssigned(tablename);

    // now suspend replication
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.disableReplicationPeer(PEER_ID2);
    }

    // put some data (lead with 0 so the edit gets sorted before the other table's edits
    // in the replication batch) write a bunch of edits, making sure we fill a batch
    try (Table droppedTable = connection1.getTable(tablename)) {
      byte[] rowKey = Bytes.toBytes(0 + " put on table to be dropped");
      Put put = new Put(rowKey);
      put.addColumn(familyName, row, row);
      droppedTable.put(put);
    }

    try (Table table1 = connection1.getTable(tableName)) {
      for (int i = 0; i < ROWS_COUNT; i++) {
        Put put = new Put(generateRowKey(i)).addColumn(famName, row, row);
        table1.put(put);
      }
    }

    try (Admin admin2 = connection2.getAdmin()) {
      admin2.disableTable(tablename);
      admin2.deleteTable(tablename);
    }

    // edit should still be stuck
    try (Admin admin1 = connection1.getAdmin()) {
      // enable the replication peer.
      admin1.enableReplicationPeer(PEER_ID2);
      // the source table still exists, replication should be stalled
      verifyReplicationStuck();

      admin1.disableTable(tablename);
      // still stuck, source table still exists
      verifyReplicationStuck();

      admin1.deleteTable(tablename);
      // now the source table is gone, replication should proceed, the
      // offending edits be dropped
      verifyReplicationProceeded();
    }
    // just to be safe
    CONF1.setBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
  }

  private boolean peerHasAllNormalRows() throws IOException {
    try (ResultScanner scanner = htable2.getScanner(new Scan())) {
      Result[] results = scanner.next(ROWS_COUNT);
      if (results.length != ROWS_COUNT) {
        return false;
      }
      for (int i = 0; i < results.length; i++) {
        Assert.assertArrayEquals(generateRowKey(i), results[i].getRow());
      }
      return true;
    }
  }

  private void verifyReplicationProceeded() throws Exception {
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for put replication");
      }
      if (!peerHasAllNormalRows()) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  private void verifyReplicationStuck() throws Exception {
    for (int i = 0; i < NB_RETRIES; i++) {
      if (peerHasAllNormalRows()) {
        fail("Edit should have been stuck behind dropped tables");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }
  }
}
