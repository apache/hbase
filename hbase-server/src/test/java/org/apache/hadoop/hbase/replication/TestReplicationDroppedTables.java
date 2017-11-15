/*
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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

@Category(LargeTests.class)
public class TestReplicationDroppedTables extends TestReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestReplicationDroppedTables.class);

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for ( JVMClusterUtil.RegionServerThread r :
        utility1.getHBaseCluster().getRegionServerThreads()) {
      utility1.getHBaseAdmin().rollWALWriter(r.getRegionServer().getServerName());
    }
    int rowCount = utility1.countRows(tableName);
    utility1.deleteTableData(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call deleteTableData on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
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
  }

  @Test(timeout = 600000)
  public void testEditsStuckBehindDroppedTable() throws Exception {
    // Sanity check
    // Make sure by default edits for dropped tables stall the replication queue, even when the
    // table(s) in question have been deleted on both ends.
    testEditsBehindDroppedTable(false, "test_dropped");
  }

  @Test(timeout = 600000)
  public void testEditsDroppedWithDroppedTable() throws Exception {
    // Make sure by default edits for dropped tables are themselves dropped when the
    // table(s) in question have been deleted on both ends.
    testEditsBehindDroppedTable(true, "test_dropped");
  }

  @Test(timeout = 600000)
  public void testEditsDroppedWithDroppedTableNS() throws Exception {
    // also try with a namespace
    Connection connection1 = ConnectionFactory.createConnection(conf1);
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.createNamespace(NamespaceDescriptor.create("NS").build());
    }
    Connection connection2 = ConnectionFactory.createConnection(conf2);
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.createNamespace(NamespaceDescriptor.create("NS").build());
    }
    testEditsBehindDroppedTable(true, "NS:test_dropped");
  }

  private void testEditsBehindDroppedTable(boolean allowProceeding, String tName) throws Exception {
    conf1.setBoolean(HConstants.REPLICATION_DROP_ON_DELETED_TABLE_KEY, allowProceeding);
    conf1.setInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY, 1);

    // make sure we have a single region server only, so that all
    // edits for all tables go there
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster(1, 1);

    TableName tablename = TableName.valueOf(tName);
    byte[] familyname = Bytes.toBytes("fam");
    byte[] row = Bytes.toBytes("row");

    HTableDescriptor table = new HTableDescriptor(tablename);
    HColumnDescriptor fam = new HColumnDescriptor(familyname);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);

    Connection connection1 = ConnectionFactory.createConnection(conf1);
    Connection connection2 = ConnectionFactory.createConnection(conf2);
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.createTable(table);
    }
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.createTable(table);
    }
    utility1.waitUntilAllRegionsAssigned(tablename);
    utility2.waitUntilAllRegionsAssigned(tablename);

    Table lHtable1 = utility1.getConnection().getTable(tablename);

    // now suspend replication
    admin.disablePeer(PEER_ID);

    // put some data (lead with 0 so the edit gets sorted before the other table's edits
    //   in the replication batch)
    // write a bunch of edits, making sure we fill a batch
    byte[] rowkey = Bytes.toBytes(0+" put on table to be dropped");
    Put put = new Put(rowkey);
    put.addColumn(familyname, row, row);
    lHtable1.put(put);

    rowkey = Bytes.toBytes("normal put");
    put = new Put(rowkey);
    put.addColumn(famName, row, row);
    htable1.put(put);

    try (Admin admin1 = connection1.getAdmin()) {
      admin1.disableTable(tablename);
      admin1.deleteTable(tablename);
    }
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.disableTable(tablename);
      admin2.deleteTable(tablename);
    }

    admin.enablePeer(PEER_ID);
    if (allowProceeding) {
      // in this we'd expect the key to make it over
      verifyReplicationProceeded(rowkey);
    } else {
      verifyReplicationStuck(rowkey);
    }
    // just to be safe
    conf1.setBoolean(HConstants.REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
  }

  @Test(timeout = 600000)
  public void testEditsBehindDroppedTableTiming() throws Exception {
    conf1.setBoolean(HConstants.REPLICATION_DROP_ON_DELETED_TABLE_KEY, true);
    conf1.setInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY, 1);

    // make sure we have a single region server only, so that all
    // edits for all tables go there
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster(1, 1);

    TableName tablename = TableName.valueOf("testdroppedtimed");
    byte[] familyname = Bytes.toBytes("fam");
    byte[] row = Bytes.toBytes("row");

    HTableDescriptor table = new HTableDescriptor(tablename);
    HColumnDescriptor fam = new HColumnDescriptor(familyname);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);

    Connection connection1 = ConnectionFactory.createConnection(conf1);
    Connection connection2 = ConnectionFactory.createConnection(conf2);
    try (Admin admin1 = connection1.getAdmin()) {
      admin1.createTable(table);
    }
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.createTable(table);
    }
    utility1.waitUntilAllRegionsAssigned(tablename);
    utility2.waitUntilAllRegionsAssigned(tablename);

    Table lHtable1 = utility1.getConnection().getTable(tablename);

    // now suspend replication
    admin.disablePeer(PEER_ID);

    // put some data (lead with 0 so the edit gets sorted before the other table's edits
    //   in the replication batch)
    // write a bunch of edits, making sure we fill a batch
    byte[] rowkey = Bytes.toBytes(0+" put on table to be dropped");
    Put put = new Put(rowkey);
    put.addColumn(familyname, row, row);
    lHtable1.put(put);

    rowkey = Bytes.toBytes("normal put");
    put = new Put(rowkey);
    put.addColumn(famName, row, row);
    htable1.put(put);

    try (Admin admin2 = connection2.getAdmin()) {
      admin2.disableTable(tablename);
      admin2.deleteTable(tablename);
    }

    admin.enablePeer(PEER_ID);
    // edit should still be stuck

    try (Admin admin1 = connection1.getAdmin()) {
      // the source table still exists, replication should be stalled
      verifyReplicationStuck(rowkey);

      admin1.disableTable(tablename);
      // still stuck, source table still exists
      verifyReplicationStuck(rowkey);

      admin1.deleteTable(tablename);
      // now the source table is gone, replication should proceed, the
      // offending edits be dropped
      verifyReplicationProceeded(rowkey);
    }
    // just to be safe
    conf1.setBoolean(HConstants.REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
  }

  private void verifyReplicationProceeded(byte[] rowkey) throws Exception {
    Get get = new Get(rowkey);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.getRow(), rowkey);
        break;
      }
    }
  }

  private void verifyReplicationStuck(byte[] rowkey) throws Exception {
    Get get = new Get(rowkey);
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Edit should have been stuck behind dropped tables");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }
  }
}
