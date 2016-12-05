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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test clone snapshots from the client
 */
@Category({LargeTests.class, ClientTests.class})
public class TestMobCloneSnapshotFromClient extends TestCloneSnapshotFromClient {

  private static boolean delayFlush = false;

  protected static void setupConfiguration() {
    TestCloneSnapshotFromClient.setupConfiguration();
    TEST_UTIL.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupConfiguration();
    TEST_UTIL.startMiniCluster(3);
  }

  @Override
  protected void createTableAndSnapshots() throws Exception {
    // create Table and disable it
    createMobTable(TEST_UTIL, tableName, SnapshotTestingUtils.getSplitKeys(), getNumReplicas(),
      FAMILY);
    delayFlush = false;
    admin.disableTable(tableName);

    // take an empty snapshot
    admin.snapshot(emptySnapshot, tableName);

    Connection c = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    Table table = c.getTable(tableName);
    try {
      // enable table and insert data
      admin.enableTable(tableName);
      SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 20, FAMILY);
      snapshot0Rows = MobSnapshotTestingUtils.countMobRows(table);
      admin.disableTable(tableName);

      // take a snapshot
      admin.snapshot(snapshotName0, tableName);

      // enable table and insert more data
      admin.enableTable(tableName);
      SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 20, FAMILY);
      snapshot1Rows = MobSnapshotTestingUtils.countMobRows(table);
      admin.disableTable(tableName);

      // take a snapshot of the updated table
      admin.snapshot(snapshotName1, tableName);

      // re-enable table
      admin.enableTable(tableName);
    } finally {
      table.close();
    }
  }

  @Test
  @Override
  public void testCloneLinksAfterDelete() throws IOException, InterruptedException {
    // delay the flush to make sure
    delayFlush = true;
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 20, FAMILY);
    long tid = System.currentTimeMillis();
    byte[] snapshotName3 = Bytes.toBytes("snaptb3-" + tid);
    TableName clonedTableName3 = TableName.valueOf("clonedtb3-" + System.currentTimeMillis());
    admin.snapshot(snapshotName3, tableName);
    delayFlush = false;
    int snapshot3Rows = -1;
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      snapshot3Rows = TEST_UTIL.countRows(table);
    }
    admin.cloneSnapshot(snapshotName3, clonedTableName3);
    admin.deleteSnapshot(snapshotName3);
    super.testCloneLinksAfterDelete();
    verifyRowCount(TEST_UTIL, clonedTableName3, snapshot3Rows);
    admin.disableTable(clonedTableName3);
    admin.deleteTable(clonedTableName3);
  }

  @Override
  protected void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, expectedRows);
  }

  /**
   * This coprocessor is used to delay the flush.
   */
  public static class DelayFlushCoprocessor extends BaseRegionObserver {
    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
      if (delayFlush) {
        try {
          if (Bytes.compareTo(e.getEnvironment().getRegionInfo().getStartKey(),
            HConstants.EMPTY_START_ROW) != 0) {
            Thread.sleep(100);
          }
        } catch (InterruptedException e1) {
          throw new InterruptedIOException(e1.getMessage());
        }
      }
      super.preFlush(e);
    }
  }

  private void createMobTable(final HBaseTestingUtility util, final TableName tableName,
    final byte[][] splitKeys, int regionReplication, final byte[]... families) throws IOException,
    InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setRegionReplication(regionReplication);
    htd.addCoprocessor(DelayFlushCoprocessor.class.getName());
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      hcd.setMobEnabled(true);
      hcd.setMobThreshold(0L);
      htd.addFamily(hcd);
    }
    util.getHBaseAdmin().createTable(htd, splitKeys);
    SnapshotTestingUtils.waitForTableToBeOnline(util, tableName);
    assertEquals((splitKeys.length + 1) * regionReplication,
      util.getHBaseAdmin().getTableRegions(tableName).size());
  }
}
