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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

@Category({MasterTests.class, MediumTests.class})
public class TestMaster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMaster.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestMaster.class);
  private static final TableName TABLENAME = TableName.valueOf("TestMaster");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static Admin admin;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // we will retry operations when PleaseHoldException is thrown
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Return the region and current deployment for the region containing the given row. If the region
   * cannot be found, returns null. If it is found, but not currently deployed, the second element
   * of the pair may be null.
   */
  private Pair<RegionInfo, ServerName> getTableRegionForRow(HMaster master, TableName tableName,
      byte[] rowKey) throws IOException {
    final AtomicReference<Pair<RegionInfo, ServerName>> result = new AtomicReference<>(null);

    ClientMetaTableAccessor.Visitor visitor = new ClientMetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result data) throws IOException {
        if (data == null || data.size() <= 0) {
          return true;
        }
        Pair<RegionInfo, ServerName> pair = new Pair<>(CatalogFamilyFormat.getRegionInfo(data),
          CatalogFamilyFormat.getServerName(data, 0));
        if (!pair.getFirst().getTable().equals(tableName)) {
          return false;
        }
        result.set(pair);
        return true;
      }
    };

    MetaTableAccessor.scanMeta(master.getConnection(), visitor, tableName, rowKey, 1);
    return result.get();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMasterOpsWhileSplitting() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();

    try (Table ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME)) {
      assertTrue(m.getTableStateManager().isTableState(TABLENAME, TableState.State.ENABLED));
      TEST_UTIL.loadTable(ht, FAMILYNAME, false);
    }

    List<Pair<RegionInfo, ServerName>> tableRegions = MetaTableAccessor.getTableRegionsAndLocations(
        m.getConnection(), TABLENAME);
    LOG.info("Regions after load: " + Joiner.on(',').join(tableRegions));
    assertEquals(1, tableRegions.size());
    assertArrayEquals(HConstants.EMPTY_START_ROW,
        tableRegions.get(0).getFirst().getStartKey());
    assertArrayEquals(HConstants.EMPTY_END_ROW,
        tableRegions.get(0).getFirst().getEndKey());

    // Now trigger a split and stop when the split is in progress
    LOG.info("Splitting table");
    TEST_UTIL.getAdmin().split(TABLENAME);

    LOG.info("Making sure we can call getTableRegions while opening");
    while (tableRegions.size() < 3) {
      tableRegions = MetaTableAccessor.getTableRegionsAndLocations(m.getConnection(),
          TABLENAME, false);
      Thread.sleep(100);
    }
    LOG.info("Regions: " + Joiner.on(',').join(tableRegions));
    // We have three regions because one is split-in-progress
    assertEquals(3, tableRegions.size());
    LOG.info("Making sure we can call getTableRegionClosest while opening");
    Pair<RegionInfo, ServerName> pair = getTableRegionForRow(m, TABLENAME, Bytes.toBytes("cde"));
    LOG.info("Result is: " + pair);
    Pair<RegionInfo, ServerName> tableRegionFromName =
        MetaTableAccessor.getRegion(m.getConnection(),
          pair.getFirst().getRegionName());
    assertTrue(RegionInfo.COMPARATOR.compare(tableRegionFromName.getFirst(), pair.getFirst()) == 0);
  }

  @Test
  public void testMoveRegionWhenNotInitialized() {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    try {
      m.setInitialized(false); // fake it, set back later
      RegionInfo meta = m.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(TableName.META_TABLE_NAME).get(0);
      m.move(meta.getEncodedNameAsBytes(), null);
      fail("Region should not be moved since master is not initialized");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof PleaseHoldException);
    } finally {
      m.setInitialized(true);
    }
  }

  @Test
  public void testMoveThrowsUnknownRegionException() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("value")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    admin.createTable(tableDescriptorBuilder.build());
    try {
      RegionInfo hri = RegionInfoBuilder.newBuilder(tableName)
          .setStartKey(Bytes.toBytes("A"))
          .setEndKey(Bytes.toBytes("Z"))
          .build();
      admin.move(hri.getEncodedNameAsBytes());
      fail("Region should not be moved since it is fake");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof UnknownRegionException);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMoveThrowsPleaseHoldException() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("value")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    admin.createTable(tableDescriptorBuilder.build());
    try {
      List<RegionInfo> tableRegions = admin.getRegions(tableName);

      master.setInitialized(false); // fake it, set back later
      admin.move(tableRegions.get(0).getEncodedNameAsBytes());
      fail("Region should not be moved since master is not initialized");
    } catch (IOException ioe) {
      assertTrue(StringUtils.stringifyException(ioe).contains("PleaseHoldException"));
    } finally {
      master.setInitialized(true);
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testFlushedSequenceIdPersistLoad() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    int msgInterval = conf.getInt("hbase.regionserver.msginterval", 100);
    // insert some data into META
    TableName tableName = TableName.valueOf("testFlushSeqId");
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(tableName);

    tableDescriptor.setColumnFamily(
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(Bytes.toBytes("cf")));
    Table table = TEST_UTIL.createTable(tableDescriptor, null);
    // flush META region
    TEST_UTIL.flush(TableName.META_TABLE_NAME);
    // wait for regionserver report
    Threads.sleep(msgInterval * 2);
    // record flush seqid before cluster shutdown
    Map<byte[], Long> regionMapBefore =
        TEST_UTIL.getHBaseCluster().getMaster().getServerManager()
            .getFlushedSequenceIdByRegion();
    // restart hbase cluster which will cause flushed sequence id persist and reload
    TEST_UTIL.getMiniHBaseCluster().shutdown();
    TEST_UTIL.restartHBaseCluster(2);
    TEST_UTIL.waitUntilNoRegionsInTransition();
    // check equality after reloading flushed sequence id map
    Map<byte[], Long> regionMapAfter =
        TEST_UTIL.getHBaseCluster().getMaster().getServerManager()
            .getFlushedSequenceIdByRegion();
    assertTrue(regionMapBefore.equals(regionMapAfter));
  }

  @Test
  public void testBlockingHbkc1WithLockFile() throws IOException {
    // This is how the patch to the lock file is created inside in HBaseFsck. Too hard to use its
    // actual method without disturbing HBaseFsck... Do the below mimic instead.
    Path hbckLockPath = new Path(HBaseFsck.getTmpDir(TEST_UTIL.getConfiguration()),
        HBaseFsck.HBCK_LOCK_FILE);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    assertTrue(fs.exists(hbckLockPath));
    TEST_UTIL.getMiniHBaseCluster().
        killMaster(TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName());
    assertTrue(fs.exists(hbckLockPath));
    TEST_UTIL.getMiniHBaseCluster().startMaster();
    TEST_UTIL.waitFor(30000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster() != null &&
        TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    assertTrue(fs.exists(hbckLockPath));
    // Start a second Master. Should be fine.
    TEST_UTIL.getMiniHBaseCluster().startMaster();
    assertTrue(fs.exists(hbckLockPath));
    fs.delete(hbckLockPath, true);
    assertFalse(fs.exists(hbckLockPath));
    // Kill all Masters.
    TEST_UTIL.getMiniHBaseCluster().getLiveMasterThreads().stream().
        map(sn -> sn.getMaster().getServerName()).forEach(sn -> {
          try {
            TEST_UTIL.getMiniHBaseCluster().killMaster(sn);
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
    // Start a new one.
    TEST_UTIL.getMiniHBaseCluster().startMaster();
    TEST_UTIL.waitFor(30000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster() != null &&
        TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    // Assert lock gets put in place again.
    assertTrue(fs.exists(hbckLockPath));
  }
}

