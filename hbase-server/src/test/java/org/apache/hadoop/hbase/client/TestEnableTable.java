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
package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ MasterTests.class, MediumTests.class })
public class TestEnableTable {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestEnableTable.class);
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      MasterSyncObserver.class.getName());
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 300000)
  public void testEnableTableWithNoRegionServers() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final Admin admin = TEST_UTIL.getAdmin();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    admin.createTable(desc);
    admin.disableTable(tableName);
    TEST_UTIL.waitTableDisabled(tableName.getName());

    admin.enableTable(tableName);
    TEST_UTIL.waitTableEnabled(tableName);
    // disable once more
    admin.disableTable(tableName);

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    // now stop region servers
    JVMClusterUtil.RegionServerThread rs = cluster.getRegionServerThreads().get(0);
    rs.getRegionServer().stop("stop");
    cluster.waitForRegionServerToStop(rs.getRegionServer().getServerName(), 10000);

    LOG.debug("Now enabling table " + tableName);

    admin.enableTable(tableName);
    assertTrue(admin.isTableEnabled(tableName));

    JVMClusterUtil.RegionServerThread rs2 = cluster.startRegionServer();
    cluster.waitForRegionServerToStart(rs2.getRegionServer().getServerName().getHostname(),
        rs2.getRegionServer().getServerName().getPort(), 60000);

    List<HRegionInfo> regions = TEST_UTIL.getAdmin().getTableRegions(tableName);
    assertEquals(1, regions.size());
    for (HRegionInfo region : regions) {
      TEST_UTIL.getAdmin().assign(region.getEncodedNameAsBytes());
    }
    LOG.debug("Waiting for table assigned " + tableName);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    List<HRegionInfo> onlineRegions = admin.getOnlineRegions(
        rs2.getRegionServer().getServerName());
    ArrayList<HRegionInfo> tableRegions = filterTableRegions(tableName, onlineRegions);
    assertEquals(1, tableRegions.size());
  }

  private ArrayList<HRegionInfo> filterTableRegions(final TableName tableName,
      List<HRegionInfo> onlineRegions) {
    return Lists.newArrayList(Iterables.filter(onlineRegions, new Predicate<HRegionInfo>() {
      @Override
      public boolean apply(HRegionInfo input) {
        return input.getTable().equals(tableName);
      }
    }));
  }

  /**
   * We were only clearing rows that had a hregioninfo column in hbase:meta.  Mangled rows that
   * were missing the hregioninfo because of error were being left behind messing up any
   * subsequent table made with the same name. HBASE-12980
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(timeout=60000)
  public void testDeleteForSureClearsAllTableRowsFromMeta()
  throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final Admin admin = TEST_UTIL.getAdmin();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    try {
      createTable(TEST_UTIL, desc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Got an exception while creating " + tableName);
    }
    // Now I have a nice table, mangle it by removing the HConstants.REGIONINFO_QUALIFIER_STR
    // content from a few of the rows.
    try (Table metaTable = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      try (ResultScanner scanner = metaTable.getScanner(
        MetaTableAccessor.getScanForTableName(TEST_UTIL.getConnection(), tableName))) {
        for (Result result : scanner) {
          // Just delete one row.
          Delete d = new Delete(result.getRow());
          d.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
          LOG.info("Mangled: " + d);
          metaTable.delete(d);
          break;
        }
      }
      admin.disableTable(tableName);
      TEST_UTIL.waitTableDisabled(tableName.getName());
      // Rely on the coprocessor based latch to make the operation synchronous.
      try {
        deleteTable(TEST_UTIL, tableName);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Got an exception while deleting " + tableName);
      }
      int rowCount = 0;
      try (ResultScanner scanner =
          metaTable.getScanner(MetaTableAccessor.getScanForTableName(TEST_UTIL.getConnection(), tableName))) {
        for (Result result : scanner) {
          LOG.info("Found when none expected: " + result);
          rowCount++;
        }
      }
      assertEquals(0, rowCount);
    }
  }

  public  static class MasterSyncObserver implements MasterObserver {
    volatile CountDownLatch tableCreationLatch = null;
    volatile CountDownLatch tableDeletionLatch = null;

    @Override
    public void postCompletedCreateTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final HTableDescriptor desc,
        final HRegionInfo[] regions) throws IOException {
      // the AccessController test, some times calls only and directly the
      // postCompletedCreateTableAction()
      if (tableCreationLatch != null) {
        tableCreationLatch.countDown();
      }
    }

    @Override
    public void postCompletedDeleteTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableName tableName)
    throws IOException {
      // the AccessController test, some times calls only and directly the postDeleteTableHandler()
      if (tableDeletionLatch != null) {
        tableDeletionLatch.countDown();
      }
    }
  }

  public static void createTable(HBaseTestingUtility testUtil,
    HTableDescriptor htd, byte [][] splitKeys)
  throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver)testUtil.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    observer.tableCreationLatch = new CountDownLatch(1);
    Admin admin = testUtil.getAdmin();
    if (splitKeys != null) {
      admin.createTable(htd, splitKeys);
    } else {
      admin.createTable(htd);
    }
    observer.tableCreationLatch.await();
    observer.tableCreationLatch = null;
    testUtil.waitUntilAllRegionsAssigned(htd.getTableName());
  }

  public static void deleteTable(HBaseTestingUtility testUtil, TableName tableName)
  throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver)testUtil.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    observer.tableDeletionLatch = new CountDownLatch(1);
    Admin admin = testUtil.getAdmin();
    try {
      admin.disableTable(tableName);
    } catch (Exception e) {
      LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
    }
    admin.deleteTable(tableName);
    observer.tableDeletionLatch.await();
    observer.tableDeletionLatch = null;
  }
}
