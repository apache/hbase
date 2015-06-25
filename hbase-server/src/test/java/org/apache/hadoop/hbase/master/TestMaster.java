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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Joiner;

@Category({MasterTests.class, MediumTests.class})
public class TestMaster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestMaster.class);
  private static final TableName TABLENAME =
      TableName.valueOf("TestMaster");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static Admin admin;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // we will retry operations when PleaseHoldException is thrown
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMasterOpsWhileSplitting() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();

    try (Table ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME)) {
      assertTrue(m.assignmentManager.getTableStateManager().isTableState(TABLENAME,
        TableState.State.ENABLED));
      TEST_UTIL.loadTable(ht, FAMILYNAME, false);
    }

    List<Pair<HRegionInfo, ServerName>> tableRegions = MetaTableAccessor.getTableRegionsAndLocations(
        m.getConnection(), TABLENAME);
    LOG.info("Regions after load: " + Joiner.on(',').join(tableRegions));
    assertEquals(1, tableRegions.size());
    assertArrayEquals(HConstants.EMPTY_START_ROW,
        tableRegions.get(0).getFirst().getStartKey());
    assertArrayEquals(HConstants.EMPTY_END_ROW,
        tableRegions.get(0).getFirst().getEndKey());

    // Now trigger a split and stop when the split is in progress
    LOG.info("Splitting table");
    TEST_UTIL.getHBaseAdmin().split(TABLENAME);
    LOG.info("Waiting for split result to be about to open");
    RegionStates regionStates = m.assignmentManager.getRegionStates();
    while (regionStates.getRegionsOfTable(TABLENAME).size() <= 1) {
      Thread.sleep(100);
    }
    LOG.info("Making sure we can call getTableRegions while opening");
    tableRegions = MetaTableAccessor.getTableRegionsAndLocations(m.getConnection(),
      TABLENAME, false);

    LOG.info("Regions: " + Joiner.on(',').join(tableRegions));
    // We have three regions because one is split-in-progress
    assertEquals(3, tableRegions.size());
    LOG.info("Making sure we can call getTableRegionClosest while opening");
    Pair<HRegionInfo, ServerName> pair =
        m.getTableRegionForRow(TABLENAME, Bytes.toBytes("cde"));
    LOG.info("Result is: " + pair);
    Pair<HRegionInfo, ServerName> tableRegionFromName =
        MetaTableAccessor.getRegion(m.getConnection(),
          pair.getFirst().getRegionName());
    assertEquals(tableRegionFromName.getFirst(), pair.getFirst());
  }

  @Test
  public void testMoveRegionWhenNotInitialized() {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    try {
      m.initialized = false; // fake it, set back later
      HRegionInfo meta = HRegionInfo.FIRST_META_REGIONINFO;
      m.move(meta.getEncodedNameAsBytes(), null);
      fail("Region should not be moved since master is not initialized");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof PleaseHoldException);
    } finally {
      m.initialized = true;
    }
  }

  @Test
  public void testMoveThrowsUnknownRegionException() throws IOException {
    TableName tableName =
        TableName.valueOf("testMoveThrowsUnknownRegionException");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null);
    try {
      HRegionInfo hri = new HRegionInfo(
        tableName, Bytes.toBytes("A"), Bytes.toBytes("Z"));
      admin.move(hri.getEncodedNameAsBytes(), null);
      fail("Region should not be moved since it is fake");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof UnknownRegionException);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMoveThrowsPleaseHoldException() throws IOException {
    TableName tableName = TableName.valueOf("testMoveThrowsPleaseHoldException");
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null);
    try {
      List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);

      master.initialized = false; // fake it, set back later
      admin.move(tableRegions.get(0).getEncodedNameAsBytes(), null);
      fail("Region should not be moved since master is not initialized");
    } catch (IOException ioe) {
      assertTrue(StringUtils.stringifyException(ioe).contains("PleaseHoldException"));
    } finally {
      master.initialized = true;
      TEST_UTIL.deleteTable(tableName);
    }
  }
}

