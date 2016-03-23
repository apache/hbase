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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Test open and close of regions using zk.
 */
@Category(MediumTests.class)
public class TestZKBasedOpenCloseRegion {
  private static final Log LOG = LogFactory.getLog(TestZKBasedOpenCloseRegion.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLENAME =
      TableName.valueOf("TestZKBasedOpenCloseRegion");
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
    Bytes.toBytes("b"), Bytes.toBytes("c")};
  private static int countOfRegions;

  @BeforeClass public static void beforeAllTests() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setBoolean("hbase.assignment.usezk", true);
    c.setBoolean("dfs.support.append", true);
    c.setInt("hbase.regionserver.info.port", 0);
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.createMultiRegionTable(TABLENAME, FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    countOfRegions = -1;
    try (RegionLocator r = t.getRegionLocator()) {
      countOfRegions = r.getStartKeys().length;
    }
    waitUntilAllRegionsAssigned();
    addToEachStartKey(countOfRegions);
    t.close();
    TEST_UTIL.getHBaseCluster().getMaster().assignmentManager.initializeHandlerTrackers();
  }

  @AfterClass public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    if (TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      LOG.info("Started new server=" +
        TEST_UTIL.getHBaseCluster().startRegionServer());

    }
    waitUntilAllRegionsAssigned();
    waitOnRIT();
  }

  /**
   * Test we reopen a region once closed.
   * @throws Exception
   */
  @Test (timeout=300000) public void testReOpenRegion()
  throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Number of region servers = " +
      cluster.getLiveRegionServerThreads().size());

    int rsIdx = 0;
    HRegionServer regionServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(rsIdx);
    HRegionInfo hri = getNonMetaRegion(
      ProtobufUtil.getOnlineRegions(regionServer.getRSRpcServices()));
    LOG.debug("Asking RS to close region " + hri.getRegionNameAsString());

    LOG.info("Unassign " + hri.getRegionNameAsString());
    cluster.getMaster().assignmentManager.unassign(hri);

    while (!cluster.getMaster().assignmentManager.wasClosedHandlerCalled(hri)) {
      Threads.sleep(100);
    }

    while (!cluster.getMaster().assignmentManager.wasOpenedHandlerCalled(hri)) {
      Threads.sleep(100);
    }

    LOG.info("Done with testReOpenRegion");
  }

  private HRegionInfo getNonMetaRegion(final Collection<HRegionInfo> regions) {
    HRegionInfo hri = null;
    for (HRegionInfo i: regions) {
      LOG.info(i.getRegionNameAsString());
      if (!i.isMetaRegion()) {
        hri = i;
        break;
      }
    }
    return hri;
  }

  /**
   * This test shows how a region won't be able to be assigned to a RS
   * if it's already "processing" it.
   * @throws Exception
   */
  @Test
  public void testRSAlreadyProcessingRegion() throws Exception {
    LOG.info("starting testRSAlreadyProcessingRegion");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    HRegionServer hr0 =
        cluster.getLiveRegionServerThreads().get(0).getRegionServer();
    HRegionServer hr1 =
        cluster.getLiveRegionServerThreads().get(1).getRegionServer();
    HRegionInfo hri = getNonMetaRegion(ProtobufUtil.getOnlineRegions(hr0.getRSRpcServices()));

    // fake that hr1 is processing the region
    hr1.getRegionsInTransitionInRS().putIfAbsent(hri.getEncodedNameAsBytes(), true);

    // now ask the master to move the region to hr1, will fail
    TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(hr1.getServerName().toString()));

    // make sure the region came back
    assertEquals(hr1.getOnlineRegion(hri.getEncodedNameAsBytes()), null);

    // remove the block and reset the boolean
    hr1.getRegionsInTransitionInRS().remove(hri.getEncodedNameAsBytes());

    // now try moving a region when there is no region in transition.
    hri = getNonMetaRegion(ProtobufUtil.getOnlineRegions(hr1.getRSRpcServices()));

    TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(hr0.getServerName().toString()));

    while (!cluster.getMaster().assignmentManager.wasOpenedHandlerCalled(hri)) {
      Threads.sleep(100);
    }

    // make sure the region has moved from the original RS
    assertTrue(hr1.getOnlineRegion(hri.getEncodedNameAsBytes()) == null);

  }

  private void waitOnRIT() {
    // Close worked but we are going to open the region elsewhere.  Before going on, make sure
    // this completes.
    while (TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().
        getRegionStates().isRegionsInTransition()) {
      LOG.info("Waiting on regions in transition: " +
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().
          getRegionStates().getRegionsInTransition());
      Threads.sleep(10);
    }
  }

  /**
   * If region open fails with IOException in openRegion() while doing tableDescriptors.get()
   * the region should not add into regionsInTransitionInRS map
   * @throws Exception
   */
  @Test
  public void testRegionOpenFailsDueToIOException() throws Exception {
    HRegionInfo REGIONINFO = new HRegionInfo(TableName.valueOf("t"),
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    TableDescriptors htd = Mockito.mock(TableDescriptors.class);
    Object orizinalState = Whitebox.getInternalState(regionServer,"tableDescriptors");
    Whitebox.setInternalState(regionServer, "tableDescriptors", htd);
    Mockito.doThrow(new IOException()).when(htd).get((TableName) Mockito.any());
    try {
      ProtobufUtil.openRegion(null, regionServer.getRSRpcServices(),
        regionServer.getServerName(), REGIONINFO);
      fail("It should throw IOException ");
    } catch (IOException e) {
    }
    Whitebox.setInternalState(regionServer, "tableDescriptors", orizinalState);
    assertFalse("Region should not be in RIT",
        regionServer.getRegionsInTransitionInRS().containsKey(REGIONINFO.getEncodedNameAsBytes()));
  }

  private static void waitUntilAllRegionsAssigned()
  throws IOException {
    HTable meta = new HTable(TEST_UTIL.getConfiguration(), TableName.META_TABLE_NAME);
    while (true) {
      int rows = 0;
      Scan scan = new Scan();
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      ResultScanner s = meta.getScanner(scan);
      for (Result r = null; (r = s.next()) != null;) {
        byte [] b =
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        if (b == null || b.length <= 0) {
          break;
        }
        rows++;
      }
      s.close();
      // If I get to here and all rows have a Server, then all have been assigned.
      if (rows >= countOfRegions) {
        break;
      }
      LOG.info("Found=" + rows);
      Threads.sleep(1000);
    }
    meta.close();
  }

  /*
   * Add to each of the regions in hbase:meta a value.  Key is the startrow of the
   * region (except its 'aaa' for first region).  Actual value is the row name.
   * @param expected
   * @return
   * @throws IOException
   */
  private static int addToEachStartKey(final int expected) throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
        TableName.META_TABLE_NAME);
    int rows = 0;
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      HRegionInfo hri = HRegionInfo.getHRegionInfo(r);
      if (hri == null) break;
      if(!hri.getTable().equals(TABLENAME)) {
        continue;
      }
      // If start key, add 'aaa'.
      byte [] row = getStartKey(hri);
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      p.add(getTestFamily(), getTestQualifier(), row);
      t.put(p);
      rows++;
    }
    s.close();
    Assert.assertEquals(expected, rows);
    t.close();
    meta.close();
    return rows;
  }

  private static byte [] getStartKey(final HRegionInfo hri) {
    return Bytes.equals(HConstants.EMPTY_START_ROW, hri.getStartKey())?
        Bytes.toBytes("aaa"): hri.getStartKey();
  }

  private static byte [] getTestFamily() {
    return FAMILIES[0];
  }

  private static byte [] getTestQualifier() {
    return getTestFamily();
  }
}

