/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRegionServerShutdown {

  private static final Log LOG = LogFactory.getLog(TestRegionServerShutdown.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "testRegionServerShutdown";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
      Bytes.toBytes("b"), Bytes.toBytes("c")};

  @BeforeClass public static void beforeAllTests() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setBoolean("dfs.support.append", true);
    c.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);
    c.setInt("hbase.master.meta.thread.rescanfrequency", 5*1000);
    TEST_UTIL.startMiniCluster(2);

    int countOfRegions = 25;
    TEST_UTIL.createTable(Bytes.toBytes(TABLENAME), FAMILIES, 3,
        Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), countOfRegions);

    addToEachStartKey(countOfRegions);
  }

  @AfterClass public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    if (TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      LOG.info("Started new server=" +
          TEST_UTIL.getHBaseCluster().startRegionServer());

    }
  }

  @Test(timeout=300000)
  public void testOpenRegionOnStoppingServer()
      throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Number of region servers = " + cluster.getLiveRegionServerThreads().size());

    HRegionServer regionServer0 = cluster.getRegionServer(0);
    HRegionServer regionServer1 = cluster.getRegionServer(1);

    Collection<HRegion> regions = regionServer1.getOnlineRegions();
    LOG.debug("RS " + regionServer1.getServerInfo().getHostnamePort() + " has "
        + regions.size() + " online regions");
    HRegion region = regions.iterator().next();
    LOG.debug("Asking RS " + regionServer1.getServerInfo().getHostnamePort()
        + " to close region " + region.getRegionNameAsString());
    regionServer1.closeRegion(region.getRegionInfo(), true);
    Assert.assertNull(regionServer1.getOnlineRegion(region.getRegionName()));

    // Let master assign the region to the RS0, otherwise the region has a chance
    // to be assign it back to RS1 and void this test.
    final HMaster master = cluster.getMaster();
    master.getRegionManager().getAssignmentManager().updateAssignmentPlan(
        region.getRegionInfo(), Arrays.asList(regionServer0.getServerInfo().getServerAddress()), 0);

    // Cause artificial delay to region opening to simulate production environment.
    HRegionServer.openRegionDelay = 3000;
    // Sleep for 1000 seconds and assume the closed region is opening on RS0.
    Thread.sleep(1000);
    HRegionServer.openRegionDelay = 0;

    // Stop RS0 when it's opening region.
    regionServer0.stop("test");

    // Wait long enough for RS0 to shut down cleanly.
    Thread.sleep(5000);
    LOG.debug("RS " + regionServer0.getServerInfo().getHostnamePort() + " has "
        + regionServer0.getOnlineRegions().size() + " online regions");
    Assert.assertEquals("No open region should exist", 0, regionServer0
        .getOnlineRegions().size());
  }

  /*
   * Add to each of the regions in .META. a value.  Key is the startrow of the
   * region (except its 'aaa' for first region).  Actual value is the row name.
   * @param expected
   * @return
   * @throws IOException
   */
  private static int addToEachStartKey(final int expected) throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);
    int rows = 0;
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      byte [] b =
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      if (b == null || b.length <= 0) break;
      HRegionInfo hri = Writables.getHRegionInfo(b);
      // If start key, add 'aaa'.
      byte [] row = getStartKey(hri);
      Put p = new Put(row);
      p.add(getTestFamily(), getTestQualifier(), row);
      t.put(p);
      rows++;
    }
    s.close();
    Assert.assertEquals(expected, rows);
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
