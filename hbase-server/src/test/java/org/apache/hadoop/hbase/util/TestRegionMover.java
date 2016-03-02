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

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.RegionMover.RegionMoverBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for Region Mover Load/Unload functionality with and without ack mode and also to test
 * exclude functionality useful for rack decommissioning
 */
@Category(MediumTests.class)
public class TestRegionMover {

  final Log LOG = LogFactory.getLog(getClass());
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    // Create a pre-split table just to populate some regions
    TableName tableName = TableName.valueOf("testRegionMover");
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    try {
      admin.setBalancerRunning(false, true);
      String startKey = "a";
      String endKey = "z";
      admin.createTable(tableDesc, startKey.getBytes(), endKey.getBytes(), 9);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  @Test
  public void testLoadWithAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    int noRegions = regionServer.getNumberOfOnlineRegions();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs).ack(true).maxthreads(8);
    RegionMover rm = rmBuilder.build();
    rm.setConf(TEST_UTIL.getConfiguration());
    LOG.info("Unloading " + rs);
    rm.unload();
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    LOG.info("Successfully Unloaded\nNow Loading");
    rm.load();
    assertEquals(noRegions, regionServer.getNumberOfOnlineRegions());
  }

  /** Test to unload a regionserver first and then load it using no Ack mode
   * we check if some regions are loaded on the region server(since no ack is best effort)
   * @throws Exception
   */
  @Test
  public void testLoadWithoutAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    int noRegions = regionServer.getNumberOfOnlineRegions();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs).ack(true);
    RegionMover rm = rmBuilder.build();
    rm.setConf(TEST_UTIL.getConfiguration());
    LOG.info("Unloading " + rs);
    rm.unload();
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    LOG.info("Successfully Unloaded\nNow Loading");
    rm = rmBuilder.ack(false).build();
    rm.setConf(TEST_UTIL.getConfiguration());
    rm.load();
    TEST_UTIL.waitFor(5000, 500, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return regionServer.getNumberOfOnlineRegions() > 0;
      }
    });
  }

  @Test
  public void testUnloadWithoutAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HRegionServer regionServer = cluster.getRegionServer(0);
    final int noRegions = regionServer.getNumberOfOnlineRegions();
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs).ack(false);
    RegionMover rm = rmBuilder.build();
    rm.setConf(TEST_UTIL.getConfiguration());
    LOG.info("Unloading " + rs);
    rm.unload();
    TEST_UTIL.waitFor(5000, 500, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return regionServer.getNumberOfOnlineRegions() < noRegions;
      }
    });
  }

  @Test
  public void testUnloadWithAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs).ack(true);
    RegionMover rm = rmBuilder.build();
    rm.setConf(TEST_UTIL.getConfiguration());
    rm.unload();
    LOG.info("Unloading " + rs);
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
  }

  /**
   * To test that we successfully exclude a server from the unloading process We test for the number
   * of regions on Excluded server and also test that regions are unloaded successfully
   * @throws Exception
   */
  @Test
  public void testExclude() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    File excludeFile = new File(TEST_UTIL.getDataTestDir().toUri().getPath(), "exclude_file");
    FileWriter fos = new FileWriter(excludeFile);
    HRegionServer excludeServer = cluster.getRegionServer(1);
    String excludeHostname = excludeServer.getServerName().getHostname();
    int excludeServerPort = excludeServer.getServerName().getPort();
    int regionsExcludeServer = excludeServer.getNumberOfOnlineRegions();
    String excludeServerName = excludeHostname + ":" + Integer.toString(excludeServerPort);
    fos.write(excludeServerName);
    fos.close();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder =
        new RegionMoverBuilder(rs).ack(true).excludeFile(excludeFile.getCanonicalPath());
    RegionMover rm = rmBuilder.build();
    rm.setConf(TEST_UTIL.getConfiguration());
    rm.unload();
    LOG.info("Unloading " + rs);
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    assertEquals(regionsExcludeServer, cluster.getRegionServer(1).getNumberOfOnlineRegions());
    LOG.info("Before:" + regionsExcludeServer + " After:"
        + cluster.getRegionServer(1).getNumberOfOnlineRegions());
  }
}
