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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests for Region Mover Load/Unload functionality with and without ack mode and also to test
 * exclude functionality useful for rack decommissioning
 */
@Category(MediumTests.class)
public class TestRegionMover {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMover.class);

  final Logger LOG = LoggerFactory.getLogger(getClass());
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
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    tableDesc.addFamily(fam1);

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
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .ack(true).maxthreads(8);
    RegionMover rm = rmBuilder.build();
    LOG.info("Unloading " + rs);
    rm.unload();
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    LOG.info("Successfully Unloaded\nNow Loading");
    rm.load();
    assertEquals(noRegions, regionServer.getNumberOfOnlineRegions());
  }

  /** Test to unload a regionserver first and then load it using no Ack mode
   * we check if some regions are loaded on the region server(since no ack is best effort)
   */
  @Test
  public void testLoadWithoutAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    int noRegions = regionServer.getNumberOfOnlineRegions();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .ack(true);
    RegionMover rm = rmBuilder.build();
    LOG.info("Unloading " + rs);
    rm.unload();
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    LOG.info("Successfully Unloaded\nNow Loading");
    rm = rmBuilder.ack(false).build();
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
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .ack(false);
    RegionMover rm = rmBuilder.build();
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
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .ack(true);
    RegionMover rm = rmBuilder.build();
    rm.unload();
    LOG.info("Unloading " + rs);
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
  }

  /**
   * Test that loading the same region set doesn't cause timeout loop during meta load.
   */
  @Test
  public void testRepeatedLoad() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String rs = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .ack(true);
    RegionMover rm = rmBuilder.build();
    rm.unload();
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration()).ack(true);
    rm = rmBuilder.build();
    rm.load();
    rm.load(); //Repeat the same load. It should be very fast because all regions are already moved.
  }

  /**
   * To test that we successfully exclude a server from the unloading process We test for the number
   * of regions on Excluded server and also test that regions are unloaded successfully
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
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .ack(true).excludeFile(excludeFile.getCanonicalPath());
    RegionMover rm = rmBuilder.build();
    rm.unload();
    LOG.info("Unloading " + rs);
    assertEquals(0, regionServer.getNumberOfOnlineRegions());
    assertEquals(regionsExcludeServer, cluster.getRegionServer(1).getNumberOfOnlineRegions());
    LOG.info("Before:" + regionsExcludeServer + " After:"
        + cluster.getRegionServer(1).getNumberOfOnlineRegions());
  }

  @Test
  public void testRegionServerPort() {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();

    final int PORT = 16021;
    Configuration conf = TEST_UTIL.getConfiguration();
    String originalPort = conf.get(HConstants.REGIONSERVER_PORT);
    conf.set(HConstants.REGIONSERVER_PORT, Integer.toString(PORT));
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rsName, conf);
    assertEquals(PORT, rmBuilder.port);
    if (originalPort != null) {
      conf.set(HConstants.REGIONSERVER_PORT, originalPort);
    }
  }
}
