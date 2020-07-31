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
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.RegionMover.RegionMoverBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for Region Mover Load/Unload functionality with and without ack mode and also to test
 * exclude functionality useful for rack decommissioning
 */
@Category({MiscTests.class, LargeTests.class})
public class TestRegionMover1 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMover1.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMover1.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam1")).build();
    String startKey = "a";
    String endKey = "z";
    TEST_UTIL.getAdmin().createTable(tableDesc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), 9);
  }

  @After
  public void tearDown() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.getAdmin().disableTable(tableName);
    TEST_UTIL.getAdmin().deleteTable(tableName);
  }

  @Test
  public void testWithAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    int numRegions = regionServer.getNumberOfOnlineRegions();
    RegionMoverBuilder rmBuilder =
      new RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true).maxthreads(8);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.info("Unloading " + regionServer.getServerName());
      rm.unload();
      assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.info("Successfully Unloaded\nNow Loading");
      rm.load();
      assertEquals(numRegions, regionServer.getNumberOfOnlineRegions());
      // Repeat the same load. It should be very fast because all regions are already moved.
      rm.load();
    }
  }

  /**
   * Test to unload a regionserver first and then load it using no Ack mode.
   */
  @Test
  public void testWithoutAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    int numRegions = regionServer.getNumberOfOnlineRegions();
    RegionMoverBuilder rmBuilder =
      new RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(false);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.info("Unloading " + regionServer.getServerName());
      rm.unload();
      TEST_UTIL.waitFor(30000, 1000, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return regionServer.getNumberOfOnlineRegions() == 0;
        }
      });
      LOG.info("Successfully Unloaded\nNow Loading");
      rm.load();
      // In UT we only have 10 regions so it is not likely to fail, so here we check for all
      // regions, in the real production this may not be true.
      TEST_UTIL.waitFor(30000, 1000, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return regionServer.getNumberOfOnlineRegions() == numRegions;
        }
      });
    }
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
    try (RegionMover rm = rmBuilder.build()) {
      rm.unload();
      LOG.info("Unloading " + rs);
      assertEquals(0, regionServer.getNumberOfOnlineRegions());
      assertEquals(regionsExcludeServer, cluster.getRegionServer(1).getNumberOfOnlineRegions());
      LOG.info("Before:" + regionsExcludeServer + " After:" +
        cluster.getRegionServer(1).getNumberOfOnlineRegions());
    }
  }

  @Test
  public void testDesignatedFile() throws Exception{
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    File designatedFile = new File(TEST_UTIL.getDataTestDir().toUri().getPath(),
      "designated_file");
    HRegionServer designatedServer = cluster.getRegionServer(0);
    try(FileWriter fos = new FileWriter(designatedFile)) {
      String designatedHostname = designatedServer.getServerName().getHostname();
      int designatedServerPort = designatedServer.getServerName().getPort();
      String excludeServerName = designatedHostname + ":" + designatedServerPort;
      fos.write(excludeServerName);
    }
    int regionsInDesignatedServer = designatedServer.getNumberOfOnlineRegions();
    HRegionServer regionServer = cluster.getRegionServer(1);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String rs = rsName + ":" + port;
    int regionsInRegionServer = regionServer.getNumberOfOnlineRegions();
    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .designatedFile(designatedFile.getCanonicalPath());
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {} regions", rs);
      rm.unload();
      assertEquals(0, regionServer.getNumberOfOnlineRegions());
      assertEquals(regionsInDesignatedServer + regionsInRegionServer,
        designatedServer.getNumberOfOnlineRegions());
      LOG.debug("Before:{} After:{}", regionsInDesignatedServer,
        designatedServer.getNumberOfOnlineRegions());
    }
  }

  @Test
  public void testExcludeAndDesignated() throws Exception{
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    // create designated file
    File designatedFile = new File(TEST_UTIL.getDataTestDir().toUri().getPath(),
      "designated_file");
    HRegionServer designatedServer = cluster.getRegionServer(0);
    try(FileWriter fos = new FileWriter(designatedFile)) {
      String designatedHostname = designatedServer.getServerName().getHostname();
      int designatedServerPort = designatedServer.getServerName().getPort();
      String excludeServerName = designatedHostname + ":" + designatedServerPort;
      fos.write(excludeServerName);
    }
    int regionsInDesignatedServer = designatedServer.getNumberOfOnlineRegions();
    // create exclude file
    File excludeFile = new File(TEST_UTIL.getDataTestDir().toUri().getPath(), "exclude_file");
    HRegionServer excludeServer = cluster.getRegionServer(1);
    try(FileWriter fos = new FileWriter(excludeFile)) {
      String excludeHostname = excludeServer.getServerName().getHostname();
      int excludeServerPort = excludeServer.getServerName().getPort();
      String excludeServerName = excludeHostname + ":" + excludeServerPort;
      fos.write(excludeServerName);
    }
    int regionsInExcludeServer = excludeServer.getNumberOfOnlineRegions();

    HRegionServer targetRegionServer = cluster.getRegionServer(2);
    String rsName = targetRegionServer.getServerName().getHostname();
    int port = targetRegionServer.getServerName().getPort();
    String rs = rsName + ":" + port;
    int regionsInTargetRegionServer = targetRegionServer.getNumberOfOnlineRegions();

    RegionMoverBuilder rmBuilder = new RegionMoverBuilder(rs, TEST_UTIL.getConfiguration())
      .designatedFile(designatedFile.getCanonicalPath())
      .excludeFile(excludeFile.getCanonicalPath());
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", rs);
      rm.unload();
      assertEquals(0, targetRegionServer.getNumberOfOnlineRegions());
      assertEquals(regionsInDesignatedServer + regionsInTargetRegionServer,
        designatedServer.getNumberOfOnlineRegions());
      LOG.debug("DesignatedServer Before:{} After:{}", regionsInDesignatedServer,
        designatedServer.getNumberOfOnlineRegions());
      assertEquals(regionsInExcludeServer, excludeServer.getNumberOfOnlineRegions());
      LOG.debug("ExcludeServer Before:{} After:{}", regionsInExcludeServer,
        excludeServer.getNumberOfOnlineRegions());
    }
  }

  @Test
  public void testRegionServerPort() throws Exception {
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

  /**
   * UT for HBASE-21746
   */
  @Test
  public void testLoadMetaRegion() throws Exception {
    HRegionServer rsWithMeta = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer())
      .filter(rs -> rs.getRegions(TableName.META_TABLE_NAME).size() > 0).findFirst().get();
    int onlineRegions = rsWithMeta.getNumberOfOnlineRegions();
    String rsName = rsWithMeta.getServerName().getAddress().toString();
    try (RegionMover rm =
      new RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true).build()) {
      LOG.info("Unloading " + rsWithMeta.getServerName());
      rm.unload();
      assertEquals(0, rsWithMeta.getNumberOfOnlineRegions());
      LOG.info("Loading " + rsWithMeta.getServerName());
      rm.load();
      assertEquals(onlineRegions, rsWithMeta.getNumberOfOnlineRegions());
    }
  }

  /**
   * UT for HBASE-21746
   */
  @Test
  public void testTargetServerDeadWhenLoading() throws Exception {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    String rsName = rs.getServerName().getAddress().toString();
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    // wait 5 seconds at most
    conf.setInt(RegionMover.SERVERSTART_WAIT_MAX_KEY, 5);
    String filename =
      new Path(TEST_UTIL.getDataTestDir(), "testTargetServerDeadWhenLoading").toString();
    // unload the region server
    try (RegionMover rm =
      new RegionMoverBuilder(rsName, conf).filename(filename).ack(true).build()) {
      LOG.info("Unloading " + rs.getServerName());
      rm.unload();
      assertEquals(0, rs.getNumberOfOnlineRegions());
    }
    String inexistRsName = "whatever:123";
    try (RegionMover rm =
      new RegionMoverBuilder(inexistRsName, conf).filename(filename).ack(true).build()) {
      // load the regions to an inexist region server, which should fail and return false
      LOG.info("Loading to an inexist region server {}", inexistRsName);
      assertFalse(rm.load());
    }
  }

  @Test
  public void testDecomServerExclusionWithAck() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer excludeServer = cluster.getRegionServer(1);
    List<HRegion> regions = excludeServer.getRegions();
    int regionsExcludeServer = excludeServer.getNumberOfOnlineRegions();
    TEST_UTIL.getAdmin().decommissionRegionServers(
      Collections.singletonList(excludeServer.getServerName()), false);

    waitForServerDecom(excludeServer);

    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String hostname = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder =
      new RegionMoverBuilder(hostname, TEST_UTIL.getConfiguration())
        .ack(true);

    int targetServerRegions = cluster.getRegionServer(2).getRegions().size();
    int sourceServerRegions = regionServer.getRegions().size();

    try (RegionMover regionMover = rmBuilder.build()) {
      Assert.assertTrue(regionMover.unload());
      LOG.info("Unloading {}", hostname);
      assertEquals(0, regionServer.getNumberOfOnlineRegions());
      assertEquals(regionsExcludeServer, cluster.getRegionServer(1).getNumberOfOnlineRegions());
      LOG.info("Before:" + regionsExcludeServer + " After:" +
        cluster.getRegionServer(1).getNumberOfOnlineRegions());
      List<HRegion> regionList = cluster.getRegionServer(1).getRegions();
      int index = 0;
      for (HRegion hRegion : regionList) {
        Assert.assertEquals(hRegion, regions.get(index++));
      }
      Assert.assertEquals(targetServerRegions + sourceServerRegions,
        cluster.getRegionServer(2).getNumberOfOnlineRegions());
      Assert.assertTrue(regionMover.load());
    }

    TEST_UTIL.getAdmin().recommissionRegionServer(excludeServer.getServerName(),
      Collections.emptyList());
  }

  private void waitForServerDecom(HRegionServer excludeServer) {

    TEST_UTIL.waitFor(3000, () -> {
      try {
        List<ServerName> decomServers = TEST_UTIL.getAdmin().listDecommissionedRegionServers();
        return decomServers.size() == 1
          && decomServers.get(0).equals(excludeServer.getServerName());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testDecomServerExclusion() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HRegionServer excludeServer = cluster.getRegionServer(0);
    List<HRegion> regions = excludeServer.getRegions();
    int regionsExcludeServer = excludeServer.getNumberOfOnlineRegions();
    TEST_UTIL.getAdmin().decommissionRegionServers(
      Collections.singletonList(excludeServer.getServerName()), false);

    waitForServerDecom(excludeServer);

    HRegionServer sourceRegionServer = cluster.getRegionServer(1);
    String rsName = sourceRegionServer.getServerName().getHostname();
    int port = sourceRegionServer.getServerName().getPort();
    String hostname = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder =
      new RegionMoverBuilder(hostname, TEST_UTIL.getConfiguration()).ack(false);

    int targetServerRegions = cluster.getRegionServer(2).getRegions().size();
    int sourceServerRegions = sourceRegionServer.getRegions().size();

    try (RegionMover regionMover = rmBuilder.build()) {
      Assert.assertTrue(regionMover.unload());
      LOG.info("Unloading {}", hostname);
      assertEquals(0, sourceRegionServer.getNumberOfOnlineRegions());
      assertEquals(regionsExcludeServer, cluster.getRegionServer(0).getNumberOfOnlineRegions());
      LOG.info("Before:" + regionsExcludeServer + " After:" +
        cluster.getRegionServer(1).getNumberOfOnlineRegions());
      List<HRegion> regionList = cluster.getRegionServer(0).getRegions();
      int index = 0;
      for (HRegion hRegion : regionList) {
        Assert.assertEquals(hRegion, regions.get(index++));
      }
      Assert.assertEquals(targetServerRegions + sourceServerRegions,
        cluster.getRegionServer(2).getNumberOfOnlineRegions());
      Assert.assertTrue(regionMover.load());
    }

    TEST_UTIL.getAdmin().recommissionRegionServer(excludeServer.getServerName(),
      Collections.emptyList());
  }

  @Test
  public void testExcludeAndDecomServers() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    File excludeFile = new File(TEST_UTIL.getDataTestDir().toUri().getPath(), "exclude_file");
    FileWriter fos = new FileWriter(excludeFile);
    HRegionServer excludeServer = cluster.getRegionServer(1);
    String excludeHostname = excludeServer.getServerName().getHostname();
    int excludeServerPort = excludeServer.getServerName().getPort();
    String excludeServerName = excludeHostname + ":" + Integer.toString(excludeServerPort);
    fos.write(excludeServerName);
    fos.close();

    HRegionServer decomServer = cluster.getRegionServer(2);
    TEST_UTIL.getAdmin().decommissionRegionServers(
      Collections.singletonList(decomServer.getServerName()), false);

    waitForServerDecom(decomServer);

    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getHostname();
    int port = regionServer.getServerName().getPort();
    String sourceServer = rsName + ":" + Integer.toString(port);
    RegionMoverBuilder rmBuilder =
      new RegionMoverBuilder(sourceServer, TEST_UTIL.getConfiguration())
        .ack(true)
        .excludeFile(excludeFile.getCanonicalPath());
    try (RegionMover regionMover = rmBuilder.build()) {
      Assert.assertFalse(regionMover.unload());
    }

    TEST_UTIL.getAdmin().recommissionRegionServer(decomServer.getServerName(),
      Collections.emptyList());
  }

}
