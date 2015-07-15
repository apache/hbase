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
package org.apache.hadoop.hbase.regionserver.handler;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MockServer;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test of the {@link OpenRegionHandler}.
 */
@Category(MediumTests.class)
public class TestOpenRegionHandler {
  static final Log LOG = LogFactory.getLog(TestOpenRegionHandler.class);
  private final static HBaseTestingUtility HTU = HBaseTestingUtility.createLocalHTU();
  private static HTableDescriptor TEST_HTD;
  private HRegionInfo TEST_HRI;

  private int testIndex = 0;

  @BeforeClass public static void before() throws Exception {
    HTU.getConfiguration().setBoolean("hbase.assignment.usezk", true);
    HTU.startMiniZKCluster();
    TEST_HTD = new HTableDescriptor(TableName.valueOf("TestOpenRegionHandler.java"));
  }

  @AfterClass public static void after() throws IOException {
    TEST_HTD = null;
    HTU.shutdownMiniZKCluster();
  }

  /**
   * Before each test, use a different HRI, so the different tests
   * don't interfere with each other. This allows us to use just
   * a single ZK cluster for the whole suite.
   */
  @Before
  public void setupHRI() {
    TEST_HRI = new HRegionInfo(TEST_HTD.getTableName(),
      Bytes.toBytes(testIndex),
      Bytes.toBytes(testIndex + 1));
    testIndex++;
  }

  /**
   * Test the openregionhandler can deal with its znode being yanked out from
   * under it.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-3627">HBASE-3627</a>
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test public void testYankingRegionFromUnderIt()
  throws IOException, NodeExistsException, KeeperException {
    final Server server = new MockServer(HTU);
    final RegionServerServices rss = HTU.createMockRegionServerService();

    HTableDescriptor htd = TEST_HTD;
    final HRegionInfo hri = TEST_HRI;
    HRegion region =
         HRegion.createHRegion(hri, HTU.getDataTestDir(), HTU
            .getConfiguration(), htd);
    assertNotNull(region);
    try {
      OpenRegionHandler handler = new OpenRegionHandler(server, rss, hri, htd) {
        HRegion openRegion() {
          // Open region first, then remove znode as though it'd been hijacked.
          HRegion region = super.openRegion();

          // Don't actually open region BUT remove the znode as though it'd
          // been hijacked on us.
          ZooKeeperWatcher zkw = this.server.getZooKeeper();
          String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
          try {
            ZKUtil.deleteNodeFailSilent(zkw, node);
          } catch (KeeperException e) {
            throw new RuntimeException("Ugh failed delete of " + node, e);
          }
          return region;
        }
      };
      rss.getRegionsInTransitionInRS().put(
        hri.getEncodedNameAsBytes(), Boolean.TRUE);
      // Call process without first creating OFFLINE region in zk, see if
      // exception or just quiet return (expected).
      handler.process();
      rss.getRegionsInTransitionInRS().put(
        hri.getEncodedNameAsBytes(), Boolean.TRUE);
      ZKAssign.createNodeOffline(server.getZooKeeper(), hri, server.getServerName());
      // Call process again but this time yank the zk znode out from under it
      // post OPENING; again will expect it to come back w/o NPE or exception.
      handler.process();
    } finally {
      HRegion.closeHRegion(region);
    }
  }
  
  /**
   * Test the openregionhandler can deal with perceived failure of transitioning to OPENED state
   * due to intermittent zookeeper malfunctioning.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-9387">HBASE-9387</a>
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test
  public void testRegionServerAbortionDueToFailureTransitioningToOpened()
      throws IOException, NodeExistsException, KeeperException {
    final Server server = new MockServer(HTU);
    final RegionServerServices rss = HTU.createMockRegionServerService();

    HTableDescriptor htd = TEST_HTD;
    final HRegionInfo hri = TEST_HRI;
    HRegion region =
         HRegion.createHRegion(hri, HTU.getDataTestDir(), HTU
            .getConfiguration(), htd);
    assertNotNull(region);
    try {
      OpenRegionHandler handler = new OpenRegionHandler(server, rss, hri, htd) {
        boolean transitionToOpened(final HRegion r) throws IOException {
          // remove znode simulating intermittent zookeeper connection issue
          ZooKeeperWatcher zkw = this.server.getZooKeeper();
          String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
          try {
            ZKUtil.deleteNodeFailSilent(zkw, node);
          } catch (KeeperException e) {
            throw new RuntimeException("Ugh failed delete of " + node, e);
          }
          // then try to transition to OPENED
          return super.transitionToOpened(r);
        }
      };
      rss.getRegionsInTransitionInRS().put(
        hri.getEncodedNameAsBytes(), Boolean.TRUE);
      // Call process without first creating OFFLINE region in zk, see if
      // exception or just quiet return (expected).
      handler.process();
      rss.getRegionsInTransitionInRS().put(
        hri.getEncodedNameAsBytes(), Boolean.TRUE);
      ZKAssign.createNodeOffline(server.getZooKeeper(), hri, server.getServerName());
      // Call process again but this time yank the zk znode out from under it
      // post OPENING; again will expect it to come back w/o NPE or exception.
      handler.process();
    } catch (IOException ioe) {
    } finally {
      HRegion.closeHRegion(region);
    }
    // Region server is expected to abort due to OpenRegionHandler perceiving transitioning
    // to OPENED as failed
    // This was corresponding to the second handler.process() call above.
    assertTrue("region server should have aborted", rss.isAborted());
  }
  
  @Test
  public void testFailedOpenRegion() throws Exception {
    Server server = new MockServer(HTU);
    RegionServerServices rsServices = HTU.createMockRegionServerService();

    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_HRI, server.getServerName());

    // Create the handler
    OpenRegionHandler handler =
      new OpenRegionHandler(server, rsServices, TEST_HRI, TEST_HTD) {
        @Override
        HRegion openRegion() {
          // Fake failure of opening a region due to an IOE, which is caught
          return null;
        }
    };
    rsServices.getRegionsInTransitionInRS().put(
      TEST_HRI.getEncodedNameAsBytes(), Boolean.TRUE);
    handler.process();

    // Handler should have transitioned it to FAILED_OPEN
    RegionTransition rt = RegionTransition.parseFrom(
      ZKAssign.getData(server.getZooKeeper(), TEST_HRI.getEncodedName()));
    assertEquals(EventType.RS_ZK_REGION_FAILED_OPEN, rt.getEventType());
  }
  
  @Test
  public void testFailedUpdateMeta() throws Exception {
    Server server = new MockServer(HTU);
    RegionServerServices rsServices = HTU.createMockRegionServerService();

    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_HRI, server.getServerName());

    // Create the handler
    OpenRegionHandler handler =
      new OpenRegionHandler(server, rsServices, TEST_HRI, TEST_HTD) {
        @Override
        boolean updateMeta(final HRegion r, long masterSystemTime) {
          // Fake failure of updating META
          return false;
        }
    };
    rsServices.getRegionsInTransitionInRS().put(
      TEST_HRI.getEncodedNameAsBytes(), Boolean.TRUE);
    handler.process();

    // Handler should have transitioned it to FAILED_OPEN
    RegionTransition rt = RegionTransition.parseFrom(
      ZKAssign.getData(server.getZooKeeper(), TEST_HRI.getEncodedName()));
    assertEquals(EventType.RS_ZK_REGION_FAILED_OPEN, rt.getEventType());
  }
  
  @Test
  public void testTransitionToFailedOpenEvenIfCleanupFails() throws Exception {
    Server server = new MockServer(HTU);
    RegionServerServices rsServices = HTU.createMockRegionServerService();
    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_HRI, server.getServerName());
    // Create the handler
    OpenRegionHandler handler = new OpenRegionHandler(server, rsServices, TEST_HRI, TEST_HTD) {
      @Override
      boolean updateMeta(HRegion r, long masterSystemTime) {
        return false;
      };

      @Override
      void cleanupFailedOpen(HRegion region) throws IOException {
        throw new IOException("FileSystem got closed.");
      }
    };
    rsServices.getRegionsInTransitionInRS().put(TEST_HRI.getEncodedNameAsBytes(), Boolean.TRUE);
    try {
      handler.process();
    } catch (Exception e) {
      // Ignore the IOException that we have thrown from cleanupFailedOpen
    }
    RegionTransition rt = RegionTransition.parseFrom(ZKAssign.getData(server.getZooKeeper(),
        TEST_HRI.getEncodedName()));
    assertEquals(EventType.RS_ZK_REGION_FAILED_OPEN, rt.getEventType());
  }

  @Test
  public void testTransitionToFailedOpenFromOffline() throws Exception {
    Server server = new MockServer(HTU);
    RegionServerServices rsServices = HTU.createMockRegionServerService(server.getServerName());
    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_HRI, server.getServerName());
    // Create the handler
    OpenRegionHandler handler = new OpenRegionHandler(server, rsServices, TEST_HRI, TEST_HTD) {

      @Override
      boolean transitionZookeeperOfflineToOpening(String encodedName, int versionOfOfflineNode) {
        return false;
      }
    };
    rsServices.getRegionsInTransitionInRS().put(TEST_HRI.getEncodedNameAsBytes(), Boolean.TRUE);

    handler.process();

    RegionTransition rt = RegionTransition.parseFrom(ZKAssign.getData(server.getZooKeeper(),
        TEST_HRI.getEncodedName()));
    assertEquals(EventType.RS_ZK_REGION_FAILED_OPEN, rt.getEventType());
  }

}

