/**
 *
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.data.Stat;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * @author pritam
 */
public class TestHRegionCloseRetry {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[][] FAMILIES = { Bytes.toBytes("f1"),
      Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4") };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCloseHRegionRetry() throws Exception {

    // Build some data.
    byte[] tableName = Bytes.toBytes("testCloseHRegionRetry");
    TEST_UTIL.createTable(tableName, FAMILIES);
    HTable table = new HTable(tableName);
    for (int i = 0; i < FAMILIES.length; i++) {
      byte[] columnFamily = FAMILIES[i];
      TEST_UTIL.createMultiRegions(table, columnFamily);
      TEST_UTIL.loadTable(table, columnFamily);
    }

    // Pick a regionserver.
    Configuration conf = TEST_UTIL.getConfiguration();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);

    // Some initializtion relevant to zk.
    HRegion[] region = server.getOnlineRegionsAsArray();
    assertTrue(region.length != 0);
    HRegionInfo regionInfo = region[0].getRegionInfo();
    ZooKeeperWrapper zkWrapper = ZooKeeperWrapper.getInstance(conf, server
        .getHServerInfo().getServerName());
    String regionZNode = zkWrapper.getZNode(
        zkWrapper.getRegionInTransitionZNode(), regionInfo.getEncodedName());

    // Ensure region is online before closing.
    assertNotNull(server.getOnlineRegion(regionInfo.getRegionName()));

    TEST_UTIL.getDFSCluster().shutdownDataNodes();
    try {
      server.closeRegion(regionInfo, true);
    } catch (IOException e) {
      LOG.warn(e);
      TEST_UTIL.getDFSCluster().startDataNodes(conf, 3, true, null, null);
      TEST_UTIL.getDFSCluster().waitClusterUp();
      LOG.info("New DFS Cluster up");
      // Close region should fail since filesystem wad down. The region should
      // be in the state "CLOSING"
      Stat stat = new Stat();
      assertTrue(zkWrapper.exists(regionZNode, false));
      byte[] data = zkWrapper.readZNode(regionZNode, stat);
      RegionTransitionEventData rsData = new RegionTransitionEventData();
      Writables.getWritable(data, rsData);
      assertEquals(rsData.getHbEvent(), HBaseEventType.RS2ZK_REGION_CLOSING);

      // Now try to close the region again.
      LOG.info("Retrying close region");
      server.closeRegion(regionInfo, true);
      data = zkWrapper.readZNode(regionZNode, stat);
      rsData = new RegionTransitionEventData();
      Writables.getWritable(data, rsData);

      // Verify region is closed.
      assertNull(server.getOnlineRegion(regionInfo.getRegionName()));
      assertEquals(HBaseEventType.RS2ZK_REGION_CLOSED, rsData.getHbEvent());
      return;
    }
    fail("Close of region did not fail, even though filesystem was down");
  }
}
