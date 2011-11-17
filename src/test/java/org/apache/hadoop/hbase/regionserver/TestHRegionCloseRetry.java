/**
 *
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
  private static final Configuration conf = HBaseConfiguration.create();
  private static HBaseTestingUtility TEST_UTIL = null;
  private static byte[][] FAMILIES = { Bytes.toBytes("f1"),
      Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4") };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Helps the unit test to exit quicker.
    conf.setInt("dfs.client.block.recovery.retries", 0);
    TEST_UTIL = new HBaseTestingUtility(conf);
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
    HTable table = TEST_UTIL.createTable(tableName, FAMILIES);
    for (int i = 0; i < FAMILIES.length; i++) {
      byte[] columnFamily = FAMILIES[i];
      TEST_UTIL.loadTable(table, columnFamily);
    }

    // Pick a regionserver.
    HRegionServer server = null;
    HRegionInfo regionInfo = null;
    Configuration conf = TEST_UTIL.getConfiguration();
    for (int i = 0; i < 3; i++) {
      server = TEST_UTIL.getHBaseCluster().getRegionServer(i);

      // Some initialiation relevant to zk.
      HRegion[] region = server.getOnlineRegionsAsArray();
      for (int j = 0; j < region.length; j++) {
        if (!region[j].getRegionInfo().isRootRegion()
            && !region[j].getRegionInfo().isMetaRegion()) {
          regionInfo = region[j].getRegionInfo();
          break;
        }
      }
      if (regionInfo != null)
        break;
    }
    assertNotNull(regionInfo);

    ZooKeeperWrapper zkWrapper = ZooKeeperWrapper.getInstance(conf,
        ZooKeeperWrapper.getWrapperNameForRS(
            server.getHServerInfo().getServerName()));
    String regionZNode = zkWrapper.getZNode(
        zkWrapper.getRegionInTransitionZNode(), regionInfo.getEncodedName());

    // Ensure region is online before closing.
    assertNotNull(server.getOnlineRegion(regionInfo.getRegionName()));

    TEST_UTIL.getDFSCluster().shutdownNameNode();
    try {
      server.closeRegion(regionInfo, true);
    } catch (IOException e) {
      LOG.warn(e);
      TEST_UTIL.getDFSCluster().restartNameNode();

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
      LOG.info("Test completed successfully");
      return;
    }
    fail("Close of region did not fail, even though filesystem was down");
  }
}
