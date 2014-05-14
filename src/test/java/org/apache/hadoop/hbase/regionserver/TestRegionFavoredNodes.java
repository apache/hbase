package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the ability to specify favored nodes for a region.
 */
@Category(MediumTests.class)
public class TestRegionFavoredNodes {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HTable table;
  private static final byte[] TABLE_NAME = Bytes.toBytes("table");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("family");
  private static final int FAVORED_NODES_NUM = 3;
  private static final int REGION_SERVERS = 3;
  private static final int FLUSHES = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Disables the permissions checker.
    TEST_UTIL.getConfiguration().setBoolean("dfs.permissions", false);
    TEST_UTIL.startMiniCluster(REGION_SERVERS);

    table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { COLUMN_FAMILY }, 3,
        Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), 25);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testFavoredNodes() throws Exception {
    // Get the addresses of the datanodes in the cluster.
    InetSocketAddress[] nodes = new InetSocketAddress[REGION_SERVERS];
    List<DataNode> datanodes = TEST_UTIL.getDFSCluster().getDataNodes();
    for (int i = 0; i < REGION_SERVERS; i++) {
      nodes[i] = datanodes.get(i).getSelfAddr();
    }

    String[] nodeNames = new String[REGION_SERVERS];
    for (int i = 0; i < REGION_SERVERS; i++) {
      nodeNames[i] = nodes[i].getAddress().getHostAddress() + ":" +
          nodes[i].getPort();
    }

    // For each region, choose some datanodes as the favored nodes then assign
    // them as favored nodes through the HRegion.
    InetSocketAddress[] favoredNodes = new InetSocketAddress[FAVORED_NODES_NUM];
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    for (int i = 0; i < regions.size(); i++) {
      for (int j = 0; j < FAVORED_NODES_NUM; j++) {
        favoredNodes[j] = nodes[(i + j) % REGION_SERVERS];
      }
      regions.get(i).setFavoredNodes(favoredNodes);
    }

    // Write some data to each region and flush. Repeat some number of times to
    // get multiple files for each region.
    for (int i = 0; i < FLUSHES; i++) {
      TEST_UTIL.loadTable(table, COLUMN_FAMILY);
      TEST_UTIL.flush();
    }

    // For each region, check the block locations of each file and ensure that
    // they are consistent with the favored nodes for that region.
    for (int i = 0; i < regions.size(); i++) {
      HRegion region = regions.get(i);
      List<String> files = region.getStoreFileList(new byte[][]{COLUMN_FAMILY});
      for (String file : files) {
        LocatedBlocks lbks = TEST_UTIL.getDFSCluster().getNameNode()
            .getBlockLocations(new URI(file).getPath(), 0, Long.MAX_VALUE);

        for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
          locations:
          for (DatanodeInfo info : lbk.getLocations()) {
            for (int j = 0; j < FAVORED_NODES_NUM; j++) {
              if (info.getName().equals(nodeNames[(i + j) % REGION_SERVERS])) {
                continue locations;
              }
            }
            // This block was at a location that was not a favored location.
            fail("Block location " + info.getName() + " not a favored node");
          }
        }
      }
    }
  }
}
