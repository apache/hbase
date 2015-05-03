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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Progressable;
import org.junit.AfterClass;
import org.junit.Assume;
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
  private static final TableName TABLE_NAME =
      TableName.valueOf("table");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("family");
  private static final int FAVORED_NODES_NUM = 3;
  private static final int REGION_SERVERS = 6;
  private static final int FLUSHES = 3;
  private static Method createWithFavoredNode = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      createWithFavoredNode = DistributedFileSystem.class.getDeclaredMethod("create", Path.class,
        FsPermission.class, boolean.class, int.class, short.class, long.class,
        Progressable.class, InetSocketAddress[].class);
    } catch (NoSuchMethodException nm) {
      return;
    }
    TEST_UTIL.startMiniCluster(REGION_SERVERS);
    table = TEST_UTIL.createTable(TABLE_NAME, COLUMN_FAMILY);
    TEST_UTIL.createMultiRegions(table, COLUMN_FAMILY);
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // guard against failure in setup
    if (table != null) {
      table.close();
    }
    if (createWithFavoredNode == null) {
      return;
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFavoredNodes() throws Exception {
    Assume.assumeTrue(createWithFavoredNode != null);
    // Get the addresses of the datanodes in the cluster.
    InetSocketAddress[] nodes = new InetSocketAddress[REGION_SERVERS];
    List<DataNode> datanodes = TEST_UTIL.getDFSCluster().getDataNodes();
    Method selfAddress;
    try {
      selfAddress = DataNode.class.getMethod("getSelfAddr");
    } catch (NoSuchMethodException ne) {
      selfAddress = DataNode.class.getMethod("getXferAddress");
    }
    for (int i = 0; i < REGION_SERVERS; i++) {
      nodes[i] = (InetSocketAddress)selfAddress.invoke(datanodes.get(i));
    }

    String[] nodeNames = new String[REGION_SERVERS];
    for (int i = 0; i < REGION_SERVERS; i++) {
      nodeNames[i] = nodes[i].getAddress().getHostAddress() + ":" +
          nodes[i].getPort();
    }

    // For each region, choose some datanodes as the favored nodes then assign
    // them as favored nodes through the HRegion.
    for (int i = 0; i < REGION_SERVERS; i++) {
      HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(i);
      List<HRegion> regions = server.getOnlineRegions(TABLE_NAME);
      for (HRegion region : regions) {
        List<org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName>favoredNodes =
            new ArrayList<org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName>(3);
        String encodedRegionName = region.getRegionInfo().getEncodedName();
        for (int j = 0; j < FAVORED_NODES_NUM; j++) {
          org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName.Builder b =
              org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName.newBuilder();
          b.setHostName(nodes[(i + j) % REGION_SERVERS].getAddress().getHostAddress());
          b.setPort(nodes[(i + j) % REGION_SERVERS].getPort());
          b.setStartCode(-1);
          favoredNodes.add(b.build());
        }
        server.updateRegionFavoredNodesMapping(encodedRegionName, favoredNodes);
      }
    }

    // Write some data to each region and flush. Repeat some number of times to
    // get multiple files for each region.
    for (int i = 0; i < FLUSHES; i++) {
      TEST_UTIL.loadTable(table, COLUMN_FAMILY, false);
      TEST_UTIL.flush();
    }

    // For each region, check the block locations of each file and ensure that
    // they are consistent with the favored nodes for that region.
    for (int i = 0; i < REGION_SERVERS; i++) {
      HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(i);
      List<HRegion> regions = server.getOnlineRegions(TABLE_NAME);
      for (HRegion region : regions) {
        List<String> files = region.getStoreFileList(new byte[][]{COLUMN_FAMILY});
        for (String file : files) {
          FileStatus status = TEST_UTIL.getDFSCluster().getFileSystem().
              getFileStatus(new Path(new URI(file).getPath()));
          BlockLocation[] lbks = 
              ((DistributedFileSystem)TEST_UTIL.getDFSCluster().getFileSystem())
              .getFileBlockLocations(status, 0, Long.MAX_VALUE);
          for (BlockLocation lbk : lbks) {
            locations:
              for (String info : lbk.getNames()) {
                for (int j = 0; j < FAVORED_NODES_NUM; j++) {
                  if (info.equals(nodeNames[(i + j) % REGION_SERVERS])) {
                    continue locations;
                  }
                }
                // This block was at a location that was not a favored location.
                fail("Block location " + info + " not a favored node");
              }
          }
        }
      }
    }
  }
}
