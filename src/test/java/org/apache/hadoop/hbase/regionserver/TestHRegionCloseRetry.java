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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(TagRunner.class)
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

  @Test(timeout = 300000)
  @TestTag({ "unstable" })
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
    for (int i = 0; i < 3; i++) {
      server = TEST_UTIL.getHBaseCluster().getRegionServer(i);

      // Some initialization relevant to zk.
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

    ZooKeeperWrapper zkWrapper = server.getZooKeeperWrapper();
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
