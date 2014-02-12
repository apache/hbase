/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.data.Stat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHRegionClose {
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static byte[][] FAMILIES = { Bytes.toBytes("f1"),
      Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4") };
  protected HRegionServer server;
  protected ZooKeeperWrapper zkWrapper;
  protected HRegionInfo regionInfo;
  protected String regionZNode;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);

    // Build some data.
    byte[] tableName = Bytes.toBytes(getClass().getSimpleName());
    TEST_UTIL.createTable(tableName, FAMILIES);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    for (int i = 0; i < FAMILIES.length; i++) {
      byte[] columnFamily = FAMILIES[i];
      TEST_UTIL.createMultiRegions(table, columnFamily);
      TEST_UTIL.loadTable(table, columnFamily);
    }

    // Pick a regionserver.
    Configuration conf = TEST_UTIL.getConfiguration();
    server = TEST_UTIL.getHBaseCluster().getRegionServer(0);

    HRegion[] region = server.getOnlineRegionsAsArray();
    regionInfo = region[0].getRegionInfo();

    // Some initializtion relevant to zk.
    zkWrapper = server.getZooKeeperWrapper();
    regionZNode = zkWrapper.getZNode(
        zkWrapper.getRegionInTransitionZNode(), regionInfo.getEncodedName());
  }

  @After
  public void tearDown() throws Exception {
    server = null;
    zkWrapper = null;
    regionInfo = null;
    regionZNode = null;
    TEST_UTIL.shutdownMiniCluster();
  }

  protected void tryCloseRegion() throws Exception {
    server.closeRegion(regionInfo, true);

    byte[] data = zkWrapper.readZNode(regionZNode, new Stat());
    RegionTransitionEventData rsData = new RegionTransitionEventData();
    Writables.getWritable(data, rsData);

    // Verify region is closed.
    assertNull(server.getOnlineRegion(regionInfo.getRegionName()));
    assertEquals(HBaseEventType.RS2ZK_REGION_CLOSED, rsData.getHbEvent());
  }

  @Test
  public void mainTest() throws Exception {
    tryCloseRegion();
  }

  @Test
  public void testMemstoreCleanup() throws Exception {
    HRegion region = server.getOnlineRegionsAsArray()[0];

    Store store = region.getStore(FAMILIES[0]);

    byte[] row = region.getStartKey();
    byte[] value = Bytes.toBytes("testMemstoreCleanup");
    Put put = new Put(row);
    put.add(FAMILIES[0], null, Bytes.toBytes("testMemstoreCleanup"));

    // First put something in current memstore, which will be in snapshot after flusher.prepare()
    region.put(put);

    StoreFlusher flusher = store.getStoreFlusher(12345);
    flusher.prepare();

    // Second put something in current memstore
    put.add(FAMILIES[0], Bytes.toBytes("abc"), value);
    region.put(put);

    region.close();
    assertEquals(0, region.getMemstoreSize().get());
  }
}
