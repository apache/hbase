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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHRegionClose {
  private static final Log LOG = LogFactory.getLog(TestHRegionClose.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[][] FAMILIES = { Bytes.toBytes("f1"),
      Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4") };
  private static final String TABLE_NAME = TestHRegionClose.class.getName();
  private static int nextRegionIdx = 0;

  private HRegionServer server;
  private ZooKeeperWrapper zkWrapper;
  private HRegionInfo regionInfo;
  private String regionZNode;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);

    // Build some data.
    byte[] tableName = Bytes.toBytes(TABLE_NAME);
    TEST_UTIL.createTable(tableName, FAMILIES, 1, Bytes.toBytes("bbb"),
        Bytes.toBytes("yyy"), 25);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    for (byte[] columnFamily : FAMILIES) {
      TEST_UTIL.loadTable(table, columnFamily);
    }
  }

  @Before
  public void setUp() throws Exception {
    // Pick a regionserver.
    server = TEST_UTIL.getHBaseCluster().getRegionServer(0);

    HRegion[] region = server.getOnlineRegionsAsArray();
    regionInfo = null;

    // We need to make sure that we don't get meta or root
    while (regionInfo == null || !regionInfo.getTableDesc().getNameAsString().equals(TABLE_NAME)) {
      regionInfo = region[nextRegionIdx++].getRegionInfo();
    }

    // Some initialization relevant to zk.
    zkWrapper = server.getZooKeeperWrapper();
    regionZNode = zkWrapper.getZNode(
        zkWrapper.getRegionInTransitionZNode(), regionInfo.getEncodedName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    server = null;
    zkWrapper = null;
    regionInfo = null;
    regionZNode = null;
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

  @Test(timeout = 180000)
  public void singleClose() throws Exception {
    tryCloseRegion();
  }

  @Test(timeout = 180000)
  public void doubleClose() throws Exception {
    tryCloseRegion();
    LOG.info("Trying to close the region again, to check that the RegionServer "
        + "is idempotent. i.e. CLOSED -> CLOSING transition bug");
    tryCloseRegion();
  }

  @Test(timeout = 180000)
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
