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
package org.apache.hadoop.hbase.master.handler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCreateTableHandler {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=300000)
  public void testCreateTableWithSplitRegion() throws Exception {
    final byte[] tableName = Bytes.toBytes("testCreateTableWithSplitRegion");
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    byte[] splitPoint = Bytes.toBytes("split-point");
    long ts = System.currentTimeMillis();
    HRegionInfo d1 = new HRegionInfo(tableName, null, splitPoint, false, ts);
    HRegionInfo d2 = new HRegionInfo(tableName, splitPoint, null, false, ts + 1);
    HRegionInfo parent = new HRegionInfo(tableName, null, null, true, ts + 2);
    parent.setOffline(true);

    Path tempdir = m.getMasterFileSystem().getTempDir();
    FileSystem fs = m.getMasterFileSystem().getFileSystem();
    Path tempTableDir = FSUtils.getTablePath(tempdir, tableName);
    fs.delete(tempTableDir, true); // Clean up temp table dir if exists

    final HRegionInfo[] hRegionInfos = new HRegionInfo[] {d1, d2, parent};
    CreateTableHandler handler = new CreateTableHandler(m, m.getMasterFileSystem(),
      m.getServerManager(), desc, cluster.getConfiguration(), hRegionInfos,
      m.getCatalogTracker(), m.getAssignmentManager());
    handler.process();
    for (int i = 0; i < 200; i++) {
      if (!TEST_UTIL.getHBaseAdmin().isTableAvailable(tableName)) {
        Thread.sleep(300);
      }
    }
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableEnabled(tableName));
    List<HRegionInfo> regions = m.getAssignmentManager().getRegionsOfTable(tableName);
    assertFalse("Split parent should not be assigned", regions.contains(parent));
  }
}
