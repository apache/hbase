/*
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
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRowCacheHRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheHRegion.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  public static final byte[] CF = Bytes.toBytes("cf1");

  @Rule
  public TestName currentTest = new TestName();

  @BeforeClass
  public static void setupCluster() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testOpenHRegion() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    WALFactory walFactory = new WALFactory(conf,
      ServerName.valueOf(currentTest.getMethodName(), 16010, EnvironmentEdgeManager.currentTime())
        .toString());
    WAL wal = walFactory.getWAL(null);
    Path hbaseRootDir = CommonFSUtils.getRootDir(conf);
    TableName tableName = TableName.valueOf(currentTest.getMethodName());
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();
    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    HRegion region = HRegion.openHRegion(conf, FileSystem.get(conf), hbaseRootDir, hri, htd, wal,
      regionServer, null);

    // Verify that rowCacheSeqNum is initialized correctly
    assertNotEquals(HConstants.NO_SEQNUM, region.getRowCacheSeqNum());
    assertEquals(region.getOpenSeqNum(), region.getRowCacheSeqNum());

    region.close();
    walFactory.close();
  }
}
