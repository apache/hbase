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

import static org.apache.hadoop.hbase.regionserver.HRegion.SPLIT_IGNORE_BLOCKING_ENABLED_KEY;
import static org.apache.hadoop.hbase.regionserver.Store.PRIORITY_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class})
public class TestSplitWithBlockingFiles {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitWithBlockingFiles.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitWithBlockingFiles.class);

  protected static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static TableName TABLE_NAME = TableName.valueOf("test");
  private static Admin ADMIN;
  private static byte[] CF = Bytes.toBytes("cf");
  private static Table TABLE;


  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE, 8 * 2 * 10240L);
    UTIL.getConfiguration().setInt(HStore.BLOCKING_STOREFILES_KEY, 1);
    UTIL.getConfiguration().set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    UTIL.getConfiguration().setBoolean(SPLIT_IGNORE_BLOCKING_ENABLED_KEY, true);
    UTIL.startMiniCluster(1);
    ADMIN = UTIL.getAdmin();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(CF).setBlocksize(1000).build()).build();
    TABLE = UTIL.createTable(td, null);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    Closeables.close(TABLE, true);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitIgnoreBlockingFiles() throws Exception {
    ADMIN.splitSwitch(false, true);
    byte[] value = new byte[1024];
    for (int m = 0; m < 10; m++) {
      String rowPrefix = "row" + m;
      for (int i = 0; i < 10; i++) {
        Put p = new Put(Bytes.toBytes(rowPrefix + i));
        p.addColumn(CF, Bytes.toBytes("qualifier"), value);
        p.addColumn(CF, Bytes.toBytes("qualifier2"), value);
        TABLE.put(p);
      }
      ADMIN.flush(TABLE_NAME);
    }
    Scan scan = new Scan();
    ResultScanner results = TABLE.getScanner(scan);
    int count = 0;
    while (results.next() != null) {
      count++;
    }
    Assert.assertEquals("There should be 100 rows!", 100, count);
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegionServer(0).getRegions();
    regions.removeIf(r -> !r.getRegionInfo().getTable().equals(TABLE_NAME));
    assertEquals(1, regions.size());
    assertNotNull(regions.get(0).getSplitPolicy().getSplitPoint());
    assertTrue(regions.get(0).getCompactPriority() >= PRIORITY_USER);
    assertTrue(UTIL.getMiniHBaseCluster().getRegionServer(0).getCompactSplitThread()
      .requestSplit(regions.get(0)));

    // split region
    ADMIN.splitSwitch(true, true);
    MasterProcedureEnv env =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment();
    final ProcedureExecutor<MasterProcedureEnv> executor =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    SplitTableRegionProcedure splitProcedure =
      new SplitTableRegionProcedure(env, regions.get(0).getRegionInfo(), Bytes.toBytes("row5"));
    executor.submitProcedure(splitProcedure);
    ProcedureTestingUtility.waitProcedure(executor, splitProcedure.getProcId());

    regions = UTIL.getMiniHBaseCluster().getRegionServer(0).getRegions();
    regions.removeIf(r -> !r.getRegionInfo().getTable().equals(TABLE_NAME));
    assertEquals(2, regions.size());
    scan = new Scan();
    results = TABLE.getScanner(scan);
    count = 0;
    while (results.next() != null) {
      count++;
    }
    Assert.assertEquals("There should be 100 rows!", 100, count);
    for (HRegion region : regions) {
      assertTrue(region.getCompactPriority() < PRIORITY_USER);
      assertFalse(
        UTIL.getMiniHBaseCluster().getRegionServer(0).getCompactSplitThread().requestSplit(region));
    }
  }
}
