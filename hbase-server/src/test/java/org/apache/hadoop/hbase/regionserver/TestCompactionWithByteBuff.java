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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(LargeTests.class)
public class TestCompactionWithByteBuff {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionWithByteBuff.class);
  @Rule
  public TestName name = new TestName();

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;

  private static final byte[] COLUMN = Bytes.toBytes("A");
  private static final int REGION_COUNT = 5;
  private static final long ROW_COUNT = 200;
  private static final int ROW_LENGTH = 20;
  private static final int VALUE_LENGTH = 5000;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    conf.setInt(ByteBuffAllocator.BUFFER_SIZE_KEY, 1024 * 5);
    conf.setInt(CompactSplit.SMALL_COMPACTION_THREADS, REGION_COUNT * 2);
    conf.setInt(CompactSplit.LARGE_COMPACTION_THREADS, REGION_COUNT * 2);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 512);
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCompaction() throws Exception {
    TableName table = TableName.valueOf("t1");
    admin.compactionSwitch(false, new ArrayList<>(0));
    try (Table t = createTable(TEST_UTIL, table)) {
      for (int i = 0; i < 2; i++) {
        put(t);
        admin.flush(table);
      }
      admin.compactionSwitch(true, new ArrayList<>(0));
      admin.majorCompact(table);
      List<JVMClusterUtil.RegionServerThread> regionServerThreads =
          TEST_UTIL.getHBaseCluster().getRegionServerThreads();
      TEST_UTIL.waitFor(2 * 60 * 1000L, () -> {
        boolean result = true;
        for (JVMClusterUtil.RegionServerThread regionServerThread : regionServerThreads) {
          HRegionServer regionServer = regionServerThread.getRegionServer();
          List<HRegion> regions = regionServer.getRegions(table);
          for (HRegion region : regions) {
            List<String> storeFileList = region.getStoreFileList(new byte[][] { COLUMN });
            if (storeFileList.size() > 1) {
              result = false;
            }
          }
        }
        return result;
      });
    }
  }

  private Table createTable(HBaseTestingUtility util, TableName tableName)
      throws IOException {
    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(COLUMN).setBlocksize(1024 * 4).build())
            .build();
    byte[][] splits = new byte[REGION_COUNT - 1][];
    for (int i = 1; i < REGION_COUNT; i++) {
      splits[i - 1] = Bytes.toBytes(buildRow((int) (ROW_COUNT / REGION_COUNT * i)));
    }
    return util.createTable(td, splits);
  }

  private void put(Table table) throws IOException {
    for (int i = 0; i < ROW_COUNT; i++) {
      Put put = new Put(Bytes.toBytes(buildRow(i)));
      put.addColumn(COLUMN, Bytes.toBytes("filed01"), buildValue(i, 1));
      put.addColumn(COLUMN, Bytes.toBytes("filed02"), buildValue(i, 2));
      put.addColumn(COLUMN, Bytes.toBytes("filed03"), buildValue(i, 3));
      put.addColumn(COLUMN, Bytes.toBytes("filed04"), buildValue(i, 4));
      put.addColumn(COLUMN, Bytes.toBytes("filed05"), buildValue(i, 5));
      table.put(put);
    }
  }

  private String buildRow(int index) {
    String value = Long.toString(index);
    String prefix = "user";
    for (int i = 0; i < ROW_LENGTH - value.length(); i++) {
      prefix += '0';
    }
    return prefix + value;
  }

  private byte[] buildValue(int index, int qualifierId) {
    String row = buildRow(index) + "/f" + qualifierId + "-";
    StringBuffer result = new StringBuffer();
    while (result.length() < VALUE_LENGTH) {
      result.append(row);
    }
    return Bytes.toBytes(result.toString().substring(0, VALUE_LENGTH));
  }
}
