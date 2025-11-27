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

import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_EVICT_ON_CLOSE_KEY;
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category({ RegionServerTests.class, MediumTests.class })
@RunWith(Parameterized.class)
public class TestRowCacheEvictOnClose {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheEvictOnClose.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] CF1 = Bytes.toBytes("cf1");
  private static final byte[] Q1 = Bytes.toBytes("q1");
  private static final byte[] Q2 = Bytes.toBytes("q2");

  @Rule
  public TestName testName = new TestName();

  @Parameterized.Parameter
  public boolean evictOnClose;

  @Parameterized.Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  @Test
  public void testEvictOnClose() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    // Enable row cache
    conf.setFloat(ROW_CACHE_SIZE_KEY, 0.01f);
    conf.setFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.39f);

    // Set ROW_CACHE_EVICT_ON_CLOSE
    conf.setBoolean(ROW_CACHE_EVICT_ON_CLOSE_KEY, evictOnClose);

    // Start cluster
    SingleProcessHBaseCluster cluster = TEST_UTIL.startMiniCluster();
    cluster.waitForActiveAndReadyMaster();
    Admin admin = TEST_UTIL.getAdmin();

    RowCache rowCache = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRSRpcServices()
      .getRowCacheService().getRowCache();

    // Create table with row cache enabled
    ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();
    TableName tableName = TableName.valueOf(testName.getMethodName().replaceAll("[\\[\\]]", "_"));
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setRowCacheEnabled(true)
      .setColumnFamily(cf1).build();
    admin.createTable(td);
    Table table = admin.getConnection().getTable(tableName);

    int numRows = 10;

    // Put rows
    for (int i = 0; i < numRows; i++) {
      byte[] rowKey = ("row" + i).getBytes();
      Put put = new Put(rowKey);
      put.addColumn(CF1, Q1, Bytes.toBytes(0L));
      put.addColumn(CF1, Q2, "12".getBytes());
      table.put(put);
    }
    // Need to flush because the row cache is not populated when reading only from the memstore.
    admin.flush(tableName);

    // Populate row caches
    for (int i = 0; i < numRows; i++) {
      byte[] rowKey = ("row" + i).getBytes();
      Get get = new Get(rowKey);
      Result result = table.get(get);
      assertArrayEquals(rowKey, result.getRow());
      assertArrayEquals(Bytes.toBytes(0L), result.getValue(CF1, Q1));
      assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));
    }

    // Verify row cache has some entries
    assertEquals(numRows, rowCache.getCount());

    // Disable table
    admin.disableTable(tableName);

    // Verify row cache is cleared on table close
    assertEquals(evictOnClose ? 0 : numRows, rowCache.getCount());

    admin.deleteTable(tableName);
    TEST_UTIL.shutdownMiniCluster();
  }
}
