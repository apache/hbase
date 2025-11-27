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
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestRowCacheWithBucketCacheAndDataBlockEncoding {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheWithBucketCacheAndDataBlockEncoding.class);

  @Parameterized.Parameter
  public static boolean uesBucketCache;

  @Parameterized.Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  @Rule
  public TestName name = new TestName();

  private static final byte[] ROW_KEY = Bytes.toBytes("checkRow");
  private static final byte[] CF = Bytes.toBytes("CF");
  private static final byte[] QUALIFIER = Bytes.toBytes("cq");
  private static final byte[] VALUE = Bytes.toBytes("checkValue");
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;
  private static RowCache rowCache;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Use bucket cache
    if (uesBucketCache) {
      conf.setInt(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, 1);
      conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
      conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 64);
    }

    // Use row cache
    conf.setFloat(ROW_CACHE_SIZE_KEY, 0.01f);
    conf.setFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.39f);
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();

    rowCache = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRSRpcServices()
      .getRowCacheService().getRowCache();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRowCacheNoEncode() throws Exception {
    testRowCache(name.getMethodName(), DataBlockEncoding.NONE);
  }

  @Test
  public void testRowCacheEncode() throws Exception {
    testRowCache(name.getMethodName(), DataBlockEncoding.FAST_DIFF);
  }

  private void testRowCache(String methodName, DataBlockEncoding dbe) throws Exception {
    TableName tableName = TableName.valueOf(methodName.replaceAll("[\\[\\]]", "_"));
    try (Table testTable = createTable(tableName, dbe)) {
      Put put = new Put(ROW_KEY);
      put.addColumn(CF, QUALIFIER, VALUE);
      testTable.put(put);
      admin.flush(testTable.getName());

      long countBase = rowCache.getCount();
      long hitCountBase = rowCache.getHitCount();

      Result result;

      // First get should not hit the row cache, and populate it
      Get get = new Get(ROW_KEY);
      result = testTable.get(get);
      assertArrayEquals(ROW_KEY, result.getRow());
      assertArrayEquals(VALUE, result.getValue(CF, QUALIFIER));
      assertEquals(1, rowCache.getCount() - countBase);
      assertEquals(0, rowCache.getHitCount() - hitCountBase);

      // Second get should hit the row cache
      result = testTable.get(get);
      assertArrayEquals(ROW_KEY, result.getRow());
      assertArrayEquals(VALUE, result.getValue(CF, QUALIFIER));
      assertEquals(1, rowCache.getCount() - countBase);
      assertEquals(1, rowCache.getHitCount() - hitCountBase);
    }
  }

  private Table createTable(TableName tableName, DataBlockEncoding dbe) throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).setBlocksize(100)
        .setDataBlockEncoding(dbe).build())
      .setRowCacheEnabled(true).build();
    return TEST_UTIL.createTable(td, null);
  }
}
