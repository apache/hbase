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

import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_ACTIVATE_MIN_HFILES_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.RowCacheKey;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestRowCacheClearRegionBlockCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheClearRegionBlockCache.class);

  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[][] SPLIT_KEY = new byte[][] { Bytes.toBytes("5") };
  private static final int NUM_RS = 2;

  private final HBaseTestingUtil HTU = new HBaseTestingUtil();

  private Admin admin;
  private Table table;
  private TableName tableName;

  @Rule
  public TestName testName = new TestName();

  @Parameterized.Parameter
  public String cacheType;

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Object[] data() {
    return new Object[] { "lru", "bucket" };
  }

  @Before
  public void setup() throws Exception {
    if (cacheType.equals("bucket")) {
      Configuration conf = HTU.getConfiguration();
      conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
      conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 30);
    }

    // To test simply, regardless of the number of HFiles
    HTU.getConfiguration().setInt(ROW_CACHE_ACTIVATE_MIN_HFILES_KEY, 0);
    HTU.startMiniCluster(NUM_RS);
    admin = HTU.getAdmin();

    // Create table
    tableName = TableName.valueOf(testName.getMethodName().replaceAll("[\\[\\]: ]", "_"));
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfd)
      .setRowCacheEnabled(true).build();
    admin.createTable(td, SPLIT_KEY);
    table = admin.getConnection().getTable(tableName);

    HTU.loadNumericRows(table, FAMILY, 1, 10);
  }

  @After
  public void teardown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testClearRowCache() throws Exception {
    HRegion region = HTU.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();

    Get get = new Get(Bytes.toBytes("1"));
    table.get(get);

    // Ensure a row cache entry exists
    RowCacheKey rowCacheKey = new RowCacheKey(region, Bytes.toBytes("1"));
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Ensure the row cache is cleared
    admin.clearBlockCache(tableName);
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
  }
}
