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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.EVICT_BLOCKS_ON_SPLIT_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, MediumTests.class })
public class TestSplitWithCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitWithCache.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitWithCache.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.getConfiguration().setBoolean(CACHE_BLOCKS_ON_WRITE_KEY, true);
    UTIL.getConfiguration().setBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    UTIL.getConfiguration().set(BUCKET_CACHE_IOENGINE_KEY, "offheap");
    UTIL.getConfiguration().setInt(BUCKET_CACHE_SIZE_KEY, 200);
  }

  @Test
  public void testEvictOnSplit() throws Exception {
    doTest("testEvictOnSplit", true,
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null),
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) == null));
  }

  @Test
  public void testDoesntEvictOnSplit() throws Exception {
    doTest("testDoesntEvictOnSplit", false,
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null),
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null));
  }

  private void doTest(String table, boolean evictOnSplit,
    BiConsumer<String, Map<String, Pair<String, Long>>> predicateBeforeSplit,
    BiConsumer<String, Map<String, Pair<String, Long>>> predicateAfterSplit) throws Exception {
    UTIL.getConfiguration().setBoolean(EVICT_BLOCKS_ON_SPLIT_KEY, evictOnSplit);
    UTIL.startMiniCluster(1);
    try {
      TableName tableName = TableName.valueOf(table);
      byte[] family = Bytes.toBytes("CF");
      TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
      UTIL.getAdmin().createTable(td);
      UTIL.waitTableAvailable(tableName);
      Table tbl = UTIL.getConnection().getTable(tableName);
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        Put p = new Put(Bytes.toBytes("row-" + i));
        p.addColumn(family, Bytes.toBytes(1), Bytes.toBytes("val-" + i));
        puts.add(p);
      }
      tbl.put(puts);
      UTIL.getAdmin().flush(tableName);
      Collection<HStoreFile> files =
        UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getStores().get(0).getStorefiles();
      checkCacheForBlocks(tableName, files, predicateBeforeSplit);
      UTIL.getAdmin().split(tableName, Bytes.toBytes("row-500"));
      Waiter.waitFor(UTIL.getConfiguration(), 30000,
        () -> UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 2);
      UTIL.waitUntilNoRegionsInTransition();
      checkCacheForBlocks(tableName, files, predicateAfterSplit);
    } finally {
      UTIL.shutdownMiniCluster();
    }

  }

  private void checkCacheForBlocks(TableName tableName, Collection<HStoreFile> files,
    BiConsumer<String, Map<String, Pair<String, Long>>> checker) {
    files.forEach(f -> {
      UTIL.getMiniHBaseCluster().getRegionServer(0).getBlockCache().ifPresent(cache -> {
        cache.getFullyCachedFiles().ifPresent(m -> {
          checker.accept(f.getPath().getName(), m);
        });
        assertTrue(cache.getFullyCachedFiles().isPresent());
      });
    });
  }
}
