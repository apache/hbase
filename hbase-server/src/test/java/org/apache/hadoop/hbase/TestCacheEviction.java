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
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.EVICT_BLOCKS_ON_SPLIT_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, MediumTests.class })
public class TestCacheEviction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCacheEviction.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheEviction.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.getConfiguration().setBoolean(CACHE_BLOCKS_ON_WRITE_KEY, true);
    UTIL.getConfiguration().setBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    UTIL.getConfiguration().setInt(BUCKET_CACHE_SIZE_KEY, 200);
    UTIL.getConfiguration().set(StoreFileTrackerFactory.TRACKER_IMPL, "FILE");
  }

  @Before
  public void testSetup() {
    UTIL.getConfiguration().set(BUCKET_CACHE_IOENGINE_KEY,
      "file:" + UTIL.getDataTestDir() + "/bucketcache");
  }

  @Test
  public void testEvictOnSplit() throws Exception {
    doTestEvictOnSplit("testEvictOnSplit", true,
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null),
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) == null));
  }

  @Test
  public void testDoesntEvictOnSplit() throws Exception {
    doTestEvictOnSplit("testDoesntEvictOnSplit", false,
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null),
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null));
  }

  @Test
  public void testEvictOnClose() throws Exception {
    doTestEvictOnClose("testEvictOnClose", true,
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null),
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) == null));
  }

  @Test
  public void testDoesntEvictOnClose() throws Exception {
    doTestEvictOnClose("testDoesntEvictOnClose", false,
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null),
      (f, m) -> Waiter.waitFor(UTIL.getConfiguration(), 1000, () -> m.get(f) != null));
  }

  private void doTestEvictOnSplit(String table, boolean evictOnSplit,
    BiConsumer<String, Map<String, Pair<String, Long>>> predicateBeforeSplit,
    BiConsumer<String, Map<String, Pair<String, Long>>> predicateAfterSplit) throws Exception {
    UTIL.startMiniCluster(1);
    try {
      TableName tableName = TableName.valueOf(table);
      createTable(tableName, true);
      Collection<HStoreFile> files =
        UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getStores().get(0).getStorefiles();
      checkCacheForBlocks(tableName, files, predicateBeforeSplit);
      UTIL.getMiniHBaseCluster().getRegionServer(0).getConfiguration()
        .setBoolean(EVICT_BLOCKS_ON_SPLIT_KEY, evictOnSplit);
      UTIL.getAdmin().split(tableName, Bytes.toBytes("row-500"));
      Waiter.waitFor(UTIL.getConfiguration(), 30000,
        () -> UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 2);
      UTIL.waitUntilNoRegionsInTransition();
      checkCacheForBlocks(tableName, files, predicateAfterSplit);
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  private void doTestEvictOnClose(String table, boolean evictOnClose,
    BiConsumer<String, Map<String, Pair<String, Long>>> predicateBeforeClose,
    BiConsumer<String, Map<String, Pair<String, Long>>> predicateAfterClose) throws Exception {
    UTIL.startMiniCluster(1);
    try {
      TableName tableName = TableName.valueOf(table);
      createTable(tableName, true);
      Collection<HStoreFile> files =
        UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getStores().get(0).getStorefiles();
      checkCacheForBlocks(tableName, files, predicateBeforeClose);
      UTIL.getMiniHBaseCluster().getRegionServer(0).getConfiguration()
        .setBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, evictOnClose);
      UTIL.getAdmin().disableTable(tableName);
      UTIL.waitUntilNoRegionsInTransition();
      checkCacheForBlocks(tableName, files, predicateAfterClose);
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  private void createTable(TableName tableName, boolean shouldFlushTable)
    throws IOException, InterruptedException {
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
    if (shouldFlushTable) {
      UTIL.getAdmin().flush(tableName);
      Thread.sleep(5000);
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

  @Test
  public void testNoCacheWithoutFlush() throws Exception {
    UTIL.startMiniCluster(1);
    try {
      TableName tableName = TableName.valueOf("tableNoCache");
      createTable(tableName, false);
      checkRegionCached(tableName, false);
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testCacheWithFlush() throws Exception {
    UTIL.startMiniCluster(1);
    try {
      TableName tableName = TableName.valueOf("tableWithFlush");
      createTable(tableName, true);
      checkRegionCached(tableName, true);
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  private void checkRegionCached(TableName tableName, boolean isCached) throws IOException {
    UTIL.getMiniHBaseCluster().getRegions(tableName).forEach(r -> {
      try {
        UTIL.getMiniHBaseCluster().getClusterMetrics().getLiveServerMetrics().forEach((sn, sm) -> {
          for (Map.Entry<byte[], RegionMetrics> rm : sm.getRegionMetrics().entrySet()) {
            if (rm.getValue().getNameAsString().equals(r.getRegionInfo().getRegionNameAsString())) {
              assertTrue(isCached == (rm.getValue().getCurrentRegionCachedRatio() > 0.0f));
            }
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
