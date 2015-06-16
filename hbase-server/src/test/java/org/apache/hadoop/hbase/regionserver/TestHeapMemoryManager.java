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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.ResizableBlockCache;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerContext;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerResult;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHeapMemoryManager {

  private long maxHeapSize = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();

  @Test
  public void testAutoTunerShouldBeOffWhenMaxMinRangesForMemstoreIsNotGiven() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.05f);
    HeapMemoryManager manager = new HeapMemoryManager(new BlockCacheStub(0),
        new MemstoreFlusherStub(0), new RegionServerStub(conf), new RegionServerAccountingStub());
    assertFalse(manager.isTunerOn());
  }

  @Test
  public void testAutoTunerShouldBeOffWhenMaxMinRangesForBlockCacheIsNotGiven() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.05f);
    HeapMemoryManager manager = new HeapMemoryManager(new BlockCacheStub(0),
        new MemstoreFlusherStub(0), new RegionServerStub(conf), new RegionServerAccountingStub());
    assertFalse(manager.isTunerOn());
  }

  @Test
  public void testWhenMemstoreAndBlockCacheMaxMinChecksFails() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub(0);
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub(0);
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.06f);
    try {
      new HeapMemoryManager(blockCache, memStoreFlusher,
          new RegionServerStub(conf), new RegionServerAccountingStub());
      fail();
    } catch (RuntimeException e) {
    }
    conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.2f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    try {
      new HeapMemoryManager(blockCache, memStoreFlusher,
          new RegionServerStub(conf), new RegionServerAccountingStub());
      fail();
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testWhenClusterIsWriteHeavyWithEmptyMemstore() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    RegionServerAccountingStub regionServerAccounting = new RegionServerAccountingStub();
    // Empty block cache and memstore
    blockCache.setTestBlockSize(0);
    regionServerAccounting.setTestMemstoreSize(0);
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.10f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.05f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    // Let the system start with default values for memstore heap and block cache size.
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), regionServerAccounting);
    long oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    memStoreFlusher.flushType = FlushType.ABOVE_HIGHER_MARK;
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.flushType = FlushType.ABOVE_LOWER_MARK;
    memStoreFlusher.requestFlush(null, false);
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    // No changes should be made by tuner as we already have lot of empty space
    assertEquals(oldMemstoreHeapSize, memStoreFlusher.memstoreSize);
    assertEquals(oldBlockCacheSize, blockCache.maxSize);
  }

  @Test
  public void testWhenClusterIsReadHeavyWithEmptyBlockCache() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    RegionServerAccountingStub regionServerAccounting = new RegionServerAccountingStub();
    // Empty block cache and memstore
    blockCache.setTestBlockSize(0);
    regionServerAccounting.setTestMemstoreSize(0);
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.10f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.05f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    // Let the system start with default values for memstore heap and block cache size.
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), regionServerAccounting);
    long oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    blockCache.evictBlock(null);
    blockCache.evictBlock(null);
    blockCache.evictBlock(null);
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    // No changes should be made by tuner as we already have lot of empty space
    assertEquals(oldMemstoreHeapSize, memStoreFlusher.memstoreSize);
    assertEquals(oldBlockCacheSize, blockCache.maxSize);
  }

  @Test
  public void testWhenClusterIsWriteHeavy() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    RegionServerAccountingStub regionServerAccounting = new RegionServerAccountingStub();
    // Empty block cache and but nearly filled memstore
    blockCache.setTestBlockSize(0);
    regionServerAccounting.setTestMemstoreSize((long) (maxHeapSize * 0.4 * 0.8));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.10f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.05f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    // Let the system start with default values for memstore heap and block cache size.
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), regionServerAccounting);
    long oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    memStoreFlusher.flushType = FlushType.ABOVE_HIGHER_MARK;
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.flushType = FlushType.ABOVE_LOWER_MARK;
    memStoreFlusher.requestFlush(null, false);
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    assertHeapSpaceDelta(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE, oldMemstoreHeapSize,
        memStoreFlusher.memstoreSize);
    assertHeapSpaceDelta(-(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE), oldBlockCacheSize,
        blockCache.maxSize);
    oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    oldBlockCacheSize = blockCache.maxSize;
    // Do some more flushes before the next run of HeapMemoryTuner
    memStoreFlusher.flushType = FlushType.ABOVE_HIGHER_MARK;
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    Thread.sleep(1500);
    assertHeapSpaceDelta(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE, oldMemstoreHeapSize,
        memStoreFlusher.memstoreSize);
    assertHeapSpaceDelta(-(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE), oldBlockCacheSize,
        blockCache.maxSize);
  }

  @Test
  public void testWhenClusterIsReadHeavy() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    RegionServerAccountingStub regionServerAccounting = new RegionServerAccountingStub();
    // Empty memstore and but nearly filled block cache
    blockCache.setTestBlockSize((long) (maxHeapSize * 0.4 * 0.8));
    regionServerAccounting.setTestMemstoreSize(0);
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.10f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.05f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    // Let the system start with default values for memstore heap and block cache size.
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), new RegionServerAccountingStub());
    long oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    blockCache.evictBlock(null);
    blockCache.evictBlock(null);
    blockCache.evictBlock(null);
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    assertHeapSpaceDelta(-(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE), oldMemstoreHeapSize,
        memStoreFlusher.memstoreSize);
    assertHeapSpaceDelta(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE, oldBlockCacheSize,
        blockCache.maxSize);
    oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    oldBlockCacheSize = blockCache.maxSize;
    // Do some more evictions before the next run of HeapMemoryTuner
    blockCache.evictBlock(null);
    Thread.sleep(1500);
    assertHeapSpaceDelta(-(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE), oldMemstoreHeapSize,
        memStoreFlusher.memstoreSize);
    assertHeapSpaceDelta(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE, oldBlockCacheSize,
        blockCache.maxSize);
  }

  @Test
  public void testWhenClusterIsHavingMoreWritesThanReads() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    RegionServerAccountingStub regionServerAccounting = new RegionServerAccountingStub();
    // Both memstore and block cache are nearly filled
    blockCache.setTestBlockSize(0);
    regionServerAccounting.setTestMemstoreSize((long) (maxHeapSize * 0.4 * 0.8));
    blockCache.setTestBlockSize((long) (maxHeapSize * 0.4 * 0.8));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.10f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.05f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    // Let the system start with default values for memstore heap and block cache size.
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), regionServerAccounting);
    long oldMemstoreHeapSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    memStoreFlusher.flushType = FlushType.ABOVE_HIGHER_MARK;
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    blockCache.evictBlock(null);
    memStoreFlusher.flushType = FlushType.ABOVE_LOWER_MARK;
    memStoreFlusher.requestFlush(null, false);
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    // No changes should happen as there is undefined increase in flushes and evictions
    assertEquals(oldMemstoreHeapSize, memStoreFlusher.memstoreSize);
    assertEquals(oldBlockCacheSize, blockCache.maxSize);
    // Do some more flushes before the next run of HeapMemoryTuner
    memStoreFlusher.flushType = FlushType.ABOVE_HIGHER_MARK;
    memStoreFlusher.requestFlush(null, false);
    memStoreFlusher.requestFlush(null, false);
    Thread.sleep(1500);
    assertHeapSpaceDelta(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE, oldMemstoreHeapSize,
        memStoreFlusher.memstoreSize);
    assertHeapSpaceDelta(-(DefaultHeapMemoryTuner.DEFAULT_MAX_STEP_VALUE), oldBlockCacheSize,
        blockCache.maxSize);
  }

  @Test
  public void testPluggingInHeapMemoryTuner() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.78f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.05f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.75f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.02f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    conf.setClass(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_CLASS, CustomHeapMemoryTuner.class,
        HeapMemoryTuner.class);
    // Let the system start with default values for memstore heap and block cache size.
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), new RegionServerAccountingStub());
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    // Now we wants to be in write mode. Set bigger memstore size from CustomHeapMemoryTuner
    CustomHeapMemoryTuner.memstoreSize = 0.78f;
    CustomHeapMemoryTuner.blockCacheSize = 0.02f;
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    assertHeapSpace(0.78f, memStoreFlusher.memstoreSize);// Memstore
    assertHeapSpace(0.02f, blockCache.maxSize);// BlockCache
    // Now we wants to be in read mode. Set bigger memstore size from CustomHeapMemoryTuner
    CustomHeapMemoryTuner.blockCacheSize = 0.75f;
    CustomHeapMemoryTuner.memstoreSize = 0.05f;
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    assertHeapSpace(0.75f, blockCache.maxSize);// BlockCache
    assertHeapSpace(0.05f, memStoreFlusher.memstoreSize);// Memstore
  }

  @Test
  public void testWhenSizeGivenByHeapTunerGoesOutsideRange() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    conf.setClass(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_CLASS, CustomHeapMemoryTuner.class,
        HeapMemoryTuner.class);
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), new RegionServerAccountingStub());
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    CustomHeapMemoryTuner.memstoreSize = 0.78f;
    CustomHeapMemoryTuner.blockCacheSize = 0.02f;
    Thread.sleep(1500); // Allow the tuner to run once and do necessary memory up
    // Even if the tuner says to set the memstore to 78%, HBase makes it as 70% as that is the
    // upper bound. Same with block cache as 10% is the lower bound.
    assertHeapSpace(0.7f, memStoreFlusher.memstoreSize);
    assertHeapSpace(0.1f, blockCache.maxSize);
  }

  @Test
  public void testWhenCombinedHeapSizesFromTunerGoesOutSideMaxLimit() throws Exception {
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.4));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    conf.setClass(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_CLASS, CustomHeapMemoryTuner.class,
        HeapMemoryTuner.class);
    HeapMemoryManager heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), new RegionServerAccountingStub());
    long oldMemstoreSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    CustomHeapMemoryTuner.memstoreSize = 0.7f;
    CustomHeapMemoryTuner.blockCacheSize = 0.3f;
    Thread.sleep(1500);
    assertEquals(oldMemstoreSize, memStoreFlusher.memstoreSize);
    assertEquals(oldBlockCacheSize, blockCache.maxSize);
  }

  @Test
  public void testWhenL2BlockCacheIsOnHeap() throws Exception {
    HeapMemoryManager heapMemoryManager = null;
    BlockCacheStub blockCache = new BlockCacheStub((long) (maxHeapSize * 0.4));
    MemstoreFlusherStub memStoreFlusher = new MemstoreFlusherStub((long) (maxHeapSize * 0.3));
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.7f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY, 0.1f);
    conf.setInt(DefaultHeapMemoryTuner.NUM_PERIODS_TO_IGNORE, 0);
    conf.setFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_KEY, 0.4F);
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.3F);
    conf.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 0.1F);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "heap");

    conf.setLong(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_PERIOD, 1000);
    conf.setClass(HeapMemoryManager.HBASE_RS_HEAP_MEMORY_TUNER_CLASS, CustomHeapMemoryTuner.class,
        HeapMemoryTuner.class);

    try {
      heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
          new RegionServerStub(conf), new RegionServerAccountingStub());
      fail("Should have failed as the collective heap memory need is above 80%");
    } catch (Exception e) {
    }

    // Change the max/min ranges for memstore and bock cache so as to pass the criteria check
    conf.setFloat(HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY, 0.6f);
    conf.setFloat(HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY, 0.6f);
    heapMemoryManager = new HeapMemoryManager(blockCache, memStoreFlusher,
        new RegionServerStub(conf), new RegionServerAccountingStub());
    long oldMemstoreSize = memStoreFlusher.memstoreSize;
    long oldBlockCacheSize = blockCache.maxSize;
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");
    heapMemoryManager.start(choreService);
    CustomHeapMemoryTuner.memstoreSize = 0.4f;
    CustomHeapMemoryTuner.blockCacheSize = 0.4f;
    Thread.sleep(1500);
    // The size should not get changes as the collection of memstore size and L1 and L2 block cache
    // size will cross the ax allowed 80% mark
    assertEquals(oldMemstoreSize, memStoreFlusher.memstoreSize);
    assertEquals(oldBlockCacheSize, blockCache.maxSize);
    CustomHeapMemoryTuner.memstoreSize = 0.1f;
    CustomHeapMemoryTuner.blockCacheSize = 0.5f;
    Thread.sleep(1500);
    assertHeapSpace(0.1f, memStoreFlusher.memstoreSize);
    assertHeapSpace(0.5f, blockCache.maxSize);
  }

  private void assertHeapSpace(float expectedHeapPercentage, long currentHeapSpace) {
    long expected = (long) (this.maxHeapSize * expectedHeapPercentage);
    assertEquals(expected, currentHeapSpace);
  }

  private void assertHeapSpaceDelta(double expectedDeltaPercent, long oldHeapSpace, long newHeapSpace) {
    double expctedMinDelta = (double) (this.maxHeapSize * expectedDeltaPercent);
    // Tolerable error
    double error = 0.999;
    if (expectedDeltaPercent > 0) {
      assertTrue(expctedMinDelta*error <= (double)(newHeapSpace - oldHeapSpace));
      assertTrue(expctedMinDelta/error >= (double)(newHeapSpace - oldHeapSpace));
    } else {
      assertTrue(-expctedMinDelta*error <= (double)(oldHeapSpace - newHeapSpace));
      assertTrue(-expctedMinDelta/error >= (double)(oldHeapSpace - newHeapSpace));
    }
  }

  private static class BlockCacheStub implements ResizableBlockCache {
    CacheStats stats = new CacheStats("test");
    long maxSize = 0;
    private long testBlockSize = 0;

    public BlockCacheStub(long size){
      this.maxSize = size;
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
        boolean cacheDataInL1) {

    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {

    }

    @Override
    public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
        boolean updateCacheMetrics) {
      return null;
    }

    @Override
    public boolean evictBlock(BlockCacheKey cacheKey) {
      stats.evicted(0);
      return false;
    }

    @Override
    public int evictBlocksByHfileName(String hfileName) {
      stats.evicted(0); // Just assuming only one block for file here.
      return 0;
    }

    @Override
    public CacheStats getStats() {
      return this.stats;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public long getFreeSize() {
      return 0;
    }

    @Override
    public long getCurrentSize() {
      return this.testBlockSize;
    }

    @Override
    public long getBlockCount() {
      return 0;
    }

    @Override
    public void setMaxSize(long size) {
      this.maxSize = size;
    }

    @Override
    public Iterator<CachedBlock> iterator() {
      return null;
    }

    @Override
    public BlockCache[] getBlockCaches() {
      return null;
    }

	public void setTestBlockSize(long testBlockSize) {
		this.testBlockSize = testBlockSize;
	}
  }

  private static class MemstoreFlusherStub implements FlushRequester {

    long memstoreSize;

    FlushRequestListener listener;

    FlushType flushType = FlushType.NORMAL;

    public MemstoreFlusherStub(long memstoreSize) {
      this.memstoreSize = memstoreSize;
    }

    @Override
    public void requestFlush(Region region, boolean forceFlushAllStores) {
      this.listener.flushRequested(flushType, region);
    }

    @Override
    public void requestDelayedFlush(Region region, long delay, boolean forceFlushAllStores) {

    }

    @Override
    public void registerFlushRequestListener(FlushRequestListener listener) {
      this.listener = listener;
    }

    @Override
    public boolean unregisterFlushRequestListener(FlushRequestListener listener) {
      return false;
    }

    @Override
    public void setGlobalMemstoreLimit(long globalMemStoreSize) {
      this.memstoreSize = globalMemStoreSize;
    }
  }

  private static class RegionServerStub implements Server {
    private Configuration conf;
    private boolean stopped = false;

    public RegionServerStub(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public void abort(String why, Throwable e) {

    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
      this.stopped = true;
    }

    @Override
    public boolean isStopped() {
      return this.stopped;
    }

    @Override
    public Configuration getConfiguration() {
      return this.conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf("server1",4000,12345);
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }
  }

  static class CustomHeapMemoryTuner implements HeapMemoryTuner {
    static float blockCacheSize = 0.4f;
    static float memstoreSize = 0.4f;

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void setConf(Configuration arg0) {

    }

    @Override
    public TunerResult tune(TunerContext context) {
      TunerResult result = new TunerResult(true);
      result.setBlockCacheSize(blockCacheSize);
      result.setMemstoreSize(memstoreSize);
      return result;
    }
  }

  private static class RegionServerAccountingStub extends RegionServerAccounting {
    private long testMemstoreSize = 0;
    @Override
    public long getGlobalMemstoreSize() {
      return testMemstoreSize;
    }
    public void setTestMemstoreSize(long testMemstoreSize) {
      this.testMemstoreSize = testMemstoreSize;
    }
  }
}
