/**
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
@SuppressWarnings("deprecation")
public class TestBlockEvictionFromClient {
  private static final Log LOG = LogFactory.getLog(TestBlockEvictionFromClient.class);
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static byte[][] ROWS = new byte[2][];
  private static int NO_OF_THREADS = 3;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] ROW1 = Bytes.toBytes("testRow1");
  private static byte[] ROW2 = Bytes.toBytes("testRow2");
  private static byte[] ROW3 = Bytes.toBytes("testRow3");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES_1 = new byte[1][0];
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
  private static byte[] data = new byte[1000];
  private static byte[] data2 = Bytes.add(data, data);
  protected static int SLAVES = 1;
  private static CountDownLatch latch;
  private static CountDownLatch getLatch;
  private static CountDownLatch compactionLatch;
  private static CountDownLatch exceptionLatch;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ROWS[0] = ROW;
    ROWS[1] = ROW1;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MultiRowMutationEndpoint.class.getName());
    conf.setBoolean("hbase.table.sanity.checks", true); // enable for below
                                                        // tests
    conf.setInt("hbase.regionserver.handler.count", 20);
    conf.setInt("hbase.bucketcache.size", 400);
    conf.setStrings("hbase.bucketcache.ioengine", "heap");
    conf.setFloat("hfile.block.cache.size", 0.2f);
    conf.setFloat("hbase.regionserver.global.memstore.size", 0.1f);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);// do not retry
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 5000);
    FAMILIES_1[0] = FAMILY;
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    CustomInnerRegionObserver.waitForGets.set(false);
    CustomInnerRegionObserver.countOfNext.set(0);
    CustomInnerRegionObserver.countOfGets.set(0);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    if (latch != null) {
      while (latch.getCount() > 0) {
        latch.countDown();
      }
    }
    if (getLatch != null) {
      getLatch.countDown();
    }
    if (compactionLatch != null) {
      compactionLatch.countDown();
    }
    if (exceptionLatch != null) {
      exceptionLatch.countDown();
    }
    latch = null;
    getLatch = null;
    compactionLatch = null;
    exceptionLatch = null;
    CustomInnerRegionObserver.throwException.set(false);
    // Clean up the tables for every test case
    TableName[] listTableNames = TEST_UTIL.getHBaseAdmin().listTableNames();
    for (TableName tableName : listTableNames) {
      if (!tableName.isSystemTable()) {
        TEST_UTIL.getHBaseAdmin().disableTable(tableName);
        TEST_UTIL.getHBaseAdmin().deleteTable(tableName);
      }
    }
  }

  @Test
  public void testBlockEvictionWithParallelScans() throws Exception {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testBlockEvictionWithParallelScans");
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      // insert data. 2 Rows are added
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
      // data was in memstore so don't expect any changes
      // flush the data
      System.out.println("Flushing cache in problematic area");
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // Load cache
      // Create three sets of scan
      ScanThread[] scanThreads = initiateScan(table, false);
      Thread.sleep(100);
      checkForBlockEviction(cache, false, false, false);
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
      // CustomInnerRegionObserver.sleepTime.set(0);
      Iterator<CachedBlock> iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // read the data and expect same blocks, one new hit, no misses
      assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // Check how this miss is happening
      // insert a second column, read the row, no new blocks, 3 new hits
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      byte[] data2 = Bytes.add(data, data);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      Result r = table.get(new Get(ROW));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // flush, one new block
      System.out.println("Flushing cache");
      region.flush(true);
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // compact, net minus two blocks, two hits, no misses
      System.out.println("Compacting");
      assertEquals(2, store.getStorefilesCount());
      store.triggerMajorCompaction();
      region.compact(true);
      waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
      assertEquals(1, store.getStorefilesCount());
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // read the row, this should be a cache miss because we don't cache data
      // blocks on compaction
      r = table.get(new Get(ROW));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testParallelGetsAndScans() throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(2);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testParallelGetsAndScans");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      insertData(table);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // Create three sets of scan
      CustomInnerRegionObserver.waitForGets.set(true);
      ScanThread[] scanThreads = initiateScan(table, false);
      // Create three sets of gets
      GetThread[] getThreads = initiateGet(table, false, false);
      checkForBlockEviction(cache, false, false, false);
      CustomInnerRegionObserver.waitForGets.set(false);
      checkForBlockEviction(cache, false, false, false);
      for (GetThread thread : getThreads) {
        thread.join();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false, false);
      getLatch.countDown();
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
      System.out.println("Scans should have returned the bloks");
      // Check with either true or false
      CustomInnerRegionObserver.waitForGets.set(false);
      // The scan should also have released the blocks by now
      checkForBlockEviction(cache, true, true, false);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testGetWithCellsInDifferentFiles() throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testGetWithCellsInDifferentFiles");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      region.flush(true);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      CustomInnerRegionObserver.waitForGets.set(true);
      // Create three sets of gets
      GetThread[] getThreads = initiateGet(table, false, false);
      Thread.sleep(200);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.join();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false, false);
      getLatch.countDown();
      System.out.println("Gets should have returned the bloks");
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  // TODO : check how block index works here
  public void testGetsWithMultiColumnsAndExplicitTracker()
      throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testGetsWithMultiColumnsAndExplicitTracker");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      BlockCache cache = setCacheProperties(region);
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      for (int i = 1; i < 10; i++) {
        put = new Put(ROW);
        put.addColumn(FAMILY, Bytes.toBytes("testQualifier" + i), data2);
        table.put(put);
        if (i % 2 == 0) {
          region.flush(true);
        }
      }
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      region.flush(true);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      CustomInnerRegionObserver.waitForGets.set(true);
      // Create three sets of gets
      GetThread[] getThreads = initiateGet(table, true, false);
      Thread.sleep(200);
      Iterator<CachedBlock> iterator = cache.iterator();
      boolean usedBlocksFound = false;
      int refCount = 0;
      int noOfBlocksWithRef = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          System.out.println("The refCount is " + refCount);
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
          noOfBlocksWithRef++;
        }
      }
      assertTrue(usedBlocksFound);
      // the number of blocks referred
      assertEquals(10, noOfBlocksWithRef);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.join();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false, false);
      getLatch.countDown();
      System.out.println("Gets should have returned the bloks");
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testGetWithMultipleColumnFamilies() throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testGetWithMultipleColumnFamilies");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      byte[][] fams = new byte[10][];
      fams[0] = FAMILY;
      for (int i = 1; i < 10; i++) {
        fams[i] = (Bytes.toBytes("testFamily" + i));
      }
      table = TEST_UTIL.createTable(tableName, fams, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      BlockCache cache = setCacheProperties(region);

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      for (int i = 1; i < 10; i++) {
        put = new Put(ROW);
        put.addColumn(Bytes.toBytes("testFamily" + i), Bytes.toBytes("testQualifier" + i), data2);
        table.put(put);
        if (i % 2 == 0) {
          region.flush(true);
        }
      }
      region.flush(true);
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      region.flush(true);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      CustomInnerRegionObserver.waitForGets.set(true);
      // Create three sets of gets
      GetThread[] getThreads = initiateGet(table, true, true);
      Thread.sleep(200);
      Iterator<CachedBlock> iterator = cache.iterator();
      boolean usedBlocksFound = false;
      int refCount = 0;
      int noOfBlocksWithRef = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          System.out.println("The refCount is " + refCount);
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
          noOfBlocksWithRef++;
        }
      }
      assertTrue(usedBlocksFound);
      // the number of blocks referred
      assertEquals(3, noOfBlocksWithRef);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.join();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false, false);
      getLatch.countDown();
      System.out.println("Gets should have returned the bloks");
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testBlockRefCountAfterSplits() throws IOException, InterruptedException {
    HTable table = null;
    try {
      TableName tableName = TableName.valueOf("testBlockRefCountAfterSplits");
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024);
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region =
          TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      put = new Put(ROW2);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      put = new Put(ROW3);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      region.flush(true);
      TEST_UTIL.getAdmin().split(tableName, ROW1);
      List<HRegionInfo> tableRegions = TEST_UTIL.getAdmin().getTableRegions(tableName);
      // Wait for splits
      while (tableRegions.size() != 2) {
        tableRegions = TEST_UTIL.getAdmin().getTableRegions(tableName);
        Thread.sleep(100);
      }
      region.compact(true);
      Iterator<CachedBlock> iterator = cache.iterator();
      // Though the split had created the HalfStorefileReader - the firstkey and lastkey scanners
      // should be closed inorder to return those blocks
      iterateBlockCache(cache, iterator);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testMultiGets() throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(2);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testMultiGets");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      region.flush(true);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      CustomInnerRegionObserver.waitForGets.set(true);
      // Create three sets of gets
      MultiGetThread[] getThreads = initiateMultiGet(table);
      Thread.sleep(200);
      int refCount;
      Iterator<CachedBlock> iterator = cache.iterator();
      boolean foundNonZeroBlock = false;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          assertEquals(NO_OF_THREADS, refCount);
          foundNonZeroBlock = true;
        }
      }
      assertTrue("Should have found nonzero ref count block",foundNonZeroBlock);
      CustomInnerRegionObserver.getCdl().get().countDown();
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (MultiGetThread thread : getThreads) {
        thread.join();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      iterateBlockCache(cache, iterator);
      getLatch.countDown();
      System.out.println("Gets should have returned the bloks");
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
  @Test
  public void testScanWithMultipleColumnFamilies() throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      TableName tableName = TableName.valueOf("testScanWithMultipleColumnFamilies");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      byte[][] fams = new byte[10][];
      fams[0] = FAMILY;
      for (int i = 1; i < 10; i++) {
        fams[i] = (Bytes.toBytes("testFamily" + i));
      }
      table = TEST_UTIL.createTable(tableName, fams, 1, 1024,
          CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      BlockCache cache = setCacheProperties(region);

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      region.flush(true);
      for (int i = 1; i < 10; i++) {
        put = new Put(ROW);
        put.addColumn(Bytes.toBytes("testFamily" + i), Bytes.toBytes("testQualifier" + i), data2);
        table.put(put);
        if (i % 2 == 0) {
          region.flush(true);
        }
      }
      region.flush(true);
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      region.flush(true);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      // Create three sets of gets
      ScanThread[] scanThreads = initiateScan(table, true);
      Thread.sleep(200);
      Iterator<CachedBlock> iterator = cache.iterator();
      boolean usedBlocksFound = false;
      int refCount = 0;
      int noOfBlocksWithRef = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          System.out.println("The refCount is " + refCount);
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
          noOfBlocksWithRef++;
        }
      }
      assertTrue(usedBlocksFound);
      // the number of blocks referred
      assertEquals(12, noOfBlocksWithRef);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false, false);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private BlockCache setCacheProperties(Region region) {
    Iterator<Store> strItr = region.getStores().iterator();
    BlockCache cache = null;
    while (strItr.hasNext()) {
      Store store = strItr.next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      // Use the last one
      cache = cacheConf.getBlockCache();
    }
    return cache;
  }

  @Test
  public void testParallelGetsAndScanWithWrappedRegionScanner() throws IOException,
      InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(2);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testParallelGetsAndScanWithWrappedRegionScanner");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      // insert data. 2 Rows are added
      insertData(table);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // CustomInnerRegionObserver.sleepTime.set(5000);
      // Create three sets of scan
      CustomInnerRegionObserver.waitForGets.set(true);
      ScanThread[] scanThreads = initiateScan(table, false);
      // Create three sets of gets
      GetThread[] getThreads = initiateGet(table, false, false);
      // The block would have been decremented for the scan case as it was
      // wrapped
      // before even the postNext hook gets executed.
      // giving some time for the block to be decremented
      Thread.sleep(100);
      CustomInnerRegionObserver.waitForGets.set(false);
      checkForBlockEviction(cache, false, false, true);
      // countdown the latch
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.join();
      }
      getLatch.countDown();
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testScanWithCompaction() throws IOException, InterruptedException {
    testScanWithCompactionInternals("testScanWithCompaction", false);
  }

  @Test
  public void testReverseScanWithCompaction() throws IOException, InterruptedException {
    testScanWithCompactionInternals("testReverseScanWithCompaction", true);
  }

  private void testScanWithCompactionInternals(String tableNameStr, boolean reversed)
      throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      compactionLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf(tableNameStr);
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      // insert data. 2 Rows are added
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // read the data and expect same blocks, one new hit, no misses
      int refCount = 0;
      // Check how this miss is happening
      // insert a second column, read the row, no new blocks, 3 new hits
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      byte[] data2 = Bytes.add(data, data);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      // flush, one new block
      System.out.println("Flushing cache");
      region.flush(true);
      Iterator<CachedBlock> iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // Create three sets of scan
      ScanThread[] scanThreads = initiateScan(table, reversed);
      Thread.sleep(100);
      iterator = cache.iterator();
      boolean usedBlocksFound = false;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      assertTrue("Blocks with non zero ref count should be found ", usedBlocksFound);
      usedBlocksFound = false;
      System.out.println("Compacting");
      assertEquals(2, store.getStorefilesCount());
      store.triggerMajorCompaction();
      region.compact(true);
      waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
      assertEquals(1, store.getStorefilesCount());
      // Even after compaction is done we will have some blocks that cannot
      // be evicted this is because the scan is still referencing them
      iterator = cache.iterator();
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3 as they are not yet cleared
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      assertTrue("Blocks with non zero ref count should be found ", usedBlocksFound);
      // Should not throw exception
      compactionLatch.countDown();
      latch.countDown();
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
      // by this time all blocks should have been evicted
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      Result r = table.get(new Get(ROW));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      // The gets would be working on new blocks
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testBlockEvictionAfterHBASE13082WithCompactionAndFlush()
      throws IOException, InterruptedException {
    // do flush and scan in parallel
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      compactionLatch = new CountDownLatch(1);
      TableName tableName =
          TableName.valueOf("testBlockEvictionAfterHBASE13082WithCompactionAndFlush");
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();

      // insert data. 2 Rows are added
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER, data);
      table.put(put);
      assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // read the data and expect same blocks, one new hit, no misses
      int refCount = 0;
      // Check how this miss is happening
      // insert a second column, read the row, no new blocks, 3 new hits
      byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      byte[] data2 = Bytes.add(data, data);
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      // flush, one new block
      System.out.println("Flushing cache");
      region.flush(true);
      Iterator<CachedBlock> iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
      // Create three sets of scan
      ScanThread[] scanThreads = initiateScan(table, false);
      Thread.sleep(100);
      iterator = cache.iterator();
      boolean usedBlocksFound = false;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      // Make a put and do a flush
      QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
      data2 = Bytes.add(data, data);
      put = new Put(ROW1);
      put.addColumn(FAMILY, QUALIFIER2, data2);
      table.put(put);
      // flush, one new block
      System.out.println("Flushing cache");
      region.flush(true);
      assertTrue("Blocks with non zero ref count should be found ", usedBlocksFound);
      usedBlocksFound = false;
      System.out.println("Compacting");
      assertEquals(3, store.getStorefilesCount());
      store.triggerMajorCompaction();
      region.compact(true);
      waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
      assertEquals(1, store.getStorefilesCount());
      // Even after compaction is done we will have some blocks that cannot
      // be evicted this is because the scan is still referencing them
      iterator = cache.iterator();
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3 as they are not yet cleared
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      assertTrue("Blocks with non zero ref count should be found ", usedBlocksFound);
      // Should not throw exception
      compactionLatch.countDown();
      latch.countDown();
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
      // by this time all blocks should have been evicted
      iterator = cache.iterator();
      // Since a flush and compaction happened after a scan started
      // we need to ensure that all the original blocks of the compacted file
      // is also removed.
      iterateBlockCache(cache, iterator);
      Result r = table.get(new Get(ROW));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
      assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      // The gets would be working on new blocks
      iterator = cache.iterator();
      iterateBlockCache(cache, iterator);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }


  @Test
  public void testScanWithException() throws IOException, InterruptedException {
    HTable table = null;
    try {
      latch = new CountDownLatch(1);
      exceptionLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf("testScanWithException");
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
          CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = table.getRegionLocator();
      String regionName = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      Region region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(
          regionName);
      Store store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache();
      // insert data. 2 Rows are added
      insertData(table);
      // flush the data
      System.out.println("Flushing cache");
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // CustomInnerRegionObserver.sleepTime.set(5000);
      CustomInnerRegionObserver.throwException.set(true);
      ScanThread[] scanThreads = initiateScan(table, false);
      // The block would have been decremented for the scan case as it was
      // wrapped
      // before even the postNext hook gets executed.
      // giving some time for the block to be decremented
      Thread.sleep(100);
      Iterator<CachedBlock> iterator = cache.iterator();
      boolean usedBlocksFound = false;
      int refCount = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      assertTrue(usedBlocksFound);
      exceptionLatch.countDown();
      // countdown the latch
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (ScanThread thread : scanThreads) {
        thread.join();
      }
      iterator = cache.iterator();
      usedBlocksFound = false;
      refCount = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      assertTrue(usedBlocksFound);
      // Sleep till the scan lease would expire? Can we reduce this value?
      Thread.sleep(5100);
      iterator = cache.iterator();
      refCount = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
        } else {
          continue;
        }
        assertEquals(0, refCount);
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private void iterateBlockCache(BlockCache cache, Iterator<CachedBlock> iterator) {
    int refCount;
    while (iterator.hasNext()) {
      CachedBlock next = iterator.next();
      BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
      if (cache instanceof BucketCache) {
        refCount = ((BucketCache) cache).getRefCount(cacheKey);
      } else if (cache instanceof CombinedBlockCache) {
        refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
      } else {
        continue;
      }
      assertEquals(0, refCount);
    }
  }

  private void insertData(HTable table) throws IOException {
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, data);
    table.put(put);
    put = new Put(ROW1);
    put.addColumn(FAMILY, QUALIFIER, data);
    table.put(put);
    byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
    put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER2, data2);
    table.put(put);
  }

  private ScanThread[] initiateScan(HTable table, boolean reverse) throws IOException,
      InterruptedException {
    ScanThread[] scanThreads = new ScanThread[NO_OF_THREADS];
    for (int i = 0; i < NO_OF_THREADS; i++) {
      scanThreads[i] = new ScanThread(table, reverse);
    }
    for (ScanThread thread : scanThreads) {
      thread.start();
    }
    return scanThreads;
  }

  private GetThread[] initiateGet(HTable table, boolean tracker, boolean multipleCFs)
      throws IOException, InterruptedException {
    GetThread[] getThreads = new GetThread[NO_OF_THREADS];
    for (int i = 0; i < NO_OF_THREADS; i++) {
      getThreads[i] = new GetThread(table, tracker, multipleCFs);
    }
    for (GetThread thread : getThreads) {
      thread.start();
    }
    return getThreads;
  }

  private MultiGetThread[] initiateMultiGet(HTable table)
      throws IOException, InterruptedException {
    MultiGetThread[] multiGetThreads = new MultiGetThread[NO_OF_THREADS];
    for (int i = 0; i < NO_OF_THREADS; i++) {
      multiGetThreads[i] = new MultiGetThread(table);
    }
    for (MultiGetThread thread : multiGetThreads) {
      thread.start();
    }
    return multiGetThreads;
  }

  private void checkForBlockEviction(BlockCache cache, boolean getClosed, boolean expectOnlyZero,
      boolean wrappedCp) throws InterruptedException {
    int counter = NO_OF_THREADS;
    if (CustomInnerRegionObserver.waitForGets.get()) {
      // Because only one row is selected, it has only 2 blocks
      counter = counter - 1;
      while (CustomInnerRegionObserver.countOfGets.get() < NO_OF_THREADS) {
        Thread.sleep(100);
      }
    } else {
      while (CustomInnerRegionObserver.countOfNext.get() < NO_OF_THREADS) {
        Thread.sleep(100);
      }
    }
    Iterator<CachedBlock> iterator = cache.iterator();
    int refCount = 0;
    while (iterator.hasNext()) {
      CachedBlock next = iterator.next();
      BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
      if (cache instanceof BucketCache) {
        refCount = ((BucketCache) cache).getRefCount(cacheKey);
      } else if (cache instanceof CombinedBlockCache) {
        refCount = ((CombinedBlockCache) cache).getRefCount(cacheKey);
      } else {
        continue;
      }
      System.out.println(" the refcount is " + refCount + " block is " + cacheKey);
      if (CustomInnerRegionObserver.waitForGets.get()) {
        if (expectOnlyZero) {
          assertTrue(refCount == 0);
        }
        if (refCount != 0) {
          // Because the scan would have also touched up on these blocks but
          // it
          // would have touched
          // all 3
          if (getClosed) {
            // If get has closed only the scan's blocks would be available
            assertEquals(refCount, CustomInnerRegionObserver.countOfGets.get());
          } else {
              assertEquals(refCount, CustomInnerRegionObserver.countOfGets.get() + (NO_OF_THREADS));
          }
        }
      } else {
        // Because the get would have also touched up on these blocks but it
        // would have touched
        // upon only 2 additionally
        if (expectOnlyZero) {
          assertTrue(refCount == 0);
        }
        if (refCount != 0) {
          if (getLatch == null || wrappedCp) {
            assertEquals(refCount, CustomInnerRegionObserver.countOfNext.get());
          } else {
            assertEquals(refCount, CustomInnerRegionObserver.countOfNext.get() + (NO_OF_THREADS));
          }
        }
      }
    }
    CustomInnerRegionObserver.getCdl().get().countDown();
  }

  private static class MultiGetThread extends Thread {
    private final HTable table;
    private final List<Get> gets = new ArrayList<Get>();
    public MultiGetThread(HTable table) {
      this.table = table;
    }
    @Override
    public void run() {
      gets.add(new Get(ROW));
      gets.add(new Get(ROW1));
      try {
        CustomInnerRegionObserver.getCdl().set(latch);
        Result[] r = table.get(gets);
        assertTrue(Bytes.equals(r[0].getRow(), ROW));
        assertTrue(Bytes.equals(r[1].getRow(), ROW1));
      } catch (IOException e) {
      }
    }
  }

  private static class GetThread extends Thread {
    private final HTable table;
    private final boolean tracker;
    private final boolean multipleCFs;

    public GetThread(HTable table, boolean tracker, boolean multipleCFs) {
      this.table = table;
      this.tracker = tracker;
      this.multipleCFs = multipleCFs;
    }

    @Override
    public void run() {
      try {
        initiateGet(table);
      } catch (IOException e) {
        // do nothing
      }
    }

    private void initiateGet(HTable table) throws IOException {
      Get get = new Get(ROW);
      if (tracker) {
        // Change this
        if (!multipleCFs) {
          get.addColumn(FAMILY, Bytes.toBytes("testQualifier" + 3));
          get.addColumn(FAMILY, Bytes.toBytes("testQualifier" + 8));
          get.addColumn(FAMILY, Bytes.toBytes("testQualifier" + 9));
          // Unknown key
          get.addColumn(FAMILY, Bytes.toBytes("testQualifier" + 900));
        } else {
          get.addColumn(Bytes.toBytes("testFamily" + 3), Bytes.toBytes("testQualifier" + 3));
          get.addColumn(Bytes.toBytes("testFamily" + 8), Bytes.toBytes("testQualifier" + 8));
          get.addColumn(Bytes.toBytes("testFamily" + 9), Bytes.toBytes("testQualifier" + 9));
          // Unknown key
          get.addColumn(Bytes.toBytes("testFamily" + 9), Bytes.toBytes("testQualifier" + 900));
        }
      }
      CustomInnerRegionObserver.getCdl().set(latch);
      Result r = table.get(get);
      System.out.println(r);
      if (!tracker) {
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
      } else {
        if (!multipleCFs) {
          assertTrue(Bytes.equals(r.getValue(FAMILY, Bytes.toBytes("testQualifier" + 3)), data2));
          assertTrue(Bytes.equals(r.getValue(FAMILY, Bytes.toBytes("testQualifier" + 8)), data2));
          assertTrue(Bytes.equals(r.getValue(FAMILY, Bytes.toBytes("testQualifier" + 9)), data2));
        } else {
          assertTrue(Bytes.equals(
              r.getValue(Bytes.toBytes("testFamily" + 3), Bytes.toBytes("testQualifier" + 3)),
              data2));
          assertTrue(Bytes.equals(
              r.getValue(Bytes.toBytes("testFamily" + 8), Bytes.toBytes("testQualifier" + 8)),
              data2));
          assertTrue(Bytes.equals(
              r.getValue(Bytes.toBytes("testFamily" + 9), Bytes.toBytes("testQualifier" + 9)),
              data2));
        }
      }
    }
  }

  private static class ScanThread extends Thread {
    private final HTable table;
    private final boolean reverse;

    public ScanThread(HTable table, boolean reverse) {
      this.table = table;
      this.reverse = reverse;
    }

    @Override
    public void run() {
      try {
        initiateScan(table);
      } catch (IOException e) {
        // do nothing
      }
    }

    private void initiateScan(HTable table) throws IOException {
      Scan scan = new Scan();
      if (reverse) {
        scan.setReversed(true);
      }
      CustomInnerRegionObserver.getCdl().set(latch);
      ResultScanner resScanner = table.getScanner(scan);
      int i = (reverse ? ROWS.length - 1 : 0);
      boolean resultFound = false;
      for (Result result : resScanner) {
        resultFound = true;
        System.out.println(result);
        if (!reverse) {
          assertTrue(Bytes.equals(result.getRow(), ROWS[i]));
          i++;
        } else {
          assertTrue(Bytes.equals(result.getRow(), ROWS[i]));
          i--;
        }
      }
      assertTrue(resultFound);
    }
  }

  private void waitForStoreFileCount(Store store, int count, int timeout)
      throws InterruptedException {
    long start = System.currentTimeMillis();
    while (start + timeout > System.currentTimeMillis() && store.getStorefilesCount() != count) {
      Thread.sleep(100);
    }
    System.out.println("start=" + start + ", now=" + System.currentTimeMillis() + ", cur="
        + store.getStorefilesCount());
    assertEquals(count, store.getStorefilesCount());
  }

  private static class CustomScanner implements RegionScanner {

    private RegionScanner delegate;

    public CustomScanner(RegionScanner delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      return delegate.next(results);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
      return delegate.next(result, scannerContext);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
      return delegate.nextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext context) throws IOException {
      boolean nextRaw = delegate.nextRaw(result, context);
      if (compactionLatch != null && compactionLatch.getCount() > 0) {
        try {
          compactionLatch.await();
        } catch (InterruptedException ie) {
        }
      }

      if (CustomInnerRegionObserver.throwException.get()) {
        if (exceptionLatch.getCount() > 0) {
          try {
            exceptionLatch.await();
          } catch (InterruptedException e) {
          }
          throw new IOException("throw exception");
        }
      }
      return nextRaw;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public HRegionInfo getRegionInfo() {
      return delegate.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
      return delegate.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
      return false;
    }

    @Override
    public long getMaxResultSize() {
      return delegate.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
      return delegate.getMvccReadPoint();
    }

    @Override
    public int getBatch() {
      return delegate.getBatch();
    }

    @Override
    public void shipped() throws IOException {
      this.delegate.shipped();
    }
  }

  public static class CustomInnerRegionObserverWrapper extends CustomInnerRegionObserver {
    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
        Scan scan, RegionScanner s) throws IOException {
      return new CustomScanner(s);
    }
  }

  public static class CustomInnerRegionObserver extends BaseRegionObserver {
    static final AtomicLong sleepTime = new AtomicLong(0);
    static final AtomicBoolean slowDownNext = new AtomicBoolean(false);
    static final AtomicInteger countOfNext = new AtomicInteger(0);
    static final AtomicInteger countOfGets = new AtomicInteger(0);
    static final AtomicBoolean waitForGets = new AtomicBoolean(false);
    static final AtomicBoolean throwException = new AtomicBoolean(false);
    private static final AtomicReference<CountDownLatch> cdl = new AtomicReference<CountDownLatch>(
        new CountDownLatch(0));

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e,
        InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
      slowdownCode(e, false);
      if (getLatch != null && getLatch.getCount() > 0) {
        try {
          getLatch.await();
        } catch (InterruptedException e1) {
        }
      }
      return super.postScannerNext(e, s, results, limit, hasMore);
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
        List<Cell> results) throws IOException {
      slowdownCode(e, true);
      super.postGetOp(e, get, results);
    }

    public static AtomicReference<CountDownLatch> getCdl() {
      return cdl;
    }

    private void slowdownCode(final ObserverContext<RegionCoprocessorEnvironment> e,
        boolean isGet) {
      CountDownLatch latch = getCdl().get();
      try {
        System.out.println(latch.getCount() + " is the count " + isGet);
        if (latch.getCount() > 0) {
          if (isGet) {
            countOfGets.incrementAndGet();
          } else {
            countOfNext.incrementAndGet();
          }
          LOG.info("Waiting for the counterCountDownLatch");
          latch.await(2, TimeUnit.MINUTES); // To help the tests to finish.
          if (latch.getCount() > 0) {
            throw new RuntimeException("Can't wait more");
          }
        }
      } catch (InterruptedException e1) {
        LOG.error(e1);
      }
    }
  }
}
