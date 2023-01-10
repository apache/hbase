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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ LargeTests.class, ClientTests.class })
@SuppressWarnings("deprecation")
public class TestBlockEvictionFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBlockEvictionFromClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBlockEvictionFromClient.class);
  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  static byte[][] ROWS = new byte[2][];
  private static int NO_OF_THREADS = 3;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] ROW1 = Bytes.toBytes("testRow1");
  private static byte[] ROW2 = Bytes.toBytes("testRow2");
  private static byte[] ROW3 = Bytes.toBytes("testRow3");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] FAMILY2 = Bytes.toBytes("testFamily1");
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

  @Rule
  public TestName name = new TestName();

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
    conf.setInt("hbase.regionserver.handler.count", 20);
    conf.setInt("hbase.bucketcache.size", 400);
    conf.setStrings(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
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
    TableName[] listTableNames = TEST_UTIL.getAdmin().listTableNames();
    for (TableName tableName : listTableNames) {
      if (!tableName.isSystemTable()) {
        TEST_UTIL.getAdmin().disableTable(tableName);
        TEST_UTIL.getAdmin().deleteTable(tableName);
      }
    }
  }

  @Test
  public void testBlockEvictionWithParallelScans() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
      // Should create one Hfile with 2 blocks
      region.flush(true);
      // Load cache
      // Create three sets of scan
      ScanThread[] scanThreads = initiateScan(table, false);
      Thread.sleep(100);
      checkForBlockEviction(cache, false, false);
      for (ScanThread thread : scanThreads) {
        thread.joinAndRethrow();
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
  public void testParallelGetsAndScans() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(2);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
      checkForBlockEviction(cache, false, false);
      CustomInnerRegionObserver.waitForGets.set(false);
      checkForBlockEviction(cache, false, false);
      for (GetThread thread : getThreads) {
        thread.joinAndRethrow();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false);
      getLatch.countDown();
      for (ScanThread thread : scanThreads) {
        thread.joinAndRethrow();
      }
      System.out.println("Scans should have returned the bloks");
      // Check with either true or false
      CustomInnerRegionObserver.waitForGets.set(false);
      // The scan should also have released the blocks by now
      checkForBlockEviction(cache, true, true);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testGetWithCellsInDifferentFiles() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
        thread.joinAndRethrow();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false);
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
  public void testGetsWithMultiColumnsAndExplicitTracker() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
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
      int noOfBlocksWithRef = countReferences(cache);
      // 3 blocks for the 3 returned cells, plus 1 extra because we don't fully exhaust one of the
      // storefiles so one remains in curBlock after we SEEK_NEXT_ROW
      assertEquals(4, noOfBlocksWithRef);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.joinAndRethrow();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false);
      getLatch.countDown();
      System.out.println("Gets should have returned the bloks");
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testGetWithMultipleColumnFamilies() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      byte[][] fams = new byte[10][];
      fams[0] = FAMILY;
      for (int i = 1; i < 10; i++) {
        fams[i] = (Bytes.toBytes("testFamily" + i));
      }
      table =
        TEST_UTIL.createTable(tableName, fams, 1, 1024, CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
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
      int noOfBlocksWithRef = countReferences(cache);
      // the number of blocks referred
      assertEquals(3, noOfBlocksWithRef);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.joinAndRethrow();
      }
      // Verify whether the gets have returned the blocks that it had
      CustomInnerRegionObserver.waitForGets.set(true);
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false);
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
    Table table = null;
    try {
      final TableName tableName = TableName.valueOf(name.getMethodName());
      TableDescriptor desc = TEST_UTIL.createTableDescriptor(tableName);
      // This test expects rpc refcount of cached data blocks to be 0 after split. After split,
      // two daughter regions are opened and a compaction is scheduled to get rid of reference
      // of the parent region hfiles. Compaction will increase refcount of cached data blocks by 1.
      // It is flakey since compaction can kick in anytime. To solve this issue, table is created
      // with compaction disabled.
      table = TEST_UTIL.createTable(
        TableDescriptorBuilder.newBuilder(desc).setCompactionEnabled(false).build(), FAMILIES_1,
        null, BloomType.ROW, 1024, null);
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
      ServerName rs = Iterables.getOnlyElement(TEST_UTIL.getAdmin().getRegionServers());
      int regionCount = TEST_UTIL.getAdmin().getRegions(rs).size();
      LOG.info("About to SPLIT on {} {}, count={}", Bytes.toString(ROW1), region.getRegionInfo(),
        regionCount);
      TEST_UTIL.getAdmin().split(tableName, ROW1);
      // Wait for splits
      TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getAdmin().getRegions(rs).size() > regionCount);
      region.compact(true);
      List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegionServer(rs).getRegions();
      for (HRegion r : regions) {
        LOG.info("" + r.getCompactionState());
        TEST_UTIL.waitFor(30000, () -> r.getCompactionState().equals(CompactionState.NONE));
      }
      LOG.info("Split finished, is region closed {} {}", region.isClosed(), cache);
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
  public void testMultiGets() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(2);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
      Iterator<CachedBlock> iterator = cache.iterator();
      int noOfBlocksWithRef = countReferences(cache);
      assertTrue("Should have found nonzero ref count block", noOfBlocksWithRef > 0);
      CustomInnerRegionObserver.getCdl().get().countDown();
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (MultiGetThread thread : getThreads) {
        thread.joinAndRethrow();
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
  public void testScanWithMultipleColumnFamilies() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      // Check if get() returns blocks on its close() itself
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      byte[][] fams = new byte[10][];
      fams[0] = FAMILY;
      for (int i = 1; i < 10; i++) {
        fams[i] = (Bytes.toBytes("testFamily" + i));
      }
      table =
        TEST_UTIL.createTable(tableName, fams, 1, 1024, CustomInnerRegionObserver.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
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
      int noOfBlocksWithRef = countReferences(cache);
      // the number of blocks referred
      assertEquals(12, noOfBlocksWithRef);
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (ScanThread thread : scanThreads) {
        thread.joinAndRethrow();
      }
      // giving some time for the block to be decremented
      checkForBlockEviction(cache, true, false);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * This test is a baseline for the below filtered tests. It proves that a full unfiltered scan
   * should retain 12 blocks based on the test data in {@link #setupStreamScanTest()}. Prior to
   * HBASE-27227, a filtered scan would retain the same number of blocks. The further tests below
   * show that with HBASE-27227 the filtered scans retain far fewer blocks. <br>
   * We use a stream scan to avoid switching to stream mid-request. This throws off the counting due
   * to how the test coordinates with a countdown latch. <br>
   * In addition to verifying the actual data returned by every scan,
   * {@link #countReferences(BlockCache)} also corrupts the byte buffs allocated to any blocks we
   * eagerly release due to checkpointing. This validates that our checkpointing does not release
   * any blocks that are necessary to serve the scan request. If we did, it'd blow up the test
   * trying to create a cell block response from corrupted cells.
   */
  @Test
  public void testStreamScan() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // setupStreamScanTest writes 12 cells, each large enough to consume an entire block.
      // A "full table scan" will retain all 12 blocks.
      assertNoBlocksWithRef(testCase.table, testCase.baseScan, testCase.cache, 12,
        new ExpectedResult(ROW, FAMILY, QUALIFIER, data2),
        new ExpectedResult(ROW, FAMILY, QUALIFIER2, data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier1"), data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier2"), data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier3"), data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier4"), data2),
        new ExpectedResult(ROW1, FAMILY2, Bytes.toBytes("testQualifier1"), data2),
        new ExpectedResult(ROW1, FAMILY2, Bytes.toBytes("testQualifier2"), data2),
        new ExpectedResult(ROW1, FAMILY2, Bytes.toBytes("testQualifier3"), data2),
        new ExpectedResult(ROW1, FAMILY2, Bytes.toBytes("testQualifier4"), data2),
        new ExpectedResult(ROW3, FAMILY, QUALIFIER, data),
        new ExpectedResult(ROW3, FAMILY2, QUALIFIER2, data2));
    }
  }

  @Test
  public void testStreamScanWithOneColumn() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // We expect 2 blocks. Everything is filtered out at the StoreScanner layer, so we can
      // just not retain the blocks for excluded cells right away.
      byte[] qualifier = Bytes.toBytes("testQualifier1");
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan
          .setFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(qualifier))),
        testCase.cache, 2, new ExpectedResult(ROW1, FAMILY, qualifier, data2),
        new ExpectedResult(ROW1, FAMILY2, qualifier, data2));
    }
  }

  @Test
  public void testStreamScanWithOneColumnQualifierFilter() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // This is the same as testStreamScanWithOneColumn but using a filter instead. Same reasoning
      // as that test.
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan
          .setFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(QUALIFIER))),
        testCase.cache, 2, new ExpectedResult(ROW, FAMILY, QUALIFIER, data2),
        new ExpectedResult(ROW3, FAMILY, QUALIFIER, data));
    }
  }

  @Test
  public void testStreamScanWithRowFilter() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // we expect 2 because the "ROW" row has 2 cell and all cells are 1 block
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan
          .setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW))),
        testCase.cache, 2, new ExpectedResult(ROW, FAMILY, QUALIFIER, data2),
        new ExpectedResult(ROW, FAMILY, QUALIFIER2, data2));
    }
  }

  @Test
  public void testStreamScanWithRowFilterMixedRows() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // we expect 2 because each of "ROW" and "ROW3" have 1 cell that match the filter
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan.addColumn(FAMILY, QUALIFIER)
          .setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
            new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW)),
            new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW3)))),
        testCase.cache, 2, new ExpectedResult(ROW, FAMILY, QUALIFIER, data2),
        new ExpectedResult(ROW3, FAMILY, QUALIFIER, data));
    }
  }

  @Test
  public void testStreamScanWithRowOffset() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // we expect 4, because 2 rows (ROW and ROW1) have enough columns in FAMILY to exceed offset.
      // ROW has 1 column, and ROW1 has 3. Each retains 1 block.
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan.addFamily(FAMILY).setRowOffsetPerColumnFamily(1), testCase.cache, 4,
        new ExpectedResult(ROW, FAMILY, QUALIFIER2, data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier2"), data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier3"), data2),
        new ExpectedResult(ROW1, FAMILY, Bytes.toBytes("testQualifier4"), data2));
    }
  }

  @Test
  public void testStreamScanWithRowOffsetAndRowFilter() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // we expect 1 because while both ROW and ROW1 have enough columns to exceed offset, we
      // drop ROW1 due to filterRow in RegionScannerImpl.
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan.addFamily(FAMILY).setRowOffsetPerColumnFamily(1)
          .setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW))),
        testCase.cache, 1, new ExpectedResult(ROW, FAMILY, QUALIFIER2, data2));
    }
  }

  @Test
  public void testStreamScanWithSingleColumnValueExcludeFilter() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // We expect 5 blocks. Initially, all 7 storefiles are opened. But FAMILY is essential due to
      // setFilterIfMissing, so only storefiles within that family are iterated unless a match is
      // found. 4 Storefiles are for FAMILY, the remaining 3 are FAMILY2. The row we match on (ROW)
      // doesn't have any cells in FAMILY2, so we don't end up iterating those storefiles. So the
      // initial blocks remain open and untouched -- this counts for 3 of the 5.
      // Multiple rows match FAMILY, two rows match FAMILY/QUALIFIER, but only one row also matches
      // the value. That row has 2 blocks, one for each cell. That's the remaining 2.
      // SingleColumnValueExcludeFilter does not return the matched column, so we're only returning
      // 1 of those cells (thus 1 of those blocks) but we can't release the other one since
      // checkpointing based on filterRow happens at the row level and the row itself is returned.
      SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(FAMILY, QUALIFIER,
        CompareOperator.EQUAL, new BinaryComparator(data2));
      filter.setFilterIfMissing(true);
      assertNoBlocksWithRef(testCase.table, testCase.baseScan.setFilter(filter), testCase.cache, 5,
        new ExpectedResult(ROW, FAMILY, QUALIFIER2, data2));
    }
  }

  @Test
  public void testStreamScanWithSingleColumnValueExcludeFilterJoinedHeap() throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // We expect 2 blocks. Unlike the last test, this one actually iterates the joined heap. So
      // we end up exhausting more of the storefiles and being able to relase more blocks. We end up
      // retaining 1 block for the match on the SingleColumnValueExcludeFilter, then 1 block from
      // the joined heap on that same row (ROW3). We can eagerly release all of the other blocks. We
      // only return 1 cell, but still retain 2 blocks checkpointing happens at the row boundary.
      // Since at least 1 cell is returned for the row, we keep both blocks.
      SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(FAMILY, QUALIFIER,
        CompareOperator.EQUAL, new BinaryComparator(data));
      filter.setFilterIfMissing(true);
      assertNoBlocksWithRef(testCase.table, testCase.baseScan.setFilter(filter), testCase.cache, 2,
        new ExpectedResult(ROW3, FAMILY2, QUALIFIER2, data2));
    }
  }

  @Test
  public void testStreamScanWithSingleColumnValueExcludeFilterJoinedHeapColumnExcluded()
    throws Throwable {
    try (TestCase testCase = setupStreamScanTest()) {
      // We expect 0 blocks. Another permutation of joined heap tests, this one uses a filter list
      // to cause the joined heap to return nothing. ROW3 is matched by
      // SingleColumnValueExcludeFilter. That filter excludes the matched column, and causes joined
      // heap to be checked. The joined heap returns nothing, because FAMILY2 only contains
      // QUALIFIER2 while we want QUALIFIER. The final result is an empty list, so we can release
      // all blocks accumulated.
      SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(FAMILY, QUALIFIER,
        CompareOperator.EQUAL, new BinaryComparator(data));
      filter.setFilterIfMissing(true);
      assertNoBlocksWithRef(testCase.table,
        testCase.baseScan.setFilter(new FilterList(filter,
          new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(QUALIFIER)))),
        testCase.cache, 0);
    }
  }

  private static final class TestCase implements AutoCloseable {
    private final Table table;
    private final BlockCache cache;
    private final Scan baseScan;

    private TestCase(Table table, BlockCache cache, Scan baseScan) {
      this.table = table;
      this.cache = cache;
      this.baseScan = baseScan;
    }

    @Override
    public void close() throws Exception {
      if (table != null) {
        table.close();
      }
    }
  }

  private TestCase setupStreamScanTest() throws IOException, InterruptedException {
    latch = new CountDownLatch(1);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[][] fams = new byte[][] { FAMILY, FAMILY2 };
    Table table =
      TEST_UTIL.createTable(tableName, fams, 1, 1024, CustomInnerRegionObserver.class.getName());
    // get the block cache and region
    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
    String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
    BlockCache cache = setCacheProperties(region);

    // this writes below all create 12 cells total, with each cell being an entire block.
    // so 12 blocks total as well.
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, data2);
    put.addColumn(FAMILY, QUALIFIER2, data2);
    table.put(put);
    region.flush(true);
    put = new Put(ROW3);
    put.addColumn(FAMILY, QUALIFIER, data);
    put.addColumn(FAMILY2, QUALIFIER2, data2);
    table.put(put);
    region.flush(true);
    // below creates 8 of the 12 cells
    for (int i = 1; i < 5; i++) {
      byte[] qualifier = Bytes.toBytes("testQualifier" + i);
      put = new Put(ROW1);
      put.addColumn(FAMILY, qualifier, data2);
      put.addColumn(FAMILY2, qualifier, data2);
      table.put(put);
      if (i % 2 == 0) {
        region.flush(true);
      }
    }
    region.flush(true);
    // flush the data
    System.out.println("Flushing cache");
    return new TestCase(table, cache, new Scan().setReadType(Scan.ReadType.STREAM));
  }

  private void assertNoBlocksWithRef(Table table, Scan scan, BlockCache cache, int expectedBlocks,
    ExpectedResult... expectedResults) throws Throwable {
    ScanThread[] scanThreads = initiateScan(table, scan, expectedResults);
    Thread.sleep(500);

    int noOfBlocksWithRef = countReferences(cache);
    // the number of blocks referred
    assertEquals(expectedBlocks, noOfBlocksWithRef);
    CustomInnerRegionObserver.getCdl().get().countDown();
    for (ScanThread thread : scanThreads) {
      thread.joinAndRethrow();
    }
    // giving some time for the block to be decremented
    checkForBlockEviction(cache, true, false);
  }

  private static final class ExpectedResult {
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private final byte[] value;

    private ExpectedResult(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
      this.row = row;
      this.family = family;
      this.qualifier = qualifier;
      this.value = value;
    }

    public void assertEquals(Cell cell, int index) {
      assertTrue(getAssertMessage(index, "row", row, cell), CellUtil.matchingRows(cell, row));
      assertTrue(getAssertMessage(index, "family", family, cell),
        CellUtil.matchingFamily(cell, family));
      assertTrue(getAssertMessage(index, "qualifier", qualifier, cell),
        CellUtil.matchingQualifier(cell, qualifier));
      assertTrue(getAssertMessage(index, "value", value, cell),
        CellUtil.matchingValue(cell, value));
    }

    private String getAssertMessage(int index, String component, byte[] value, Cell cell) {
      return "Expected cell " + (index + 1) + " to have " + component + " "
        + Bytes.toStringBinary(value) + ": " + CellUtil.toString(cell, true);
    }
  }

  /**
   * Counts how many blocks still have references, expecting each of those blocks to have 1
   * reference per NO_OF_THREADS. <br>
   * Additionally, manipulates the bucket cache to "corrupt" any cells still referencing blocks that
   * should not have any references. It does this by evicting those blocks and re-caching them in a
   * different order. This causes the content of the buffers backing those cells to be the wrong
   * size/position/data. As a result, if any cells do still reference blocks that they shouldn't,
   * the requests will fail loudly at the RPC serialization step, failing the tests.
   */
  private int countReferences(BlockCache cache) {
    BucketCache bucketCache;
    if (cache instanceof CombinedBlockCache) {
      bucketCache = (BucketCache) ((CombinedBlockCache) cache).getSecondLevelCache();
    } else if (cache instanceof BucketCache) {
      bucketCache = (BucketCache) cache;
    } else {
      throw new RuntimeException("Expected bucket cache but got " + cache);
    }

    Iterator<CachedBlock> iterator = bucketCache.iterator();
    int refCount;
    int noOfBlocksWithRef = 0;

    Map<BlockCacheKey, Cacheable> unreferencedBlocks = new HashMap<>();
    List<BlockCacheKey> cacheKeys = new ArrayList<>();

    while (iterator.hasNext()) {
      CachedBlock next = iterator.next();
      BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
      refCount = bucketCache.getRpcRefCount(cacheKey);

      if (refCount != 0) {
        // Blocks will be with count 3
        System.out.println("The refCount is " + refCount);
        assertEquals(NO_OF_THREADS, refCount);
        noOfBlocksWithRef++;
      } else if (cacheKey.getBlockType().isData()) {
        System.out.println("Corrupting block " + cacheKey);
        HFileBlock block = (HFileBlock) bucketCache.getBlock(cacheKey, false, false, false);

        // Clone to heap, then release and evict the block. This will cause the bucket cache
        // to reclaim memory that is currently referenced by these blocks.
        HFileBlock clonedBlock = HFileBlock.deepCloneOnHeap(block);
        block.release();
        bucketCache.evictBlock(cacheKey);

        cacheKeys.add(cacheKey);
        unreferencedBlocks.put(cacheKey, clonedBlock);
      }
    }

    // Write the blocks back to the bucket cache in a random order so they end up
    // in the wrong offsets. This causes the ByteBufferExtendedCell in our results to be
    // referencing the wrong spots if we erroneously released blocks that matter for the scanner
    Collections.shuffle(cacheKeys);

    for (BlockCacheKey cacheKey : cacheKeys) {
      bucketCache.cacheBlock(cacheKey, unreferencedBlocks.get(cacheKey));
    }

    System.out.println("Done corrupting blocks");

    return noOfBlocksWithRef;
  }

  private BlockCache setCacheProperties(HRegion region) {
    Iterator<HStore> strItr = region.getStores().iterator();
    BlockCache cache = null;
    while (strItr.hasNext()) {
      HStore store = strItr.next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      // Use the last one
      cache = cacheConf.getBlockCache().get();
    }
    return cache;
  }

  @Test
  public void testParallelGetsAndScanWithWrappedRegionScanner() throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(2);
      // Check if get() returns blocks on its close() itself
      getLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
      checkForBlockEviction(cache, false, false);
      // countdown the latch
      CustomInnerRegionObserver.getCdl().get().countDown();
      for (GetThread thread : getThreads) {
        thread.joinAndRethrow();
      }
      getLatch.countDown();
      for (ScanThread thread : scanThreads) {
        thread.joinAndRethrow();
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testScanWithCompaction() throws Throwable {
    testScanWithCompactionInternals(name.getMethodName(), false);
  }

  @Test
  public void testReverseScanWithCompaction() throws Throwable {
    testScanWithCompactionInternals(name.getMethodName(), true);
  }

  private void testScanWithCompactionInternals(String tableNameStr, boolean reversed)
    throws Throwable {
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      compactionLatch = new CountDownLatch(1);
      TableName tableName = TableName.valueOf(tableNameStr);
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
          refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
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
          refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
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
        thread.joinAndRethrow();
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
  public void testBlockEvictionAfterHBASE13082WithCompactionAndFlush() throws Throwable {
    // do flush and scan in parallel
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      compactionLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();

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
          refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
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
          refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
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
        thread.joinAndRethrow();
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
    Table table = null;
    try {
      latch = new CountDownLatch(1);
      exceptionLatch = new CountDownLatch(1);
      final TableName tableName = TableName.valueOf(name.getMethodName());
      // Create KV that will give you two blocks
      // Create a table with block size as 1024
      table = TEST_UTIL.createTable(tableName, FAMILIES_1, 1, 1024,
        CustomInnerRegionObserverWrapper.class.getName());
      // get the block cache and region
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      HStore store = region.getStores().iterator().next();
      CacheConfig cacheConf = store.getCacheConfig();
      cacheConf.setCacheDataOnWrite(true);
      cacheConf.setEvictOnClose(true);
      BlockCache cache = cacheConf.getBlockCache().get();
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
          refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
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
        // expect it to fail
        try {
          thread.joinAndRethrow();
          fail("Expected failure");
        } catch (Throwable t) {
          assertTrue(t instanceof UncheckedIOException);
        }
      }
      iterator = cache.iterator();
      usedBlocksFound = false;
      refCount = 0;
      while (iterator.hasNext()) {
        CachedBlock next = iterator.next();
        BlockCacheKey cacheKey = new BlockCacheKey(next.getFilename(), next.getOffset());
        if (cache instanceof BucketCache) {
          refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        } else if (cache instanceof CombinedBlockCache) {
          refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
        } else {
          continue;
        }
        if (refCount != 0) {
          // Blocks will be with count 3
          assertEquals(NO_OF_THREADS, refCount);
          usedBlocksFound = true;
        }
      }
      assertFalse(usedBlocksFound);
      // you should always see 0 ref count. since after HBASE-16604 we always recreate the scanner
      assertEquals(0, refCount);
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
        refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
        LOG.info("BucketCache {} {}", cacheKey, refCount);
      } else if (cache instanceof CombinedBlockCache) {
        refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
        LOG.info("CombinedBlockCache {} {}", cacheKey, refCount);
      } else {
        continue;
      }
      assertEquals(0, refCount);
    }
  }

  private void insertData(Table table) throws IOException {
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

  private ScanThread[] initiateScan(Table table, Scan scan, ExpectedResult... expectedResults) {
    ScanThread[] scanThreads = new ScanThread[NO_OF_THREADS];
    for (int i = 0; i < NO_OF_THREADS; i++) {
      scanThreads[i] = new ScanThread(table, scan, expectedResults);
    }
    for (ScanThread thread : scanThreads) {
      thread.start();
    }
    return scanThreads;
  }

  private ScanThread[] initiateScan(Table table, boolean reverse)
    throws IOException, InterruptedException {
    Scan scan = new Scan();
    if (reverse) {
      scan.setReversed(true);
    }
    return initiateScan(table, scan);
  }

  private GetThread[] initiateGet(Table table, boolean tracker, boolean multipleCFs)
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

  private MultiGetThread[] initiateMultiGet(Table table) throws IOException, InterruptedException {
    MultiGetThread[] multiGetThreads = new MultiGetThread[NO_OF_THREADS];
    for (int i = 0; i < NO_OF_THREADS; i++) {
      multiGetThreads[i] = new MultiGetThread(table);
    }
    for (MultiGetThread thread : multiGetThreads) {
      thread.start();
    }
    return multiGetThreads;
  }

  private void checkForBlockEviction(BlockCache cache, boolean getClosed, boolean expectOnlyZero)
    throws InterruptedException {
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
        refCount = ((BucketCache) cache).getRpcRefCount(cacheKey);
      } else if (cache instanceof CombinedBlockCache) {
        refCount = ((CombinedBlockCache) cache).getRpcRefCount(cacheKey);
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
          if (getLatch == null) {
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
    private final Table table;
    private final List<Get> gets = new ArrayList<>();
    private volatile Throwable throwable = null;

    public MultiGetThread(Table table) {
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
      } catch (Throwable t) {
        throwable = t;
      }
    }

    /**
     * Joins the thread and re-throws any throwable that was thrown by the runnable method. The
     * thread runnable itself has assertions in it. Without this rethrow, if those other assertions
     * failed we would never actually know because they don't bubble up to the main thread.
     */
    public void joinAndRethrow() throws Throwable {
      join();
      if (throwable != null) {
        throw throwable;
      }
    }
  }

  private static class GetThread extends Thread {
    private final Table table;
    private final boolean tracker;
    private final boolean multipleCFs;

    private volatile Throwable throwable = null;

    public GetThread(Table table, boolean tracker, boolean multipleCFs) {
      this.table = table;
      this.tracker = tracker;
      this.multipleCFs = multipleCFs;
    }

    @Override
    public void run() {
      try {
        initiateGet(table);
      } catch (Throwable t) {
        throwable = t;
      }
    }

    /**
     * Joins the thread and re-throws any throwable that was thrown by the runnable method. The
     * thread runnable itself has assertions in it. Without this rethrow, if those other assertions
     * failed we would never actually know because they don't bubble up to the main thread.
     */
    public void joinAndRethrow() throws Throwable {
      join();
      if (throwable != null) {
        throw throwable;
      }
    }

    private void initiateGet(Table table) throws IOException {
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
    private final Table table;
    private final Scan scan;
    private final ExpectedResult[] expectedResults;

    private volatile Throwable throwable = null;

    public ScanThread(Table table, Scan scan, ExpectedResult... expectedResults) {
      this.table = table;
      this.scan = scan;
      this.expectedResults = expectedResults;
    }

    @Override
    public void run() {
      try {
        initiateScan(table);
      } catch (Throwable t) {
        throwable = t;
      }
    }

    /**
     * Joins the thread and re-throws any throwable that was thrown by the runnable method. The
     * thread runnable itself has assertions in it. Without this rethrow, if those other assertions
     * failed we would never actually know because they don't bubble up to the main thread.
     */
    public void joinAndRethrow() throws Throwable {
      join();
      if (throwable != null) {
        throw throwable;
      }
    }

    private void initiateScan(Table table) throws IOException {
      CustomInnerRegionObserver.getCdl().set(latch);
      ResultScanner resScanner = table.getScanner(scan);
      if (expectedResults != null && expectedResults.length > 0) {
        assertExpectedRows(resScanner);
      } else {
        assertRowsMatch(resScanner);
      }
    }

    private void assertExpectedRows(ResultScanner scanner) {
      int i = 0;
      for (Result result : scanner) {
        for (Cell cell : result.listCells()) {
          expectedResults[i].assertEquals(cell, i++);
        }
      }
      // verify we covered the full expected results
      assertEquals(i, expectedResults.length);
    }

    private void assertRowsMatch(ResultScanner scanner) {
      int i = (scan.isReversed() ? ROWS.length - 1 : 0);
      boolean resultFound = false;
      for (Result result : scanner) {
        resultFound = true;
        System.out.println("result: " + result);
        if (!scan.isReversed()) {
          assertTrue(Bytes.equals(result.getRow(), ROWS[i]));
          i++;
        } else {
          assertTrue(Bytes.equals(result.getRow(), ROWS[i]));
          i--;
        }
      }
      assertEquals(!scan.hasFilter(), resultFound);
    }
  }

  private void waitForStoreFileCount(HStore store, int count, int timeout)
    throws InterruptedException {
    long start = EnvironmentEdgeManager.currentTime();
    while (
      start + timeout > EnvironmentEdgeManager.currentTime() && store.getStorefilesCount() != count
    ) {
      Thread.sleep(100);
    }
    System.out.println("start=" + start + ", now=" + EnvironmentEdgeManager.currentTime() + ", cur="
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
    public RegionInfo getRegionInfo() {
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
  }

  public static class CustomInnerRegionObserverWrapper extends CustomInnerRegionObserver {
    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
      RegionScanner s) throws IOException {
      return new CustomScanner(s);
    }
  }

  public static class CustomInnerRegionObserver implements RegionCoprocessor, RegionObserver {
    static final AtomicInteger countOfNext = new AtomicInteger(0);
    static final AtomicInteger countOfGets = new AtomicInteger(0);
    static final AtomicBoolean waitForGets = new AtomicBoolean(false);
    static final AtomicBoolean throwException = new AtomicBoolean(false);
    private static final AtomicReference<CountDownLatch> cdl =
      new AtomicReference<>(new CountDownLatch(0));

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

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
      return hasMore;
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<Cell> results) throws IOException {
      slowdownCode(e, true);
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
        LOG.error(e1.toString(), e1);
      }
    }
  }
}
