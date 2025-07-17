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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.io.hfile.NoOpIndexBlockEncoder;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, LargeTests.class })
public class TestBytesReadServerSideScanMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBytesReadServerSideScanMetrics.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBytesReadServerSideScanMetrics.class);

  private HBaseTestingUtility UTIL;

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final byte[] ROW4 = Bytes.toBytes("row4");

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    UTIL = new HBaseTestingUtility();
    conf = UTIL.getConfiguration();
    conf.setInt(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0);
    conf.setBoolean(CompactSplit.HBASE_REGION_SERVER_ENABLE_COMPACTION, false);
  }

  @Test
  public void testScanMetricsDisabled() throws Exception {
    conf.setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, false, BloomType.NONE);
      writeData(tableName, true);
      Scan scan = new Scan();
      scan.withStartRow(ROW2, true);
      scan.withStopRow(ROW4, true);
      scan.setCaching(1);
      try (Table table = UTIL.getConnection().getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
        int rowCount = 0;
        for (Result r : scanner) {
          rowCount++;
        }
        Assert.assertEquals(2, rowCount);
        Assert.assertNull(scanner.getScanMetrics());
      }
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadFromFsForSerialSeeks() throws Exception {
    conf.setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, false, BloomType.ROW);
      writeData(tableName, true);
      ScanMetrics scanMetrics = readDataAndGetScanMetrics(tableName, true);

      // Use oldest timestamp to make sure the fake key is not less than the first key in
      // the file containing key: row2
      KeyValue keyValue = new KeyValue(ROW2, CF, CQ, HConstants.OLDEST_TIMESTAMP, VALUE);
      assertBytesReadFromFs(tableName, scanMetrics.bytesReadFromFs.get(), keyValue,
        scanMetrics.blockReadOpsCount.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadFromFsForParallelSeeks() throws Exception {
    conf.setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    // This property doesn't work correctly if only applied at column family level.
    conf.setBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, true);
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, false, BloomType.NONE);
      writeData(tableName, true);
      HRegionServer server = UTIL.getMiniHBaseCluster().getRegionServer(0);
      ThreadPoolExecutor executor =
        server.getExecutorService().getExecutorThreadPool(ExecutorType.RS_PARALLEL_SEEK);
      long tasksCompletedBeforeRead = executor.getCompletedTaskCount();
      ScanMetrics scanMetrics = readDataAndGetScanMetrics(tableName, true);
      long tasksCompletedAfterRead = executor.getCompletedTaskCount();
      // Assert both of the HFiles were read using parallel seek executor
      Assert.assertEquals(2, tasksCompletedAfterRead - tasksCompletedBeforeRead);

      // Use oldest timestamp to make sure the fake key is not less than the first key in
      // the file containing key: row2
      KeyValue keyValue = new KeyValue(ROW2, CF, CQ, HConstants.OLDEST_TIMESTAMP, VALUE);
      assertBytesReadFromFs(tableName, scanMetrics.bytesReadFromFs.get(), keyValue,
        scanMetrics.blockReadOpsCount.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadFromBlockCache() throws Exception {
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, true, BloomType.NONE);
      HRegionServer server = UTIL.getMiniHBaseCluster().getRegionServer(0);
      LruBlockCache blockCache = (LruBlockCache) server.getBlockCache().get();

      // Assert that acceptable size of LRU block cache is greater than 1MB
      Assert.assertTrue(blockCache.acceptableSize() > 1024 * 1024);
      writeData(tableName, true);
      readDataAndGetScanMetrics(tableName, false);
      KeyValue keyValue = new KeyValue(ROW2, CF, CQ, HConstants.OLDEST_TIMESTAMP, VALUE);
      assertBlockCacheWarmUp(tableName, keyValue);
      ScanMetrics scanMetrics = readDataAndGetScanMetrics(tableName, true);
      Assert.assertEquals(0, scanMetrics.bytesReadFromFs.get());
      assertBytesReadFromBlockCache(tableName, scanMetrics.bytesReadFromBlockCache.get(), keyValue);
      Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadFromMemstore() throws Exception {
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, false, BloomType.NONE);
      writeData(tableName, false);
      ScanMetrics scanMetrics = readDataAndGetScanMetrics(tableName, true);

      // Assert no flush has happened for the table
      List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
      for (HRegion region : regions) {
        HStore store = region.getStore(CF);
        // Assert no HFile is there
        Assert.assertEquals(0, store.getStorefiles().size());
      }

      KeyValue keyValue = new KeyValue(ROW2, CF, CQ, HConstants.LATEST_TIMESTAMP, VALUE);
      int singleKeyValueSize = Segment.getCellLength(keyValue);
      // First key value will be read on doing seek and second one on doing next() to determine
      // there are no more cells in the row. We don't count key values read on SegmentScanner
      // instance creation.
      int totalKeyValueSize = 2 * singleKeyValueSize;
      Assert.assertEquals(0, scanMetrics.bytesReadFromFs.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
      Assert.assertEquals(totalKeyValueSize, scanMetrics.bytesReadFromMemstore.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadWithSwitchFromPReadToStream() throws Exception {
    // Set pread max bytes to 3 to make sure that the first row is read using pread and the second
    // one using stream read
    Map<String, String> configuration = new HashMap<>();
    configuration.put(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, "3");
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, true, BloomType.ROW, configuration);
      writeData(tableName, true);
      Scan scan = new Scan();
      scan.withStartRow(ROW2, true);
      scan.withStopRow(ROW4, true);
      scan.setScanMetricsEnabled(true);
      // Set caching to 1 so that one row is read via PREAD and other via STREAM
      scan.setCaching(1);
      ScanMetrics scanMetrics = null;
      StoreScanner.instrument();
      try (Table table = UTIL.getConnection().getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
        int rowCount = 0;
        Assert.assertFalse(StoreScanner.hasSwitchedToStreamRead());
        for (Result r : scanner) {
          rowCount++;
        }
        Assert.assertTrue(StoreScanner.hasSwitchedToStreamRead());
        Assert.assertEquals(2, rowCount);
        scanMetrics = scanner.getScanMetrics();
      }
      int bytesReadFromFs = getBytesReadFromFsForNonGetScan(tableName, scanMetrics, 2);
      Assert.assertEquals(bytesReadFromFs, scanMetrics.bytesReadFromFs.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());
      // There are 2 HFiles so, 1 read op per HFile was done by actual scan to read data block.
      // No bloom blocks will be read as this is non Get scan and only bloom filter type is ROW.
      Assert.assertEquals(2, scanMetrics.blockReadOpsCount.get());
      // With scan caching set to 1 and 2 rows being scanned, 2 RPC calls will be needed.
      Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadWhenFlushHappenedInTheMiddleOfScan() throws Exception {
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, true, BloomType.ROW);
      writeData(tableName, false);
      Scan scan = new Scan();
      scan.withStartRow(ROW2, true);
      scan.withStopRow(ROW4, true);
      scan.setScanMetricsEnabled(true);
      // Set caching to 1 so that one row is read per RPC call
      scan.setCaching(1);
      ScanMetrics scanMetrics = null;
      try (Table table = UTIL.getConnection().getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
        int rowCount = 0;
        for (Result r : scanner) {
          rowCount++;
          if (rowCount == 1) {
            flushAndWaitUntilFlushed(tableName, true);
          }
        }
        Assert.assertEquals(2, rowCount);
        scanMetrics = scanner.getScanMetrics();
      }

      // Only 1 HFile will be created and it will have only one data block.
      int bytesReadFromFs = getBytesReadFromFsForNonGetScan(tableName, scanMetrics, 1);
      Assert.assertEquals(bytesReadFromFs, scanMetrics.bytesReadFromFs.get());

      Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());

      // Flush happens after first row is returned from server but before second row is returned.
      // So, 2 cells will be read from memstore i.e. the cell for the first row and the next cell
      // at which scanning will stop. Per row we have 1 cell.
      int bytesReadFromMemstore =
        Segment.getCellLength(new KeyValue(ROW2, CF, CQ, HConstants.LATEST_TIMESTAMP, VALUE));
      Assert.assertEquals(2 * bytesReadFromMemstore, scanMetrics.bytesReadFromMemstore.get());

      // There will be 1 read op to read the only data block present in the HFile.
      Assert.assertEquals(1, scanMetrics.blockReadOpsCount.get());

      // More than 1 RPC call should be there
      Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadInReverseScan() throws Exception {
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, true, BloomType.ROW);
      writeData(tableName, true);
      Scan scan = new Scan();
      scan.withStartRow(ROW4, true);
      scan.withStopRow(ROW2, true);
      scan.setScanMetricsEnabled(true);
      scan.setReversed(true);
      // Set caching to 1 so that one row is read per RPC call
      scan.setCaching(1);
      ScanMetrics scanMetrics = null;
      try (Table table = UTIL.getConnection().getTable(tableName);
        ResultScanner scanner = table.getScanner(scan)) {
        int rowCount = 0;
        for (Result r : scanner) {
          rowCount++;
        }
        Assert.assertEquals(2, rowCount);
        scanMetrics = scanner.getScanMetrics();
        System.out.println("Scan metrics: " + scanMetrics.toString());
      }

      // 1 data block per HFile was read.
      int bytesReadFromFs = getBytesReadFromFsForNonGetScan(tableName, scanMetrics, 2);
      Assert.assertEquals(bytesReadFromFs, scanMetrics.bytesReadFromFs.get());

      // For the HFile containing both the rows, the data block will be read from block cache when
      // KeyValueHeap.next() will be called to read the second row.
      // KeyValueHeap.next() will call StoreFileScanner.next() when on ROW4 which is last row of the
      // file causing curBlock to be set to null in underlying HFileScanner. As curBlock is null,
      // kvNext will be null and call to StoreFileScanner.seekToPreviousRow() will be made. As the
      // curBlock of HFileScanner is null so, StoreFileScanner.seekToPreviousRow() will load data
      // block from BlockCache. So, 1 data block will be read from block cache.
      Assert.assertEquals(bytesReadFromFs / 2, scanMetrics.bytesReadFromBlockCache.get());
      Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());

      // 1 read op per HFile was done by actual scan to read data block.
      Assert.assertEquals(2, scanMetrics.blockReadOpsCount.get());

      // 2 RPC calls will be there
      Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testBytesReadWithLazySeek() throws Exception {
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      createTable(tableName, true, BloomType.NONE);
      writeData(tableName, true);
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        byte[] newValue = Bytes.toBytes("new value");
        // Update the value of ROW2 and let it stay in memstore. Will assert that lazy seek doesn't
        // lead to seek on the HFile.
        table.put(new Put(ROW2).addColumn(CF, CQ, newValue));
        Scan scan = new Scan();
        scan.withStartRow(ROW2, true);
        scan.withStopRow(ROW2, true);
        scan.setScanMetricsEnabled(true);
        Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<>();
        familyMap.put(CF, new TreeSet<>(Bytes.BYTES_COMPARATOR));
        familyMap.get(CF).add(CQ);
        scan.setFamilyMap(familyMap);
        ScanMetrics scanMetrics = null;
        try (ResultScanner scanner = table.getScanner(scan)) {
          int rowCount = 0;
          for (Result r : scanner) {
            rowCount++;
            Assert.assertArrayEquals(newValue, r.getValue(CF, CQ));
          }
          Assert.assertEquals(1, rowCount);
          scanMetrics = scanner.getScanMetrics();
        }
        // No real seek should be done on the HFile.
        Assert.assertEquals(0, scanMetrics.bytesReadFromFs.get());
        Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
        Assert.assertEquals(0, scanMetrics.blockReadOpsCount.get());

        // The cell should be coming purely from memstore.
        int cellSize =
          Segment.getCellLength(new KeyValue(ROW2, CF, CQ, HConstants.LATEST_TIMESTAMP, newValue));
        Assert.assertEquals(cellSize, scanMetrics.bytesReadFromMemstore.get());
        Assert.assertEquals(1, scanMetrics.countOfRPCcalls.get());
      }
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Test consecutive calls to RegionScannerImpl.next() to make sure populating scan metrics from
   * ThreadLocalServerSideScanMetrics is done correctly.
   */
  @Test
  public void testConsecutiveRegionScannerNextCalls() throws Exception {
    // We will be setting a very small block size so, make sure to set big enough pread max bytes
    Map<String, String> configuration = new HashMap<>();
    configuration.put(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, Integer.toString(64 * 1024));
    UTIL.startMiniCluster();
    try {
      TableName tableName = TableName.valueOf(name.getMethodName());
      // Set the block size to 4 bytes to get 1 row per data block in HFile.
      createTable(tableName, true, BloomType.NONE, 4, configuration);
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        // Add 3 rows to the table.
        table.put(new Put(ROW2).addColumn(CF, CQ, VALUE));
        table.put(new Put(ROW3).addColumn(CF, CQ, VALUE));
        table.put(new Put(ROW4).addColumn(CF, CQ, VALUE));

        ScanMetrics scanMetrics = null;

        // Scan the added rows. The rows should be read from memstore.
        Scan scan = createScanToReadOneRowAtATimeFromServer(ROW2, ROW3);
        try (ResultScanner scanner = table.getScanner(scan)) {
          int rowCount = 0;
          for (Result r : scanner) {
            rowCount++;
          }
          Assert.assertEquals(2, rowCount);
          scanMetrics = scanner.getScanMetrics();
        }

        // Assert that rows were read from only memstore and involved 2 RPC calls.
        int cellSize =
          Segment.getCellLength(new KeyValue(ROW2, CF, CQ, HConstants.LATEST_TIMESTAMP, VALUE));
        Assert.assertEquals(0, scanMetrics.bytesReadFromFs.get());
        Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
        Assert.assertEquals(0, scanMetrics.blockReadOpsCount.get());
        Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
        Assert.assertEquals(3 * cellSize, scanMetrics.bytesReadFromMemstore.get());

        // Flush the table and make sure that the rows are read from HFiles.
        flushAndWaitUntilFlushed(tableName, false);
        scan = createScanToReadOneRowAtATimeFromServer(ROW2, ROW3);
        scanMetrics = null;
        try (ResultScanner scanner = table.getScanner(scan)) {
          int rowCount = 0;
          for (Result r : scanner) {
            rowCount++;
          }
          Assert.assertEquals(2, rowCount);
          scanMetrics = scanner.getScanMetrics();
        }

        // Assert that rows were read from HFiles and involved 2 RPC calls.
        int bytesReadFromFs = getBytesReadToReadConsecutiveDataBlocks(tableName, 1, 3, true);
        Assert.assertEquals(bytesReadFromFs, scanMetrics.bytesReadFromFs.get());
        Assert.assertEquals(0, scanMetrics.bytesReadFromBlockCache.get());
        Assert.assertEquals(3, scanMetrics.blockReadOpsCount.get());
        Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
        Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());

        // Make sure that rows are read from Blockcache now.
        scan = createScanToReadOneRowAtATimeFromServer(ROW2, ROW3);
        scanMetrics = null;
        try (ResultScanner scanner = table.getScanner(scan)) {
          int rowCount = 0;
          for (Result r : scanner) {
            rowCount++;
          }
          Assert.assertEquals(2, rowCount);
          scanMetrics = scanner.getScanMetrics();
        }

        // Assert that rows were read from Blockcache and involved 2 RPC calls.
        int bytesReadFromBlockCache =
          getBytesReadToReadConsecutiveDataBlocks(tableName, 1, 3, false);
        Assert.assertEquals(bytesReadFromBlockCache, scanMetrics.bytesReadFromBlockCache.get());
        Assert.assertEquals(0, scanMetrics.bytesReadFromFs.get());
        Assert.assertEquals(0, scanMetrics.blockReadOpsCount.get());
        Assert.assertEquals(2, scanMetrics.countOfRPCcalls.get());
        Assert.assertEquals(0, scanMetrics.bytesReadFromMemstore.get());
      }
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  private Scan createScanToReadOneRowAtATimeFromServer(byte[] startRow, byte[] stopRow) {
    Scan scan = new Scan();
    scan.withStartRow(startRow, true);
    scan.withStopRow(stopRow, true);
    scan.setScanMetricsEnabled(true);
    scan.setCaching(1);
    return scan;
  }

  private void flushAndWaitUntilFlushed(TableName tableName, boolean waitForUpdatedReaders)
    throws Exception {
    if (waitForUpdatedReaders) {
      StoreScanner.instrument();
    }
    UTIL.flush(tableName);
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    Assert.assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    HStore store = region.getStore(CF);
    // In milliseconds
    int maxWaitTime = 100000;
    int totalWaitTime = 0;
    int sleepTime = 10000;
    while (
      store.getStorefiles().size() == 0
        || (waitForUpdatedReaders && !StoreScanner.hasUpdatedReaders())
    ) {
      Thread.sleep(sleepTime);
      totalWaitTime += sleepTime;
      if (totalWaitTime >= maxWaitTime) {
        throw new Exception("Store files not flushed after " + maxWaitTime + "ms");
      }
    }
    Assert.assertEquals(1, store.getStorefiles().size());
  }

  private int getBytesReadToReadConsecutiveDataBlocks(TableName tableName,
    int expectedStoreFileCount, int expectedDataBlockCount, boolean isReadFromFs) throws Exception {
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    Assert.assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    HStore store = region.getStore(CF);
    Collection<HStoreFile> storeFiles = store.getStorefiles();
    Assert.assertEquals(expectedStoreFileCount, storeFiles.size());
    int bytesReadFromFs = 0;
    for (HStoreFile storeFile : storeFiles) {
      StoreFileReader reader = storeFile.getReader();
      HFile.Reader hfileReader = reader.getHFileReader();
      HFileBlock.FSReader blockReader = hfileReader.getUncachedBlockReader();
      FixedFileTrailer trailer = hfileReader.getTrailer();
      int dataIndexLevels = trailer.getNumDataIndexLevels();
      long loadOnOpenDataOffset = trailer.getLoadOnOpenDataOffset();
      HFileBlock.BlockIterator blockIterator = blockReader.blockRange(0, loadOnOpenDataOffset);
      HFileBlock block;
      boolean readNextBlock = false;
      int blockCount = 0;
      while ((block = blockIterator.nextBlock()) != null) {
        blockCount++;
        bytesReadFromFs += block.getOnDiskSizeWithHeader();
        if (isReadFromFs && readNextBlock) {
          // This accounts for savings we get from prefetched header but these saving are only
          // applicable when reading from FS and not from BlockCache.
          bytesReadFromFs -= block.headerSize();
          readNextBlock = false;
        }
        if (block.getNextBlockOnDiskSize() > 0) {
          bytesReadFromFs += block.headerSize();
          readNextBlock = true;
        }
        Assert.assertTrue(block.getBlockType().isData());
      }
      blockIterator.freeBlocks();
      // No intermediate or leaf index blocks are expected.
      Assert.assertEquals(1, dataIndexLevels);
      Assert.assertEquals(expectedDataBlockCount, blockCount);
    }
    return bytesReadFromFs;
  }

  private int getBytesReadFromFsForNonGetScan(TableName tableName, ScanMetrics scanMetrics,
    int expectedStoreFileCount) throws Exception {
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    Assert.assertEquals(1, regions.size());
    HRegion region = regions.get(0);
    HStore store = region.getStore(CF);
    Collection<HStoreFile> storeFiles = store.getStorefiles();
    Assert.assertEquals(expectedStoreFileCount, storeFiles.size());
    int bytesReadFromFs = 0;
    for (HStoreFile storeFile : storeFiles) {
      StoreFileReader reader = storeFile.getReader();
      HFile.Reader hfileReader = reader.getHFileReader();
      HFileBlock.FSReader blockReader = hfileReader.getUncachedBlockReader();
      FixedFileTrailer trailer = hfileReader.getTrailer();
      int dataIndexLevels = trailer.getNumDataIndexLevels();
      // Read the first block of the HFile. First block is always expected to be a DATA block and
      // the HFile is expected to have only one DATA block.
      HFileBlock block = blockReader.readBlockData(0, -1, true, true, true);
      Assert.assertTrue(block.getBlockType().isData());
      bytesReadFromFs += block.getOnDiskSizeWithHeader();
      if (block.getNextBlockOnDiskSize() > 0) {
        bytesReadFromFs += block.headerSize();
      }
      block.release();
      // Each of the HFiles is expected to have only root index but no intermediate or leaf index
      // blocks.
      Assert.assertEquals(1, dataIndexLevels);
    }
    return bytesReadFromFs;
  }

  private ScanMetrics readDataAndGetScanMetrics(TableName tableName, boolean isScanMetricsEnabled)
    throws Exception {
    Scan scan = new Scan();
    scan.withStartRow(ROW2, true);
    scan.withStopRow(ROW2, true);
    scan.setScanMetricsEnabled(isScanMetricsEnabled);
    ScanMetrics scanMetrics;
    try (Table table = UTIL.getConnection().getTable(tableName);
      ResultScanner scanner = table.getScanner(scan)) {
      int rowCount = 0;
      StoreFileScanner.instrument();
      for (Result r : scanner) {
        rowCount++;
      }
      Assert.assertEquals(1, rowCount);
      scanMetrics = scanner.getScanMetrics();
    }
    if (isScanMetricsEnabled) {
      LOG.info("Bytes read from fs: " + scanMetrics.bytesReadFromFs.get());
      LOG.info("Bytes read from block cache: " + scanMetrics.bytesReadFromBlockCache.get());
      LOG.info("Bytes read from memstore: " + scanMetrics.bytesReadFromMemstore.get());
      LOG.info("Count of bytes scanned: " + scanMetrics.countOfBlockBytesScanned.get());
      LOG.info("StoreFileScanners seek count: " + StoreFileScanner.getSeekCount());
    }
    return scanMetrics;
  }

  private void writeData(TableName tableName, boolean shouldFlush) throws Exception {
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      table.put(new Put(ROW2).addColumn(CF, CQ, VALUE));
      table.put(new Put(ROW4).addColumn(CF, CQ, VALUE));
      if (shouldFlush) {
        // Create a HFile
        UTIL.flush(tableName);
      }

      table.put(new Put(Bytes.toBytes("row1")).addColumn(CF, CQ, VALUE));
      table.put(new Put(Bytes.toBytes("row5")).addColumn(CF, CQ, VALUE));
      if (shouldFlush) {
        // Create a HFile
        UTIL.flush(tableName);
      }
    }
  }

  private void createTable(TableName tableName, boolean blockCacheEnabled, BloomType bloomType)
    throws Exception {
    createTable(tableName, blockCacheEnabled, bloomType, HConstants.DEFAULT_BLOCKSIZE,
      new HashMap<>());
  }

  private void createTable(TableName tableName, boolean blockCacheEnabled, BloomType bloomType,
    Map<String, String> configuration) throws Exception {
    createTable(tableName, blockCacheEnabled, bloomType, HConstants.DEFAULT_BLOCKSIZE,
      configuration);
  }

  private void createTable(TableName tableName, boolean blockCacheEnabled, BloomType bloomType,
    int blocksize, Map<String, String> configuration) throws Exception {
    Admin admin = UTIL.getAdmin();
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(CF);
    columnFamilyDescriptorBuilder.setBloomFilterType(bloomType);
    columnFamilyDescriptorBuilder.setBlockCacheEnabled(blockCacheEnabled);
    columnFamilyDescriptorBuilder.setBlocksize(blocksize);
    for (Map.Entry<String, String> entry : configuration.entrySet()) {
      columnFamilyDescriptorBuilder.setConfiguration(entry.getKey(), entry.getValue());
    }
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
    admin.createTable(tableDescriptorBuilder.build());
    UTIL.waitUntilAllRegionsAssigned(tableName);
  }

  private void assertBytesReadFromFs(TableName tableName, long actualBytesReadFromFs,
    KeyValue keyValue, long actualReadOps) throws Exception {
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    Assert.assertEquals(1, regions.size());
    MutableInt totalExpectedBytesReadFromFs = new MutableInt(0);
    MutableInt totalExpectedReadOps = new MutableInt(0);
    for (HRegion region : regions) {
      Assert.assertNull(region.getBlockCache());
      HStore store = region.getStore(CF);
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      Assert.assertEquals(2, storeFiles.size());
      for (HStoreFile storeFile : storeFiles) {
        StoreFileReader reader = storeFile.getReader();
        HFile.Reader hfileReader = reader.getHFileReader();
        BloomFilter bloomFilter = reader.getGeneralBloomFilter();
        Assert.assertTrue(bloomFilter == null || bloomFilter instanceof CompoundBloomFilter);
        CompoundBloomFilter cbf = bloomFilter == null ? null : (CompoundBloomFilter) bloomFilter;
        Consumer<HFileBlock> bytesReadFunction = new Consumer<HFileBlock>() {
          @Override
          public void accept(HFileBlock block) {
            totalExpectedBytesReadFromFs.add(block.getOnDiskSizeWithHeader());
            if (block.getNextBlockOnDiskSize() > 0) {
              totalExpectedBytesReadFromFs.add(block.headerSize());
            }
            totalExpectedReadOps.add(1);
          }
        };
        readHFile(hfileReader, cbf, keyValue, bytesReadFunction);
      }
    }
    Assert.assertEquals(totalExpectedBytesReadFromFs.longValue(), actualBytesReadFromFs);
    Assert.assertEquals(totalExpectedReadOps.longValue(), actualReadOps);
  }

  private void readHFile(HFile.Reader hfileReader, CompoundBloomFilter cbf, KeyValue keyValue,
    Consumer<HFileBlock> bytesReadFunction) throws Exception {
    HFileBlock.FSReader blockReader = hfileReader.getUncachedBlockReader();
    FixedFileTrailer trailer = hfileReader.getTrailer();
    HFileContext meta = hfileReader.getFileContext();
    long fileSize = hfileReader.length();

    // Read the bloom block from FS
    if (cbf != null) {
      // Read a block in load-on-open section to make sure prefetched header is not bloom
      // block's header
      blockReader.readBlockData(trailer.getLoadOnOpenDataOffset(), -1, true, true, true).release();

      HFileBlockIndex.BlockIndexReader index = cbf.getBloomIndex();
      byte[] row = ROW2;
      int blockIndex = index.rootBlockContainingKey(row, 0, row.length);
      HFileBlock bloomBlock = cbf.getBloomBlock(blockIndex);
      boolean fileContainsKey = BloomFilterUtil.contains(row, 0, row.length,
        bloomBlock.getBufferReadOnly(), bloomBlock.headerSize(),
        bloomBlock.getUncompressedSizeWithoutHeader(), cbf.getHash(), cbf.getHashCount());
      bytesReadFunction.accept(bloomBlock);
      // Asser that the block read is a bloom block
      Assert.assertEquals(bloomBlock.getBlockType(), BlockType.BLOOM_CHUNK);
      bloomBlock.release();
      if (!fileContainsKey) {
        // Key is not in th file, so we don't need to read the data block
        return;
      }
    }

    // Indexes use NoOpEncodedSeeker
    MyNoOpEncodedSeeker seeker = new MyNoOpEncodedSeeker();
    HFileBlock.BlockIterator blockIter = blockReader.blockRange(trailer.getLoadOnOpenDataOffset(),
      fileSize - trailer.getTrailerSize());
    HFileBlock block = blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX);

    // Comparator class name is stored in the trailer in version 3.
    CellComparator comparator = trailer.createComparator();
    // Initialize the seeker
    seeker.initRootIndex(block, trailer.getDataIndexCount(), comparator,
      trailer.getNumDataIndexLevels());

    int blockLevelsRead = 1; // Root index is the first level

    int rootLevIndex = seeker.rootBlockContainingKey(keyValue);
    long currentOffset = seeker.getBlockOffset(rootLevIndex);
    int currentDataSize = seeker.getBlockDataSize(rootLevIndex);

    HFileBlock prevBlock = null;
    do {
      prevBlock = block;
      block = blockReader.readBlockData(currentOffset, currentDataSize, true, true, true);
      HFileBlock unpacked = block.unpack(meta, blockReader);
      if (unpacked != block) {
        block.release();
        block = unpacked;
      }
      bytesReadFunction.accept(block);
      if (!block.getBlockType().isData()) {
        ByteBuff buffer = block.getBufferWithoutHeader();
        // Place the buffer at the correct position
        HFileBlockIndex.BlockIndexReader.locateNonRootIndexEntry(buffer, keyValue, comparator);
        currentOffset = buffer.getLong();
        currentDataSize = buffer.getInt();
      }
      prevBlock.release();
      blockLevelsRead++;
    } while (!block.getBlockType().isData());
    block.release();
    blockIter.freeBlocks();

    Assert.assertEquals(blockLevelsRead, trailer.getNumDataIndexLevels() + 1);
  }

  private void assertBytesReadFromBlockCache(TableName tableName,
    long actualBytesReadFromBlockCache, KeyValue keyValue) throws Exception {
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    Assert.assertEquals(1, regions.size());
    MutableInt totalExpectedBytesReadFromBlockCache = new MutableInt(0);
    for (HRegion region : regions) {
      Assert.assertNotNull(region.getBlockCache());
      HStore store = region.getStore(CF);
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      Assert.assertEquals(2, storeFiles.size());
      for (HStoreFile storeFile : storeFiles) {
        StoreFileReader reader = storeFile.getReader();
        HFile.Reader hfileReader = reader.getHFileReader();
        BloomFilter bloomFilter = reader.getGeneralBloomFilter();
        Assert.assertTrue(bloomFilter == null || bloomFilter instanceof CompoundBloomFilter);
        CompoundBloomFilter cbf = bloomFilter == null ? null : (CompoundBloomFilter) bloomFilter;
        Consumer<HFileBlock> bytesReadFunction = new Consumer<HFileBlock>() {
          @Override
          public void accept(HFileBlock block) {
            totalExpectedBytesReadFromBlockCache.add(block.getOnDiskSizeWithHeader());
            if (block.getNextBlockOnDiskSize() > 0) {
              totalExpectedBytesReadFromBlockCache.add(block.headerSize());
            }
          }
        };
        readHFile(hfileReader, cbf, keyValue, bytesReadFunction);
      }
    }
    Assert.assertEquals(totalExpectedBytesReadFromBlockCache.longValue(),
      actualBytesReadFromBlockCache);
  }

  private void assertBlockCacheWarmUp(TableName tableName, KeyValue keyValue) throws Exception {
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    Assert.assertEquals(1, regions.size());
    for (HRegion region : regions) {
      Assert.assertNotNull(region.getBlockCache());
      HStore store = region.getStore(CF);
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      Assert.assertEquals(2, storeFiles.size());
      for (HStoreFile storeFile : storeFiles) {
        StoreFileReader reader = storeFile.getReader();
        HFile.Reader hfileReader = reader.getHFileReader();
        BloomFilter bloomFilter = reader.getGeneralBloomFilter();
        Assert.assertTrue(bloomFilter == null || bloomFilter instanceof CompoundBloomFilter);
        CompoundBloomFilter cbf = bloomFilter == null ? null : (CompoundBloomFilter) bloomFilter;
        Consumer<HFileBlock> bytesReadFunction = new Consumer<HFileBlock>() {
          @Override
          public void accept(HFileBlock block) {
            assertBlockIsCached(hfileReader, block, region.getBlockCache());
          }
        };
        readHFile(hfileReader, cbf, keyValue, bytesReadFunction);
      }
    }
  }

  private void assertBlockIsCached(HFile.Reader hfileReader, HFileBlock block,
    BlockCache blockCache) {
    if (blockCache == null) {
      return;
    }
    Path path = hfileReader.getPath();
    BlockCacheKey key = new BlockCacheKey(path, block.getOffset(), true, block.getBlockType());
    HFileBlock cachedBlock = (HFileBlock) blockCache.getBlock(key, true, false, true);
    Assert.assertNotNull(cachedBlock);
    Assert.assertEquals(block.getOnDiskSizeWithHeader(), cachedBlock.getOnDiskSizeWithHeader());
    Assert.assertEquals(block.getNextBlockOnDiskSize(), cachedBlock.getNextBlockOnDiskSize());
    cachedBlock.release();
  }

  private static class MyNoOpEncodedSeeker extends NoOpIndexBlockEncoder.NoOpEncodedSeeker {
    public long getBlockOffset(int i) {
      return blockOffsets[i];
    }

    public int getBlockDataSize(int i) {
      return blockDataSizes[i];
    }
  }
}
