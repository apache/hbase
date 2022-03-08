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
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.BUFFER_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MAX_BUFFER_COUNT_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.MAX_CHUNK_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.MIN_INDEX_NUM_ENTRIES_KEY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileReaderImpl.HFileScannerImpl;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.hfile.bucket.TestBucketCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({ IOTests.class, LargeTests.class })
public class TestHFileScannerImplReferenceCount {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileScannerImplReferenceCount.class);

  @Rule
  public TestName CASE = new TestName();

  @Parameters(name = "{index}: ioengine={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[] { "file" }, new Object[] { "offheap" },
      new Object[] { "mmap" }, new Object[] { "pmem" });
  }

  @Parameter
  public String ioengine;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHFileScannerImplReferenceCount.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final Random RNG = new Random(9713312); // Just a fixed seed.
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] SUFFIX = randLongBytes();
  private static final int CELL_COUNT = 1000;

  private static byte[] randLongBytes() {
    byte[] keys = new byte[30];
    Bytes.random(keys);
    return keys;
  }

  // It's a deep copy of configuration of UTIL, DON'T use shallow copy.
  private Configuration conf;
  private Path workDir;
  private FileSystem fs;
  private Path hfilePath;
  private Cell firstCell = null;
  private Cell secondCell = null;
  private ByteBuffAllocator allocator;

  @BeforeClass
  public static void setUpBeforeClass() {
    Configuration conf = UTIL.getConfiguration();
    // Set the max chunk size and min entries key to be very small for index block, so that we can
    // create an index block tree with level >= 2.
    conf.setInt(MAX_CHUNK_SIZE_KEY, 10);
    conf.setInt(MIN_INDEX_NUM_ENTRIES_KEY, 2);
    // Create a bucket cache with 32MB.
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(BUCKET_CACHE_SIZE_KEY, 32);
    conf.setInt(BUFFER_SIZE_KEY, 1024);
    conf.setInt(MAX_BUFFER_COUNT_KEY, 32 * 1024);
    // All allocated ByteBuff are pooled ByteBuff.
    conf.setInt(MIN_ALLOCATE_SIZE_KEY, 0);
  }

  @Before
  public void setUp() throws IOException {
    String caseName = CASE.getMethodName().replaceAll("[^a-zA-Z0-9]", "_");
    this.workDir = UTIL.getDataTestDir(caseName);
    if (!"offheap".equals(ioengine)) {
      ioengine = ioengine + ":" + workDir.toString() + "/cachedata";
    }
    UTIL.getConfiguration().set(BUCKET_CACHE_IOENGINE_KEY, ioengine);
    this.firstCell = null;
    this.secondCell = null;
    this.allocator = ByteBuffAllocator.create(UTIL.getConfiguration(), true);
    this.conf = new Configuration(UTIL.getConfiguration());
    this.fs = this.workDir.getFileSystem(conf);
    this.hfilePath = new Path(this.workDir, caseName + EnvironmentEdgeManager.currentTime());
    LOG.info("Start to write {} cells into hfile: {}, case:{}", CELL_COUNT, hfilePath, caseName);
  }

  @After
  public void tearDown() throws IOException {
    this.allocator.clean();
    this.fs.delete(this.workDir, true);
  }

  private void waitBucketCacheFlushed(BlockCache cache) throws InterruptedException {
    Assert.assertTrue(cache instanceof CombinedBlockCache);
    BlockCache[] blockCaches = cache.getBlockCaches();
    Assert.assertEquals(blockCaches.length, 2);
    Assert.assertTrue(blockCaches[1] instanceof BucketCache);
    TestBucketCache.waitUntilAllFlushedToBucket((BucketCache) blockCaches[1]);
  }

  private void writeHFile(Configuration conf, FileSystem fs, Path hfilePath, Algorithm compression,
      DataBlockEncoding encoding, int cellCount) throws IOException {
    HFileContext context =
        new HFileContextBuilder().withBlockSize(1).withDataBlockEncoding(DataBlockEncoding.NONE)
            .withCompression(compression).withDataBlockEncoding(encoding).build();
    try (HFile.Writer writer =
        new HFile.WriterFactory(conf, new CacheConfig(conf)).withPath(fs, hfilePath)
            .withFileContext(context).create()) {
      for (int i = 0; i < cellCount; ++i) {
        byte[] keyBytes = Bytes.add(Bytes.toBytes(i), SUFFIX);
        // A random-length random value.
        byte[] valueBytes = RandomKeyValueUtil.randomValue(RNG);
        KeyValue keyValue =
            new KeyValue(keyBytes, FAMILY, QUALIFIER, HConstants.LATEST_TIMESTAMP, valueBytes);
        if (firstCell == null) {
          firstCell = keyValue;
        } else if (secondCell == null) {
          secondCell = keyValue;
        }
        writer.append(keyValue);
      }
    }
  }

  /**
   * A careful UT for validating the reference count mechanism, if want to change this UT please
   * read the design doc in HBASE-21879 firstly and make sure that understand the refCnt design.
   */
  private void testReleaseBlock(Algorithm compression, DataBlockEncoding encoding)
      throws Exception {
    writeHFile(conf, fs, hfilePath, compression, encoding, CELL_COUNT);
    HFileBlock curBlock, prevBlock;
    BlockCache defaultBC = BlockCacheFactory.createBlockCache(conf);
    CacheConfig cacheConfig = new CacheConfig(conf, null, defaultBC, allocator);
    Assert.assertNotNull(defaultBC);
    Assert.assertTrue(cacheConfig.isCombinedBlockCache());
    HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConfig, true, conf);
    Assert.assertTrue(reader instanceof HFileReaderImpl);
    // We've build a HFile tree with index = 16.
    Assert.assertEquals(16, reader.getTrailer().getNumDataIndexLevels());

    HFileScannerImpl scanner = (HFileScannerImpl) reader.getScanner(conf, true, true, false);
    HFileBlock block1 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(firstCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    waitBucketCacheFlushed(defaultBC);
    Assert.assertTrue(block1.getBlockType().isData());
    Assert.assertFalse(block1 instanceof ExclusiveMemHFileBlock);

    HFileBlock block2 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(secondCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    waitBucketCacheFlushed(defaultBC);
    Assert.assertTrue(block2.getBlockType().isData());
    Assert.assertFalse(block2 instanceof ExclusiveMemHFileBlock);
    // Only one refCnt for RPC path.
    Assert.assertEquals(block1.refCnt(), 1);
    Assert.assertEquals(block2.refCnt(), 1);
    Assert.assertFalse(block1 == block2);

    scanner.seekTo(firstCell);
    curBlock = scanner.curBlock;
    this.assertRefCnt(curBlock, 2);

    // Seek to the block again, the curBlock won't change and won't read from BlockCache. so
    // refCnt should be unchanged.
    scanner.seekTo(firstCell);
    Assert.assertTrue(curBlock == scanner.curBlock);
    this.assertRefCnt(curBlock, 2);
    prevBlock = curBlock;

    scanner.seekTo(secondCell);
    curBlock = scanner.curBlock;
    this.assertRefCnt(prevBlock, 2);
    this.assertRefCnt(curBlock, 2);

    // After shipped, the prevBlock will be release, but curBlock is still referenced by the
    // curBlock.
    scanner.shipped();
    this.assertRefCnt(prevBlock, 1);
    this.assertRefCnt(curBlock, 2);

    // Try to ship again, though with nothing to client.
    scanner.shipped();
    this.assertRefCnt(prevBlock, 1);
    this.assertRefCnt(curBlock, 2);

    // The curBlock will also be released.
    scanner.close();
    this.assertRefCnt(curBlock, 1);

    // Finish the block & block2 RPC path
    Assert.assertTrue(block1.release());
    Assert.assertTrue(block2.release());

    // Evict the LRUBlockCache
    Assert.assertTrue(defaultBC.evictBlocksByHfileName(hfilePath.getName()) >= 2);
    Assert.assertEquals(prevBlock.refCnt(), 0);
    Assert.assertEquals(curBlock.refCnt(), 0);

    int count = 0;
    Assert.assertTrue(scanner.seekTo());
    ++count;
    while (scanner.next()) {
      count++;
    }
    assertEquals(CELL_COUNT, count);
  }

  /**
   * See HBASE-22480
   */
  @Test
  public void testSeekBefore() throws Exception {
    HFileBlock curBlock, prevBlock;
    writeHFile(conf, fs, hfilePath, Algorithm.NONE, DataBlockEncoding.NONE, CELL_COUNT);
    BlockCache defaultBC = BlockCacheFactory.createBlockCache(conf);
    CacheConfig cacheConfig = new CacheConfig(conf, null, defaultBC, allocator);
    Assert.assertNotNull(defaultBC);
    Assert.assertTrue(cacheConfig.isCombinedBlockCache());
    HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConfig, true, conf);
    Assert.assertTrue(reader instanceof HFileReaderImpl);
    // We've build a HFile tree with index = 16.
    Assert.assertEquals(16, reader.getTrailer().getNumDataIndexLevels());

    HFileScannerImpl scanner = (HFileScannerImpl) reader.getScanner(conf, true, true, false);
    HFileBlock block1 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(firstCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    Assert.assertTrue(block1.getBlockType().isData());
    Assert.assertFalse(block1 instanceof ExclusiveMemHFileBlock);
    HFileBlock block2 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(secondCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    Assert.assertTrue(block2.getBlockType().isData());
    Assert.assertFalse(block2 instanceof ExclusiveMemHFileBlock);
    // Wait until flushed to IOEngine;
    waitBucketCacheFlushed(defaultBC);
    // One RPC reference path.
    Assert.assertEquals(block1.refCnt(), 1);
    Assert.assertEquals(block2.refCnt(), 1);

    // Let the curBlock refer to block2.
    scanner.seekTo(secondCell);
    curBlock = scanner.curBlock;
    Assert.assertFalse(curBlock == block2);
    Assert.assertEquals(1, block2.refCnt());
    this.assertRefCnt(curBlock, 2);
    prevBlock = scanner.curBlock;

    // Release the block1, no other reference.
    Assert.assertTrue(block1.release());
    Assert.assertEquals(0, block1.refCnt());
    // Release the block2, no other reference.
    Assert.assertTrue(block2.release());
    Assert.assertEquals(0, block2.refCnt());

    // Do the seekBefore: the newBlock will be the previous block of curBlock.
    Assert.assertTrue(scanner.seekBefore(secondCell));
    Assert.assertEquals(scanner.prevBlocks.size(), 1);
    Assert.assertTrue(scanner.prevBlocks.get(0) == prevBlock);
    curBlock = scanner.curBlock;
    // the curBlock is read from IOEngine, so a different block.
    Assert.assertFalse(curBlock == block1);
    // Two reference for curBlock: 1. scanner; 2. blockCache.
    this.assertRefCnt(curBlock, 2);
    // Reference count of prevBlock must be unchanged because we haven't shipped.
    this.assertRefCnt(prevBlock, 2);

    // Do the shipped
    scanner.shipped();
    Assert.assertEquals(scanner.prevBlocks.size(), 0);
    Assert.assertNotNull(scanner.curBlock);
    this.assertRefCnt(curBlock, 2);
    this.assertRefCnt(prevBlock, 1);

    // Do the close
    scanner.close();
    Assert.assertNull(scanner.curBlock);
    this.assertRefCnt(curBlock, 1);
    this.assertRefCnt(prevBlock, 1);

    Assert.assertTrue(defaultBC.evictBlocksByHfileName(hfilePath.getName()) >= 2);
    Assert.assertEquals(0, curBlock.refCnt());
    Assert.assertEquals(0, prevBlock.refCnt());

    // Reload the block1 again.
    block1 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(firstCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    // Wait until flushed to IOEngine;
    waitBucketCacheFlushed(defaultBC);
    Assert.assertTrue(block1.getBlockType().isData());
    Assert.assertFalse(block1 instanceof ExclusiveMemHFileBlock);
    Assert.assertTrue(block1.release());
    Assert.assertEquals(0, block1.refCnt());
    // Re-seek to the begin.
    Assert.assertTrue(scanner.seekTo());
    curBlock = scanner.curBlock;
    Assert.assertFalse(curBlock == block1);
    this.assertRefCnt(curBlock, 2);
    // Return false because firstCell <= c[0]
    Assert.assertFalse(scanner.seekBefore(firstCell));
    // The block1 shouldn't be released because we still don't do the shipped or close.
    this.assertRefCnt(curBlock, 2);

    scanner.close();
    this.assertRefCnt(curBlock, 1);
    Assert.assertTrue(defaultBC.evictBlocksByHfileName(hfilePath.getName()) >= 1);
    Assert.assertEquals(0, curBlock.refCnt());
  }

  private void assertRefCnt(HFileBlock block, int value) {
    if (ioengine.startsWith("offheap") || ioengine.startsWith("pmem")) {
      Assert.assertEquals(value, block.refCnt());
    } else {
      Assert.assertEquals(value - 1, block.refCnt());
    }
  }

  @Test
  public void testDefault() throws Exception {
    testReleaseBlock(Algorithm.NONE, DataBlockEncoding.NONE);
  }

  @Test
  public void testCompression() throws Exception {
    testReleaseBlock(Algorithm.GZ, DataBlockEncoding.NONE);
  }

  @Test
  public void testDataBlockEncoding() throws Exception {
    testReleaseBlock(Algorithm.NONE, DataBlockEncoding.ROW_INDEX_V1);
  }

  @Test
  public void testDataBlockEncodingAndCompression() throws Exception {
    testReleaseBlock(Algorithm.GZ, DataBlockEncoding.ROW_INDEX_V1);
  }

  @Test
  public void testWithLruBlockCache() throws Exception {
    HFileBlock curBlock;
    writeHFile(conf, fs, hfilePath, Algorithm.NONE, DataBlockEncoding.NONE, CELL_COUNT);
    // Set LruBlockCache
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "");
    BlockCache defaultBC = BlockCacheFactory.createBlockCache(conf);
    CacheConfig cacheConfig = new CacheConfig(conf, null, defaultBC, allocator);
    Assert.assertNotNull(defaultBC);
    Assert.assertFalse(cacheConfig.isCombinedBlockCache()); // Must be LruBlockCache.
    HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConfig, true, conf);
    Assert.assertTrue(reader instanceof HFileReaderImpl);
    // We've build a HFile tree with index = 16.
    Assert.assertEquals(16, reader.getTrailer().getNumDataIndexLevels());

    HFileScannerImpl scanner = (HFileScannerImpl) reader.getScanner(conf, true, true, false);
    HFileBlock block1 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(firstCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    Assert.assertTrue(block1.getBlockType().isData());
    Assert.assertTrue(block1 instanceof ExclusiveMemHFileBlock);
    HFileBlock block2 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(secondCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();
    Assert.assertTrue(block2.getBlockType().isData());
    Assert.assertTrue(block2 instanceof ExclusiveMemHFileBlock);
    // One RPC reference path.
    Assert.assertEquals(block1.refCnt(), 0);
    Assert.assertEquals(block2.refCnt(), 0);

    scanner.seekTo(firstCell);
    curBlock = scanner.curBlock;
    Assert.assertTrue(curBlock == block1);
    Assert.assertEquals(curBlock.refCnt(), 0);
    Assert.assertTrue(scanner.prevBlocks.isEmpty());

    // Switch to next block
    scanner.seekTo(secondCell);
    curBlock = scanner.curBlock;
    Assert.assertTrue(curBlock == block2);
    Assert.assertEquals(curBlock.refCnt(), 0);
    Assert.assertEquals(curBlock.retain().refCnt(), 0);
    // Only pooled HFileBlock will be kept in prevBlocks and ExclusiveMemHFileBlock will never keep
    // in prevBlocks.
    Assert.assertTrue(scanner.prevBlocks.isEmpty());

    // close the scanner
    scanner.close();
    Assert.assertNull(scanner.curBlock);
    Assert.assertTrue(scanner.prevBlocks.isEmpty());
  }

  @Test
  public void testDisabledBlockCache() throws Exception {
    writeHFile(conf, fs, hfilePath, Algorithm.NONE, DataBlockEncoding.NONE, CELL_COUNT);
    // Set LruBlockCache
    conf.setFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    BlockCache defaultBC = BlockCacheFactory.createBlockCache(conf);
    Assert.assertNull(defaultBC);
    CacheConfig cacheConfig = new CacheConfig(conf, null, defaultBC, allocator);
    Assert.assertFalse(cacheConfig.isCombinedBlockCache()); // Must be LruBlockCache.
    HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConfig, true, conf);
    Assert.assertTrue(reader instanceof HFileReaderImpl);
    // We've build a HFile tree with index = 16.
    Assert.assertEquals(16, reader.getTrailer().getNumDataIndexLevels());

    HFileBlock block1 = reader.getDataBlockIndexReader()
        .loadDataBlockWithScanInfo(firstCell, null, true, true, false,
            DataBlockEncoding.NONE, reader).getHFileBlock();

    Assert.assertTrue(block1.isSharedMem());
    Assert.assertTrue(block1 instanceof SharedMemHFileBlock);
    Assert.assertEquals(1, block1.refCnt());
    Assert.assertTrue(block1.release());
  }
}
