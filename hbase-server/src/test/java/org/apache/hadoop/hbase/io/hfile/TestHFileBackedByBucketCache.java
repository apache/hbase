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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

/**
 * Test for file-backed BucketCache.
 */
// This is marked a LargeTest so it runs in its own JVM. We do this because we are making use of
// the cache and the cache is global. We don't want any other concurrent test polluting ours which
// can happen if more than one test in a single JVM which can happen when tests are small.
@Category({IOTests.class, LargeTests.class})
public class TestHFileBackedByBucketCache {
  private static final Log LOG = LogFactory.getLog(TestHFileBackedByBucketCache.class);
  @Rule public TestName name = new TestName();
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int ROW_LENGTH = 4;
  private Configuration conf;
  private FileSystem fs;

  // MATH! SIZING FOR THE TEST!
  // Set bucketcache to be smallest size possible which is 1MB. We do that in the test
  // @Before <code>before</code> method. Into out 1MB cache, have it so only one bucket. If
  // bucketsize is set to 125kb in size, we will have one bucket in our 1MB bucketcache. It is
  // cryptic how this comes about but basically comes down to
  // {@link BucketAllocator#FEWEST_ITEMS_IN_BUCKET} being '4'... so 4 * 125 = just over 500k or so
  // which makes for one bucket only in 1M which you can see from TRACE logging:
  //
  // Cache totalSize=532480, buckets=1, bucket capacity=
  //   532480=(4*133120)=(FEWEST_ITEMS_IN_BUCKET*(largest configured bucketcache size))
  //
  // Now into this one big bucket, we write hfileblocks....Each hfileblock has two keys because
  // first is under the BLOCKSIZE of 64k and then the second puts us over the 64k...
  // so two Cells per block...
  /**
   * Default size.
   */
  private static final int BLOCKSIZE = 64 * 1024;

  /**
   * Bucket sizes get multiplied by 4 for actual bucket size.
   * See {@link BucketAllocator#FEWEST_ITEMS_IN_BUCKET}.
   */
  private static final int BUCKETSIZE = 125 * 1024;

  /**
   * Make it so one Cell is just under a BLOCKSIZE. The second Cell puts us over the BLOCKSIZE
   * so we have two Cells per HFilBlock.
   */
  private static final int VALUE_SIZE = 33 * 1024;

  @Before
  public void before() throws IOException {
    // Do setup of a bucketcache that has one bucket only. Enable trace-level logging for
    // key classes.
    this.conf = TEST_UTIL.getConfiguration();
    this.fs = FileSystem.get(conf);

    // Set BucketCache and HFileBlock to log at trace level.
    setTraceLevel(BucketCache.class);
    setTraceLevel(HFileBlock.class);
    setTraceLevel(HFileReaderImpl.class);
    setTraceLevel(BucketAllocator.class);
  }

  //  Assumes log4j logging.
  private static void setTraceLevel(final Class<?> clazz) {
    Log testlog = LogFactory.getLog(clazz.getName());
    ((org.apache.commons.logging.impl.Log4JLogger)testlog).getLogger().
      setLevel(org.apache.log4j.Level.TRACE);
  }

  /**
   * Test that bucketcache is caching and that the persist of in-memory map works
   * @throws IOException
   */
  @Test
  public void testBucketCacheCachesAndPersists() throws IOException {
    // Set up a bucket cache. Set up one that will persist by passing a
    // hbase.bucketcache.persistent.path value to store the in-memory map of what is out in
    // the file-backed bucketcache. Set bucketcache to have one size only, BUCKETSIZE.
    // See "MATH! SIZING FOR THE TEST!" note above around declaration of BUCKETSIZE
    String bucketCacheDataFile =
      (new Path(TEST_UTIL.getDataTestDir(), "bucketcache.data")).toString();
    (new File(bucketCacheDataFile)).getParentFile().mkdirs();
    this.conf.set("hbase.bucketcache.ioengine", "file:" + bucketCacheDataFile);
    this.conf.set("hbase.bucketcache.persistent.path", bucketCacheDataFile + ".map");
    this.conf.setStrings("hbase.bucketcache.bucket.sizes", Integer.toString(BUCKETSIZE));
    // This is minimum bucketcache size.... 1MB.
    this.conf.setInt("hbase.bucketcache.size", 1);
    // Write 8 entries which should make for four hfileBlocks.
    final int count = 8;
    final int hfileBlockCount = 4;
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), this.name.getMethodName());
    // Clear out any existing global cache instance. Will pollute our tests below. Any concurrent
    // running test will pollute our results below.
    CacheConfig.GLOBAL_BLOCK_CACHE_INSTANCE = null;
    CacheConfig cacheConfig = new CacheConfig(conf);
    List<Cell> writtenCells = writeFile(hfilePath, Compression.Algorithm.NONE, cacheConfig, count);
    CacheStats stats = cacheConfig.getBlockCache().getStats();
    List<Cell> readCells = readFile(hfilePath, cacheConfig);
    assertTrue(!writtenCells.isEmpty());
    assertEquals(writtenCells.size(), readCells.size());
    assertEquals(hfileBlockCount, stats.getMissCount());
    assertEquals(1, stats.getHitCount()); // readFile will read first block is from cache.

    // Now, close out the cache and then reopen and verify that cache still has our blocks.
    // Assert that persistence works.
    cacheConfig.getBlockCache().shutdown();
    // Need to clear the global cache else the new CacheConfig won't create a bucketcache but
    // just reuse the old one.
    CacheConfig.GLOBAL_BLOCK_CACHE_INSTANCE = null;
    cacheConfig = new CacheConfig(conf);
    stats = cacheConfig.getBlockCache().getStats();
    assertEquals(0, stats.getHitCachingCount());
    readCells = readFile(hfilePath, cacheConfig);
    // readFile will read all hfileblocs in the file, hfileBlockCount, and then one more, so + 1.
    assertEquals(hfileBlockCount + 1, stats.getHitCachingCount());
  }

  /**
   * Write a file with <code>count</code> entries.
   * @return The Cells written to the file.
   * @throws IOException
   */
  private List<Cell> writeFile(final Path hfilePath, final Compression.Algorithm compressAlgo,
      final CacheConfig cacheConfig, final int count)
  throws IOException {
    List<Cell> cells = new ArrayList<Cell>(count);
    HFileContext context =
        new HFileContextBuilder().withBlockSize(BLOCKSIZE).withCompression(compressAlgo).build();
    try (HFile.Writer writer = new HFile.WriterFactory(conf, cacheConfig).
        withPath(fs, hfilePath).
        withFileContext(context).
        withComparator(CellComparator.COMPARATOR).
        create()) {
      byte [] valueBytes = new byte [VALUE_SIZE];
      for (int i = 0; i < valueBytes.length; i++) valueBytes[i] = '0';
      for (int i = 0; i < count; ++i) {
        byte[] keyBytes = format(i);
        KeyValue keyValue = new KeyValue(keyBytes, HConstants.CATALOG_FAMILY, keyBytes,
            HConstants.LATEST_TIMESTAMP, valueBytes);
        writer.append(keyValue);
        cells.add(keyValue);
      }
    }
    return cells;
  }

  /**
   * Read the whole file, then read the first block so we get something from cache for sure.
   * So... there are TOTAL_BLOCKS_IN_FILE read + 1. See math at head of this class.
   * @return The Cells read from the file.
   */
  private List<Cell> readFile(final Path hfilePath, final CacheConfig cacheConfig)
  throws IOException {
    List<Cell> cells = new ArrayList<Cell>();
    try (HFile.Reader reader = HFile.createReader(this.fs, hfilePath, cacheConfig, this.conf);
        HFileScanner scanner = reader.getScanner(true, true)) {
      scanner.seekTo();
      do {
        cells.add(scanner.getCell());
        LOG.info(scanner.getKey());
      } while (scanner.next());
      // Do a random seek just so we see a block coming from cache.
      scanner.seekTo(reader.getFirstKey());
      scanner.next();
      LOG.info(scanner.getCell());
    }
    return cells;
  }

  /*
   * Format passed integer.
   * @param number
   * @return Returns zero-prefixed ROW_LENGTH-byte wide decimal version of passed
   * number (Does absolute in case number is negative).
   */
  private static byte [] format(final int number) {
    byte [] b = new byte[ROW_LENGTH];
    int d = Math.abs(number);
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte)((d % 10) + '0');
      d /= 10;
    }
    return b;
  }
}
