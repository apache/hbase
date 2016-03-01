/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Make sure we always cache important block types, such as index blocks, as
 * long as we have a block cache, even though block caching might be disabled
 * for the column family.
 * 
 * <p>TODO: This test writes a lot of data and only tests the most basic of metrics.  Cache stats
 * need to reveal more about what is being cached whether DATA or INDEX blocks and then we could
 * do more verification in this test.
 */
@Category({IOTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class TestForceCacheImportantBlocks {
  private final HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();

  private static final String TABLE = "myTable";
  private static final String CF = "myCF";
  private static final byte[] CF_BYTES = Bytes.toBytes(CF);
  private static final int MAX_VERSIONS = 3;
  private static final int NUM_HFILES = 5;

  private static final int ROWS_PER_HFILE = 100;
  private static final int NUM_ROWS = NUM_HFILES * ROWS_PER_HFILE;
  private static final int NUM_COLS_PER_ROW = 50;
  private static final int NUM_TIMESTAMPS_PER_COL = 50;

  /** Extremely small block size, so that we can get some index blocks */
  private static final int BLOCK_SIZE = 256;
  
  private static final Algorithm COMPRESSION_ALGORITHM =
      Compression.Algorithm.GZ;
  private static final BloomType BLOOM_TYPE = BloomType.ROW;

  @SuppressWarnings("unused")
  // Currently unused.
  private final int hfileVersion;
  private final boolean cfCacheEnabled;

  @Parameters
  public static Collection<Object[]> parameters() {
    // HFile versions
    return Arrays.asList(
      new Object[] { 3, true },
      new Object[] { 3, false }
    );
  }

  public TestForceCacheImportantBlocks(int hfileVersion, boolean cfCacheEnabled) {
    this.hfileVersion = hfileVersion;
    this.cfCacheEnabled = cfCacheEnabled;
    TEST_UTIL.getConfiguration().setInt(HFile.FORMAT_VERSION_KEY, hfileVersion);
  }

  @Before
  public void setup() {
    // Make sure we make a new one each time.
    CacheConfig.GLOBAL_BLOCK_CACHE_INSTANCE = null;
    HFile.DATABLOCK_READ_COUNT.set(0);
  }

  @Test
  public void testCacheBlocks() throws IOException {
    // Set index block size to be the same as normal block size.
    TEST_UTIL.getConfiguration().setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, BLOCK_SIZE);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(CF)).setMaxVersions(MAX_VERSIONS).
      setCompressionType(COMPRESSION_ALGORITHM).
      setBloomFilterType(BLOOM_TYPE);
    hcd.setBlocksize(BLOCK_SIZE);
    hcd.setBlockCacheEnabled(cfCacheEnabled);
    Region region = TEST_UTIL.createTestRegion(TABLE, hcd);
    BlockCache cache = region.getStore(hcd.getName()).getCacheConfig().getBlockCache();
    CacheStats stats = cache.getStats();
    writeTestData(region);
    assertEquals(0, stats.getHitCount());
    assertEquals(0, HFile.DATABLOCK_READ_COUNT.get());
    // Do a single get, take count of caches.  If we are NOT caching DATA blocks, the miss
    // count should go up.  Otherwise, all should be cached and the miss count should not rise.
    region.get(new Get(Bytes.toBytes("row" + 0)));
    assertTrue(stats.getHitCount() > 0);
    assertTrue(HFile.DATABLOCK_READ_COUNT.get() > 0);
    long missCount = stats.getMissCount();
    region.get(new Get(Bytes.toBytes("row" + 0)));
    if (this.cfCacheEnabled) assertEquals(missCount, stats.getMissCount());
    else assertTrue(stats.getMissCount() > missCount);
  }

  private void writeTestData(Region region) throws IOException {
    for (int i = 0; i < NUM_ROWS; ++i) {
      Put put = new Put(Bytes.toBytes("row" + i));
      for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
        for (long ts = 1; ts < NUM_TIMESTAMPS_PER_COL; ++ts) {
          put.addColumn(CF_BYTES, Bytes.toBytes("col" + j), ts,
                  Bytes.toBytes("value" + i + "_" + j + "_" + ts));
        }
      }
      region.put(put);
      if ((i + 1) % ROWS_PER_HFILE == 0) {
        region.flush(true);
      }
    }
  }
}
