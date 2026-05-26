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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Make sure we always cache important block types, such as index blocks, as long as we have a block
 * cache, even though block caching might be disabled for the column family.
 * <p>
 * TODO: This test writes a lot of data and only tests the most basic of metrics. Cache stats need
 * to reveal more about what is being cached whether DATA or INDEX blocks and then we could do more
 * verification in this test.
 */
@Tag(IOTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: hfileVersion={0}, cfCacheEnabled={1}")
public class TestForceCacheImportantBlocks {

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

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

  private static final Algorithm COMPRESSION_ALGORITHM = Compression.Algorithm.GZ;
  private static final BloomType BLOOM_TYPE = BloomType.ROW;

  @SuppressWarnings("unused")
  // Currently unused.
  private final int hfileVersion;
  private final boolean cfCacheEnabled;

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(3, true), Arguments.of(3, false));
  }

  public TestForceCacheImportantBlocks(int hfileVersion, boolean cfCacheEnabled) {
    this.hfileVersion = hfileVersion;
    this.cfCacheEnabled = cfCacheEnabled;
    TEST_UTIL.getConfiguration().setInt(HFile.FORMAT_VERSION_KEY, hfileVersion);
  }

  @BeforeEach
  public void setup() {
    HFile.DATABLOCK_READ_COUNT.reset();
  }

  @TestTemplate
  public void testCacheBlocks() throws IOException {
    // Set index block size to be the same as normal block size.
    TEST_UTIL.getConfiguration().setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, BLOCK_SIZE);
    BlockCache blockCache = BlockCacheFactory.createBlockCache(TEST_UTIL.getConfiguration());
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(CF)).setMaxVersions(MAX_VERSIONS)
        .setCompressionType(COMPRESSION_ALGORITHM).setBloomFilterType(BLOOM_TYPE)
        .setBlocksize(BLOCK_SIZE).setBlockCacheEnabled(cfCacheEnabled).build();
    HRegion region = TEST_UTIL.createTestRegion(TABLE, cfd, blockCache);
    CacheStats stats = blockCache.getStats();
    writeTestData(region);
    assertEquals(0, stats.getHitCount());
    assertEquals(0, HFile.DATABLOCK_READ_COUNT.sum());
    // Do a single get, take count of caches. If we are NOT caching DATA blocks, the miss
    // count should go up. Otherwise, all should be cached and the miss count should not rise.
    region.get(new Get(Bytes.toBytes("row" + 0)));
    assertTrue(stats.getHitCount() > 0);
    assertTrue(HFile.DATABLOCK_READ_COUNT.sum() > 0);
    long missCount = stats.getMissCount();
    region.get(new Get(Bytes.toBytes("row" + 0)));
    if (this.cfCacheEnabled) assertEquals(missCount, stats.getMissCount());
    else assertTrue(stats.getMissCount() > missCount);
  }

  private void writeTestData(HRegion region) throws IOException {
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
