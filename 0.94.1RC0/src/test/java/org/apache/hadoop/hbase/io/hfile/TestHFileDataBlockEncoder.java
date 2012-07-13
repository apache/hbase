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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.RedundantKVGenerator;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestHFileDataBlockEncoder {
  private Configuration conf;
  private final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private HFileDataBlockEncoderImpl blockEncoder;
  private RedundantKVGenerator generator = new RedundantKVGenerator();
  private SchemaConfigured UNKNOWN_TABLE_AND_CF =
      SchemaConfigured.createUnknown();
  private boolean includesMemstoreTS;

  /**
   * Create test for given data block encoding configuration.
   * @param blockEncoder What kind of encoding policy will be used.
   */
  public TestHFileDataBlockEncoder(HFileDataBlockEncoderImpl blockEncoder,
      boolean includesMemstoreTS) {
    this.blockEncoder = blockEncoder;
    this.includesMemstoreTS = includesMemstoreTS;
    System.err.println("On-disk encoding: " + blockEncoder.getEncodingOnDisk()
        + ", in-cache encoding: " + blockEncoder.getEncodingInCache()
        + ", includesMemstoreTS: " + includesMemstoreTS);
  }

  /**
   * Preparation before JUnit test.
   */
  @Before
  public void setUp() {
    conf = TEST_UTIL.getConfiguration();
    SchemaMetrics.configureGlobally(conf);
  }

  /**
   * Cleanup after JUnit test.
   */
  @After
  public void tearDown() throws IOException {
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test putting and taking out blocks into cache with different
   * encoding options.
   */
  @Test
  public void testEncodingWithCache() {
    HFileBlock block = getSampleHFileBlock();
    LruBlockCache blockCache =
        new LruBlockCache(8 * 1024 * 1024, 32 * 1024);
    HFileBlock cacheBlock = blockEncoder.diskToCacheFormat(block, false);
    BlockCacheKey cacheKey = new BlockCacheKey("test", 0);
    blockCache.cacheBlock(cacheKey, cacheBlock);

    HeapSize heapSize = blockCache.getBlock(cacheKey, false);
    assertTrue(heapSize instanceof HFileBlock);

    HFileBlock returnedBlock = (HFileBlock) heapSize;;

    if (blockEncoder.getEncodingInCache() ==
        DataBlockEncoding.NONE) {
      assertEquals(block.getBufferWithHeader(),
          returnedBlock.getBufferWithHeader());
    } else {
      if (BlockType.ENCODED_DATA != returnedBlock.getBlockType()) {
        System.out.println(blockEncoder);
      }
      assertEquals(BlockType.ENCODED_DATA, returnedBlock.getBlockType());
    }
  }

  /**
   * Test writing to disk.
   */
  @Test
  public void testEncodingWritePath() {
    // usually we have just block without headers, but don't complicate that
    HFileBlock block = getSampleHFileBlock();
    Pair<ByteBuffer, BlockType> result =
        blockEncoder.beforeWriteToDisk(block.getBufferWithoutHeader(),
            includesMemstoreTS, HFileBlock.DUMMY_HEADER);

    int size = result.getFirst().limit() - HFileBlock.HEADER_SIZE;
    HFileBlock blockOnDisk = new HFileBlock(result.getSecond(),
        size, size, -1, result.getFirst(), HFileBlock.FILL_HEADER, 0,
        includesMemstoreTS, block.getMinorVersion(),
        block.getBytesPerChecksum(), block.getChecksumType(),
        block.getOnDiskDataSizeWithHeader());

    if (blockEncoder.getEncodingOnDisk() !=
        DataBlockEncoding.NONE) {
      assertEquals(BlockType.ENCODED_DATA, blockOnDisk.getBlockType());
      assertEquals(blockEncoder.getEncodingOnDisk().getId(),
          blockOnDisk.getDataBlockEncodingId());
    } else {
      assertEquals(BlockType.DATA, blockOnDisk.getBlockType());
    }
  }

  /**
   * Test converting blocks from disk to cache format.
   */
  @Test
  public void testEncodingReadPath() {
    HFileBlock origBlock = getSampleHFileBlock();
    blockEncoder.diskToCacheFormat(origBlock, false);
  }

  private HFileBlock getSampleHFileBlock() {
    ByteBuffer keyValues = RedundantKVGenerator.convertKvToByteBuffer(
        generator.generateTestKeyValues(60), includesMemstoreTS);
    int size = keyValues.limit();
    ByteBuffer buf = ByteBuffer.allocate(size + HFileBlock.HEADER_SIZE);
    buf.position(HFileBlock.HEADER_SIZE);
    keyValues.rewind();
    buf.put(keyValues);
    HFileBlock b = new HFileBlock(BlockType.DATA, size, size, -1, buf,
        HFileBlock.FILL_HEADER, 0, includesMemstoreTS, 
        HFileReaderV2.MAX_MINOR_VERSION, 0, ChecksumType.NULL.getCode(), 0);
    UNKNOWN_TABLE_AND_CF.passSchemaMetricsTo(b);
    return b;
  }

  /**
   * @return All possible data block encoding configurations
   */
  @Parameters
  public static Collection<Object[]> getAllConfigurations() {
    List<Object[]> configurations =
        new ArrayList<Object[]>();

    for (DataBlockEncoding diskAlgo : DataBlockEncoding.values()) {
      for (DataBlockEncoding cacheAlgo : DataBlockEncoding.values()) {
        if (diskAlgo != cacheAlgo && diskAlgo != DataBlockEncoding.NONE) {
          // We allow (1) the same encoding on disk and in cache, and
          // (2) some encoding in cache but no encoding on disk (for testing).
          continue;
        }
        for (boolean includesMemstoreTS : new boolean[] {false, true}) {
          configurations.add(new Object[] {
              new HFileDataBlockEncoderImpl(diskAlgo, cacheAlgo),
              new Boolean(includesMemstoreTS)});
        }
      }
    }

    return configurations;
  }
}
