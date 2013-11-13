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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.test.RedundantKVGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestHFileDataBlockEncoder {
  private HFileDataBlockEncoderImpl blockEncoder;
  private RedundantKVGenerator generator = new RedundantKVGenerator();
  private boolean includesMemstoreTS;

  /**
   * Create test for given data block encoding configuration.
   * @param blockEncoder What kind of encoding policy will be used.
   */
  public TestHFileDataBlockEncoder(HFileDataBlockEncoderImpl blockEncoder,
      boolean includesMemstoreTS) {
    this.blockEncoder = blockEncoder;
    this.includesMemstoreTS = includesMemstoreTS;
    System.err.println("Encoding: " + blockEncoder.getDataBlockEncoding()
        + ", includesMemstoreTS: " + includesMemstoreTS);
  }

  /**
   * Test putting and taking out blocks into cache with different
   * encoding options.
   * @throws IOException 
   */
  @Test
  public void testEncodingWithCache() throws IOException {
    HFileBlock block = getSampleHFileBlock();
    LruBlockCache blockCache =
        new LruBlockCache(8 * 1024 * 1024, 32 * 1024);
    HFileBlock cacheBlock = createBlockOnDisk(block);
    BlockCacheKey cacheKey = new BlockCacheKey("test", 0);
    blockCache.cacheBlock(cacheKey, cacheBlock);

    HeapSize heapSize = blockCache.getBlock(cacheKey, false, false);
    assertTrue(heapSize instanceof HFileBlock);

    HFileBlock returnedBlock = (HFileBlock) heapSize;;

    if (blockEncoder.getDataBlockEncoding() ==
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

  /** Test for HBASE-5746. */
  @Test
  public void testHeaderSizeInCacheWithoutChecksum() throws Exception {
    int headerSize = HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
    // Create some KVs and create the block with old-style header.
    ByteBuffer keyValues = RedundantKVGenerator.convertKvToByteBuffer(
        generator.generateTestKeyValues(60), includesMemstoreTS);
    int size = keyValues.limit();
    ByteBuffer buf = ByteBuffer.allocate(size + headerSize);
    buf.position(headerSize);
    keyValues.rewind();
    buf.put(keyValues);
    HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, buf,
        HFileBlock.FILL_HEADER, 0, includesMemstoreTS,
        HFileBlock.MINOR_VERSION_NO_CHECKSUM, 0, ChecksumType.NULL.getCode(), 0);
    HFileBlock cacheBlock = createBlockOnDisk(block);
    assertEquals(headerSize, cacheBlock.getDummyHeaderForVersion().length);
  }

  private HFileBlock createBlockOnDisk(HFileBlock block) throws IOException {
    int size;
    HFileBlockEncodingContext context = new HFileBlockDefaultEncodingContext(
        Compression.Algorithm.NONE, blockEncoder.getDataBlockEncoding(),
        HConstants.HFILEBLOCK_DUMMY_HEADER);
    context.setDummyHeader(block.getDummyHeaderForVersion());
    blockEncoder.beforeWriteToDisk(block.getBufferWithoutHeader(),
            includesMemstoreTS, context, block.getBlockType());
    byte[] encodedBytes = context.getUncompressedBytesWithHeader();
    size = encodedBytes.length - block.getDummyHeaderForVersion().length;
    return new HFileBlock(context.getBlockType(), size, size, -1,
            ByteBuffer.wrap(encodedBytes), HFileBlock.FILL_HEADER, 0, includesMemstoreTS,
            block.getMinorVersion(), block.getBytesPerChecksum(), block.getChecksumType(),
            block.getOnDiskDataSizeWithHeader());
  }

  /**
   * Test writing to disk.
   * @throws IOException
   */
  @Test
  public void testEncodingWritePath() throws IOException {
    // usually we have just block without headers, but don't complicate that
    HFileBlock block = getSampleHFileBlock();
    HFileBlock blockOnDisk = createBlockOnDisk(block);

    if (blockEncoder.getDataBlockEncoding() !=
        DataBlockEncoding.NONE) {
      assertEquals(BlockType.ENCODED_DATA, blockOnDisk.getBlockType());
      assertEquals(blockEncoder.getDataBlockEncoding().getId(),
          blockOnDisk.getDataBlockEncodingId());
    } else {
      assertEquals(BlockType.DATA, blockOnDisk.getBlockType());
    }
  }

  private HFileBlock getSampleHFileBlock() {
    ByteBuffer keyValues = RedundantKVGenerator.convertKvToByteBuffer(
        generator.generateTestKeyValues(60), includesMemstoreTS);
    int size = keyValues.limit();
    ByteBuffer buf = ByteBuffer.allocate(size + HConstants.HFILEBLOCK_HEADER_SIZE);
    buf.position(HConstants.HFILEBLOCK_HEADER_SIZE);
    keyValues.rewind();
    buf.put(keyValues);
    HFileBlock b = new HFileBlock(BlockType.DATA, size, size, -1, buf,
        HFileBlock.FILL_HEADER, 0, includesMemstoreTS, 
        HFileReaderV2.MAX_MINOR_VERSION, 0, ChecksumType.NULL.getCode(), 0);
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
      for (boolean includesMemstoreTS : new boolean[] {false, true}) {
        configurations.add(new Object[] {
            new HFileDataBlockEncoderImpl(diskAlgo),
            new Boolean(includesMemstoreTS)});
      }
    }

    return configurations;
  }
}
