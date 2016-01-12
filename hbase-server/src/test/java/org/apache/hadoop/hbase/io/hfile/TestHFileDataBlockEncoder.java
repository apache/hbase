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
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.test.RedundantKVGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({IOTests.class, SmallTests.class})
public class TestHFileDataBlockEncoder {
  private HFileDataBlockEncoder blockEncoder;
  private RedundantKVGenerator generator = new RedundantKVGenerator();
  private boolean includesMemstoreTS;

  /**
   * Create test for given data block encoding configuration.
   * @param blockEncoder What kind of encoding policy will be used.
   */
  public TestHFileDataBlockEncoder(HFileDataBlockEncoder blockEncoder,
      boolean includesMemstoreTS) {
    this.blockEncoder = blockEncoder;
    this.includesMemstoreTS = includesMemstoreTS;
    System.err.println("Encoding: " + blockEncoder.getDataBlockEncoding()
        + ", includesMemstoreTS: " + includesMemstoreTS);
  }

  /**
   * Test putting and taking out blocks into cache with different
   * encoding options.
   */
  @Test
  public void testEncodingWithCache() throws IOException {
    testEncodingWithCacheInternals(false);
    testEncodingWithCacheInternals(true);
  }

  private void testEncodingWithCacheInternals(boolean useTag) throws IOException {
    List<KeyValue> kvs = generator.generateTestKeyValues(60, useTag);
    HFileBlock block = getSampleHFileBlock(kvs, useTag);
    HFileBlock cacheBlock = createBlockOnDisk(kvs, block, useTag);

    LruBlockCache blockCache =
        new LruBlockCache(8 * 1024 * 1024, 32 * 1024);
    BlockCacheKey cacheKey = new BlockCacheKey("test", 0);
    blockCache.cacheBlock(cacheKey, cacheBlock);

    HeapSize heapSize = blockCache.getBlock(cacheKey, false, false, true);
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
    testHeaderSizeInCacheWithoutChecksumInternals(false);
    testHeaderSizeInCacheWithoutChecksumInternals(true);
  }

  private void testHeaderSizeInCacheWithoutChecksumInternals(boolean useTags) throws IOException {
    int headerSize = HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
    // Create some KVs and create the block with old-style header.
    List<KeyValue> kvs = generator.generateTestKeyValues(60, useTags);
    ByteBuffer keyValues = RedundantKVGenerator.convertKvToByteBuffer(kvs, includesMemstoreTS);
    int size = keyValues.limit();
    ByteBuffer buf = ByteBuffer.allocate(size + headerSize);
    buf.position(headerSize);
    keyValues.rewind();
    buf.put(keyValues);
    HFileContext hfileContext = new HFileContextBuilder().withHBaseCheckSum(false)
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(useTags)
                        .withBlockSize(0)
                        .withChecksumType(ChecksumType.NULL)
                        .build();
    HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, buf,
        HFileBlock.FILL_HEADER, 0,
        0, hfileContext);
    HFileBlock cacheBlock = createBlockOnDisk(kvs, block, useTags);
    assertEquals(headerSize, cacheBlock.getDummyHeaderForVersion().length);
  }

  /**
   * Test encoding.
   * @throws IOException
   */
  @Test
  public void testEncoding() throws IOException {
    testEncodingInternals(false);
    testEncodingInternals(true);
  }

  /**
   * Test encoding with offheap keyvalue. This test just verifies if the encoders
   * work with DBB and does not use the getXXXArray() API
   * @throws IOException
   */
  @Test
  public void testEncodingWithOffheapKeyValue() throws IOException {
    // usually we have just block without headers, but don't complicate that
    if(blockEncoder.getDataBlockEncoding() == DataBlockEncoding.PREFIX_TREE) {
      // This is a TODO: Only after PrefixTree is fixed we can remove this check
      return;
    }
    try {
      List<Cell> kvs = generator.generateTestExtendedOffheapKeyValues(60, true);
      HFileContext meta = new HFileContextBuilder().withIncludesMvcc(includesMemstoreTS)
          .withIncludesTags(true).withHBaseCheckSum(true).withCompression(Algorithm.NONE)
          .withBlockSize(0).withChecksumType(ChecksumType.NULL).build();
      writeBlock(kvs, meta, true);
    } catch (IllegalArgumentException e) {
      fail("No exception should have been thrown");
    }
  }

  private void testEncodingInternals(boolean useTag) throws IOException {
    // usually we have just block without headers, but don't complicate that
    List<KeyValue> kvs = generator.generateTestKeyValues(60, useTag);
    HFileBlock block = getSampleHFileBlock(kvs, useTag);
    HFileBlock blockOnDisk = createBlockOnDisk(kvs, block, useTag);

    if (blockEncoder.getDataBlockEncoding() !=
        DataBlockEncoding.NONE) {
      assertEquals(BlockType.ENCODED_DATA, blockOnDisk.getBlockType());
      assertEquals(blockEncoder.getDataBlockEncoding().getId(),
          blockOnDisk.getDataBlockEncodingId());
    } else {
      assertEquals(BlockType.DATA, blockOnDisk.getBlockType());
    }
  }

  private HFileBlock getSampleHFileBlock(List<KeyValue> kvs, boolean useTag) {
    ByteBuffer keyValues = RedundantKVGenerator.convertKvToByteBuffer(kvs, includesMemstoreTS);
    int size = keyValues.limit();
    ByteBuffer buf = ByteBuffer.allocate(size + HConstants.HFILEBLOCK_HEADER_SIZE);
    buf.position(HConstants.HFILEBLOCK_HEADER_SIZE);
    keyValues.rewind();
    buf.put(keyValues);
    HFileContext meta = new HFileContextBuilder()
                        .withIncludesMvcc(includesMemstoreTS)
                        .withIncludesTags(useTag)
                        .withHBaseCheckSum(true)
                        .withCompression(Algorithm.NONE)
                        .withBlockSize(0)
                        .withChecksumType(ChecksumType.NULL)
                        .build();
    HFileBlock b = new HFileBlock(BlockType.DATA, size, size, -1, buf,
        HFileBlock.FILL_HEADER, 0, 
         0, meta);
    return b;
  }

  private HFileBlock createBlockOnDisk(List<KeyValue> kvs, HFileBlock block, boolean useTags)
      throws IOException {
    int size;
    HFileBlockEncodingContext context = new HFileBlockDefaultEncodingContext(
        blockEncoder.getDataBlockEncoding(), HConstants.HFILEBLOCK_DUMMY_HEADER,
        block.getHFileContext());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(block.getDummyHeaderForVersion());
    DataOutputStream dos = new DataOutputStream(baos);
    blockEncoder.startBlockEncoding(context, dos);
    for (KeyValue kv : kvs) {
      blockEncoder.encode(kv, context, dos);
    }
    blockEncoder.endBlockEncoding(context, dos, baos.getBuffer(), BlockType.DATA);
    byte[] encodedBytes = baos.toByteArray();
    size = encodedBytes.length - block.getDummyHeaderForVersion().length;
    return new HFileBlock(context.getBlockType(), size, size, -1, ByteBuffer.wrap(encodedBytes),
        HFileBlock.FILL_HEADER, 0, block.getOnDiskDataSizeWithHeader(), block.getHFileContext());
  }

  private void writeBlock(List<Cell> kvs, HFileContext fileContext, boolean useTags)
      throws IOException {
    HFileBlockEncodingContext context = new HFileBlockDefaultEncodingContext(
        blockEncoder.getDataBlockEncoding(), HConstants.HFILEBLOCK_DUMMY_HEADER,
        fileContext);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(HConstants.HFILEBLOCK_DUMMY_HEADER);
    DataOutputStream dos = new DataOutputStream(baos);
    blockEncoder.startBlockEncoding(context, dos);
    for (Cell kv : kvs) {
      blockEncoder.encode(kv, context, dos);
    }
  }

  /**
   * @return All possible data block encoding configurations
   */
  @Parameters
  public static Collection<Object[]> getAllConfigurations() {
    List<Object[]> configurations =
        new ArrayList<Object[]>();

    for (DataBlockEncoding diskAlgo : DataBlockEncoding.values()) {
      for (boolean includesMemstoreTS : new boolean[] { false, true }) {
        HFileDataBlockEncoder dbe = (diskAlgo == DataBlockEncoding.NONE) ? 
            NoOpDataBlockEncoder.INSTANCE : new HFileDataBlockEncoderImpl(diskAlgo);
        configurations.add(new Object[] { dbe, new Boolean(includesMemstoreTS) });
      }
    }

    return configurations;
  }
}
