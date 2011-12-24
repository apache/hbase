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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncodings;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncodings.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.server.ByteBufferInputStream;

/**
 * Do different kinds of data block encoding according to column family
 * options.
 */
public class HFileDataBlockEncoderImpl implements HFileDataBlockEncoder {
  private final DataBlockEncodings.Algorithm onDisk;
  private final DataBlockEncodings.Algorithm inCache;

  public static final boolean NO_ENCODED_SEEK = false;

  private final boolean encodedSeek;

  /**
   * Do data block encoding as with specified options.
   * @param onDisk What kind of data block encoding will be used before writing
   *          HFileBlock to disk.
   * @param inCache What kind of data block encoding will be used in block
   *          cache.
   * @param encodedSeek should we seek over encoded data blocks (true) or
   *          decode blocks first and use normal seek operations (false)
   */
  public HFileDataBlockEncoderImpl(Algorithm onDisk, Algorithm inCache,
      boolean encodedSeek) {
    this.onDisk = onDisk != null ?
        onDisk : DataBlockEncodings.Algorithm.NONE;
    this.inCache = inCache != null ?
        inCache : DataBlockEncodings.Algorithm.NONE;
    this.encodedSeek = encodedSeek;
  }

  /**
   * @return the data block encoding algorithm used on disk
   */
  public DataBlockEncodings.Algorithm getOnDisk() {
    return onDisk;
  }

  /**
   * @return the data block encoding algorithm used in the block cache
   */
  public DataBlockEncodings.Algorithm getInCache() {
    return inCache;
  }

  /**
   * @return whether we should do seek operations on encoded blocks
   */
  public boolean useEncodedSeek() {
    return encodedSeek
        && inCache != DataBlockEncodings.Algorithm.NONE;
  }

  // Preconditions: any HFileBlock format
  // Postconditions: HFileBlock not encoded
  //                 or encoded same format as inCache
  @Override
  public HFileBlock afterReadFromDisk(HFileBlock block) {
    if (ignoreBlock(block)) {
      return block; // non DATA block, skip it
    }

    // is already encoded in desired encoding
    if (block.getBlockType() == BlockType.ENCODED_DATA &&
        block.getDataBlockEncodingId() == inCache.getId()) {
      return block;
    }

    // decode if we need it
    HFileBlock decompressedBlock;
    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      decompressedBlock = decodeDataBlock(block, false, (short) 0,
          block.doesIncludeMemstoreTS());
    } else {
      decompressedBlock = block;
    }

    // check if we want to encode it here
    if (encodedSeek && inCache != DataBlockEncodings.Algorithm.NONE &&
          onDisk != DataBlockEncodings.Algorithm.NONE) {
      return encodeDataBlock(decompressedBlock, inCache,
          block.doesIncludeMemstoreTS());
    }

    return decompressedBlock;
  }

  /**
   * Preconditions: HFileBlock not encoded or encoded in the {@link #inCache}
   * format.
   * <p>
   * Postconditions:
   * <ul>
   * <li>if isCompaction is set and {@link #onDisk} is NONE there is no
   * encoding</li>
   * <li>if {@link #encodedSeek} is set there is same encoding as inCache
   * Otherwise there is no encoding</li>
   * </ul>
   */
  @Override
  public HFileBlock afterReadFromDiskAndPuttingInCache(HFileBlock block,
        boolean isCompaction, boolean includesMemstoreTS) {
    if (ignoreBlock(block)) {
      return block; // non DATA block, skip it
    }

    // use decoded buffer in case of compaction
    if (dontEncodeBeforeCompaction(isCompaction)) {
      if (block.getBlockType() != BlockType.DATA) {
        return decodeDataBlock(block, true, inCache.getId(),
            includesMemstoreTS);
      }
      return block;
    }

    if (!encodedSeek) {
      // we need to have it decoded in memory
      if (block.getBlockType() != BlockType.DATA) {
        return decodeDataBlock(block, true, inCache.getId(),
            includesMemstoreTS);
      }
      return block;
    }

    // got already data in desired format
    if (block.getBlockType() == BlockType.ENCODED_DATA &&
        block.getDataBlockEncodingId() == inCache.getId()) {
      return block;
    }

    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      throw new IllegalStateException("Unexpected encoding");
    }

    // need to encode it
    if (inCache != DataBlockEncodings.Algorithm.NONE) {
      return encodeDataBlock(block, inCache, includesMemstoreTS);
    }

    return block;
  }

  // Precondition: not encoded buffer
  // Postcondition: same encoding as onDisk
  @Override
  public Pair<ByteBuffer, BlockType> beforeWriteToDisk(ByteBuffer in,
      boolean includesMemstoreTS) {
    if (onDisk == DataBlockEncodings.Algorithm.NONE) {
      // there is no need to encode the block before writing it to disk
      return new Pair<ByteBuffer, BlockType>(in, BlockType.DATA);
    }

    ByteBuffer encodedBuffer = encodeBufferToHFileBlockBuffer(in,
        onDisk, includesMemstoreTS);
    return new Pair<ByteBuffer, BlockType>(encodedBuffer,
        BlockType.ENCODED_DATA);
  }

  // Precondition: an unencoded block or the same encoding as inCache
  // Postcondition: same encoding as inCache
  @Override
  public HFileBlock beforeBlockCache(HFileBlock block,
      boolean includesMemstoreTS) {
    if (ignoreBlock(block)) {
      return block; // non DATA block skip it
    }

    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      if (block.getDataBlockEncodingId() == inCache.getId()) {
        // is already encoded in right format
        return block;
      }

      // expecting either the "in-cache" encoding or no encoding
      throw new IllegalStateException(String.format(
          "Expected the in-cache encoding ('%s') or no encoding, " +
          "but got encoding '%s'", inCache.toString(),
          DataBlockEncodings.getNameFromId(
              block.getDataBlockEncodingId())));
    }

    if (inCache != DataBlockEncodings.Algorithm.NONE) {
      // encode data
      HFileBlock encodedBlock = encodeDataBlock(block, inCache,
          includesMemstoreTS);
      block.passSchemaMetricsTo(encodedBlock);
      return encodedBlock;
    }

    return block;
  }

  /**
   * Precondition: same encoding as in inCache
   * <p>
   * Postcondition: if (isCompaction is set and {@link #onDisk} is not NONE) or
   *                {@link #encodedSeek} is not set -> don't encode.
   */
  @Override
  public HFileBlock afterBlockCache(HFileBlock block, boolean isCompaction,
      boolean includesMemstoreTS) {
    if (block == null || ignoreBlock(block)) {
      return block; // skip no DATA block
    }

    if (inCache == DataBlockEncodings.Algorithm.NONE) {
      // no need of decoding
      if (block.getBlockType() == BlockType.ENCODED_DATA) {
        throw new IllegalStateException("Expected non-encoded data in cache.");
      }
      return block;
    }

    if (block.getBlockType() != BlockType.ENCODED_DATA) {
      throw new IllegalStateException("Expected encoded data in cache.");
    }

    if (dontEncodeBeforeCompaction(isCompaction)) {
      // If we don't use dataBlockEncoding on disk,
      // we would also avoid using it for compactions.
      // That way we don't change disk format.
      return null;
    }

    if (encodedSeek) {
      // we use encoding in memory
      return block;
    }

    return decodeDataBlock(block, true, inCache.getId(), includesMemstoreTS);
  }

  @Override
  public boolean useEncodedScanner(boolean isCompaction) {
    if (isCompaction && onDisk == DataBlockEncodings.Algorithm.NONE) {
      return false;
    }
    return encodedSeek && inCache != DataBlockEncodings.Algorithm.NONE;
  }

  @Override
  public void saveMetadata(StoreFile.Writer storeFileWriter)
      throws IOException {
    storeFileWriter.appendFileInfo(StoreFile.DATA_BLOCK_ENCODING,
        onDisk.getNameInBytes());
  }

  private HFileBlock decodeDataBlock(HFileBlock block, boolean verifyEncoding,
      short expectedEncoderId, boolean includesMemstoreTS) {
    assert block.getBlockType() == BlockType.ENCODED_DATA;
    short dataBlockEncoderId = block.getDataBlockEncodingId();

    // (optional) sanity check of encoder type
    if (verifyEncoding && expectedEncoderId != dataBlockEncoderId) {
      throw new IllegalStateException(String.format(
          "Expected encoding type '%d', but found '%d'",
          expectedEncoderId, dataBlockEncoderId));
    }

    ByteBuffer originalBuf = block.getBufferReadOnly();
    ByteBuffer withoutEncodedHeader = ByteBuffer.wrap(originalBuf.array(),
        originalBuf.arrayOffset() + HFileBlock.ENCODED_HEADER_SIZE,
        originalBuf.limit() - HFileBlock.ENCODED_HEADER_SIZE).slice();
    ByteBufferInputStream bbis =
        new ByteBufferInputStream(withoutEncodedHeader);
    DataInputStream dis;
    ByteBuffer newBuf;
    DataBlockEncoder dataBlockEncoder = null;

    try {
      dis = new DataInputStream(bbis);
      dataBlockEncoder =
          DataBlockEncodings.getDataBlockEncoderFromId(dataBlockEncoderId);
      int preReadLength = originalBuf.limit() -
          HFileBlock.HEADER_SIZE - block.getUncompressedSizeWithoutHeader();
      // Sometimes buffer is larger, because it also contains next's block
      // header. In that case we want to skip it.
      newBuf = dataBlockEncoder.uncompressKeyValues(dis, HFileBlock.HEADER_SIZE,
          preReadLength, includesMemstoreTS);
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Bug while decoding the block using '%s'", dataBlockEncoder), e);
    }

    // Create a decoded HFileBlock. Offset will be set later.
    return new HFileBlock(BlockType.DATA, block.getOnDiskSizeWithoutHeader(),
        newBuf.limit() - HFileBlock.HEADER_SIZE, block.getPrevBlockOffset(),
        newBuf, HFileBlock.FILL_HEADER, 0, includesMemstoreTS);
  }

  private ByteBuffer encodeBufferToHFileBlockBuffer(ByteBuffer in,
      DataBlockEncodings.Algorithm algo, boolean includesMemstoreTS) {
    ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(encodedStream);
    DataBlockEncoder encoder = algo.getEncoder();
    try {
      encodedStream.write(HFileBlock.DUMMY_HEADER);
      algo.writeIdInBytes(dataOut);
      encoder.compressKeyValues(dataOut, in,
          includesMemstoreTS);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Bug in data block encoder " +
          "'%s', it probably requested too much data", algo.toString()), e);
    }
    return ByteBuffer.wrap(encodedStream.toByteArray());
  }

  private HFileBlock encodeDataBlock(HFileBlock block,
      DataBlockEncodings.Algorithm algo, boolean includesMemstoreTS) {
    ByteBuffer compressedBuffer = encodeBufferToHFileBlockBuffer(
        block.getBufferWithoutHeader(), algo, includesMemstoreTS);
    int sizeWithoutHeader = compressedBuffer.limit() - HFileBlock.HEADER_SIZE;
    return new HFileBlock(BlockType.ENCODED_DATA,
        block.getOnDiskSizeWithoutHeader(),
        sizeWithoutHeader, block.getPrevBlockOffset(),
        compressedBuffer, HFileBlock.FILL_HEADER, block.getOffset(),
        includesMemstoreTS);
  }

  private boolean ignoreBlock(HFileBlock block) {
    BlockType type = block.getBlockType();
    return type != BlockType.DATA && type != BlockType.ENCODED_DATA;
  }

  private boolean dontEncodeBeforeCompaction(boolean isCompaction) {
    return isCompaction
        && onDisk == DataBlockEncodings.Algorithm.NONE;
  }

  @Override
  public String toString() {
    return String.format(getClass().getSimpleName()
        + " onDisk='%s' inCache='%s' encodedSeek=%s", onDisk.toString(),
        inCache.toString(), encodedSeek);
  }
}
