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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

/**
 * Do different kinds of data block encoding according to column family
 * options.
 */
@InterfaceAudience.Private
public class HFileDataBlockEncoderImpl implements HFileDataBlockEncoder {
  private final DataBlockEncoding onDisk;
  private final DataBlockEncoding inCache;
  private final HFileBlockEncodingContext inCacheEncodeCtx;

  public HFileDataBlockEncoderImpl(DataBlockEncoding encoding) {
    this(encoding, encoding);
  }

  /**
   * Do data block encoding with specified options.
   * @param onDisk What kind of data block encoding will be used before writing
   *          HFileBlock to disk. This must be either the same as inCache or
   *          {@link DataBlockEncoding#NONE}.
   * @param inCache What kind of data block encoding will be used in block
   *          cache.
   */
  public HFileDataBlockEncoderImpl(DataBlockEncoding onDisk,
      DataBlockEncoding inCache) {
    this(onDisk, inCache, null);
  }

  /**
   * Do data block encoding with specified options.
   * @param onDisk What kind of data block encoding will be used before writing
   *          HFileBlock to disk. This must be either the same as inCache or
   *          {@link DataBlockEncoding#NONE}.
   * @param inCache What kind of data block encoding will be used in block
   *          cache.
   * @param dummyHeader dummy header bytes
   */
  public HFileDataBlockEncoderImpl(DataBlockEncoding onDisk,
      DataBlockEncoding inCache, byte[] dummyHeader) {
    dummyHeader = dummyHeader == null ? HFileBlock.DUMMY_HEADER : dummyHeader;
    this.onDisk = onDisk != null ?
        onDisk : DataBlockEncoding.NONE;
    this.inCache = inCache != null ?
        inCache : DataBlockEncoding.NONE;
    if (inCache != DataBlockEncoding.NONE) {
      inCacheEncodeCtx =
          this.inCache.getEncoder().newDataBlockEncodingContext(
              Algorithm.NONE, this.inCache, dummyHeader);
    } else {
      // create a default encoding context
      inCacheEncodeCtx =
          new HFileBlockDefaultEncodingContext(Algorithm.NONE,
              this.inCache, dummyHeader);
    }

    Preconditions.checkArgument(onDisk == DataBlockEncoding.NONE ||
        onDisk == inCache, "on-disk encoding (" + onDisk + ") must be " +
        "either the same as in-cache encoding (" + inCache + ") or " +
        DataBlockEncoding.NONE);
  }

  public static HFileDataBlockEncoder createFromFileInfo(
      FileInfo fileInfo, DataBlockEncoding preferredEncodingInCache)
      throws IOException {
    byte[] dataBlockEncodingType =
        fileInfo.get(StoreFile.DATA_BLOCK_ENCODING);
    if (dataBlockEncodingType == null) {
      return NoOpDataBlockEncoder.INSTANCE;
    }

    String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
    DataBlockEncoding onDisk;
    try {
      onDisk = DataBlockEncoding.valueOf(dataBlockEncodingStr);
    } catch (IllegalArgumentException ex) {
      throw new IOException("Invalid data block encoding type in file info: " +
          dataBlockEncodingStr, ex);
    }

    DataBlockEncoding inCache;
    if (onDisk == DataBlockEncoding.NONE) {
      // This is an "in-cache-only" encoding or fully-unencoded scenario.
      // Either way, we use the given encoding (possibly NONE) specified by
      // the column family in cache.
      inCache = preferredEncodingInCache;
    } else {
      // Leave blocks in cache encoded the same way as they are on disk.
      // If we switch encoding type for the CF or the in-cache-only encoding
      // flag, old files will keep their encoding both on disk and in cache,
      // but new files will be generated with the new encoding.
      inCache = onDisk;
    }
    return new HFileDataBlockEncoderImpl(onDisk, inCache);
  }

  @Override
  public void saveMetadata(StoreFile.Writer storeFileWriter)
      throws IOException {
    storeFileWriter.appendFileInfo(StoreFile.DATA_BLOCK_ENCODING,
        onDisk.getNameInBytes());
  }

  @Override
  public DataBlockEncoding getEncodingOnDisk() {
    return onDisk;
  }

  @Override
  public DataBlockEncoding getEncodingInCache() {
    return inCache;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    if (!useEncodedScanner(isCompaction)) {
      return DataBlockEncoding.NONE;
    }
    return inCache;
  }

  @Override
  public HFileBlock diskToCacheFormat(HFileBlock block, boolean isCompaction) {
    if (block.getBlockType() == BlockType.DATA) {
      if (!useEncodedScanner(isCompaction)) {
        // Unencoded block, and we don't want to encode in cache.
        return block;
      }
      // Encode the unencoded block with the in-cache encoding.
      return encodeDataBlock(block, inCache, block.doesIncludeMemstoreTS(),
          inCacheEncodeCtx);
    }

    if (block.getBlockType() == BlockType.ENCODED_DATA) {
      if (block.getDataBlockEncodingId() == onDisk.getId()) {
        // The block is already in the desired in-cache encoding.
        return block;
      }
      // We don't want to re-encode a block in a different encoding. The HFile
      // reader should have been instantiated in such a way that we would not
      // have to do this.
      throw new AssertionError("Expected on-disk data block encoding " +
          onDisk + ", got " + block.getDataBlockEncoding());
    }
    return block;
  }

  /**
   * Precondition: a non-encoded buffer. Postcondition: on-disk encoding.
   *
   * The encoded results can be stored in {@link HFileBlockEncodingContext}.
   *
   * @throws IOException
   */
  @Override
  public void beforeWriteToDisk(ByteBuffer in,
      boolean includesMemstoreTS,
      HFileBlockEncodingContext encodeCtx,
      BlockType blockType) throws IOException {
    if (onDisk == DataBlockEncoding.NONE) {
      // there is no need to encode the block before writing it to disk
      ((HFileBlockDefaultEncodingContext) encodeCtx).compressAfterEncoding(
          in.array(), blockType);
      return;
    }
    encodeBufferToHFileBlockBuffer(in, onDisk,
        includesMemstoreTS, encodeCtx);
  }

  @Override
  public boolean useEncodedScanner(boolean isCompaction) {
    if (isCompaction && onDisk == DataBlockEncoding.NONE) {
      return false;
    }
    return inCache != DataBlockEncoding.NONE;
  }

  /**
   * Encode a block of key value pairs.
   *
   * @param in input data to encode
   * @param algo encoding algorithm
   * @param includesMemstoreTS includes memstore timestamp or not
   * @param encodeCtx where will the output data be stored
   */
  private void encodeBufferToHFileBlockBuffer(ByteBuffer in,
      DataBlockEncoding algo, boolean includesMemstoreTS,
      HFileBlockEncodingContext encodeCtx) {
    DataBlockEncoder encoder = algo.getEncoder();
    try {
      encoder.encodeKeyValues(in, includesMemstoreTS, encodeCtx);
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Bug in data block encoder "
              + "'%s', it probably requested too much data, " +
              "exception message: %s.",
              algo.toString(), e.getMessage()), e);
    }
  }

  private HFileBlock encodeDataBlock(HFileBlock block,
      DataBlockEncoding algo, boolean includesMemstoreTS,
      HFileBlockEncodingContext encodingCtx) {
    encodeBufferToHFileBlockBuffer(
      block.getBufferWithoutHeader(), algo, includesMemstoreTS, encodingCtx);
    byte[] encodedUncompressedBytes =
      encodingCtx.getUncompressedBytesWithHeader();
    ByteBuffer bufferWrapper = ByteBuffer.wrap(encodedUncompressedBytes);
    int sizeWithoutHeader = bufferWrapper.limit() - encodingCtx.getHeaderSize();
    HFileBlock encodedBlock = new HFileBlock(BlockType.ENCODED_DATA,
        block.getOnDiskSizeWithoutHeader(),
        sizeWithoutHeader, block.getPrevBlockOffset(),
        bufferWrapper, HFileBlock.FILL_HEADER, block.getOffset(),
        includesMemstoreTS, block.getMinorVersion(),
        block.getBytesPerChecksum(), block.getChecksumType(),
        block.getOnDiskDataSizeWithHeader());
    block.passSchemaMetricsTo(encodedBlock);
    return encodedBlock;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(onDisk=" + onDisk + ", inCache=" +
        inCache + ")";
  }

  @Override
  public HFileBlockEncodingContext newOnDiskDataBlockEncodingContext(
      Algorithm compressionAlgorithm,  byte[] dummyHeader) {
    if (onDisk != null) {
      DataBlockEncoder encoder = onDisk.getEncoder();
      if (encoder != null) {
        return encoder.newDataBlockEncodingContext(
            compressionAlgorithm, onDisk, dummyHeader);
      }
    }
    return new HFileBlockDefaultEncodingContext(compressionAlgorithm,
        null, dummyHeader);
  }

  @Override
  public HFileBlockDecodingContext newOnDiskDataBlockDecodingContext(
      Algorithm compressionAlgorithm) {
    if (onDisk != null) {
      DataBlockEncoder encoder = onDisk.getEncoder();
      if (encoder != null) {
        return encoder.newDataBlockDecodingContext(
            compressionAlgorithm);
      }
    }
    return new HFileBlockDefaultDecodingContext(compressionAlgorithm);
  }

}
