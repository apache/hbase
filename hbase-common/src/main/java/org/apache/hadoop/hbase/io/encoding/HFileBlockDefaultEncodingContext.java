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
package org.apache.hadoop.hbase.io.encoding;

import static org.apache.hadoop.hbase.io.compress.Compression.Algorithm.NONE;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import com.google.common.base.Preconditions;

/**
 * A default implementation of {@link HFileBlockEncodingContext}. It will
 * compress the data section as one continuous buffer.
 *
 * @see HFileBlockDefaultDecodingContext for the decompression part
 *
 */
@InterfaceAudience.Private
public class HFileBlockDefaultEncodingContext implements
    HFileBlockEncodingContext {

  private byte[] onDiskBytesWithHeader;
  private byte[] uncompressedBytesWithHeader;
  private BlockType blockType;
  private final DataBlockEncoding encodingAlgo;

  /** Compressor, which is also reused between consecutive blocks. */
  private Compressor compressor;

  /** Compression output stream */
  private CompressionOutputStream compressionStream;

  /** Underlying stream to write compressed bytes to */
  private ByteArrayOutputStream compressedByteStream;

  /** Compression algorithm for all blocks this instance writes. */
  private final Compression.Algorithm compressionAlgorithm;

  private ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
  private DataOutputStream dataOut = new DataOutputStream(encodedStream);

  private byte[] dummyHeader;

  /**
   * @param compressionAlgorithm compression algorithm used
   * @param encoding encoding used
   * @param headerBytes dummy header bytes
   */
  public HFileBlockDefaultEncodingContext(
      Compression.Algorithm compressionAlgorithm,
      DataBlockEncoding encoding, byte[] headerBytes) {
    this.encodingAlgo = encoding;
    this.compressionAlgorithm =
        compressionAlgorithm == null ? NONE : compressionAlgorithm;
    if (this.compressionAlgorithm != NONE) {
      compressor = compressionAlgorithm.getCompressor();
      compressedByteStream = new ByteArrayOutputStream();
      try {
        compressionStream =
            compressionAlgorithm.createPlainCompressionStream(
                compressedByteStream, compressor);
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not create compression stream for algorithm "
                + compressionAlgorithm, e);
      }
    }
    dummyHeader = Preconditions.checkNotNull(headerBytes,
      "Please pass HConstants.HFILEBLOCK_DUMMY_HEADER instead of null for param headerBytes");
  }

  @Override
  public void setDummyHeader(byte[] headerBytes) {
    dummyHeader = headerBytes;
  }

  /**
   * prepare to start a new encoding.
   * @throws IOException
   */
  public void prepareEncoding() throws IOException {
    encodedStream.reset();
    dataOut.write(dummyHeader);
    if (encodingAlgo != null
        && encodingAlgo != DataBlockEncoding.NONE) {
      encodingAlgo.writeIdInBytes(dataOut);
    }
  }

  @Override
  public void postEncoding(BlockType blockType)
      throws IOException {
    dataOut.flush();
    compressAfterEncodingWithBlockType(encodedStream.toByteArray(), blockType);
    this.blockType = blockType;
  }

  /**
   * @param uncompressedBytesWithHeader
   * @param blockType
   * @throws IOException
   */
  public void compressAfterEncodingWithBlockType(byte[] uncompressedBytesWithHeader,
      BlockType blockType) throws IOException {
    compressAfterEncoding(uncompressedBytesWithHeader, blockType, dummyHeader);
  }

  /**
   * @param uncompressedBytesWithHeader
   * @param blockType
   * @param headerBytes
   * @throws IOException
   */
  protected void compressAfterEncoding(byte[] uncompressedBytesWithHeader,
      BlockType blockType, byte[] headerBytes) throws IOException {
    this.uncompressedBytesWithHeader = uncompressedBytesWithHeader;
    if (compressionAlgorithm != NONE) {
      compressedByteStream.reset();
      compressedByteStream.write(headerBytes);
      compressionStream.resetState();
      compressionStream.write(uncompressedBytesWithHeader,
          headerBytes.length, uncompressedBytesWithHeader.length
              - headerBytes.length);

      compressionStream.flush();
      compressionStream.finish();
      onDiskBytesWithHeader = compressedByteStream.toByteArray();
    } else {
      onDiskBytesWithHeader = uncompressedBytesWithHeader;
    }
    this.blockType = blockType;
  }

  @Override
  public byte[] getOnDiskBytesWithHeader() {
    return onDiskBytesWithHeader;
  }

  @Override
  public byte[] getUncompressedBytesWithHeader() {
    return uncompressedBytesWithHeader;
  }

  @Override
  public BlockType getBlockType() {
    return blockType;
  }

  /**
   * Releases the compressor this writer uses to compress blocks into the
   * compressor pool.
   */
  @Override
  public void close() {
    if (compressor != null) {
      compressionAlgorithm.returnCompressor(compressor);
      compressor = null;
    }
  }

  @Override
  public Algorithm getCompression() {
    return this.compressionAlgorithm;
  }

  public DataOutputStream getOutputStreamForEncoder() {
    return this.dataOut;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return this.encodingAlgo;
  }
}
