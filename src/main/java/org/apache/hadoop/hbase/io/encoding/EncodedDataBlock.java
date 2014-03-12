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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.Compressor;

import com.google.common.base.Preconditions;
import org.apache.commons.io.output.NullOutputStream;

/**
 * Encapsulates a data block compressed using a particular encoding algorithm.
 * Useful for testing and benchmarking.
 */
public class EncodedDataBlock {
  private byte[] rawKVs;
  private ByteBuffer rawBuffer;
  private DataBlockEncoder dataBlockEncoder;

  private byte[] cachedEncodedData;
  private ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
  private boolean includesMemstoreTS;

  /**
   * Create a buffer which will be encoded using dataBlockEncoder.
   * @param dataBlockEncoder Algorithm used for compression.
   */
  public EncodedDataBlock(DataBlockEncoder dataBlockEncoder,
      boolean includesMemstoreTS, DataBlockEncoding encoding, byte[] rawKVs) {
    Preconditions.checkNotNull(encoding,
        "Cannot create encoded data block with null encoder");
    this.dataBlockEncoder = dataBlockEncoder;
    this.rawKVs = rawKVs;
  }

  /**
   * Provides access to compressed value.
   * @return Forwards sequential iterator.
   */
  public Iterator<KeyValue> getIterator() {
    final int rawSize = rawKVs.length;
    byte[] encodedDataWithHeader = getEncodedData();
    int bytesToSkip = 0;
    ByteArrayInputStream bais = new ByteArrayInputStream(encodedDataWithHeader,
        bytesToSkip, encodedDataWithHeader.length - bytesToSkip);
    final DataInputStream dis = new DataInputStream(bais);

    return new Iterator<KeyValue>() {
      private ByteBuffer decompressedData = null;

      @Override
      public boolean hasNext() {
        if (decompressedData == null) {
          return rawSize > 0;
        }
        return decompressedData.hasRemaining();
      }

      @Override
      public KeyValue next() {
        if (decompressedData == null) {
          try {
            decompressedData = dataBlockEncoder.decodeKeyValues(
                dis, 0, includesMemstoreTS, dis.available()); 
          } catch (IOException e) {
            throw new RuntimeException("Problem with data block encoder, " +
                "most likely it requested more bytes than are available.", e);
          }
          decompressedData.rewind();
        }

        int offset = decompressedData.position();
        KeyValue kv = new KeyValue(decompressedData.array(), offset);
        decompressedData.position(offset + kv.getLength());

        return kv;
      }

      @Override
      public void remove() {
        throw new NotImplementedException("remove() is not supported!");
      }

      @Override
      public String toString() {
        return "Iterator of: " + dataBlockEncoder.getClass().getName();
      }

    };
  }

  /**
   * Find the size of minimal buffer that could store compressed data.
   * @return Size in bytes of compressed data.
   */
  public int getSize() {
    return getEncodedData().length;
  }

  /**
   * Find the size of compressed data assuming that buffer will be compressed
   * using given algorithm.
   * @param algo compression algorithm
   * @param compressor compressor already requested from codec
   * @param inputBuffer Array to be compressed.
   * @param offset Offset to beginning of the data.
   * @param length Length to be compressed.
   * @return Size of compressed data in bytes.
   */
  public static int getCompressedSize(Algorithm algo, Compressor compressor,
      byte[] inputBuffer, int offset, int length) throws IOException {
    DataOutputStream compressedStream =
        new DataOutputStream(NullOutputStream.NULL_OUTPUT_STREAM);
    if (compressor != null) {
      compressor.reset();
    }
    OutputStream compressingStream = algo.createCompressionStream(
        compressedStream, compressor, 0);

    compressingStream.write(inputBuffer, offset, length);
    compressingStream.flush();
    compressingStream.close();

    return compressedStream.size();
  }

  /**
   * Estimate size after second stage of compression (e.g. LZO).
   * @param comprAlgo compression algorithm to be used for compression
   * @param compressor compressor corresponding to the given compression
   *          algorithm
   * @return Size after second stage of compression.
   */
  public int getEncodedCompressedSize(Algorithm comprAlgo,
      Compressor compressor) throws IOException {
    byte[] compressedBytes = getEncodedData();
    return getCompressedSize(comprAlgo, compressor, compressedBytes, 0,
        compressedBytes.length);
  }

  /** @return encoded data with header */
  private byte[] getEncodedData() {
    if (cachedEncodedData != null) {
      return cachedEncodedData;
    }
    cachedEncodedData = encodeData();
    return cachedEncodedData;
  }

  private ByteBuffer getUncompressedBuffer() {
    if (rawBuffer == null || rawBuffer.limit() < rawKVs.length) {
      rawBuffer = ByteBuffer.wrap(rawKVs);
    }
    return rawBuffer;
  }

  /**
   * Do the encoding, but do not cache the encoded data.
   * @return encoded data block with header and checksum
   */
  public byte[] encodeData() {
    compressedStream.reset();
    DataOutputStream dataOut = new DataOutputStream(compressedStream);
    try {
      this.dataBlockEncoder.encodeKeyValues(
          dataOut, getUncompressedBuffer(), includesMemstoreTS);
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Bug in decoding part of algorithm %s. " +
          "Probably it requested more bytes than are available.",
          toString()), e);
    }
    return compressedStream.toByteArray();
  }

  @Override
  public String toString() {
    return dataBlockEncoder.toString();
  }
}
