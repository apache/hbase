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
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.compress.Compressor;

/**
 * Encapsulates a data block compressed using a particular encoding algorithm.
 * Useful for testing and benchmarking.
 */
public class EncodedDataBlock {
  private static final int BUFFER_SIZE = 4 * 1024;
  protected DataBlockEncoder dataBlockEncoder;
  ByteArrayOutputStream uncompressedOutputStream;
  ByteBuffer uncompressedBuffer;
  private byte[] cacheCompressData;
  private ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
  private boolean includesMemstoreTS;

  /**
   * Create a buffer which will be encoded using dataBlockEncoder.
   * @param dataBlockEncoder Algorithm used for compression.
   */
  public EncodedDataBlock(DataBlockEncoder dataBlockEncoder,
      boolean includesMemstoreTS) {
    this.dataBlockEncoder = dataBlockEncoder;
    uncompressedOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
  }

  /**
   * Add KeyValue and compress it.
   * @param kv Item to be added and compressed.
   */
  public void addKv(KeyValue kv) {
    cacheCompressData = null;
    uncompressedOutputStream.write(
        kv.getBuffer(), kv.getOffset(), kv.getLength());
  }

  /**
   * Provides access to compressed value.
   * @return Forwards sequential iterator.
   */
  public Iterator<KeyValue> getIterator() {
    final int uncompressedSize = uncompressedOutputStream.size();
    final ByteArrayInputStream bais = new ByteArrayInputStream(
        getCompressedData());
    final DataInputStream dis = new DataInputStream(bais);


    return new Iterator<KeyValue>() {
      private ByteBuffer decompressedData = null;

      @Override
      public boolean hasNext() {
        if (decompressedData == null) {
          return uncompressedSize > 0;
        }
        return decompressedData.hasRemaining();
      }

      @Override
      public KeyValue next() {
        if (decompressedData == null) {
          try {
            decompressedData = dataBlockEncoder.uncompressKeyValues(
                dis, includesMemstoreTS);
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
    return getCompressedData().length;
  }

  /**
   * Find the size of compressed data assuming that buffer will be compressed
   * using given algorithm.
   * @param compressor Algorithm used for compression.
   * @param buffer Array to be compressed.
   * @param offset Offset to beginning of the data.
   * @param length Length to be compressed.
   * @return Size of compressed data in bytes.
   */
  public static int checkCompressedSize(Compressor compressor, byte[] buffer,
      int offset, int length) {
    byte[] compressedBuffer = new byte[buffer.length];
    // in fact the buffer could be of any positive size
    compressor.setInput(buffer, offset, length);
    compressor.finish();
    int currentPos = 0;
    while (!compressor.finished()) {
      try {
        // we don't care about compressed data,
        // we just want to callculate number of bytes
        currentPos += compressor.compress(compressedBuffer, 0,
            compressedBuffer.length);
      } catch (IOException e) {
        throw new RuntimeException(
            "For some reason compressor couldn't read data. " +
            "It is likely a problem with " +
            compressor.getClass().getName(), e);
      }
    }
    return currentPos;
  }

  /**
   * Estimate size after second stage of compression (e.g. LZO).
   * @param compressor Algorithm which will be used for compressions.
   * @return Size after second stage of compression.
   */
  public int checkCompressedSize(Compressor compressor) {
    // compress
    byte[] compressedBytes = getCompressedData();
    return checkCompressedSize(compressor, compressedBytes, 0,
        compressedBytes.length);
  }

  private byte[] getCompressedData() {
    // is cached
    if (cacheCompressData != null) {
      return cacheCompressData;
    }
    cacheCompressData = doCompressData();

    return cacheCompressData;
  }

  private ByteBuffer getUncompressedBuffer() {
    if (uncompressedBuffer == null ||
        uncompressedBuffer.limit() < uncompressedOutputStream.size()) {
      uncompressedBuffer = ByteBuffer.wrap(
          uncompressedOutputStream.toByteArray());
    }
    return uncompressedBuffer;
  }

  /**
   * Do the compression.
   * @return Compressed byte buffer.
   */
  public byte[] doCompressData() {
    compressedStream.reset();
    DataOutputStream dataOut = new DataOutputStream(compressedStream);
    try {
      this.dataBlockEncoder.compressKeyValues(
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

  /**
   * Get uncompressed buffer.
   * @return The buffer.
   */
  public byte[] getRawKeyValues() {
    return uncompressedOutputStream.toByteArray();
  }
}
