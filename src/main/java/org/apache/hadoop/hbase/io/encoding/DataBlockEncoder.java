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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.RawComparator;

/**
 * Encoding of KeyValue. It aims to be fast and efficient using assumptions:
 * <ul>
 * <li>the KeyValues are stored sorted by key</li>
 * <li>we know the structure of KeyValue</li>
 * <li>the values are always iterated forward from beginning of block</li>
 * <li>knowledge of Key Value format</li>
 * </ul>
 * It is designed to work fast enough to be feasible as in memory encoding.
 */
public interface DataBlockEncoder {
  /**
   * Encode KeyValues and write them to output buffer.
   * @param out Where to write encoded data.
   * @param in Source of KeyValue for encoding.
   * @param includesMemstoreTS true if including memstore timestamp after every
   *          key-value pair
   * @throws IOException If there is an error writing to output stream.
   */
  public void encodeKeyValues(DataOutputStream out,
      ByteBuffer in, boolean includesMemstoreTS) throws IOException;

  /**
   * Decode.
   * @param source encoded stream of KeyValues.
   * @param allocateHeaderLength allocate this many bytes for the header.
   * @param includesMemstoreTS true if including memstore timestamp after every
   *          key-value pair
   * @param totalEncodedSize the total size of the encoded data to read        
   * @return decoded block of KeyValues.
   * @throws IOException If there is an error in source.
   */
  public ByteBuffer decodeKeyValues(DataInputStream source,
      int allocateHeaderLength, boolean includesMemstoreTS, int totalEncodedSize)
      throws IOException;

  /**
   * Return first key in block. Useful for indexing. Typically does not make
   * a deep copy but returns a buffer wrapping a segment of the actual block's
   * byte array. This is because the first key in block is usually stored
   * unencoded.
   * @param block encoded block we want index, the position will not change
   * @return First key in block.
   */
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block);

  /**
   * Create a HFileBlock seeker which find KeyValues within a block.
   * @param comparator what kind of comparison should be used
   * @param includesMemstoreTS true if including memstore timestamp after every
   *          key-value pair
   * @return A newly created seeker.
   */
  public EncodedSeeker createSeeker(RawComparator<byte[]> comparator,
      boolean includesMemstoreTS);

  /** @return unencoded size of the given encoded block or -1 if unknown */
  public int getUnencodedSize(ByteBuffer bufferWithoutHeader);

  /**
   * Create an incremental writer
   * @param out Where to write encoded data
   * @param includesMemstoreTS True if including memstore timestamp after every
   *          key-value pair
   * @return
   */
  public EncodedWriter createWriter(DataOutputStream out,
      boolean includesMemstoreTS) throws IOException;

  /**
   * An interface for performing incremental encoding
   */
  public static interface EncodedWriter {
    /**
     * Sets next key to insert for the writer and updates the encoded data
     * in the stream if necessary.
     * @param memstoreTS Current memstore timestamp
     * @param key Source of key bytes
     * @param keyOffset Offset of initial byte in key array
     * @param keyLength Length of key bytes in key array
     * @param value Source of value bytes
     * @param valueOffset Offset of initial byte in value array
     * @param valueLength Length of value bytes in value array
     * @throws IOException
     */
    public void update(final long memstoreTS, final byte[] key,
        final int keyOffset, final int keyLength, final byte[] value,
        final int valueOffset, final int valueLength) throws IOException;

    /**
     * Completes the encoding process on the given byte array
     * @param data The encoded stream as a byte array
     * @param offset Offset of initial byte in data array
     * @param length Length of the portion of the array containing encoded data
     * @return True if encoding was performed
     * @throws IOException
     */
    public boolean finishEncoding(byte[] data, final int offset,
        final int length) throws IOException;

    /**
     * Called before writing anything to the stream to reserve space for the necessary metadata
     * such as unencoded length.
     */
    void reserveMetadataSpace() throws IOException;
  }

  /**
   * An interface which enable to seek while underlying data is encoded.
   *
   * It works on one HFileBlock, but it is reusable. See
   * {@link #setCurrentBuffer(ByteBuffer)}.
   */
  public static interface EncodedSeeker {
    /**
     * Set on which buffer there will be done seeking.
     * @param buffer Used for seeking.
     */
    public void setCurrentBuffer(ByteBuffer buffer);

    /**
     * Does a deep copy of the key at the current position. A deep copy is
     * necessary because buffers are reused in the decoder.
     * @return key at current position
     */
    public ByteBuffer getKeyDeepCopy();

    /**
     * Does a shallow copy of the value at the current position. A shallow
     * copy is possible because the returned buffer refers to the backing array
     * of the original encoded buffer.
     * @return value at current position
     */
    public ByteBuffer getValueShallowCopy();

    /** @return key value at current position. */
    public ByteBuffer getKeyValueBuffer();

    /**
     * @return the KeyValue object at the current position. Includes memstore
     *         timestamp.
     */
    public KeyValue getKeyValue();

    /** Set position to beginning of given block */
    public void rewind();

    /**
     * Move to next position
     * @return true on success, false if there is no more positions.
     */
    public boolean next();

    /**
     * Moves the seeker position within the current block to:
     * <ul>
     * <li>the last key that that is less than or equal to the given key if
     * <code>seekBefore</code> is false</li>
     * <li>the last key that is strictly less than the given key if <code>
     * seekBefore</code> is true. The caller is responsible for loading the
     * previous block if the requested key turns out to be the first key of the
     * current block.</li>
     * </ul>
     * @param key byte array containing the key
     * @param offset key position the array
     * @param length key length in bytes
     * @param seekBefore find the key strictly less than the given key in case
     *          of an exact match. Does not matter in case of an inexact match.
     * @return 0 on exact match, 1 on inexact match.
     */
    public int seekToKeyInBlock(byte[] key, int offset, int length,
        boolean seekBefore);
  }
}
