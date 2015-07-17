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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.nio.ByteBuff;

/**
 * Encoding of KeyValue. It aims to be fast and efficient using assumptions:
 * <ul>
 * <li>the KeyValues are stored sorted by key</li>
 * <li>we know the structure of KeyValue</li>
 * <li>the values are always iterated forward from beginning of block</li>
 * <li>knowledge of Key Value format</li>
 * </ul>
 * It is designed to work fast enough to be feasible as in memory compression.
 */
@InterfaceAudience.Private
public interface DataBlockEncoder {

  /**
   * Starts encoding for a block of KeyValues. Call
   * {@link #endBlockEncoding(HFileBlockEncodingContext, DataOutputStream, byte[])} to finish
   * encoding of a block.
   * @param encodingCtx
   * @param out
   * @throws IOException
   */
  void startBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out)
      throws IOException;

  /**
   * Encodes a KeyValue.
   * @param cell
   * @param encodingCtx
   * @param out
   * @return unencoded kv size written
   * @throws IOException
   */
  int encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
      throws IOException;

  /**
   * Ends encoding for a block of KeyValues. Gives a chance for the encoder to do the finishing
   * stuff for the encoded block. It must be called at the end of block encoding.
   * @param encodingCtx
   * @param out
   * @param uncompressedBytesWithHeader
   * @throws IOException
   */
  void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
      byte[] uncompressedBytesWithHeader) throws IOException;

  /**
   * Decode.
   * @param source Compressed stream of KeyValues.
   * @param decodingCtx
   * @return Uncompressed block of KeyValues.
   * @throws IOException If there is an error in source.
   */
  ByteBuffer decodeKeyValues(DataInputStream source, HFileBlockDecodingContext decodingCtx)
      throws IOException;

  /**
   * Return first key in block as a cell. Useful for indexing. Typically does not make
   * a deep copy but returns a buffer wrapping a segment of the actual block's
   * byte array. This is because the first key in block is usually stored
   * unencoded.
   * @param block encoded block we want index, the position will not change
   * @return First key in block as a cell.
   */
  Cell getFirstKeyCellInBlock(ByteBuff block);

  /**
   * Create a HFileBlock seeker which find KeyValues within a block.
   * @param comparator what kind of comparison should be used
   * @param decodingCtx
   * @return A newly created seeker.
   */
  EncodedSeeker createSeeker(CellComparator comparator, 
      HFileBlockDecodingContext decodingCtx);

  /**
   * Creates a encoder specific encoding context
   *
   * @param encoding
   *          encoding strategy used
   * @param headerBytes
   *          header bytes to be written, put a dummy header here if the header
   *          is unknown
   * @param meta
   *          HFile meta data
   * @return a newly created encoding context
   */
  HFileBlockEncodingContext newDataBlockEncodingContext(
      DataBlockEncoding encoding, byte[] headerBytes, HFileContext meta);

  /**
   * Creates an encoder specific decoding context, which will prepare the data
   * before actual decoding
   *
   * @param meta
   *          HFile meta data        
   * @return a newly created decoding context
   */
  HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext meta);

  /**
   * An interface which enable to seek while underlying data is encoded.
   *
   * It works on one HFileBlock, but it is reusable. See
   * {@link #setCurrentBuffer(ByteBuffer)}.
   */
  interface EncodedSeeker {
    /**
     * Set on which buffer there will be done seeking.
     * @param buffer Used for seeking.
     */
    void setCurrentBuffer(ByteBuffer buffer);

    /**
     * From the current position creates a cell using the key part
     * of the current buffer
     * @return key at current position
     */
    Cell getKey();

    /**
     * Does a shallow copy of the value at the current position. A shallow
     * copy is possible because the returned buffer refers to the backing array
     * of the original encoded buffer.
     * @return value at current position
     */
    ByteBuffer getValueShallowCopy();
    
    //TODO : to be removed - currently used in testcases only
    /** @return a key value buffer with the position set at the beginning of the buffer */
    ByteBuffer getKeyValueBuffer();

    /**
     * @return the KeyValue object at the current position. Includes memstore
     *         timestamp.
     */
    Cell getKeyValue();

    /** Set position to beginning of given block */
    void rewind();

    /**
     * Move to next position
     * @return true on success, false if there is no more positions.
     */
    boolean next();

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
     * @param key - Cell to which the seek should happen
     * @param seekBefore find the key strictly less than the given key in case
     *          of an exact match. Does not matter in case of an inexact match.
     * @return 0 on exact match, 1 on inexact match.
     */
    int seekToKeyInBlock(Cell key, boolean seekBefore);

    /**
     * Compare the given key against the current key
     * @param comparator
     * @param key
     * @return -1 is the passed key is smaller than the current key, 0 if equal and 1 if greater
     */
    public int compareKey(CellComparator comparator, Cell key);
  }
}
