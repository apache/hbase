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

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Controls what kind of data block encoding is used. If data block encoding is
 * not set or the given block is not a data block (encoded or not), methods
 * should just return the unmodified block.
 */
public interface HFileDataBlockEncoder {
  /** Type of encoding used for data blocks in HFile. Stored in file info. */
  public static final byte[] DATA_BLOCK_ENCODING = Bytes.toBytes("DATA_BLOCK_ENCODING");
  
  /**
   * Converts a block from the on-disk format to the in-cache format. Called in
   * the following cases:
   * <ul>
   * <li>After an encoded or unencoded data block is read from disk, but before
   * it is put into the cache.</li>
   * <li>To convert brand-new blocks to the in-cache format when doing
   * cache-on-write.</li>
   * </ul>
   * @param block a block in an on-disk format (read from HFile or freshly
   *          generated).
   * @return non null block which is coded according to the settings.
   */
  public HFileBlock diskToCacheFormat(HFileBlock block,
      boolean isCompaction);

  /**
   * Should be called before an encoded or unencoded data block is written to
   * disk.
   * @param in KeyValues next to each other
   * @param dummyHeader A dummy header to be written as a placeholder
   * @return a non-null on-heap buffer containing the contents of the
   *         HFileBlock with unfilled header and block type
   */
  public Pair<ByteBuffer, BlockType> beforeWriteToDisk(
      ByteBuffer in, boolean includesMemstoreTS, byte[] dummyHeader);

  /**
   * Decides whether we should use a scanner over encoded blocks.
   * @param isCompaction whether we are in a compaction.
   * @return Whether to use encoded scanner.
   */
  public boolean useEncodedScanner(boolean isCompaction);

  /**
   * Save metadata in HFile which will be written to disk
   * @param writer writer for a given HFile
   * @exception IOException on disk problems
   */
  public void saveMetadata(HFile.Writer writer) throws IOException;

  /** @return the on-disk data block encoding */
  public DataBlockEncoding getEncodingOnDisk();

  /** @return the preferred in-cache data block encoding for normal reads */
  public DataBlockEncoding getEncodingInCache();

  /**
   * @return the effective in-cache data block encoding, taking into account
   *         whether we are doing a compaction.
   */
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction);

}
