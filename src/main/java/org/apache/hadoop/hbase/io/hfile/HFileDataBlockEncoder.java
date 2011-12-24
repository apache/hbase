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

import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Controls what kind of data block encoding is used. If data block encoding is
 * not set, methods should just return unmodified block. All of the methods do
 * something meaningful if BlockType is DATA_BLOCK or ENCODED_DATA. Otherwise
 * they just return the unmodified block.
 * <p>
 * Read path: [parsed from disk] -> {@link #afterReadFromDisk(HFileBlock)} ->
 * [caching] ->
 * {@link #afterReadFromDiskAndPuttingInCache(HFileBlock, boolean)} -> [used
 * somewhere]
 * <p>
 * where [caching] looks:
 * <pre>
 * ------------------------------------>
 *   \----> {@link #beforeBlockCache(HFileBlock)}
 * </pre>
 * <p>
 * Write path: [sorted KeyValues have been created] ->
 * {@link #beforeWriteToDisk(ByteBuffer)} -> [(optional) compress] -> [write to
 * disk]
 * <p>
 * Reading from cache path: [get from cache] ->
 * {@link #afterBlockCache(HFileBlock, boolean)}
 * <p>
 * Storing data in file info: {@link #saveMetadata(StoreFile.Writer)}
 * <p>
 * Creating algorithm specific Scanner: {@link #useEncodedScanner()}
 */
public interface HFileDataBlockEncoder {
  /**
   * Should be called after each HFileBlock of type DATA_BLOCK or
   * ENCODED_DATA_BLOCK is read from disk, but before it is put into the cache.
   * @param block Block read from HFile stored on disk.
   * @return non null block which is coded according to the settings.
   */
  public HFileBlock afterReadFromDisk(HFileBlock block);

  /**
   * Should be called after each HFileBlock of type DATA_BLOCK or
   * ENCODED_DATA_BLOCK is read from disk and after it is saved in cache
   * @param block Block read from HFile stored on disk.
   * @param isCompaction Will block be used for compaction.
   * @return non null block which is coded according to the settings.
   */
  public HFileBlock afterReadFromDiskAndPuttingInCache(HFileBlock block,
      boolean isCompaction, boolean includesMemsoreTS);

  /**
   * Should be called before an encoded or unencoded data block is written to
   * disk.
   * @param in KeyValues next to each other
   * @return a non-null on-heap buffer containing the contents of the
   *         HFileBlock with unfilled header and block type
   */
  public Pair<ByteBuffer, BlockType> beforeWriteToDisk(
      ByteBuffer in, boolean includesMemstoreTS);

  /**
   * Should always be called before putting a block into cache.
   * @param block block that needs to be put into cache.
   * @return the block to put into cache instead (possibly the same)
   */
  public HFileBlock beforeBlockCache(HFileBlock block,
      boolean includesMemstoreTS);

  /**
   * After getting block from cache.
   * @param block block which was returned from cache, may be null.
   * @param isCompaction Will block be used for compaction.
   * @param includesMemstoreTS whether we have a memstore timestamp encoded
   *    as a variable-length integer after each key-value pair
   * @return HFileBlock to use. Can be null, even if argument is not null.
   */
  public HFileBlock afterBlockCache(HFileBlock block,
      boolean isCompaction, boolean includesMemstoreTS);

  /**
   * Should special version of scanner be used.
   * @param isCompaction Will scanner be used for compaction.
   * @return Whether to use encoded scanner.
   */
  public boolean useEncodedScanner(boolean isCompaction);

  /**
   * Save metadata in StoreFile which will be written to disk
   * @param storeFileWriter writer for a given StoreFile
   * @exception IOException on disk problems
   */
  public void saveMetadata(StoreFile.Writer storeFileWriter) throws IOException;
}
