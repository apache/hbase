/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

/**
 * Interface for a secondary level block cache that deals with byte arrays
 * identical to what is written to disk (i.e., usually encoded and compressed),
 * as opposed to HFile objects.
 */
public interface L2Cache {
  /**
   * Retrieve a block from the L2Cache. The block is retrieved as a byte
   * array, in the same exact format as it is stored on disk.
   * @param cacheKey Key associated with the block
   * @return raw byte string representing the block if present in cache, null
   *    otherwise
   */
  public byte[] getRawBlockBytes(BlockCacheKey cacheKey);


  /**
   * Add a block to the L2Cache. The block must be represented by a
   * byte array identical to what would be written to disk.
   * @param cacheKey key associated with the block
   * @param block the raw HFileBlock to be cached
   * @return true if the block was cached, false otherwise
   */
  public boolean cacheRawBlock(BlockCacheKey cacheKey, RawHFileBlock block);

  /**
   * Evict a block from the L2Cache.
   * @param cacheKey Key associated with the block to be evicted
   * @return true if the block was evicted, false otherwise
   */
  public boolean evictRawBlock(BlockCacheKey cacheKey);

  /**
   * Evict all blocks matching a given filename. This operation should be
   * efficient and can be called on each close of a store file.
   * @param hfileName Filename whose blocks to evict
   * @return the number of evicted blocks
   */
  public int evictBlocksByHfileName(String hfileName);

  /**
   * @return true if the cache is enabled, false otherwise
   */
  public boolean isEnabled();

  /**
   * Shutdown the L2 cache.
   */
  public void shutdown();
}
