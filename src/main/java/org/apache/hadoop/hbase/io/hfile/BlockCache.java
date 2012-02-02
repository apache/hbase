/**
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.hadoop.hbase.io.hfile.LruBlockCache.CacheStats;

/**
 * Block cache interface.
 * TODO: Add filename or hash of filename to block cache key.
 */
public interface BlockCache {
  /**
   * Add block to cache.
   * @param blockName Zero-based file block number.
   * @param buf The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory
   */
  public void cacheBlock(String blockName, Cacheable buf, boolean inMemory);

  /**
   * Add block to cache (defaults to not in-memory).
   * @param blockName Zero-based file block number.
   * @param buf The block contents wrapped in a ByteBuffer.
   */
  public void cacheBlock(String blockName, Cacheable buf);

  /**
   * Fetch block from cache.
   * @param blockName Block number to fetch.
   * @param caching true if the caller caches blocks on a miss
   * @return Block or null if block is not in the cache.
   */
  public Cacheable getBlock(String blockName, boolean caching);

  /**
   * Evict block from cache.
   * @param blockName Block name to evict
   * @return true if block existed and was evicted, false if not
   */
  public boolean evictBlock(String blockName);

  /**
   * Evicts all blocks with the given prefix in the name
   * @return the number of blocks evicted
   */
  public int evictBlocksByPrefix(String string);

  /**
   * Get the statistics for this block cache.
   * @return
   */
  public CacheStats getStats();

  /**
   * Shutdown the cache.
   */
  public void shutdown();

  /**
   * Returns the number of blocks currently cached in the block cache.
   * @return number of blocks in the cache
   */
  public long getBlockCount();
}
