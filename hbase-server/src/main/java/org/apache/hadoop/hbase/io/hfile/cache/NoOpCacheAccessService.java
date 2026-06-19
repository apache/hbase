/*
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
package org.apache.hadoop.hbase.io.hfile.cache;

import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Disabled-cache implementation of {@link CacheAccessService}.
 * <p>
 * {@code NoOpCacheAccessService} is useful when block cache access is disabled but callers still
 * want to depend on a non-null {@link CacheAccessService}. It never stores blocks, never returns
 * cached blocks, reports zero capacity and occupancy, and treats all invalidation requests as
 * no-ops.
 * </p>
 * <p>
 * Even though this implementation does not perform cache operations, it still validates caller
 * inputs consistently with other {@link CacheAccessService} implementations. This prevents the
 * disabled-cache path from masking caller bugs that would fail when a real cache is configured.
 * </p>
 * <p>
 * This implementation should not be used to hide configuration mistakes. It represents an explicit
 * disabled-cache state and should be selected only when the caller has determined that no block
 * cache is available or desired.
 * </p>
 */
@InterfaceAudience.Private
public final class NoOpCacheAccessService implements CacheAccessService {

  private static final String NAME = "NoOpCacheAccessService";

  private final CacheStats stats;

  /**
   * Creates a disabled cache access service with its own {@link CacheStats} instance.
   */
  public NoOpCacheAccessService() {
    this(new CacheStats(NAME));
  }

  /**
   * Creates a disabled cache access service with the supplied statistics object.
   * <p>
   * Allowing the statistics object to be supplied makes tests easier and allows callers to preserve
   * existing metrics conventions if needed.
   * </p>
   * @param stats cache statistics object
   */
  public NoOpCacheAccessService(CacheStats stats) {
    this.stats = Objects.requireNonNull(stats, "stats must not be null");
  }

  /**
   * Returns the service name.
   * @return service name
   */
  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Always returns {@code null} because this service does not store blocks.
   * @param cacheKey block to fetch
   * @param context  cache request context
   * @return always {@code null}
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, CacheRequestContext context) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(context, "context must not be null");
    return null;
  }

  /**
   * Validates the request and ignores the cache insertion.
   * @param cacheKey block cache key
   * @param block    block contents
   * @param context  cache write context
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");
  }

  /**
   * Always returns {@code false} because this service does not store blocks.
   * @param cacheKey block to remove
   * @return always {@code false}
   */
  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    return false;
  }

  /**
   * Always returns {@code 0} because this service does not store blocks.
   * @param hfileName HFile name
   * @return always {@code 0}
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");
    return 0;
  }

  /**
   * Always returns {@code 0} because this service does not store blocks.
   * @param hfileName  HFile name
   * @param initOffset inclusive start offset
   * @param endOffset  inclusive end offset
   * @return always {@code 0}
   */
  @Override
  public int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");
    return 0;
  }

  /**
   * Always returns {@code 0} because this service does not store blocks.
   * @param regionName region name
   * @return always {@code 0}
   */
  @Override
  public int evictBlocksByRegionName(String regionName) {
    Objects.requireNonNull(regionName, "regionName must not be null");
    return 0;
  }

  /**
   * Returns this service's statistics object.
   * @return cache statistics
   */
  @Override
  public CacheStats getStats() {
    return stats;
  }

  /**
   * Does nothing because this service owns no resources.
   */
  @Override
  public void shutdown() {
    // noop
  }

  /**
   * Always returns {@code 0} because this service has no capacity.
   * @return always {@code 0}
   */
  @Override
  public long getMaxSize() {
    return 0L;
  }

  /**
   * Always returns {@code 0} because this service has no capacity.
   * @return always {@code 0}
   */
  @Override
  public long getFreeSize() {
    return 0L;
  }

  /**
   * Always returns {@code 0} because this service stores no blocks.
   * @return always {@code 0}
   */
  @Override
  public long size() {
    return 0L;
  }

  /**
   * Always returns {@code 0} because this service stores no data blocks.
   * @return always {@code 0}
   */
  @Override
  public long getCurrentDataSize() {
    return 0L;
  }

  /**
   * Always returns {@code 0} because this service stores no blocks.
   * @return always {@code 0}
   */
  @Override
  public long getBlockCount() {
    return 0L;
  }

  /**
   * Always returns {@code 0} because this service stores no data blocks.
   * @return always {@code 0}
   */
  @Override
  public long getDataBlockCount() {
    return 0L;
  }

  /**
   * Always returns {@link Optional#empty()} because this service has no capacity.
   * @param block block to check
   * @return always {@link Optional#empty()}
   */
  @Override
  public Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    Objects.requireNonNull(block, "block must not be null");
    return Optional.empty();
  }

  /**
   * Always returns {@link Optional#empty()} because this service does not store blocks.
   * @param key block cache key
   * @return always {@link Optional#empty()}
   */
  @Override
  public Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");
    return Optional.empty();
  }

  /**
   * Always returns {@link Optional#empty()} because this service does not store blocks.
   * @param key block cache key
   * @return always {@link Optional#empty()}
   */
  @Override
  public Optional<Integer> getBlockSize(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");
    return Optional.empty();
  }

  /**
   * Always returns {@code false} because cache access is disabled.
   * @return always {@code false}
   */
  @Override
  public boolean isCacheEnabled() {
    return false;
  }

  /**
   * Always returns {@code false} because cache access is disabled.
   * @param timeout maximum time to wait
   * @return always {@code false}
   */
  @Override
  public boolean waitForCacheInitialization(long timeout) {
    return false;
  }

  /**
   * Validates the request and ignores the configuration change.
   * @param config new configuration
   */
  @Override
  public void onConfigurationChange(Configuration config) {
    Objects.requireNonNull(config, "config must not be null");
  }

  @Override
  public void notifyFileCachingCompleted(Path fileName, int totalBlockCount, int dataBlockCount,
    long size) {
    Objects.requireNonNull(fileName, "fileName must not be null");
  }

  @Override
  public Optional<Boolean> shouldCacheFile(HFileInfo hFileInfo, Configuration conf) {
    Objects.requireNonNull(hFileInfo, "hFileInfo must not be null");
    Objects.requireNonNull(conf, "conf must not be null");
    return Optional.empty();
  }

  @Override
  public Optional<Boolean> shouldCacheBlock(BlockCacheKey key, long maxTimestamp,
    Configuration conf) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(conf, "conf must not be null");
    return Optional.empty();
  }

  @Override
  public long getCurrentSize() {
    // TODO Auto-generated method stub
    return 0;
  }
}
