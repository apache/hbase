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
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HBase-facing access layer for block cache operations.
 * <p>
 * {@code CacheAccessService} is the cache abstraction intended for HBase read and write path
 * callers such as HFile readers, HFile writers, prefetch code, and file lifecycle code. It
 * preserves the core operational shape of the existing {@code BlockCache} API so callers can be
 * migrated incrementally, but it removes methods that are specific to a concrete cache engine,
 * tiered topology implementation, or diagnostics-only use case.
 * </p>
 * <p>
 * This interface is deliberately close to the hot-path subset of {@code BlockCache}:
 * </p>
 * <ul>
 * <li>lookup a block</li>
 * <li>cache a block</li>
 * <li>explicitly evict or invalidate blocks</li>
 * <li>report aggregate statistics and sizing information</li>
 * <li>manage cache lifecycle</li>
 * </ul>
 * <p>
 * The important difference from {@link CacheEngine} is the abstraction level. A {@code CacheEngine}
 * is a storage back-end implemented by a concrete cache such as LruBlockCache, BucketCache, or a
 * future CarrotCache engine. {@code CacheAccessService} is the HBase-facing facade above topology
 * and engines. It receives request intent through {@link CacheRequestContext} and insertion intent
 * through {@link CacheWriteContext}. A service implementation may initially delegate to an existing
 * {@code BlockCache}; a later implementation may delegate to {@link CacheTopology},
 * {@link CachePlacementAdmissionPolicy}, and one or more {@link CacheEngine} instances.
 * </p>
 * <p>
 * Normal HBase read and write path code should depend on this interface rather than accessing
 * {@link CacheTopology} or {@link CacheEngine} directly. Direct access to topology or engines is
 * reserved for cache assembly, topology execution, policy code, diagnostics, and tests. This keeps
 * placement, admission, representation, promotion, and engine-specific behavior outside of HFile
 * read/write logic.
 * </p>
 * <p>
 * This interface intentionally does not expose methods such as victim-cache wiring, backing-map
 * inspection, per-engine iterators, file fully-cached diagnostics, or BucketCache-specific helper
 * methods. Those concerns belong in lower-level engine APIs, topology views, metrics, or admin
 * tooling rather than the main read/write path abstraction.
 * </p>
 */
@InterfaceAudience.Private
public interface CacheAccessService extends ConfigurationObserver {

  /**
   * Returns a human-readable name for this cache access service instance.
   * <p>
   * The name is intended for logging, metrics, diagnostics, and configuration reporting. For a
   * legacy adapter this may be derived from the wrapped cache. For a topology-backed service it may
   * identify the configured topology or service implementation.
   * </p>
   * @return service name
   */
  String getName();

  /**
   * Fetches a block from the cache.
   * <p>
   * This is the main read-path lookup method. The supplied {@link CacheRequestContext} carries
   * caller intent that used to be passed as individual boolean arguments to {@code BlockCache},
   * such as whether caching is enabled for this request, whether this lookup is a repeated lookup
   * for the same logical request, and whether cache metrics should be updated.
   * </p>
   * <p>
   * The service is responsible for preserving the current HBase lookup semantics. A legacy
   * implementation may translate this call directly to {@code BlockCache#getBlock}. A future
   * topology-backed implementation may route the lookup through {@link CacheTopology}, apply
   * hit-driven promotion decisions, and aggregate metrics across engines.
   * </p>
   * @param cacheKey block to fetch
   * @param context  cache request context
   * @return cached block, or {@code null} if not present
   */
  Cacheable getBlock(BlockCacheKey cacheKey, CacheRequestContext context);

  /**
   * Fetches a block from the cache using explicit lookup flags.
   * <p>
   * This compatibility helper mirrors the current {@code BlockCache#getBlock} call shape and builds
   * a {@link CacheRequestContext} before delegating to
   * {@link #getBlock(BlockCacheKey, CacheRequestContext)}. New call sites should prefer the context
   * based method directly.
   * </p>
   * @param cacheKey           block to fetch
   * @param caching            whether caching is enabled for the request; used for metrics
   * @param repeat             whether this is a repeated lookup for the same logical request
   * @param updateCacheMetrics whether cache metrics should be updated
   * @return cached block, or {@code null} if not present
   */
  default Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    CacheRequestContext context = CacheRequestContext.newBuilder().withCaching(caching)
      .withRepeat(repeat).withUpdateCacheMetrics(updateCacheMetrics).build();
    return getBlock(cacheKey, context);
  }

  /**
   * Fetches a block from the cache using explicit lookup flags and a block type hint.
   * <p>
   * This compatibility helper mirrors the current {@code BlockCache#getBlock} overload that accepts
   * a {@link BlockType}. The block type is a lookup hint and must not be treated as part of the
   * cache key.
   * </p>
   * @param cacheKey           block to fetch
   * @param caching            whether caching is enabled for the request; used for metrics
   * @param repeat             whether this is a repeated lookup for the same logical request
   * @param updateCacheMetrics whether cache metrics should be updated
   * @param blockType          optional block type hint
   * @return cached block, or {@code null} if not present
   */
  default Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics, BlockType blockType) {
    CacheRequestContext context =
      CacheRequestContext.newBuilder().withCaching(caching).withRepeat(repeat)
        .withUpdateCacheMetrics(updateCacheMetrics).withBlockType(blockType).build();
    return getBlock(cacheKey, context);
  }

  /**
   * Adds a block to the cache.
   * <p>
   * This is the main write-side cache population method. The supplied {@link CacheWriteContext}
   * identifies the source and intent of the insertion, for example read-miss population, flush
   * output, compaction output, prefetch, or promotion. Existing {@code CacheConfig} logic should
   * decide whether cache population should be attempted before this method is called.
   * </p>
   * <p>
   * A legacy implementation may translate this call directly to {@code BlockCache#cacheBlock}. A
   * future topology-backed implementation may first ask {@link CachePlacementAdmissionPolicy} for
   * admission, tier placement, and representation decisions, then execute the selected operation
   * through {@link CacheTopology} and its engines.
   * </p>
   * @param cacheKey block cache key
   * @param block    block contents
   * @param context  cache write context
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context);

  /**
   * Adds a block to the cache using explicit insertion flags.
   * <p>
   * This compatibility helper mirrors the current {@code BlockCache#cacheBlock} call shape and
   * builds a {@link CacheWriteContext} before delegating to
   * {@link #cacheBlock(BlockCacheKey, Cacheable, CacheWriteContext)}. New call sites should prefer
   * the context based method directly.
   * </p>
   * @param cacheKey block cache key
   * @param block    block contents
   * @param inMemory whether the block should be treated as in-memory
   */
  default void cacheBlock(BlockCacheKey cacheKey, Cacheable block, boolean inMemory) {
    CacheWriteContext context = CacheWriteContext.newBuilder().withInMemory(inMemory).build();
    cacheBlock(cacheKey, block, context);
  }

  /**
   * Adds a block to the cache using explicit insertion flags.
   * <p>
   * This compatibility helper mirrors the current {@code BlockCache#cacheBlock} overload that
   * carries {@code waitWhenCache}. The flag is useful for asynchronous cache backends such as
   * BucketCache. New call sites should prefer the context based method directly.
   * </p>
   * @param cacheKey      block cache key
   * @param block         block contents
   * @param inMemory      whether the block should be treated as in-memory
   * @param waitWhenCache whether to wait for the cache operation to be accepted/flushed
   */
  default void cacheBlock(BlockCacheKey cacheKey, Cacheable block, boolean inMemory,
    boolean waitWhenCache) {
    CacheWriteContext context = CacheWriteContext.newBuilder().withInMemory(inMemory)
      .withWaitWhenCache(waitWhenCache).build();
    cacheBlock(cacheKey, block, context);
  }

  /**
   * Adds a block to the cache, defaulting to non in-memory treatment.
   * <p>
   * This compatibility helper mirrors {@code BlockCache#cacheBlock(BlockCacheKey, Cacheable)}.
   * </p>
   * @param cacheKey block cache key
   * @param block    block contents
   */
  default void cacheBlock(BlockCacheKey cacheKey, Cacheable block) {
    cacheBlock(cacheKey, block, CacheWriteContext.newBuilder().build());
  }

  /**
   * Explicitly removes a single block from the cache.
   * <p>
   * This method represents caller-requested invalidation/removal. It is not the normal replacement
   * path used by a cache implementation under capacity pressure. A service implementation should
   * propagate this operation to the appropriate topology or underlying cache implementation.
   * </p>
   * @param cacheKey block to remove
   * @return {@code true} if the block existed and was removed, {@code false} otherwise
   */
  boolean evictBlock(BlockCacheKey cacheKey);

  /**
   * Explicitly removes all cached blocks for the given HFile.
   * <p>
   * This method is used for file-scoped invalidation during HFile lifecycle events. The method name
   * intentionally follows the existing {@code BlockCache} API for migration compatibility, although
   * the operation is better understood as explicit invalidation rather than replacement eviction.
   * </p>
   * @param hfileName HFile name
   * @return number of blocks removed
   */
  int evictBlocksByHfileName(String hfileName);

  /**
   * Explicitly removes cached blocks for the given HFile within the specified offset range.
   * <p>
   * This optional method mirrors the storage-oriented range invalidation support in
   * {@link CacheEngine}. Implementations that cannot perform efficient range invalidation may
   * return {@code 0}. Callers must not rely on this method for correctness unless the specific
   * service implementation documents support for it.
   * </p>
   * @param hfileName  HFile name
   * @param initOffset inclusive start offset
   * @param endOffset  inclusive end offset
   * @return number of blocks removed
   */
  default int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    return 0;
  }

  /**
   * Explicitly removes cached blocks associated with the specified region.
   * <p>
   * This optional method supports region-scoped invalidation without requiring callers to enumerate
   * files first. Implementations that cannot perform efficient region invalidation may return
   * {@code 0}. Callers must not rely on this method for correctness unless the specific service
   * implementation documents support for it.
   * </p>
   * @param regionName region name
   * @return number of blocks removed
   */
  default int evictBlocksByRegionName(String regionName) {
    return 0;
  }

  /**
   * Returns aggregate cache statistics visible at the service level.
   * <p>
   * For a legacy adapter this may be the statistics object from the wrapped cache. For a
   * topology-backed implementation this should represent aggregate statistics across all engines in
   * the topology, preserving the semantics expected by existing HBase metrics and callers.
   * </p>
   * @return cache statistics
   */
  CacheStats getStats();

  /**
   * Shuts down the cache access service and releases owned resources.
   * <p>
   * A service implementation should also shut down any underlying topology, engines, or wrapped
   * legacy cache it owns. If the service does not own the underlying cache lifecycle, the
   * implementation should document that behavior.
   * </p>
   */
  void shutdown();

  /**
   * Returns the maximum configured cache size visible through this service, in bytes.
   * <p>
   * For a topology-backed service this should normally be the aggregate configured capacity of the
   * participating engines, unless the topology has a different externally visible capacity model.
   * </p>
   * @return maximum cache size
   */
  long getMaxSize();

  /**
   * Returns the amount of free cache space visible through this service, in bytes.
   * <p>
   * Free space may not always be exactly {@code getMaxSize() - size()} because engines may have
   * fragmentation, reserved capacity, asynchronous insertion queues, or topology-specific capacity
   * rules.
   * </p>
   * @return free size
   */
  long getFreeSize();

  /**
   * Returns the currently occupied cache size visible through this service, in bytes.
   * <p>
   * The method name follows the existing {@code BlockCache} and {@link CacheEngine} shape for
   * migration compatibility.
   * </p>
   * @return occupied cache size
   */
  long size();

  /**
   * Returns the occupied size of the block cache, in bytes.
   * @return occupied space in cache, in bytes
   */
  long getCurrentSize();

  /**
   * Returns the currently occupied size of cached data blocks, in bytes.
   * <p>
   * This is an aggregate service-level value. Implementations that cannot distinguish data-block
   * occupancy should return the best value available from the underlying cache implementation.
   * </p>
   * @return occupied data-block size
   */
  long getCurrentDataSize();

  /**
   * Returns the total number of cached blocks visible through this service.
   * @return total block count
   */
  long getBlockCount();

  /**
   * Returns the number of cached data blocks visible through this service.
   * @return data block count
   */
  long getDataBlockCount();

  /**
   * Checks whether the given block can fit into the cache represented by this service.
   * <p>
   * This method is optional and exists primarily for migration compatibility with existing cache
   * call sites. It should not be used as a general admission policy API. Admission and placement
   * decisions belong to {@link CachePlacementAdmissionPolicy} in the topology-backed architecture.
   * </p>
   * @param block block to check
   * @return empty if unsupported; otherwise whether the block fits
   */
  default Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    return Optional.empty();
  }

  /**
   * Checks whether the block represented by the given key is already cached.
   * <p>
   * This method is optional and exists primarily for migration compatibility and diagnostics. It is
   * not required for the normal read/write path because callers should use
   * {@link #getBlock(BlockCacheKey, CacheRequestContext)} for lookup.
   * </p>
   * @param key block cache key
   * @return empty if unsupported; otherwise whether the block is cached
   */
  default Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    return Optional.empty();
  }

  /**
   * Returns the size of the cached block represented by the given key.
   * <p>
   * This method is optional because not all implementations expose per-block size cheaply. It is
   * not part of the normal hot-path lookup contract.
   * </p>
   * @param key block cache key
   * @return empty if unsupported or not present; otherwise cached block size
   */
  default Optional<Integer> getBlockSize(BlockCacheKey key) {
    return Optional.empty();
  }

  /**
   * Returns whether cache access is enabled.
   * <p>
   * Implementations may override this when cache availability is controlled by initialization
   * state, configuration, or a wrapped legacy cache. The default assumes the service is enabled.
   * </p>
   * @return {@code true} if enabled, {@code false} otherwise
   */
  default boolean isCacheEnabled() {
    return true;
  }

  /**
   * Waits for asynchronous cache initialization to complete.
   * <p>
   * Some cache implementations, especially those backed by persistent or asynchronous engines, may
   * require initialization before they should be used. Services that do not require asynchronous
   * initialization may return {@code true} immediately.
   * </p>
   * @param timeout maximum time to wait
   * @return {@code true} if the cache is enabled and ready, {@code false} otherwise
   */
  default boolean waitForCacheInitialization(long timeout) {
    return true;
  }

  /**
   * Refreshes this service's configuration.
   * <p>
   * A service implementation may propagate this notification to the wrapped legacy cache, topology,
   * policy, or engines. The default implementation is a no-op.
   * </p>
   * @param config new configuration
   */
  @Override
  default void onConfigurationChange(Configuration config) {
    // noop
  }

  /**
   * Notifies the cache service that cache population for an HFile has completed.
   * <p>
   * This callback is used for file-level cache lifecycle notifications. Some cache implementations
   * may use it to finalize file-scoped metadata, update fully-cached-file tracking, publish cache
   * population statistics, or trigger implementation-specific bookkeeping after a writer/prefetcher
   * has finished caching blocks for an HFile.
   * </p>
   * <p>
   * The default implementation is a no-op because not all cache implementations need
   * file-completion notifications. Callers may invoke this method unconditionally after file-level
   * cache population completes.
   * </p>
   * @param fileName        path of the HFile whose cache population completed
   * @param totalBlockCount total number of cached blocks for the file
   * @param dataBlockCount  number of cached data blocks for the file
   * @param size            total cached size for the file, in bytes
   */
  default void notifyFileCachingCompleted(Path fileName, int totalBlockCount, int dataBlockCount,
    long size) {
    // noop
  }

  /**
   * Executes the supplied action when this cache service is enabled.
   * <p>
   * This helper is intended to preserve the old {@code getBlockCache().ifPresent(...)} style for
   * call sites that should do nothing when block cache is disabled. It keeps callers independent of
   * concrete implementations such as {@code NoOpCacheAccessService} while still allowing disabled
   * cache wiring to behave like an absent cache.
   * </p>
   * <p>
   * Implementations normally do not need to override this method. The default implementation checks
   * {@link #isCacheEnabled()} and invokes the supplied action only when cache access is enabled.
   * </p>
   * @param action action to execute with this cache service when enabled
   */
  default void ifEnabled(Consumer<CacheAccessService> action) {
    Objects.requireNonNull(action, "action must not be null");
    if (isCacheEnabled()) {
      action.accept(this);
    }
  }

  /**
   * Returns whether blocks from the given HFile should be cached. TODO: this method is a temporary
   * adapter for file-level admission decisions. It will be removed and replaced by a more general
   * admission API in the future.
   * <p>
   * This is a file-level admission hook used by cache population paths. Implementations may use
   * file metadata, configuration, data tiering state, or implementation-specific bookkeeping to
   * decide whether the file should be admitted into cache.
   * </p>
   * <p>
   * The returned {@link Optional} is empty when the cache service does not support file-level
   * admission decisions. In that case, callers should preserve existing default behavior, typically
   * treating the result as "no opinion" rather than as a rejection.
   * </p>
   * @param hFileInfo HFile metadata used for the admission decision
   * @param conf      configuration
   * @return empty if unsupported; otherwise whether the file should be cached
   */
  default Optional<Boolean> shouldCacheFile(HFileInfo hFileInfo, Configuration conf) {
    return Optional.empty();
  }

  /**
   * Returns whether the block represented by the given key and timestamp should be cached. TODO:
   * this method is a temporary adapter for file-level admission decisions. It will be removed and
   * replaced by a more general admission API in the future.
   * <p>
   * <p>
   * This is a block-level admission hook used by cache population paths. Implementations may use
   * block identity, maximum timestamp, configuration, data tiering state, or
   * implementation-specific policy to decide whether the block should be admitted into cache.
   * </p>
   * <p>
   * The returned {@link Optional} is empty when the cache service does not support block-level
   * admission decisions. In that case, callers should preserve existing default behavior, typically
   * treating the result as "no opinion" rather than as a rejection.
   * </p>
   * @param key          block cache key
   * @param maxTimestamp maximum timestamp associated with the block
   * @param conf         configuration
   * @return empty if unsupported; otherwise whether the block should be cached
   */
  default Optional<Boolean> shouldCacheBlock(BlockCacheKey key, long maxTimestamp,
    Configuration conf) {
    return Optional.empty();
  }
}
