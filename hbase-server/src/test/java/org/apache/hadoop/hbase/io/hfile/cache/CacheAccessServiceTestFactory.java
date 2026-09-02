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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.LruAdaptiveBlockCache;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.io.hfile.TinyLfuBlockCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Test-only factory helpers for creating {@link CacheAccessService} instances backed by legacy
 * {@link BlockCache} implementations.
 * <p>
 * These helpers are intended to keep integration-style tests concise while the block cache
 * architecture migrates toward {@link CacheAccessService}. They deliberately do not replace
 * implementation-specific tests for {@link LruBlockCache}, {@link LruAdaptiveBlockCache},
 * {@link TinyLfuBlockCache}, {@link BucketCache}, or other concrete cache implementations.
 * </p>
 * <p>
 * Each helper mirrors a public constructor on one of the legacy cache implementations and returns
 * either:
 * </p>
 * <ul>
 * <li>a {@link CacheAccessService} facade backed by the constructed cache, or</li>
 * <li>a {@link CacheAccessServiceTestInstance} containing both the concrete cache and the
 * facade.</li>
 * </ul>
 * <p>
 * The factory currently creates legacy {@link BlockCache} implementations and wraps them using
 * {@link CacheAccessServices#fromBlockCache(BlockCache)}. Later, when cache engines and topologies
 * are wired directly, the internals of this factory can change without forcing integration-style
 * tests to be rewritten again.
 * </p>
 */
@InterfaceAudience.Private
public final class CacheAccessServiceTestFactory {

  private CacheAccessServiceTestFactory() {
  }

  /**
   * Wraps an existing {@link BlockCache} in a {@link CacheAccessService}.
   * @param blockCache block cache to wrap
   * @return cache access service backed by the supplied block cache
   * @throws NullPointerException if {@code blockCache} is {@code null}
   */
  public static CacheAccessService fromBlockCache(BlockCache blockCache) {
    return CacheAccessServices.fromBlockCache(blockCache);
  }

  /**
   * Creates a {@link CacheAccessService} from the block cache configuration.
   * <p>
   * This method delegates to {@link CacheAccessServices#fromConfiguration(Configuration)} and is
   * useful for tests that want the normal legacy
   * {@link org.apache.hadoop.hbase.io.hfile.BlockCacheFactory} construction path while receiving a
   * {@link CacheAccessService} facade.
   * </p>
   * @param conf configuration used to create the legacy block cache
   * @return cache access service created from configuration, or disabled when no cache is
   *         configured
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessService fromConfiguration(Configuration conf) {
    return CacheAccessServices.fromConfiguration(conf);
  }

  /**
   * Returns a disabled/no-op cache access service.
   * @return disabled cache access service
   */
  public static CacheAccessService disabled() {
    return CacheAccessServices.disabled();
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruBlockCache}.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @return cache access service backed by {@link LruBlockCache}
   */
  public static CacheAccessService lru(long maxSize, long blockSize) {
    return lruInstance(maxSize, blockSize).service();
  }

  /**
   * Creates a test instance containing {@link LruBlockCache} and its {@link CacheAccessService}
   * facade.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @return test instance
   */
  public static CacheAccessServiceTestInstance<LruBlockCache> lruInstance(long maxSize,
    long blockSize) {
    return new CacheAccessServiceTestInstance<>(new LruBlockCache(maxSize, blockSize));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruBlockCache}.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @return cache access service backed by {@link LruBlockCache}
   */
  public static CacheAccessService lru(long maxSize, long blockSize, boolean evictionThread) {
    return lruInstance(maxSize, blockSize, evictionThread).service();
  }

  /**
   * Creates a test instance containing {@link LruBlockCache} and its {@link CacheAccessService}
   * facade.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @return test instance
   */
  public static CacheAccessServiceTestInstance<LruBlockCache> lruInstance(long maxSize,
    long blockSize, boolean evictionThread) {
    return new CacheAccessServiceTestInstance<>(
      new LruBlockCache(maxSize, blockSize, evictionThread));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruBlockCache}.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @param conf           configuration used by the cache constructor
   * @return cache access service backed by {@link LruBlockCache}
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessService lru(long maxSize, long blockSize, boolean evictionThread,
    Configuration conf) {
    return lruInstance(maxSize, blockSize, evictionThread, conf).service();
  }

  /**
   * Creates a test instance containing {@link LruBlockCache} and its {@link CacheAccessService}
   * facade.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @param conf           configuration used by the cache constructor
   * @return test instance
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<LruBlockCache> lruInstance(long maxSize,
    long blockSize, boolean evictionThread, Configuration conf) {
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(
      new LruBlockCache(maxSize, blockSize, evictionThread, conf));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruBlockCache}.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @param conf      configuration used by the cache constructor
   * @return cache access service backed by {@link LruBlockCache}
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessService lru(long maxSize, long blockSize, Configuration conf) {
    return lruInstance(maxSize, blockSize, conf).service();
  }

  /**
   * Creates a test instance containing {@link LruBlockCache} and its {@link CacheAccessService}
   * facade.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @param conf      configuration used by the cache constructor
   * @return test instance
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<LruBlockCache> lruInstance(long maxSize,
    long blockSize, Configuration conf) {
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(new LruBlockCache(maxSize, blockSize, conf));
  }

  /**
   * Creates a {@link CacheAccessService} backed by a fully configured {@link LruBlockCache}.
   * @param maxSize             maximum size of this cache, in bytes
   * @param blockSize           expected average size of blocks, in bytes
   * @param evictionThread      whether to run evictions in a background thread
   * @param mapInitialSize      initial size of the backing concurrent map
   * @param mapLoadFactor       initial load factor of the backing concurrent map
   * @param mapConcurrencyLevel initial concurrency level of the backing concurrent map
   * @param minFactor           percentage of total size to evict down to
   * @param acceptableFactor    percentage of total size that triggers eviction
   * @param singleFactor        percentage of total size for single-access blocks
   * @param multiFactor         percentage of total size for multi-access blocks
   * @param memoryFactor        percentage of total size for in-memory blocks
   * @param hardLimitFactor     hard capacity limit factor
   * @param forceInMemory       whether in-memory HFile data blocks have higher priority
   * @param maxBlockSize        maximum block size accepted by the cache
   * @return cache access service backed by {@link LruBlockCache}
   */
  public static CacheAccessService lru(long maxSize, long blockSize, boolean evictionThread,
    int mapInitialSize, float mapLoadFactor, int mapConcurrencyLevel, float minFactor,
    float acceptableFactor, float singleFactor, float multiFactor, float memoryFactor,
    float hardLimitFactor, boolean forceInMemory, long maxBlockSize) {
    return lruInstance(maxSize, blockSize, evictionThread, mapInitialSize, mapLoadFactor,
      mapConcurrencyLevel, minFactor, acceptableFactor, singleFactor, multiFactor, memoryFactor,
      hardLimitFactor, forceInMemory, maxBlockSize).service();
  }

  /**
   * Creates a test instance containing a fully configured {@link LruBlockCache} and its
   * {@link CacheAccessService} facade.
   * @param maxSize             maximum size of this cache, in bytes
   * @param blockSize           expected average size of blocks, in bytes
   * @param evictionThread      whether to run evictions in a background thread
   * @param mapInitialSize      initial size of the backing concurrent map
   * @param mapLoadFactor       initial load factor of the backing concurrent map
   * @param mapConcurrencyLevel initial concurrency level of the backing concurrent map
   * @param minFactor           percentage of total size to evict down to
   * @param acceptableFactor    percentage of total size that triggers eviction
   * @param singleFactor        percentage of total size for single-access blocks
   * @param multiFactor         percentage of total size for multi-access blocks
   * @param memoryFactor        percentage of total size for in-memory blocks
   * @param hardLimitFactor     hard capacity limit factor
   * @param forceInMemory       whether in-memory HFile data blocks have higher priority
   * @param maxBlockSize        maximum block size accepted by the cache
   * @return test instance
   */
  public static CacheAccessServiceTestInstance<LruBlockCache> lruInstance(long maxSize,
    long blockSize, boolean evictionThread, int mapInitialSize, float mapLoadFactor,
    int mapConcurrencyLevel, float minFactor, float acceptableFactor, float singleFactor,
    float multiFactor, float memoryFactor, float hardLimitFactor, boolean forceInMemory,
    long maxBlockSize) {
    return new CacheAccessServiceTestInstance<>(
      new LruBlockCache(maxSize, blockSize, evictionThread, mapInitialSize, mapLoadFactor,
        mapConcurrencyLevel, minFactor, acceptableFactor, singleFactor, multiFactor, memoryFactor,
        hardLimitFactor, forceInMemory, maxBlockSize));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruAdaptiveBlockCache}.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @return cache access service backed by {@link LruAdaptiveBlockCache}
   */
  public static CacheAccessService lruAdaptive(long maxSize, long blockSize) {
    return lruAdaptiveInstance(maxSize, blockSize).service();
  }

  /**
   * Creates a test instance containing {@link LruAdaptiveBlockCache} and its
   * {@link CacheAccessService} facade.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @return test instance
   */
  public static CacheAccessServiceTestInstance<LruAdaptiveBlockCache>
    lruAdaptiveInstance(long maxSize, long blockSize) {
    return new CacheAccessServiceTestInstance<>(new LruAdaptiveBlockCache(maxSize, blockSize));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruAdaptiveBlockCache}.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @return cache access service backed by {@link LruAdaptiveBlockCache}
   */
  public static CacheAccessService lruAdaptive(long maxSize, long blockSize,
    boolean evictionThread) {
    return lruAdaptiveInstance(maxSize, blockSize, evictionThread).service();
  }

  /**
   * Creates a test instance containing {@link LruAdaptiveBlockCache} and its
   * {@link CacheAccessService} facade.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @return test instance
   */
  public static CacheAccessServiceTestInstance<LruAdaptiveBlockCache>
    lruAdaptiveInstance(long maxSize, long blockSize, boolean evictionThread) {
    return new CacheAccessServiceTestInstance<>(
      new LruAdaptiveBlockCache(maxSize, blockSize, evictionThread));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruAdaptiveBlockCache}.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @param conf           configuration used by the cache constructor
   * @return cache access service backed by {@link LruAdaptiveBlockCache}
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessService lruAdaptive(long maxSize, long blockSize, boolean evictionThread,
    Configuration conf) {
    return lruAdaptiveInstance(maxSize, blockSize, evictionThread, conf).service();
  }

  /**
   * Creates a test instance containing {@link LruAdaptiveBlockCache} and its
   * {@link CacheAccessService} facade.
   * @param maxSize        maximum size of cache, in bytes
   * @param blockSize      approximate size of each block, in bytes
   * @param evictionThread whether to start the eviction thread
   * @param conf           configuration used by the cache constructor
   * @return test instance
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<LruAdaptiveBlockCache>
    lruAdaptiveInstance(long maxSize, long blockSize, boolean evictionThread, Configuration conf) {
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(
      new LruAdaptiveBlockCache(maxSize, blockSize, evictionThread, conf));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link LruAdaptiveBlockCache}.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @param conf      configuration used by the cache constructor
   * @return cache access service backed by {@link LruAdaptiveBlockCache}
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessService lruAdaptive(long maxSize, long blockSize, Configuration conf) {
    return lruAdaptiveInstance(maxSize, blockSize, conf).service();
  }

  /**
   * Creates a test instance containing {@link LruAdaptiveBlockCache} and its
   * {@link CacheAccessService} facade.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @param conf      configuration used by the cache constructor
   * @return test instance
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<LruAdaptiveBlockCache>
    lruAdaptiveInstance(long maxSize, long blockSize, Configuration conf) {
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(
      new LruAdaptiveBlockCache(maxSize, blockSize, conf));
  }

  /**
   * Creates a {@link CacheAccessService} backed by a fully configured
   * {@link LruAdaptiveBlockCache}.
   * @param maxSize                          maximum size of this cache, in bytes
   * @param blockSize                        expected average size of blocks, in bytes
   * @param evictionThread                   whether to run evictions in a background thread
   * @param mapInitialSize                   initial size of the backing concurrent map
   * @param mapLoadFactor                    initial load factor of the backing concurrent map
   * @param mapConcurrencyLevel              initial concurrency level of the backing concurrent map
   * @param minFactor                        percentage of total size to evict down to
   * @param acceptableFactor                 percentage of total size that triggers eviction
   * @param singleFactor                     percentage of total size for single-access blocks
   * @param multiFactor                      percentage of total size for multi-access blocks
   * @param memoryFactor                     percentage of total size for in-memory blocks
   * @param hardLimitFactor                  hard capacity limit factor
   * @param forceInMemory                    whether in-memory HFile data blocks have higher
   *                                         priority
   * @param maxBlockSize                     maximum block size accepted by the cache
   * @param heavyEvictionCountLimit          threshold count for heavy-eviction adaptive behavior
   * @param heavyEvictionMbSizeLimit         threshold size for heavy-eviction adaptive behavior
   * @param heavyEvictionOverheadCoefficient coefficient controlling adaptive reduction
   *                                         aggressiveness
   * @return cache access service backed by {@link LruAdaptiveBlockCache}
   */
  public static CacheAccessService lruAdaptive(long maxSize, long blockSize, boolean evictionThread,
    int mapInitialSize, float mapLoadFactor, int mapConcurrencyLevel, float minFactor,
    float acceptableFactor, float singleFactor, float multiFactor, float memoryFactor,
    float hardLimitFactor, boolean forceInMemory, long maxBlockSize, int heavyEvictionCountLimit,
    long heavyEvictionMbSizeLimit, float heavyEvictionOverheadCoefficient) {
    return lruAdaptiveInstance(maxSize, blockSize, evictionThread, mapInitialSize, mapLoadFactor,
      mapConcurrencyLevel, minFactor, acceptableFactor, singleFactor, multiFactor, memoryFactor,
      hardLimitFactor, forceInMemory, maxBlockSize, heavyEvictionCountLimit,
      heavyEvictionMbSizeLimit, heavyEvictionOverheadCoefficient).service();
  }

  /**
   * Creates a test instance containing a fully configured {@link LruAdaptiveBlockCache} and its
   * {@link CacheAccessService} facade.
   * @param maxSize                          maximum size of this cache, in bytes
   * @param blockSize                        expected average size of blocks, in bytes
   * @param evictionThread                   whether to run evictions in a background thread
   * @param mapInitialSize                   initial size of the backing concurrent map
   * @param mapLoadFactor                    initial load factor of the backing concurrent map
   * @param mapConcurrencyLevel              initial concurrency level of the backing concurrent map
   * @param minFactor                        percentage of total size to evict down to
   * @param acceptableFactor                 percentage of total size that triggers eviction
   * @param singleFactor                     percentage of total size for single-access blocks
   * @param multiFactor                      percentage of total size for multi-access blocks
   * @param memoryFactor                     percentage of total size for in-memory blocks
   * @param hardLimitFactor                  hard capacity limit factor
   * @param forceInMemory                    whether in-memory HFile data blocks have higher
   *                                         priority
   * @param maxBlockSize                     maximum block size accepted by the cache
   * @param heavyEvictionCountLimit          threshold count for heavy-eviction adaptive behavior
   * @param heavyEvictionMbSizeLimit         threshold size for heavy-eviction adaptive behavior
   * @param heavyEvictionOverheadCoefficient coefficient controlling adaptive reduction
   *                                         aggressiveness
   * @return test instance
   */
  public static CacheAccessServiceTestInstance<LruAdaptiveBlockCache> lruAdaptiveInstance(
    long maxSize, long blockSize, boolean evictionThread, int mapInitialSize, float mapLoadFactor,
    int mapConcurrencyLevel, float minFactor, float acceptableFactor, float singleFactor,
    float multiFactor, float memoryFactor, float hardLimitFactor, boolean forceInMemory,
    long maxBlockSize, int heavyEvictionCountLimit, long heavyEvictionMbSizeLimit,
    float heavyEvictionOverheadCoefficient) {
    return new CacheAccessServiceTestInstance<>(
      new LruAdaptiveBlockCache(maxSize, blockSize, evictionThread, mapInitialSize, mapLoadFactor,
        mapConcurrencyLevel, minFactor, acceptableFactor, singleFactor, multiFactor, memoryFactor,
        hardLimitFactor, forceInMemory, maxBlockSize, heavyEvictionCountLimit,
        heavyEvictionMbSizeLimit, heavyEvictionOverheadCoefficient));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link TinyLfuBlockCache}.
   * @param maximumSizeInBytes maximum size of this cache, in bytes
   * @param avgBlockSize       expected average size of blocks, in bytes
   * @param executor           executor used by the underlying Caffeine cache
   * @param conf               configuration used by the cache constructor
   * @return cache access service backed by {@link TinyLfuBlockCache}
   * @throws NullPointerException if {@code executor} or {@code conf} is {@code null}
   */
  public static CacheAccessService tinyLfu(long maximumSizeInBytes, long avgBlockSize,
    Executor executor, Configuration conf) {
    return tinyLfuInstance(maximumSizeInBytes, avgBlockSize, executor, conf).service();
  }

  /**
   * Creates a test instance containing {@link TinyLfuBlockCache} and its {@link CacheAccessService}
   * facade.
   * @param maximumSizeInBytes maximum size of this cache, in bytes
   * @param avgBlockSize       expected average size of blocks, in bytes
   * @param executor           executor used by the underlying Caffeine cache
   * @param conf               configuration used by the cache constructor
   * @return test instance
   * @throws NullPointerException if {@code executor} or {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<TinyLfuBlockCache> tinyLfuInstance(
    long maximumSizeInBytes, long avgBlockSize, Executor executor, Configuration conf) {
    Objects.requireNonNull(executor, "executor must not be null");
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(
      new TinyLfuBlockCache(maximumSizeInBytes, avgBlockSize, executor, conf));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link TinyLfuBlockCache}.
   * @param maximumSizeInBytes maximum size of this cache, in bytes
   * @param avgBlockSize       expected average size of blocks, in bytes
   * @param maxBlockSize       maximum size of a block, in bytes
   * @param executor           executor used by the underlying Caffeine cache
   * @return cache access service backed by {@link TinyLfuBlockCache}
   * @throws NullPointerException if {@code executor} is {@code null}
   */
  public static CacheAccessService tinyLfu(long maximumSizeInBytes, long avgBlockSize,
    long maxBlockSize, Executor executor) {
    return tinyLfuInstance(maximumSizeInBytes, avgBlockSize, maxBlockSize, executor).service();
  }

  /**
   * Creates a test instance containing {@link TinyLfuBlockCache} and its {@link CacheAccessService}
   * facade.
   * @param maximumSizeInBytes maximum size of this cache, in bytes
   * @param avgBlockSize       expected average size of blocks, in bytes
   * @param maxBlockSize       maximum size of a block, in bytes
   * @param executor           executor used by the underlying Caffeine cache
   * @return test instance
   * @throws NullPointerException if {@code executor} is {@code null}
   */
  public static CacheAccessServiceTestInstance<TinyLfuBlockCache> tinyLfuInstance(
    long maximumSizeInBytes, long avgBlockSize, long maxBlockSize, Executor executor) {
    Objects.requireNonNull(executor, "executor must not be null");
    return new CacheAccessServiceTestInstance<>(
      new TinyLfuBlockCache(maximumSizeInBytes, avgBlockSize, maxBlockSize, executor));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link BucketCache}.
   * @param ioEngineName    IO engine name
   * @param capacity        cache capacity, in bytes
   * @param blockSize       expected block size, in bytes
   * @param bucketSizes     configured bucket sizes, or {@code null} to use defaults
   * @param writerThreadNum number of writer threads
   * @param writerQLen      writer queue length
   * @param persistencePath persistence path, or {@code null} for non-persistent cache
   * @return cache access service backed by {@link BucketCache}
   * @throws IOException          if the bucket cache cannot be created
   * @throws NullPointerException if {@code ioEngineName} is {@code null}
   */
  public static CacheAccessService bucket(String ioEngineName, long capacity, int blockSize,
    int[] bucketSizes, int writerThreadNum, int writerQLen, String persistencePath)
    throws IOException {
    return bucketInstance(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum,
      writerQLen, persistencePath).service();
  }

  /**
   * Creates a test instance containing {@link BucketCache} and its {@link CacheAccessService}
   * facade.
   * @param ioEngineName    IO engine name
   * @param capacity        cache capacity, in bytes
   * @param blockSize       expected block size, in bytes
   * @param bucketSizes     configured bucket sizes, or {@code null} to use defaults
   * @param writerThreadNum number of writer threads
   * @param writerQLen      writer queue length
   * @param persistencePath persistence path, or {@code null} for non-persistent cache
   * @return test instance
   * @throws IOException          if the bucket cache cannot be created
   * @throws NullPointerException if {@code ioEngineName} is {@code null}
   */
  public static CacheAccessServiceTestInstance<BucketCache> bucketInstance(String ioEngineName,
    long capacity, int blockSize, int[] bucketSizes, int writerThreadNum, int writerQLen,
    String persistencePath) throws IOException {
    Objects.requireNonNull(ioEngineName, "ioEngineName must not be null");
    return new CacheAccessServiceTestInstance<>(new BucketCache(ioEngineName, capacity, blockSize,
      bucketSizes, writerThreadNum, writerQLen, persistencePath));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link BucketCache}.
   * @param ioEngineName               IO engine name
   * @param capacity                   cache capacity, in bytes
   * @param blockSize                  expected block size, in bytes
   * @param bucketSizes                configured bucket sizes, or {@code null} to use defaults
   * @param writerThreadNum            number of writer threads
   * @param writerQLen                 writer queue length
   * @param persistencePath            persistence path, or {@code null} for non-persistent cache
   * @param ioErrorsTolerationDuration duration for tolerating IO engine errors
   * @param conf                       configuration used by the bucket cache
   * @return cache access service backed by {@link BucketCache}
   * @throws IOException          if the bucket cache cannot be created
   * @throws NullPointerException if {@code ioEngineName} or {@code conf} is {@code null}
   */
  public static CacheAccessService bucket(String ioEngineName, long capacity, int blockSize,
    int[] bucketSizes, int writerThreadNum, int writerQLen, String persistencePath,
    int ioErrorsTolerationDuration, Configuration conf) throws IOException {
    return bucketInstance(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum,
      writerQLen, persistencePath, ioErrorsTolerationDuration, conf).service();
  }

  /**
   * Creates a test instance containing {@link BucketCache} and its {@link CacheAccessService}
   * facade.
   * @param ioEngineName               IO engine name
   * @param capacity                   cache capacity, in bytes
   * @param blockSize                  expected block size, in bytes
   * @param bucketSizes                configured bucket sizes, or {@code null} to use defaults
   * @param writerThreadNum            number of writer threads
   * @param writerQLen                 writer queue length
   * @param persistencePath            persistence path, or {@code null} for non-persistent cache
   * @param ioErrorsTolerationDuration duration for tolerating IO engine errors
   * @param conf                       configuration used by the bucket cache
   * @return test instance
   * @throws IOException          if the bucket cache cannot be created
   * @throws NullPointerException if {@code ioEngineName} or {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<BucketCache> bucketInstance(String ioEngineName,
    long capacity, int blockSize, int[] bucketSizes, int writerThreadNum, int writerQLen,
    String persistencePath, int ioErrorsTolerationDuration, Configuration conf) throws IOException {
    Objects.requireNonNull(ioEngineName, "ioEngineName must not be null");
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(new BucketCache(ioEngineName, capacity, blockSize,
      bucketSizes, writerThreadNum, writerQLen, persistencePath, ioErrorsTolerationDuration, conf));
  }

  /**
   * Creates a {@link CacheAccessService} backed by {@link BucketCache}.
   * @param ioEngineName               IO engine name
   * @param capacity                   cache capacity, in bytes
   * @param blockSize                  expected block size, in bytes
   * @param bucketSizes                configured bucket sizes, or {@code null} to use defaults
   * @param writerThreadNum            number of writer threads
   * @param writerQLen                 writer queue length
   * @param persistencePath            persistence path, or {@code null} for non-persistent cache
   * @param ioErrorsTolerationDuration duration for tolerating IO engine errors
   * @param conf                       configuration used by the bucket cache
   * @param onlineRegions              online regions map used by the bucket cache, or {@code null}
   * @return cache access service backed by {@link BucketCache}
   * @throws IOException          if the bucket cache cannot be created
   * @throws NullPointerException if {@code ioEngineName} or {@code conf} is {@code null}
   */
  public static CacheAccessService bucket(String ioEngineName, long capacity, int blockSize,
    int[] bucketSizes, int writerThreadNum, int writerQLen, String persistencePath,
    int ioErrorsTolerationDuration, Configuration conf, Map<String, HRegion> onlineRegions)
    throws IOException {
    return bucketInstance(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum,
      writerQLen, persistencePath, ioErrorsTolerationDuration, conf, onlineRegions).service();
  }

  /**
   * Creates a test instance containing {@link BucketCache} and its {@link CacheAccessService}
   * facade.
   * @param ioEngineName               IO engine name
   * @param capacity                   cache capacity, in bytes
   * @param blockSize                  expected block size, in bytes
   * @param bucketSizes                configured bucket sizes, or {@code null} to use defaults
   * @param writerThreadNum            number of writer threads
   * @param writerQLen                 writer queue length
   * @param persistencePath            persistence path, or {@code null} for non-persistent cache
   * @param ioErrorsTolerationDuration duration for tolerating IO engine errors
   * @param conf                       configuration used by the bucket cache
   * @param onlineRegions              online regions map used by the bucket cache, or {@code null}
   * @return test instance
   * @throws IOException          if the bucket cache cannot be created
   * @throws NullPointerException if {@code ioEngineName} or {@code conf} is {@code null}
   */
  public static CacheAccessServiceTestInstance<BucketCache> bucketInstance(String ioEngineName,
    long capacity, int blockSize, int[] bucketSizes, int writerThreadNum, int writerQLen,
    String persistencePath, int ioErrorsTolerationDuration, Configuration conf,
    Map<String, HRegion> onlineRegions) throws IOException {
    Objects.requireNonNull(ioEngineName, "ioEngineName must not be null");
    Objects.requireNonNull(conf, "conf must not be null");
    return new CacheAccessServiceTestInstance<>(
      new BucketCache(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
        persistencePath, ioErrorsTolerationDuration, conf, onlineRegions));
  }

  /**
   * Returns the legacy {@link BlockCache} backing the supplied {@link CacheAccessService}.
   * <p>
   * This helper is intended for tests only. It supports transitional test migration where
   * production code is exercised through {@link CacheAccessService}, but the test still needs
   * direct access to the underlying legacy {@link BlockCache} for implementation-specific
   * assertions, cached-block iteration, metrics inspection, or other diagnostic checks.
   * </p>
   * @param cacheAccessService cache access service
   * @return backing legacy block cache
   * @throws NullPointerException     if {@code cacheAccessService} is {@code null}
   * @throws IllegalArgumentException if {@code cacheAccessService} is not backed by a legacy
   *                                  {@link BlockCache}
   */
  public static BlockCache blockCache(CacheAccessService cacheAccessService) {
    Objects.requireNonNull(cacheAccessService, "cacheAccessService must not be null");
    return TopologyBackedCacheAccessServices.getBlockCache(cacheAccessService);
  }

  /**
   * Returns whether the supplied cache access service represents a cache layout that is equivalent
   * to the legacy {@link CombinedBlockCache} L1/L2 organization.
   * <p>
   * During the block cache migration, {@link CombinedBlockCache} may no longer be the object used
   * by HFile readers and writers directly. The active cache access path may instead be a
   * {@link TopologyBackedCacheAccessService} backed by a {@link TieredExclusiveTopology}. That
   * topology models the same combined-cache style of exclusive L1/L2 orchestration where the
   * participating engines correspond to the legacy L1 and L2 block caches.
   * </p>
   * <p>
   * This helper is intentionally based on {@link CacheAccessService}, not on the legacy
   * {@link BlockCache} field, so callers can reason about the active cache access path during the
   * migration away from {@link BlockCache}-centric APIs.
   * </p>
   * @param cacheAccessService cache access service to inspect
   * @return {@code true} when the supplied service represents a CombinedBlockCache-compatible
   *         topology; {@code false} otherwise
   */
  public static boolean isCombinedBlockCacheEquivalent(CacheAccessService cacheAccessService) {
    if (!(cacheAccessService instanceof TopologyBackedCacheAccessService)) {
      return false;
    }

    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) cacheAccessService;
    return topologyBackedService.getTopology().getType() == CacheTopologyType.TIERED_EXCLUSIVE;
  }

  /**
   * Returns the legacy first-level {@link BlockCache} from a topology-backed cache access service.
   * <p>
   * This helper is intended only for tests that need to verify compatibility with the legacy
   * {@code CombinedBlockCache} L1/L2 layout after the runtime path has moved to
   * {@link TopologyBackedCacheAccessService}. Production code should not unwrap
   * {@link CacheAccessService} back to {@link BlockCache}.
   * </p>
   * @param cacheAccessService cache access service to inspect
   * @return first-level block cache
   */
  public static BlockCache getFirstLevelBlockCache(CacheAccessService cacheAccessService) {
    return getBlockCache(cacheAccessService, CacheTier.L1);
  }

  /**
   * Returns the legacy second-level {@link BlockCache} from a topology-backed cache access service.
   * <p>
   * This helper is intended only for tests that need to verify compatibility with the legacy
   * {@code CombinedBlockCache} L1/L2 layout after the runtime path has moved to
   * {@link TopologyBackedCacheAccessService}. Production code should not unwrap
   * {@link CacheAccessService} back to {@link BlockCache}.
   * </p>
   * @param cacheAccessService cache access service to inspect
   * @return second-level block cache
   */
  public static BlockCache getSecondLevelBlockCache(CacheAccessService cacheAccessService) {
    return getBlockCache(cacheAccessService, CacheTier.L2);
  }

  /**
   * Returns the legacy {@link BlockCache} backing the requested topology tier.
   * <p>
   * This method expects the supplied {@link CacheAccessService} to be a
   * {@link TopologyBackedCacheAccessService} and the requested tier to be backed by a
   * {@link BlockCacheBackedCacheEngine}. It is deliberately strict so tests fail clearly when the
   * cache access service is not using the expected CombinedBlockCache-compatible topology-backed
   * wiring.
   * </p>
   * @param cacheAccessService cache access service to inspect
   * @param tier               topology tier to unwrap
   * @return block cache backing the requested tier
   */
  public static BlockCache getBlockCache(CacheAccessService cacheAccessService, CacheTier tier) {
    Objects.requireNonNull(cacheAccessService, "cacheAccessService must not be null");
    Objects.requireNonNull(tier, "tier must not be null");

    if (!(cacheAccessService instanceof TopologyBackedCacheAccessService)) {
      throw new IllegalArgumentException(
        "cacheAccessService must be a TopologyBackedCacheAccessService");
    }

    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) cacheAccessService;
    CacheEngine engine = topologyBackedService.getTopology().getEngine(tier)
      .orElseThrow(() -> new IllegalArgumentException("No cache engine found for tier " + tier));

    if (!(engine instanceof BlockCacheBackedCacheEngine)) {
      throw new IllegalArgumentException(
        "Cache engine for tier " + tier + " must be a BlockCacheBackedCacheEngine");
    }

    return ((BlockCacheBackedCacheEngine) engine).getBlockCache();
  }
}
