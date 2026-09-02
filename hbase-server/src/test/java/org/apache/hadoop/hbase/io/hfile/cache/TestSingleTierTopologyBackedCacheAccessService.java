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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestSingleTierTopologyBackedCacheAccessService {

  /**
   * Verifies that a plain single-tier {@link BlockCache} is adapted to a topology-backed cache
   * access service.
   * <p>
   * HBASE-30329 routes legacy block caches through {@link TopologyBackedCacheAccessService}. A
   * plain non-combined block cache should be represented by {@link SingleTierTopology}, not by the
   * old {@link BlockCacheBackedCacheAccessService} runtime path.
   * </p>
   */
  @Test
  void testSingleTierBlockCacheUsesTopologyBackedAccessService() {
    BlockCache blockCache = mock(BlockCache.class);

    CacheAccessService service = CacheAccessServices.fromBlockCache(blockCache);

    assertTrue(service instanceof TopologyBackedCacheAccessService);
    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) service;
    assertEquals(CacheTopologyType.SINGLE_TIER, topologyBackedService.getTopology().getType());
  }

  /**
   * Verifies that a single-tier topology exposes the expected topology metadata.
   * <p>
   * The topology should contain exactly one engine, expose only the SINGLE tier, and return the
   * same engine for {@link CacheTier#SINGLE}. Other tiers should not resolve to an engine.
   * </p>
   */
  @Test
  void testSingleTierTopologyMetadata() {
    CacheEngine engine = mock(CacheEngine.class);
    SingleTierTopology topology = new SingleTierTopology("single", engine);

    assertEquals("single", topology.getName());
    assertEquals(CacheTopologyType.SINGLE_TIER, topology.getType());
    assertEquals(Arrays.asList(engine), topology.getEngines());
    assertEquals(Arrays.asList(CacheTier.SINGLE), topology.getTiers());
    assertSame(engine, topology.getEngine(CacheTier.SINGLE).orElseThrow());
    assertFalse(topology.getEngine(CacheTier.L1).isPresent());
    assertFalse(topology.getEngine(CacheTier.L2).isPresent());
  }

  /**
   * Verifies that a single-tier topology-backed service reads blocks from the single backing cache.
   * <p>
   * Since there is only one tier, the access service should delegate the read to the L1 engine and
   * return the block supplied by the wrapped block cache.
   * </p>
   */
  @Test
  void testGetBlockDelegatesToSingleTier() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(blockCache.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(blockCache, noPromotionPolicy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(blockCache).getBlock(key, true, false, true);
  }

  /**
   * Verifies that a single-tier topology-backed service returns {@code null} when the backing cache
   * misses.
   * <p>
   * The service should not attempt any tier fallback because the topology contains only one cache
   * engine.
   * </p>
   */
  @Test
  void testGetBlockReturnsNullOnMiss() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    when(blockCache.getBlock(key, true, false, true)).thenReturn(null);

    TopologyBackedCacheAccessService service = service(blockCache, noPromotionPolicy());

    assertSame(null, service.getBlock(key, requestContext()));

    verify(blockCache).getBlock(key, true, false, true);
  }

  /**
   * Verifies that cache population for a single-tier topology writes to the single backing cache.
   * <p>
   * The placement policy selects L1. Since single-tier topology exposes only L1, the service should
   * delegate the cache write to the wrapped block cache.
   * </p>
   */
  @Test
  void testCacheBlockDelegatesToSingleTier() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service =
      service(blockCache, admitToTiersPolicy(CacheTier.SINGLE));

    service.cacheBlock(key, block, writeContext());

    verify(blockCache).cacheBlock(key, block, false, false);
  }

  /**
   * Verifies that a rejected block is not written to the single backing cache.
   * <p>
   * When the placement and admission policy rejects the write, the topology-backed service should
   * not call any {@code cacheBlock} overload on the wrapped block cache.
   * </p>
   */
  @Test
  void testRejectedBlockIsNotCached() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(blockCache, rejectPolicy());

    service.cacheBlock(key, block, writeContext());

    verify(blockCache, never()).cacheBlock(any(), any());
    verify(blockCache, never()).cacheBlock(any(), any(), anyBoolean(), anyBoolean());
  }

  /**
   * Verifies that service-level eviction for a single-tier topology delegates to the backing cache.
   * <p>
   * A single-tier topology has only one possible resident tier, so eviction should be a direct
   * delegation to that tier.
   * </p>
   */
  @Test
  void testEvictBlockDelegatesToSingleTier() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    when(blockCache.evictBlock(key)).thenReturn(true);

    TopologyBackedCacheAccessService service = service(blockCache, noPromotionPolicy());

    assertTrue(service.evictBlock(key));

    verify(blockCache).evictBlock(key);
  }

  /**
   * Verifies that single-tier cached-block iteration is exposed through the topology-backed access
   * service.
   * <p>
   * Diagnostic callers use {@link CacheAccessServices#asCachedBlockIterable(CacheAccessService)}
   * rather than unwrapping the legacy block cache. The single-tier topology-backed path should
   * expose the same cached blocks as the wrapped block cache.
   * </p>
   */
  @Test
  void testCachedBlockIterableDelegatesToSingleTier() {
    BlockCache blockCache = mock(BlockCache.class);
    CachedBlock firstBlock = mock(CachedBlock.class);
    CachedBlock secondBlock = mock(CachedBlock.class);

    when(blockCache.iterator()).thenReturn(Arrays.asList(firstBlock, secondBlock).iterator());

    TopologyBackedCacheAccessService service = service(blockCache, noPromotionPolicy());

    Optional<Iterable<CachedBlock>> iterable = CacheAccessServices.asCachedBlockIterable(service);

    assertTrue(iterable.isPresent());
    assertEquals(Arrays.asList(firstBlock, secondBlock), toList(iterable.get()));
  }

  /**
   * Verifies that single-tier topology statistics are delegated to the backing engine.
   * <p>
   * The topology itself does not aggregate multiple tiers, so its statistics should be exactly the
   * statistics exposed by the single cache engine.
   * </p>
   */
  @Test
  void testTopologyStatsDelegatesToSingleEngine() {
    CacheEngine engine = mock(CacheEngine.class);
    CacheStats stats = mock(CacheStats.class);

    when(engine.getStats()).thenReturn(stats);

    SingleTierTopology topology = new SingleTierTopology("single", engine);

    assertSame(stats, topology.getStats());
  }

  /**
   * Verifies that promotion is not supported by a single-tier topology.
   * <p>
   * Promotion requires a source tier and a different target tier. Since this topology has only one
   * tier, promotion is a no-op and should return {@code false}.
   * </p>
   */
  @Test
  void testSingleTierTopologyDoesNotPromote() {
    CacheEngine engine = mock(CacheEngine.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);
    SingleTierTopology topology = new SingleTierTopology("single", engine);

    assertFalse(topology.promote(key, block, engine, engine));

    verify(engine, never()).cacheBlock(any(), any());
    verify(engine, never()).evictBlock(any());
  }

  /**
   * Verifies that demotion is not supported by a single-tier topology.
   * <p>
   * Demotion requires a source tier and a different lower target tier. Since this topology has only
   * one tier, demotion is a no-op and should return {@code false}.
   * </p>
   */
  @Test
  void testSingleTierTopologyDoesNotDemote() {
    CacheEngine engine = mock(CacheEngine.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);
    SingleTierTopology topology = new SingleTierTopology("single", engine);

    assertFalse(topology.demote(key, block, engine, engine));

    verify(engine, never()).cacheBlock(any(), any());
    verify(engine, never()).evictBlock(any());
  }

  /**
   * Verifies that shutting down a single-tier topology-backed service shuts down the backing cache.
   * <p>
   * The topology-backed service owns the topology lifecycle. For a single-tier topology, shutdown
   * should be delegated to the only backing engine.
   * </p>
   */
  @Test
  void testShutdownDelegatesToSingleTier() {
    BlockCache blockCache = mock(BlockCache.class);

    TopologyBackedCacheAccessService service = service(blockCache, noPromotionPolicy());

    service.shutdown();

    verify(blockCache).shutdown();
  }

  /**
   * Verifies that single-tier topology-backed cache access preserves legacy current-size reporting.
   * <p>
   * The old {@link BlockCacheBackedCacheAccessService} path delegated
   * {@link CacheAccessService#getCurrentSize()} to {@link BlockCache#getCurrentSize()}. After
   * routing plain block caches through {@link TopologyBackedCacheAccessService}, the same value
   * must still be reported through the cache access service. This is intentionally different from
   * {@link CacheAccessService#getCurrentDataSize()} because some cache implementations include
   * metadata or allocator overhead in their current size.
   * </p>
   */
  @Test
  void testSingleTierCurrentSizeDelegatesToBlockCacheCurrentSize() {
    BlockCache blockCache = mock(BlockCache.class);

    when(blockCache.getCurrentSize()).thenReturn(1234L);
    when(blockCache.getCurrentDataSize()).thenReturn(1000L);

    TopologyBackedCacheAccessService service = service(blockCache, noPromotionPolicy());

    assertEquals(1234L, service.getCurrentSize());
    assertEquals(1000L, service.getCurrentDataSize());
  }

  /**
   * Creates a topology-backed cache access service using a single-tier topology.
   * @param blockCache legacy block cache backing the single tier
   * @param policy     cache placement and admission policy
   * @return topology-backed cache access service using a single-tier topology
   */
  private static TopologyBackedCacheAccessService service(BlockCache blockCache,
    CachePlacementAdmissionPolicy policy) {
    return TopologyBackedCacheAccessServices.fromSingleBlockCache("single", blockCache, policy);
  }

  /**
   * Creates a cache request context used by read-path tests.
   * <p>
   * The returned context enables caching, marks the request as non-repeat, and asks the cache to
   * update cache metrics. These values match the read-path behavior covered by the topology-backed
   * cache access service tests.
   * </p>
   * @return cache request context for read-path tests
   */
  private static CacheRequestContext requestContext() {
    return CacheRequestContext.newBuilder().withCaching(true).withRepeat(false)
      .withUpdateCacheMetrics(true).build();
  }

  /**
   * Creates a cache write context used by cache population tests.
   * <p>
   * The returned context uses the default non-in-memory and non-blocking write behavior expected by
   * the existing topology-backed cache access service tests.
   * </p>
   * @return cache write context for cache population tests
   */
  private static CacheWriteContext writeContext() {
    return CacheWriteContext.newBuilder().withInMemory(false).withWaitWhenCache(false).build();
  }

  /**
   * Creates a placement policy that never promotes a block after a cache hit.
   * <p>
   * This policy is useful for lookup tests that need to verify only lookup delegation and returned
   * block behavior without introducing promotion side effects.
   * </p>
   * @return cache placement and admission policy that disables promotion
   */
  private static CachePlacementAdmissionPolicy noPromotionPolicy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldPromote(any(), any(), any(), any(), any()))
      .thenReturn(PromotionDecision.none());
    return policy;
  }

  /**
   * Creates a placement policy that admits writes to the supplied tiers.
   * <p>
   * The returned policy admits every block and returns a multi-tier placement decision containing
   * the tiers supplied by the caller.
   * </p>
   * @param tiers cache tiers selected by the policy
   * @return cache placement and admission policy that writes to the supplied tiers
   */
  private static CachePlacementAdmissionPolicy admitToTiersPolicy(CacheTier... tiers) {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldAdmit(any(), any(), any(), any(), any()))
      .thenReturn(AdmissionDecision.admit());
    when(policy.selectTier(any(), any(), any(), any()))
      .thenReturn(TierDecision.multiple(Arrays.asList(tiers)));
    return policy;
  }

  /**
   * Creates a placement policy that rejects every block.
   * <p>
   * The returned policy is used to verify that rejected cache writes are not delegated to the
   * backing cache.
   * </p>
   * @return cache placement and admission policy that rejects every block
   */
  private static CachePlacementAdmissionPolicy rejectPolicy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldAdmit(any(), any(), any(), any(), any()))
      .thenReturn(AdmissionDecision.reject("test rejection"));
    return policy;
  }

  /**
   * Copies an iterable of cached blocks into a list.
   * <p>
   * The helper makes cached-block iterable assertions deterministic and easy to compare with the
   * expected order.
   * </p>
   * @param iterable cached-block iterable to copy
   * @return list containing all cached blocks produced by the iterable
   */
  private static List<CachedBlock> toList(Iterable<CachedBlock> iterable) {
    List<CachedBlock> blocks = new ArrayList<>();
    for (CachedBlock block : iterable) {
      blocks.add(block);
    }
    return blocks;
  }
}
