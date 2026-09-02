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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
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
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.FirstLevelBlockCache;
import org.apache.hadoop.hbase.io.hfile.InclusiveCombinedBlockCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestInclusiveCombinedBlockCacheCompatibleTopologyBackedCacheAccessService {

  /**
   * Verifies that {@link InclusiveCombinedBlockCache} is routed to a topology-backed cache access
   * service using {@link CacheTopologyType#TIERED_INCLUSIVE}.
   * <p>
   * This protects against accidentally routing {@link InclusiveCombinedBlockCache} through the
   * exclusive combined-cache topology path. Inclusive and exclusive combined caches have different
   * residency, promotion, and eviction semantics, so they must be represented by different topology
   * types.
   * </p>
   */
  @Test
  void testInclusiveCombinedBlockCacheUsesTieredInclusiveTopology() {
    InclusiveCombinedBlockCache combinedBlockCache = mock(InclusiveCombinedBlockCache.class);
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    when(combinedBlockCache.getBlockCaches()).thenReturn(new BlockCache[] { l1, l2 });

    CacheAccessService service = CacheAccessServices.fromBlockCache(combinedBlockCache);

    assertTrue(service instanceof TopologyBackedCacheAccessService);
    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) service;
    assertEquals(CacheTopologyType.TIERED_INCLUSIVE, topologyBackedService.getTopology().getType());
  }

  /**
   * Verifies that inclusive topology lookup checks L1 first and returns the L1 block when it is
   * present.
   * <p>
   * Inclusive caches prefer the first-level cache for reads. If a block is found in L1, the
   * topology-backed service should return it without consulting L2.
   * </p>
   */
  @Test
  void testInclusiveL1HitReturnsBlockWithoutCheckingL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).getBlock(key, true, false, true);
    verify(l2, never()).getBlock(any(), anyBoolean(), anyBoolean(), anyBoolean());
  }

  /**
   * Verifies that inclusive topology lookup checks L2 after an L1 miss.
   * <p>
   * Unlike the exclusive combined-cache compatibility path, inclusive lookup does not need the L1
   * membership shortcut. A normal ordered tier scan is appropriate: L1 is checked first, and L2 is
   * checked only if L1 does not contain the block.
   * </p>
   */
  @Test
  void testInclusiveL2HitReturnsBlockAfterL1Miss() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).getBlock(key, true, false, true);
    verify(l2).getBlock(key, true, false, true);
  }

  /**
   * Verifies that promotion in an inclusive topology copies a block to L1 without evicting it from
   * L2.
   * <p>
   * This is the key semantic difference from the exclusive topology. In an exclusive topology,
   * promotion moves the block from L2 to L1 and removes the L2 copy. In an inclusive topology, the
   * block may remain resident in both tiers.
   * </p>
   */
  @Test
  void testInclusivePromotionCopiesBlockToL1WithoutEvictingL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, promoteL2HitToL1Policy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).getBlock(key, true, false, true);
    verify(l2).getBlock(key, true, false, true);
    verify(l1).cacheBlock(key, block);
    verify(l2, never()).evictBlock(key);
  }

  /**
   * Verifies that service-level eviction for an inclusive topology evicts from all tiers.
   * <p>
   * An inclusive cache may contain the same block in both L1 and L2. Evicting only the first
   * matching tier could leave another resident copy behind, so the topology-backed service must ask
   * both tiers to evict the key.
   * </p>
   */
  @Test
  void testInclusiveEvictBlockEvictsFromBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    when(l1.evictBlock(key)).thenReturn(true);
    when(l2.evictBlock(key)).thenReturn(true);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertTrue(service.evictBlock(key));

    verify(l1).evictBlock(key);
    verify(l2).evictBlock(key);
  }

  /**
   * Verifies that an inclusive topology can cache a block into both tiers when the placement policy
   * selects both L1 and L2.
   * <p>
   * This covers the inclusive cache residency model where the same block can be intentionally
   * present in multiple tiers.
   * </p>
   */
  @Test
  void testInclusiveCacheBlockToBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service =
      service(l1, l2, admitToTiersPolicy(CacheTier.L1, CacheTier.L2));

    service.cacheBlock(key, block, writeContext());

    verify(l1).cacheBlock(key, block, false, false);
    verify(l2).cacheBlock(key, block, false, false);
  }

  /**
   * Verifies that cached-block iteration is aggregated across both tiers for inclusive topology.
   * <p>
   * Diagnostic code and compatibility tests use
   * {@link CacheAccessServices#asCachedBlockIterable(CacheAccessService)} to enumerate cached
   * blocks through the active cache access service. Since an inclusive topology has multiple
   * backing engines, the service must expose cached blocks from both L1 and L2.
   * </p>
   */
  @Test
  void testInclusiveCachedBlockIterableAggregatesBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    CachedBlock l1Block = mock(CachedBlock.class);
    CachedBlock l2Block = mock(CachedBlock.class);

    when(l1.iterator()).thenReturn(Arrays.asList(l1Block).iterator());
    when(l2.iterator()).thenReturn(Arrays.asList(l2Block).iterator());

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    Optional<Iterable<CachedBlock>> iterable = CacheAccessServices.asCachedBlockIterable(service);

    assertTrue(iterable.isPresent());
    assertEquals(Arrays.asList(l1Block, l2Block), toList(iterable.get()));
  }

  /**
   * Verifies that shutting down an inclusive topology-backed service shuts down both cache tiers.
   * <p>
   * The topology-backed service owns the topology-level lifecycle. For a two-tier inclusive
   * topology, shutdown should be delegated to both L1 and L2 engines.
   * </p>
   */
  @Test
  void testInclusiveShutdownShutsDownBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    service.shutdown();

    verify(l1).shutdown();
    verify(l2).shutdown();
  }

  /**
   * Verifies that a real {@link InclusiveCombinedBlockCache} is adapted so topology-backed lookup
   * controls L1 and L2 access explicitly.
   * <p>
   * The legacy inclusive combined-cache constructor wires the first-level cache to the second-level
   * cache through the victim-cache path. When the cache is adapted to the topology-backed model,
   * that legacy victim wiring must be removed so an L1 miss does not internally delegate to L2.
   * </p>
   * <p>
   * After the wiring is removed, the topology-backed service should perform a normal inclusive
   * lookup: query L1 first, observe the miss, then query L2 explicitly.
   * </p>
   */
  @Test
  void testRealInclusiveCombinedCacheDoesNotDelegateL1MissThroughVictimCache() {
    FirstLevelBlockCache l1 = mock(FirstLevelBlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    InclusiveCombinedBlockCache combinedBlockCache = new InclusiveCombinedBlockCache(l1, l2);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(block);

    CacheAccessService service = CacheAccessServices.fromBlockCache(combinedBlockCache);

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).unsetVictimCache();
    verify(l1).getBlock(key, true, false, true);
    verify(l2).getBlock(key, true, false, true);
  }

  /**
   * Verifies that the default placement policy writes inclusive topology blocks to both tiers.
   * <p>
   * Inclusive topology allows the same block to be resident in L1 and L2, so default placement
   * should not split blocks by data or metadata type the way the exclusive combined-cache path
   * does.
   * </p>
   */
  @Test
  void testDefaultPolicyWritesInclusiveBlocksToBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service =
      service(l1, l2, DefaultHBaseCachePlacementAdmissionPolicy.INSTANCE);

    service.cacheBlock(key, block, writeContext());

    verify(l1).cacheBlock(key, block, false, false);
    verify(l2).cacheBlock(key, block, false, false);
  }

  /**
   * Verifies that inclusive topology construction disables legacy L1 victim-cache delegation.
   * <p>
   * Inclusive combined-cache construction may wire L1 to L2 before the cache is adapted to the
   * topology-backed model. The topology-backed inclusive service needs L1 and L2 as independent
   * engines so L2 lookup and promotion policy are applied by the topology.
   * </p>
   */
  @Test
  void testTieredInclusiveFactoryUnsetsL1VictimCache() {
    FirstLevelBlockCache l1 = mock(FirstLevelBlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    TopologyBackedCacheAccessServices.fromTieredInclusiveBlockCaches("inclusive-combined", l1, l2,
      noPromotionPolicy());

    verify(l1).unsetVictimCache();
  }

  /**
   * Creates a topology-backed cache access service using a tiered inclusive topology.
   * @param l1     first-level block cache
   * @param l2     second-level block cache
   * @param policy cache placement and admission policy
   * @return topology-backed cache access service using the inclusive topology path
   */
  private static TopologyBackedCacheAccessService service(BlockCache l1, BlockCache l2,
    CachePlacementAdmissionPolicy policy) {
    return TopologyBackedCacheAccessServices.fromTieredInclusiveBlockCaches("inclusive-combined",
      l1, l2, policy);
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
   * the existing compatibility tests.
   * </p>
   * @return cache write context for cache population tests
   */
  private static CacheWriteContext writeContext() {
    return CacheWriteContext.newBuilder().withInMemory(false).withWaitWhenCache(false).build();
  }

  /**
   * Creates a placement policy that never promotes a block after a cache hit.
   * <p>
   * This policy is useful for lookup tests that need to verify only the lookup order and returned
   * block without introducing promotion side effects.
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
   * Creates a placement policy that promotes an L2 hit to L1.
   * <p>
   * The policy returns no promotion for L1 hits and requests promotion to L1 for L2 hits. In an
   * inclusive topology, this should copy the block into L1 without evicting it from L2.
   * </p>
   * @return cache placement and admission policy that promotes L2 hits to L1
   */
  private static CachePlacementAdmissionPolicy promoteL2HitToL1Policy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldPromote(any(), any(), eq(CacheTier.L1), any(), any()))
      .thenReturn(PromotionDecision.none());
    when(policy.shouldPromote(any(), any(), eq(CacheTier.L2), any(), any()))
      .thenReturn(PromotionDecision.promoteTo(CacheTier.L1, false));
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
   * Copies an iterable of cached blocks into a list.
   * <p>
   * The helper makes cached-block iterable assertions deterministic and easy to compare with the
   * expected tier order.
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
