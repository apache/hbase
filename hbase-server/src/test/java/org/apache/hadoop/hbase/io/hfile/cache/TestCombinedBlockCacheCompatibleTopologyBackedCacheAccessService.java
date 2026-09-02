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

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.FirstLevelBlockCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestCombinedBlockCacheCompatibleTopologyBackedCacheAccessService {

  @Test
  void testL1HitReturnsBlockWithoutCheckingL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.isAlreadyCached(key)).thenReturn(Optional.of(true));
    when(l1.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).isAlreadyCached(key);
    verify(l1).getBlock(key, true, false, true);
    verify(l2, never()).getBlock(any(), anyBoolean(), anyBoolean(), anyBoolean());
  }

  @Test
  void testL2HitReturnsBlock() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);
    when(l1.isAlreadyCached(key)).thenReturn(Optional.of(false));
    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertSame(block, service.getBlock(key, requestContext()));
    verify(l1).isAlreadyCached(key);
    verify(l2).getBlock(key, true, false, true);
  }

  @Test
  void testMissReturnsNull() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    when(l1.isAlreadyCached(key)).thenReturn(Optional.of(false));
    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(null);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertNull(service.getBlock(key, requestContext()));
    verify(l1).isAlreadyCached(key);
    verify(l2).getBlock(key, true, false, true);
  }

  @Test
  void testL2HitWithPromotionMovesBlockToL1() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);
    when(l1.isAlreadyCached(key)).thenReturn(Optional.of(false));
    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, promoteL2HitToL1Policy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).isAlreadyCached(key);
    verify(l2).getBlock(key, true, false, true);
    assertCachedExactlyOnce(l1, key, block);
    verify(l2).evictBlock(key);
    assertNotCached(l2);
  }

  @Test
  void testRejectedBlockIsNotCached() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(l1, l2, rejectPolicy());

    service.cacheBlock(key, block, writeContext());

    verify(l1, never()).cacheBlock(any(), any());
    verify(l1, never()).cacheBlock(any(), any(), anyBoolean(), anyBoolean());
    verify(l2, never()).cacheBlock(any(), any());
    verify(l2, never()).cacheBlock(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testCacheBlockToL1() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(l1, l2, admitToTiersPolicy(CacheTier.L1));

    service.cacheBlock(key, block, writeContext());

    verify(l1).cacheBlock(key, block, false, false);
    verify(l2, never()).cacheBlock(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testCacheBlockToL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(l1, l2, admitToTiersPolicy(CacheTier.L2));

    service.cacheBlock(key, block, writeContext());

    verify(l2).cacheBlock(key, block, false, false);
    verify(l1, never()).cacheBlock(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testCacheBlockToBothTiers() {
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

  @Test
  void testEvictBlockEvictsFromBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    service.evictBlock(key);

    verify(l1).evictBlock(key);
    verify(l2).evictBlock(key);
  }

  @Test
  void testShutdownShutsDownBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    service.shutdown();

    verify(l1).shutdown();
    verify(l2).shutdown();
  }

  /**
   * Verifies that topology-backed current-size reporting aggregates current size across tiers.
   * <p>
   * Current size includes implementation-specific overhead and is distinct from current data size.
   * The topology-backed service should therefore aggregate {@link CacheEngine#getCurrentSize()}
   * from all engines rather than using data-size counters.
   * </p>
   */
  @Test
  void testCurrentSizeAggregatesAllTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    when(l1.getCurrentSize()).thenReturn(100L);
    when(l2.getCurrentSize()).thenReturn(200L);
    when(l1.getCurrentDataSize()).thenReturn(10L);
    when(l2.getCurrentDataSize()).thenReturn(20L);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertEquals(300L, service.getCurrentSize());
    assertEquals(30L, service.getCurrentDataSize());
  }

  /**
   * Verifies that tiered topology construction disables legacy L1 victim-cache delegation.
   * <p>
   * Legacy combined-cache construction wires the first-level cache to the second-level cache
   * through a victim cache. Once the caches are adapted as independent topology engines, the
   * topology-backed service must control L2 lookup directly. The factory therefore removes the
   * legacy victim-cache wiring before adapting L1.
   * </p>
   */
  @Test
  void testTieredExclusiveFactoryUnsetsL1VictimCache() {
    FirstLevelBlockCache l1 = mock(FirstLevelBlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    TopologyBackedCacheAccessServices.fromTieredExclusiveBlockCaches("combined", l1, l2,
      noPromotionPolicy());

    verify(l1).unsetVictimCache();
  }

  private static TopologyBackedCacheAccessService service(BlockCache l1, BlockCache l2,
    CachePlacementAdmissionPolicy policy) {
    return TopologyBackedCacheAccessServices.fromTieredExclusiveBlockCaches("combined", l1, l2,
      policy);
  }

  private static CacheRequestContext requestContext() {
    return CacheRequestContext.newBuilder().withCaching(true).withRepeat(false)
      .withUpdateCacheMetrics(true).build();
  }

  private static CacheWriteContext writeContext() {
    return CacheWriteContext.newBuilder().withInMemory(false).withWaitWhenCache(false).build();
  }

  private static CachePlacementAdmissionPolicy noPromotionPolicy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldPromote(any(), any(), any(), any(), any()))
      .thenReturn(PromotionDecision.none());
    return policy;
  }

  private static CachePlacementAdmissionPolicy promoteL2HitToL1Policy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldPromote(any(), any(), eq(CacheTier.L1), any(), any()))
      .thenReturn(PromotionDecision.none());
    when(policy.shouldPromote(any(), any(), eq(CacheTier.L2), any(), any()))
      .thenReturn(PromotionDecision.promoteTo(CacheTier.L1, false));
    return policy;
  }

  private static CachePlacementAdmissionPolicy admitToTiersPolicy(CacheTier... tiers) {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldAdmit(any(), any(), any(), any(), any()))
      .thenReturn(AdmissionDecision.admit());
    when(policy.selectTier(any(), any(), any(), any()))
      .thenReturn(TierDecision.multiple(Arrays.asList(tiers)));
    return policy;
  }

  private static CachePlacementAdmissionPolicy rejectPolicy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldAdmit(any(), any(), any(), any(), any()))
      .thenReturn(AdmissionDecision.reject("test rejection"));
    return policy;
  }

  private static void assertCachedExactlyOnce(BlockCache cache, BlockCacheKey key,
    Cacheable block) {
    long cacheBlockCalls = mockingDetails(cache).getInvocations().stream()
      .filter(invocation -> isCacheBlockInvocation(invocation, key, block)).count();

    assertEquals(1, cacheBlockCalls);
  }

  private static void assertNotCached(BlockCache cache) {
    long cacheBlockCalls = mockingDetails(cache).getInvocations().stream()
      .filter(invocation -> "cacheBlock".equals(invocation.getMethod().getName())).count();

    assertEquals(0, cacheBlockCalls);
  }

  private static boolean isCacheBlockInvocation(Invocation invocation, BlockCacheKey key,
    Cacheable block) {
    if (!"cacheBlock".equals(invocation.getMethod().getName())) {
      return false;
    }

    Object[] arguments = invocation.getArguments();
    return arguments.length >= 2 && arguments[0] == key && arguments[1] == block;
  }
}
