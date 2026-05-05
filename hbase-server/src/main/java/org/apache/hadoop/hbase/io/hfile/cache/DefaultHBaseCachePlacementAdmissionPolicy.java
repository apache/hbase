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
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Default compatibility policy that preserves current HBase block cache behavior.
 * <p>
 * This policy is intentionally conservative. It admits cache insertion requests by default, assumes
 * existing {@code CacheConfig} logic has already decided whether cache population should be
 * attempted, preserves the current HBase block representation, and maps metadata-like blocks to L1
 * and data blocks to L2 when a tiered topology is available.
 * </p>
 */
@InterfaceAudience.Private
public class DefaultHBaseCachePlacementAdmissionPolicy implements CachePlacementAdmissionPolicy {

  @Override
  public AdmissionDecision shouldAdmit(BlockCacheKey cacheKey, Cacheable block,
    CacheWriteContext context, AdmissionPriority priority, CacheTopologyView topologyView) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(priority, "priority must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");

    return AdmissionDecision.admit();
  }

  @Override
  public TierDecision selectTier(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context,
    CacheTopologyView topologyView) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");
    /*
     * Default compatibility placement prefers metadata/index/bloom blocks in L1 and data blocks in
     * L2 when both tiers are available, but falls back to any available tier rather than rejecting
     * placement.
     */
    if (topologyView.getType() == CacheTopologyType.SINGLE) {
      return TierDecision.single(CacheTier.SINGLE);
    }

    if (isMetaOrIndexBlock(block)) {
      if (topologyView.getEngine(CacheTier.L1).isPresent()) {
        return TierDecision.single(CacheTier.L1);
      }
      if (topologyView.getEngine(CacheTier.L2).isPresent()) {
        return TierDecision.single(CacheTier.L2);
      }
      return TierDecision.none();
    }

    if (topologyView.getEngine(CacheTier.L2).isPresent()) {
      return TierDecision.single(CacheTier.L2);
    }
    if (topologyView.getEngine(CacheTier.L1).isPresent()) {
      return TierDecision.single(CacheTier.L1);
    }

    return TierDecision.none();
  }

  @Override
  public RepresentationDecision selectRepresentation(BlockCacheKey cacheKey, Cacheable block,
    CacheWriteContext context, CacheTopologyView topologyView) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");

    return RepresentationDecision.currentHBaseDefault();
  }

  @Override
  public PromotionDecision shouldPromote(BlockCacheKey cacheKey, Cacheable block,
    CacheTier sourceTier, CacheRequestContext context, CacheTopologyView topologyView) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(sourceTier, "sourceTier must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");
    /*
     * DefaultHBaseCachePlacementAdmissionPolicy should preserve existing placement behavior. If
     * meta/index/bloom blocks belong in L1, they should be inserted into L1 when cached. If data
     * blocks belong in L2, promoting them to L1 on hit would change current behavior and may
     * pollute L1. Therefore the compatibility policy should not promote by default.
     */
    return PromotionDecision.none();

  }

  private static boolean isMetaOrIndexBlock(Cacheable block) {
    if (!(block instanceof HFileBlock)) {
      return false;
    }

    BlockType blockType = ((HFileBlock) block).getBlockType();
    return blockType != null && blockType.getCategory() != BlockType.BlockCategory.DATA;
  }
}
