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

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Cache placement and admission policy that preserves the admission behavior of the legacy
 * {@code IndexOnlyLruBlockCache}.
 * <p>
 * Despite its historical name, {@code IndexOnlyLruBlockCache} admits all non-data blocks. This
 * policy preserves that behavior while delegating placement, representation, and promotion
 * decisions to {@link DefaultHBaseCachePlacementAdmissionPolicy}.
 * </p>
 */
@InterfaceAudience.Private
public class IndexOnlyCachePlacementAdmissionPolicy
  extends DefaultHBaseCachePlacementAdmissionPolicy {

  public static final IndexOnlyCachePlacementAdmissionPolicy INSTANCE =
    new IndexOnlyCachePlacementAdmissionPolicy();

  /**
   * Admits non-data blocks and rejects data blocks.
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      write context describing insertion source and hints
   * @param priority     admission priority hint
   * @param topologyView read-only topology view
   * @return admission decision
   */
  @Override
  public AdmissionDecision shouldAdmit(BlockCacheKey cacheKey, Cacheable block,
    CacheWriteContext context, AdmissionPriority priority, CacheTopologyView topologyView) {
    super.shouldAdmit(cacheKey, block, context, priority, topologyView);

    BlockType blockType = block.getBlockType();
    return blockType != null && blockType.getCategory() != BlockType.BlockCategory.DATA
      ? AdmissionDecision.admit()
      : AdmissionDecision.reject("IndexOnlyLRU admits only non-data blocks");
  }
}
