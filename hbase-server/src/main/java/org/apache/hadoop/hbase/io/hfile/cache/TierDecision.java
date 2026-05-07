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

import java.util.List;
import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Decision describing the target cache tiers for a cache insertion.
 * <p>
 * This decision explicitly lists the {@link CacheTier}s that should receive the block. It replaces
 * implicit or topology-derived placement rules with a clear, ordered set of target tiers.
 * </p>
 * <p>
 * The order of tiers is significant and should reflect placement priority. For example, a policy
 * may return {@code [L1, L2]} to indicate that the block should be stored in L1 first and also
 * written to L2 (e.g. for inclusive topologies).
 * </p>
 * <p>
 * An empty decision indicates that the block should not be stored in any tier.
 * </p>
 * <p>
 * This class is intentionally simple and immutable. It does not encode topology-specific behavior
 * (e.g. inclusive vs exclusive). The {@link CacheTopology} is responsible for interpreting and
 * executing this decision.
 * </p>
 */
@InterfaceAudience.Private
public final class TierDecision {

  private static final TierDecision NONE = new TierDecision(List.of());

  private final List<CacheTier> tiers;

  private TierDecision(List<CacheTier> tiers) {
    this.tiers = tiers;
  }

  /**
   * Returns a decision that does not place the block into any cache tier.
   * @return empty tier decision
   */
  public static TierDecision none() {
    return NONE;
  }

  /**
   * Returns a decision targeting a single cache tier.
   * @param tier target tier
   * @return tier decision for a single tier
   */
  public static TierDecision single(CacheTier tier) {
    Objects.requireNonNull(tier, "tier must not be null");
    return new TierDecision(List.of(tier));
  }

  /**
   * Returns a decision targeting multiple cache tiers.
   * <p>
   * The provided list must not be null, must not contain null elements, and will be copied to
   * preserve immutability.
   * </p>
   * @param tiers ordered list of target tiers
   * @return tier decision for multiple tiers, or {@link #none()} if empty
   */
  public static TierDecision multiple(List<CacheTier> tiers) {
    Objects.requireNonNull(tiers, "tiers must not be null");

    if (tiers.isEmpty()) {
      return NONE;
    }

    for (CacheTier tier : tiers) {
      Objects.requireNonNull(tier, "tier in tiers must not be null");
    }

    return new TierDecision(List.copyOf(tiers));
  }

  /**
   * Returns the ordered list of target tiers.
   * <p>
   * The returned list is immutable. The order reflects placement priority and may be interpreted by
   * the topology when executing the decision.
   * </p>
   * @return ordered list of target tiers
   */
  public List<CacheTier> getTiers() {
    return tiers;
  }

  /**
   * Returns whether this decision contains no target tiers.
   * @return true when no tiers are selected
   */
  public boolean isEmpty() {
    return tiers.isEmpty();
  }
}
