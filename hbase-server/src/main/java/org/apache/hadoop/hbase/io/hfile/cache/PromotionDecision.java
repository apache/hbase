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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Decision describing whether and how a cache hit should trigger promotion.
 * <p>
 * This decision is produced by {@link CachePlacementAdmissionPolicy} and executed by
 * {@link CacheTopology}. The policy decides whether promotion should happen and which tier should
 * receive the promoted block. The topology decides how promotion is performed for its semantics,
 * for example copy for inclusive topology or move for exclusive topology.
 * </p>
 * <p>
 * This class is immutable. The no-promotion decision is represented by {@link #none()}. Promotion
 * decisions must include a non-null target tier.
 * </p>
 */
@InterfaceAudience.Private
public final class PromotionDecision {

  private static final PromotionDecision NONE =
    new PromotionDecision(PromotionAction.NONE, null, false);

  private final PromotionAction action;
  private final CacheTier targetTier;
  private final boolean asynchronous;

  private PromotionDecision(PromotionAction action, CacheTier targetTier, boolean asynchronous) {
    this.action = Objects.requireNonNull(action, "action must not be null");
    this.targetTier = targetTier;
    this.asynchronous = asynchronous;
  }

  /**
   * Returns a decision that performs no promotion.
   * @return no-promotion decision
   */
  public static PromotionDecision none() {
    return NONE;
  }

  /**
   * Returns a decision to promote the block to the specified target tier.
   * <p>
   * The target tier must not be null. The actual promotion mechanics are topology-specific. For
   * example, an inclusive topology may copy the block into the target tier while an exclusive
   * topology may move the block.
   * </p>
   * @param targetTier   target tier for promotion
   * @param asynchronous whether promotion may be performed asynchronously
   * @return promotion decision
   */
  public static PromotionDecision promoteTo(CacheTier targetTier, boolean asynchronous) {
    Objects.requireNonNull(targetTier, "targetTier must not be null");
    return new PromotionDecision(PromotionAction.PROMOTE, targetTier, asynchronous);
  }

  /**
   * Returns the promotion action.
   * @return promotion action
   */
  public PromotionAction getAction() {
    return action;
  }

  /**
   * Returns whether this decision requests promotion.
   * @return true when promotion is requested
   */
  public boolean shouldPromote() {
    return action == PromotionAction.PROMOTE;
  }

  /**
   * Returns whether this decision has a target tier.
   * @return true when target tier is available
   */
  public boolean hasTargetTier() {
    return targetTier != null;
  }

  /**
   * Returns the target tier for promotion.
   * <p>
   * This method is valid only when {@link #shouldPromote()} returns true.
   * </p>
   * @return target tier
   * @throws IllegalStateException if this decision does not request promotion
   */
  public CacheTier getTargetTier() {
    if (targetTier == null) {
      throw new IllegalStateException("Promotion target tier is not available");
    }
    return targetTier;
  }

  /**
   * Returns whether promotion may be executed asynchronously.
   * @return true when asynchronous promotion is allowed
   */
  public boolean isAsynchronous() {
    return asynchronous;
  }
}
