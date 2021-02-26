/*
 *
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
package org.apache.hadoop.hbase.master.normalizer;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Normalization plan to split a region.
 */
@InterfaceAudience.Private
final class SplitNormalizationPlan implements NormalizationPlan {

  private final NormalizationTarget splitTarget;

  SplitNormalizationPlan(final RegionInfo splitTarget, final long splitTargetSizeMb) {
    this.splitTarget = new NormalizationTarget(splitTarget, splitTargetSizeMb);
  }

  @Override
  public PlanType getType() {
    return PlanType.SPLIT;
  }

  public NormalizationTarget getSplitTarget() {
    return splitTarget;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("splitTarget", splitTarget)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitNormalizationPlan that = (SplitNormalizationPlan) o;

    return new EqualsBuilder()
      .append(splitTarget, that.splitTarget)
      .isEquals();
  }

  @Override public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(splitTarget)
      .toHashCode();
  }
}
