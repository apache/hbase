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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Normalization plan to merge adjacent regions. As with any call to
 * {@link MasterServices#mergeRegions(RegionInfo[], boolean, long, long)}
 * with {@code forcible=false}, Region order and adjacency are important. It's the caller's
 * responsibility to ensure the provided parameters are ordered according to the
 * {code mergeRegions} method requirements.
 */
@InterfaceAudience.Private
final class MergeNormalizationPlan implements NormalizationPlan {

  private final List<NormalizationTarget> normalizationTargets;

  private MergeNormalizationPlan(List<NormalizationTarget> normalizationTargets) {
    Preconditions.checkNotNull(normalizationTargets);
    Preconditions.checkState(normalizationTargets.size() >= 2,
      "normalizationTargets.size() must be >= 2 but was %s", normalizationTargets.size());
    this.normalizationTargets = Collections.unmodifiableList(normalizationTargets);
  }

  @Override
  public PlanType getType() {
    return PlanType.MERGE;
  }

  public List<NormalizationTarget> getNormalizationTargets() {
    return normalizationTargets;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("normalizationTargets", normalizationTargets)
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

    MergeNormalizationPlan that = (MergeNormalizationPlan) o;

    return new EqualsBuilder()
      .append(normalizationTargets, that.normalizationTargets)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(normalizationTargets)
      .toHashCode();
  }

  /**
   * A helper for constructing instances of {@link MergeNormalizationPlan}.
   */
  static class Builder {

    private final List<NormalizationTarget> normalizationTargets = new LinkedList<>();

    public Builder setTargets(final List<NormalizationTarget> targets) {
      normalizationTargets.clear();
      normalizationTargets.addAll(targets);
      return this;
    }

    public Builder addTarget(final RegionInfo regionInfo, final long regionSizeMb) {
      normalizationTargets.add(new NormalizationTarget(regionInfo, regionSizeMb));
      return this;
    }

    public MergeNormalizationPlan build() {
      return new MergeNormalizationPlan(normalizationTargets);
    }
  }
}
