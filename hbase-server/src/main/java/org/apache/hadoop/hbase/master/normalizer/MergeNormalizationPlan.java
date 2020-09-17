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

import java.io.IOException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Normalization plan to merge regions (smallest region in the table with its smallest neighbor).
 */
@InterfaceAudience.Private
public class MergeNormalizationPlan implements NormalizationPlan {

  private final RegionInfo firstRegion;
  private final RegionInfo secondRegion;

  public MergeNormalizationPlan(RegionInfo firstRegion, RegionInfo secondRegion) {
    this.firstRegion = firstRegion;
    this.secondRegion = secondRegion;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long submit(MasterServices masterServices) throws IOException {
    // Do not use force=true as corner cases can happen, non adjacent regions,
    // merge with a merged child region with no GC done yet, it is going to
    // cause all different issues.
    return masterServices
      .mergeRegions(new RegionInfo[] { firstRegion, secondRegion }, false, HConstants.NO_NONCE,
        HConstants.NO_NONCE);
  }

  @Override
  public PlanType getType() {
    return PlanType.MERGE;
  }

  RegionInfo getFirstRegion() {
    return firstRegion;
  }

  RegionInfo getSecondRegion() {
    return secondRegion;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("firstRegion", firstRegion)
      .append("secondRegion", secondRegion)
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
      .append(firstRegion, that.firstRegion)
      .append(secondRegion, that.secondRegion)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(firstRegion)
      .append(secondRegion)
      .toHashCode();
  }
}
