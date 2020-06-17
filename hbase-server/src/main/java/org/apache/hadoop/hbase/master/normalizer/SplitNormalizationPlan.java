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
 * Normalization plan to split region.
 */
@InterfaceAudience.Private
public class SplitNormalizationPlan implements NormalizationPlan {

  private final RegionInfo regionInfo;

  public SplitNormalizationPlan(RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  @Override
  public long submit(MasterServices masterServices) throws IOException {
    return masterServices.splitRegion(regionInfo, null, HConstants.NO_NONCE,
      HConstants.NO_NONCE);
  }

  @Override
  public PlanType getType() {
    return PlanType.SPLIT;
  }

  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("regionInfo", regionInfo)
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
      .append(regionInfo, that.regionInfo)
      .isEquals();
  }

  @Override public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(regionInfo)
      .toHashCode();
  }
}
