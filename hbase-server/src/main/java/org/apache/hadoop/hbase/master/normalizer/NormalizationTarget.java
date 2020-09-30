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
package org.apache.hadoop.hbase.master.normalizer;

import java.util.Objects;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A POJO that caries details about a region selected for normalization through the pipeline.
 */
@InterfaceAudience.Private
class NormalizationTarget {
  private final RegionInfo regionInfo;
  private final long regionSizeMb;

  NormalizationTarget(final RegionInfo regionInfo, final long regionSizeMb) {
    this.regionInfo = Objects.requireNonNull(regionInfo);
    this.regionSizeMb = regionSizeMb;
  }

  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  public long getRegionSizeMb() {
    return regionSizeMb;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NormalizationTarget that = (NormalizationTarget) o;

    return new EqualsBuilder()
      .append(regionSizeMb, that.regionSizeMb)
      .append(regionInfo, that.regionInfo)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(regionInfo)
      .append(regionSizeMb)
      .toHashCode();
  }

  @Override public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("regionInfo", regionInfo)
      .append("regionSizeMb", regionSizeMb)
      .toString();
  }
}
