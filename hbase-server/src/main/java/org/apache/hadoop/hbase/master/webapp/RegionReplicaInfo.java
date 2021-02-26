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
package org.apache.hadoop.hbase.master.webapp;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A POJO that consolidates the information about a single region replica that's stored in meta.
 */
@InterfaceAudience.Private
public final class RegionReplicaInfo {
  private final byte[] row;
  private final RegionInfo regionInfo;
  private final RegionState.State regionState;
  private final ServerName serverName;
  private final long seqNum;
  /** See {@link org.apache.hadoop.hbase.HConstants#SERVERNAME_QUALIFIER_STR}. */
  private final ServerName targetServerName;
  private final Map<String, RegionInfo> mergeRegionInfo;
  private final Map<String, RegionInfo> splitRegionInfo;

  private RegionReplicaInfo(final Result result, final HRegionLocation location) {
    this.row = result != null ? result.getRow() : null;
    this.regionInfo = location != null ? location.getRegion() : null;
    this.regionState = (result != null && regionInfo != null)
      ? RegionStateStore.getRegionState(result, regionInfo)
      : null;
    this.serverName = location != null ? location.getServerName() : null;
    this.seqNum = (location != null) ? location.getSeqNum() : HConstants.NO_SEQNUM;
    this.targetServerName = (result != null && regionInfo != null)
      ? MetaTableAccessor.getTargetServerName(result, regionInfo.getReplicaId())
      : null;
    this.mergeRegionInfo = (result != null)
      ? MetaTableAccessor.getMergeRegionsWithName(result.rawCells())
      : null;

    if (result != null) {
      PairOfSameType<RegionInfo> daughterRegions = MetaTableAccessor.getDaughterRegions(result);
      this.splitRegionInfo = new LinkedHashMap<>();
      if (daughterRegions.getFirst() != null) {
        splitRegionInfo.put(HConstants.SPLITA_QUALIFIER_STR, daughterRegions.getFirst());
      }
      if (daughterRegions.getSecond() != null) {
        splitRegionInfo.put(HConstants.SPLITB_QUALIFIER_STR, daughterRegions.getSecond());
      }
    } else {
      this.splitRegionInfo = null;
    }
  }

  public static List<RegionReplicaInfo> from(final Result result) {
    if (result == null) {
      return Collections.singletonList(null);
    }

    final RegionLocations locations = MetaTableAccessor.getRegionLocations(result);
    if (locations == null) {
      return Collections.singletonList(null);
    }

    return StreamSupport.stream(locations.spliterator(), false)
      .map(location -> new RegionReplicaInfo(result, location))
      .collect(Collectors.toList());
  }

  public byte[] getRow() {
    return row;
  }

  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  public byte[] getRegionName() {
    return regionInfo != null ? regionInfo.getRegionName() : null;
  }

  public byte[] getStartKey() {
    return regionInfo != null ? regionInfo.getStartKey() : null;
  }

  public byte[] getEndKey() {
    return regionInfo != null ? regionInfo.getEndKey() : null;
  }

  public Integer getReplicaId() {
    return regionInfo != null ? regionInfo.getReplicaId() : null;
  }

  public RegionState.State getRegionState() {
    return regionState;
  }

  public ServerName getServerName() {
    return serverName;
  }

  public long getSeqNum() {
    return seqNum;
  }

  public ServerName getTargetServerName() {
    return targetServerName;
  }

  public Map<String, RegionInfo> getMergeRegionInfo() {
    return mergeRegionInfo;
  }

  public Map<String, RegionInfo> getSplitRegionInfo() {
    return splitRegionInfo;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    RegionReplicaInfo that = (RegionReplicaInfo) other;

    return new EqualsBuilder()
      .append(row, that.row)
      .append(regionInfo, that.regionInfo)
      .append(regionState, that.regionState)
      .append(serverName, that.serverName)
      .append(seqNum, that.seqNum)
      .append(targetServerName, that.targetServerName)
      .append(mergeRegionInfo, that.mergeRegionInfo)
      .append(splitRegionInfo, that.splitRegionInfo)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(row)
      .append(regionInfo)
      .append(regionState)
      .append(serverName)
      .append(seqNum)
      .append(targetServerName)
      .append(mergeRegionInfo)
      .append(splitRegionInfo)
      .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("row", Bytes.toStringBinary(row))
      .append("regionInfo", regionInfo)
      .append("regionState", regionState)
      .append("serverName", serverName)
      .append("seqNum", seqNum)
      .append("transitioningOnServerName", targetServerName)
      .append("merge*", mergeRegionInfo)
      .append("split*", splitRegionInfo)
      .toString();
  }
}
