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
package org.apache.hadoop.hbase.master.janitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor.SplitParentFirstComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Report made by ReportMakingVisitor
 */
@InterfaceAudience.Private
public class Report {
  private final long now = EnvironmentEdgeManager.currentTime();

  // Keep Map of found split parents. These are candidates for cleanup.
  // Use a comparator that has split parents come before its daughters.
  final Map<RegionInfo, Result> splitParents = new TreeMap<>(new SplitParentFirstComparator());
  final Map<RegionInfo, Result> mergedRegions = new TreeMap<>(RegionInfo.COMPARATOR);
  int count = 0;

  final List<Pair<RegionInfo, RegionInfo>> holes = new ArrayList<>();
  final List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();

  /**
   * TODO: If CatalogJanitor finds an 'Unknown Server', it should 'fix' it by queuing a
   * {@link org.apache.hadoop.hbase.master.procedure.HBCKServerCrashProcedure} for found server for
   * it to clean up meta.
   */
  final List<Pair<RegionInfo, ServerName>> unknownServers = new ArrayList<>();

  final List<byte[]> emptyRegionInfo = new ArrayList<>();

  public long getCreateTime() {
    return this.now;
  }

  public List<Pair<RegionInfo, RegionInfo>> getHoles() {
    return this.holes;
  }

  /**
   * @return Overlap pairs found as we scanned hbase:meta; ordered by hbase:meta table sort. Pairs
   *         of overlaps may have overlap with subsequent pairs.
   * @see MetaFixer#calculateMerges(int, List) where we aggregate overlaps for a single 'merge'
   *      call.
   */
  public List<Pair<RegionInfo, RegionInfo>> getOverlaps() {
    return this.overlaps;
  }

  public Map<RegionInfo, Result> getMergedRegions() {
    return this.mergedRegions;
  }

  public List<Pair<RegionInfo, ServerName>> getUnknownServers() {
    return unknownServers;
  }

  public List<byte[]> getEmptyRegionInfo() {
    return emptyRegionInfo;
  }

  /**
   * @return True if an 'empty' lastReport -- no problems found.
   */
  public boolean isEmpty() {
    return this.holes.isEmpty() && this.overlaps.isEmpty() && this.unknownServers.isEmpty() &&
      this.emptyRegionInfo.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Pair<RegionInfo, RegionInfo> p : this.holes) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("hole=").append(p.getFirst().getRegionNameAsString()).append("/")
        .append(p.getSecond().getRegionNameAsString());
    }
    for (Pair<RegionInfo, RegionInfo> p : this.overlaps) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("overlap=").append(p.getFirst().getRegionNameAsString()).append("/")
        .append(p.getSecond().getRegionNameAsString());
    }
    for (byte[] r : this.emptyRegionInfo) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("empty=").append(Bytes.toStringBinary(r));
    }
    for (Pair<RegionInfo, ServerName> p : this.unknownServers) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("unknown_server=").append(p.getSecond()).append("/")
        .append(p.getFirst().getRegionNameAsString());
    }
    return sb.toString();
  }
}