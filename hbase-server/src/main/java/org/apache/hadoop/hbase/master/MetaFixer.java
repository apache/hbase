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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;


/**
 * Server-side fixing of bad or inconsistent state in hbase:meta.
 * Distinct from MetaTableAccessor because {@link MetaTableAccessor} is about low-level
 * manipulations driven by the Master. This class MetaFixer is
 * employed by the Master and it 'knows' about holes and orphans
 * and encapsulates their fixing on behalf of the Master.
 */
@InterfaceAudience.Private
class MetaFixer {
  private static final Logger LOG = LoggerFactory.getLogger(MetaFixer.class);
  private static final String MAX_MERGE_COUNT_KEY = "hbase.master.metafixer.max.merge.count";
  private static final int MAX_MERGE_COUNT_DEFAULT = 10;
  private final MasterServices masterServices;
  /**
   * Maximum for many regions to merge at a time.
   */
  private final int maxMergeCount;

  MetaFixer(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.maxMergeCount = this.masterServices.getConfiguration().
        getInt(MAX_MERGE_COUNT_KEY, MAX_MERGE_COUNT_DEFAULT);
  }

  void fix() throws IOException {
    CatalogJanitor.Report report = this.masterServices.getCatalogJanitor().getLastReport();
    if (report == null) {
      LOG.info("CatalogJanitor has not generated a report yet; run 'catalogjanitor_run' in " +
          "shell or wait until CatalogJanitor chore runs.");
      return;
    }
    fixHoles(report);
    fixOverlaps(report);
  }

  /**
   * If hole, it papers it over by adding a region in the filesystem and to hbase:meta.
   * Does not assign.
   */
  void fixHoles(CatalogJanitor.Report report) throws IOException {
    List<Pair<RegionInfo, RegionInfo>> holes = report.getHoles();
    if (holes.isEmpty()) {
      LOG.debug("No holes.");
      return;
    }
    for (Pair<RegionInfo, RegionInfo> p: holes) {
      RegionInfo ri = getHoleCover(p);
      if (ri == null) {
        continue;
      }
      Configuration configuration = this.masterServices.getConfiguration();
      HRegion.createRegionDir(configuration, ri, FSUtils.getRootDir(configuration));
      // If an error here, then we'll have a region in the filesystem but not
      // in hbase:meta (if the below fails). Should be able to rerun the fix.
      // The second call to createRegionDir will just go through. Idempotent.
      MetaTableAccessor.addRegionToMeta(this.masterServices.getConnection(), ri);
      LOG.info("Fixed hole by adding {} in CLOSED state; region NOT assigned (assign to ONLINE).",
          ri);
    }
  }

  /**
   * @return Calculated RegionInfo that covers the hole <code>hole</code>
   */
  private RegionInfo getHoleCover(Pair<RegionInfo, RegionInfo> hole) {
    RegionInfo holeCover = null;
    RegionInfo left = hole.getFirst();
    RegionInfo right = hole.getSecond();
    if (left.getTable().equals(right.getTable())) {
      // Simple case.
      if (Bytes.compareTo(left.getEndKey(), right.getStartKey()) >= 0) {
        LOG.warn("Skipping hole fix; left-side endKey is not less than right-side startKey; " +
            "left=<{}>, right=<{}>", left, right);
        return holeCover;
      }
      holeCover = buildRegionInfo(left.getTable(), left.getEndKey(), right.getStartKey());
    } else {
      boolean leftUndefined = left.equals(RegionInfo.UNDEFINED);
      boolean rightUnefined = right.equals(RegionInfo.UNDEFINED);
      boolean last = left.isLast();
      boolean first = right.isFirst();
      if (leftUndefined && rightUnefined) {
        LOG.warn("Skipping hole fix; both the hole left-side and right-side RegionInfos are " +
            "UNDEFINED; left=<{}>, right=<{}>", left, right);
        return holeCover;
      }
      if (leftUndefined || last) {
        holeCover = buildRegionInfo(right.getTable(), HConstants.EMPTY_START_ROW,
            right.getStartKey());
      } else if (rightUnefined || first) {
        holeCover = buildRegionInfo(left.getTable(), left.getEndKey(), HConstants.EMPTY_END_ROW);
      } else {
        LOG.warn("Skipping hole fix; don't know what to do with left=<{}>, right=<{}>",
            left, right);
        return holeCover;
      }
    }
    return holeCover;
  }

  private RegionInfo buildRegionInfo(TableName tn, byte [] start, byte [] end) {
    return RegionInfoBuilder.newBuilder(tn).setStartKey(start).setEndKey(end).build();
  }

  /**
   * Fix overlaps noted in CJ consistency report.
   */
  void fixOverlaps(CatalogJanitor.Report report) throws IOException {
    for (Set<RegionInfo> regions: calculateMerges(maxMergeCount, report.getOverlaps())) {
      RegionInfo [] regionsArray = regions.toArray(new RegionInfo [] {});
      try {
        this.masterServices.mergeRegions(regionsArray,
            false, HConstants.NO_NONCE, HConstants.NO_NONCE);
      } catch (MergeRegionException mre) {
        LOG.warn("Failed overlap fix of {}", regionsArray, mre);
      }
    }
  }

  /**
   * Run through <code>overlaps</code> and return a list of merges to run.
   * Presumes overlaps are ordered (which they are coming out of the CatalogJanitor
   * consistency report).
   * @param maxMergeCount Maximum regions to merge at a time (avoid merging
   *   100k regions in one go!)
   */
  @VisibleForTesting
  static List<SortedSet<RegionInfo>> calculateMerges(int maxMergeCount,
      List<Pair<RegionInfo, RegionInfo>> overlaps) {
    if (overlaps.isEmpty()) {
      LOG.debug("No overlaps.");
      return Collections.emptyList();
    }
    List<SortedSet<RegionInfo>> merges = new ArrayList<>();
    SortedSet<RegionInfo> currentMergeSet = new TreeSet<>();
    RegionInfo regionInfoWithlargestEndKey =  null;
    for (Pair<RegionInfo, RegionInfo> pair: overlaps) {
      if (regionInfoWithlargestEndKey != null) {
        if (!isOverlap(regionInfoWithlargestEndKey, pair) ||
            currentMergeSet.size() >= maxMergeCount) {
          merges.add(currentMergeSet);
          currentMergeSet = new TreeSet<>();
        }
      }
      currentMergeSet.add(pair.getFirst());
      currentMergeSet.add(pair.getSecond());
      regionInfoWithlargestEndKey = getRegionInfoWithLargestEndKey(
        getRegionInfoWithLargestEndKey(pair.getFirst(), pair.getSecond()),
          regionInfoWithlargestEndKey);
    }
    merges.add(currentMergeSet);
    return merges;
  }

  /**
   * @return Either <code>a</code> or <code>b</code>, whichever has the
   *   endkey that is furthest along in the Table.
   */
  @VisibleForTesting
  static RegionInfo getRegionInfoWithLargestEndKey(RegionInfo a, RegionInfo b) {
    if (a == null) {
      // b may be null.
      return b;
    }
    if (b == null) {
      // Both are null. The return is not-defined.
      return a;
    }
    if (!a.getTable().equals(b.getTable())) {
      // This is an odd one. This should be the right answer.
      return b;
    }
    if (a.isLast()) {
      return a;
    }
    if (b.isLast()) {
      return b;
    }
    int compare = Bytes.compareTo(a.getEndKey(), b.getEndKey());
    return compare == 0 || compare > 0? a: b;
  }

  /**
   * @return True if an overlap found between passed in <code>ri</code> and
   *   the <code>pair</code>. Does NOT check the pairs themselves overlap.
   */
  @VisibleForTesting
  static boolean isOverlap(RegionInfo ri, Pair<RegionInfo, RegionInfo> pair) {
    if (ri == null || pair == null) {
      // Can't be an overlap in either of these cases.
      return false;
    }
    return ri.isOverlap(pair.getFirst()) || ri.isOverlap(pair.getSecond());
  }
}
