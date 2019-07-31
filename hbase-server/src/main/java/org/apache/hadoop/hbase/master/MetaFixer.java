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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side fixing of bad or inconsistent state in hbase:meta.
 * Distinct from MetaTableAccessor because {@link MetaTableAccessor} is about low-level
 * manipulations driven by the Master. This class MetaFixer is
 * employed by the Master and it 'knows' about holes and orphan
 * and encapsulates their fixing on behalf of the Master.
 */
@InterfaceAudience.Private
class MetaFixer {
  private static final Logger LOG = LoggerFactory.getLogger(MetaFixer.class);
  private final MasterServices masterServices;

  MetaFixer(MasterServices masterServices) {
    this.masterServices = masterServices;
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
   * @return True if we fixed any 'holes'.
   */
  boolean fixHoles(CatalogJanitor.Report report) throws IOException {
    boolean result = false;
    List<Pair<RegionInfo, RegionInfo>> holes = report.getHoles();
    if (holes.isEmpty()) {
      LOG.debug("No holes.");
      return result;
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
      Put put = MetaTableAccessor.makePutFromRegionInfo(ri, HConstants.LATEST_TIMESTAMP);
      MetaTableAccessor.putsToMetaTable(this.masterServices.getConnection(), Arrays.asList(put));
      LOG.info("Fixed hole by adding {}; region is NOT assigned (assign to online).", ri);
      result = true;
    }
    return result;
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

  boolean fixOverlaps(CatalogJanitor.Report report) throws IOException {
    boolean result = false;
    List<Pair<RegionInfo, RegionInfo>> overlaps = report.getOverlaps();
    if (overlaps.isEmpty()) {
      LOG.debug("No overlaps.");
      return result;
    }
    for (Pair<RegionInfo, RegionInfo> p: overlaps) {
      RegionInfo ri = getHoleCover(p);
      if (ri == null) {
        continue;
      }
      Configuration configuration = this.masterServices.getConfiguration();
      HRegion.createRegionDir(configuration, ri, FSUtils.getRootDir(configuration));
      // If an error here, then we'll have a region in the filesystem but not
      // in hbase:meta (if the below fails). Should be able to rerun the fix.
      // The second call to createRegionDir will just go through. Idempotent.
      Put put = MetaTableAccessor.makePutFromRegionInfo(ri, HConstants.LATEST_TIMESTAMP);
      MetaTableAccessor.putsToMetaTable(this.masterServices.getConnection(), Arrays.asList(put));
      LOG.info("Fixed hole by adding {}; region is NOT assigned (assign to online).", ri);
      result = true;
    }
    return result;
  }
}
