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

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Visitor we use in here in CatalogJanitor to go against hbase:meta table. Generates a Report made
 * of a collection of split parents and counts of rows in the hbase:meta table. Also runs hbase:meta
 * consistency checks to generate more report. Report is NOT ready until after this visitor has been
 * {@link #close()}'d.
 */
@InterfaceAudience.Private
class ReportMakingVisitor implements MetaTableAccessor.CloseableVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(ReportMakingVisitor.class);

  private final MasterServices services;
  private volatile boolean closed;

  /**
   * Report is not done until after the close has been called.
   */
  private Report report = new Report();

  /**
   * RegionInfo from previous row.
   */
  private RegionInfo previous = null;

  /**
   * Keep account of the highest end key seen as we move through hbase:meta. Usually, the current
   * RegionInfo has the highest end key but if an overlap, this may no longer hold. An overlap may
   * be a region with startkey 'd' and endkey 'g'. The next region in meta may be 'e' to 'f' and
   * then 'f' to 'g'. Looking at previous and current meta row, we won't know about the 'd' to 'g'
   * overlap unless we keep a running 'highest-endpoint-seen'.
   */
  private RegionInfo highestEndKeyRegionInfo = null;

  ReportMakingVisitor(MasterServices services) {
    this.services = services;
  }

  /**
   * Do not call until after {@link #close()}. Will throw a {@link RuntimeException} if you do.
   */
  Report getReport() {
    if (!this.closed) {
      throw new RuntimeException("Report not ready until after close()");
    }
    return this.report;
  }

  @Override
  public boolean visit(Result r) {
    if (r == null || r.isEmpty()) {
      return true;
    }
    this.report.count++;
    RegionInfo regionInfo = null;
    try {
      regionInfo = metaTableConsistencyCheck(r);
    } catch (Throwable t) {
      LOG.warn("Failed consistency check on {}", Bytes.toStringBinary(r.getRow()), t);
    }
    if (regionInfo != null) {
      LOG.trace(regionInfo.toString());
      if (regionInfo.isSplitParent()) { // splitParent means split and offline.
        this.report.splitParents.put(regionInfo, r);
      }
      if (MetaTableAccessor.hasMergeRegions(r.rawCells())) {
        this.report.mergedRegions.put(regionInfo, r);
      }
    }
    // Returning true means "keep scanning"
    return true;
  }

  /**
   * Check row.
   * @param metaTableRow Row from hbase:meta table.
   * @return Returns default regioninfo found in row parse as a convenience to save on having to do
   *         a double-parse of Result.
   */
  private RegionInfo metaTableConsistencyCheck(Result metaTableRow) {
    RegionInfo ri;
    // Locations comes back null if the RegionInfo field is empty.
    // If locations is null, ensure the regioninfo is for sure empty before progressing.
    // If really empty, report as missing regioninfo! Otherwise, can run server check
    // and get RegionInfo from locations.
    RegionLocations locations = MetaTableAccessor.getRegionLocations(metaTableRow);
    if (locations == null) {
      ri = MetaTableAccessor.getRegionInfo(metaTableRow, HConstants.REGIONINFO_QUALIFIER);
    } else {
      ri = locations.getDefaultRegionLocation().getRegion();
      checkServer(locations);
    }

    if (ri == null) {
      this.report.emptyRegionInfo.add(metaTableRow.getRow());
      return ri;
    }

    if (!Bytes.equals(metaTableRow.getRow(), ri.getRegionName())) {
      LOG.warn(
        "INCONSISTENCY: Row name is not equal to serialized info:regioninfo content; " +
          "row={} {}; See if RegionInfo is referenced in another hbase:meta row? Delete?",
        Bytes.toStringBinary(metaTableRow.getRow()), ri.getRegionNameAsString());
      return null;
    }
    // Skip split parent region
    if (ri.isSplitParent()) {
      return ri;
    }
    // If table is disabled, skip integrity check.
    if (!isTableDisabled(ri)) {
      if (isTableTransition(ri)) {
        // HBCK1 used to have a special category for missing start or end keys.
        // We'll just lump them in as 'holes'.

        // This is a table transition. If this region is not first region, report a hole.
        if (!ri.isFirst()) {
          addHole(RegionInfoBuilder.UNDEFINED, ri);
        }
        // This is a table transition. If last region was not last region of previous table,
        // report a hole
        if (this.previous != null && !this.previous.isLast()) {
          addHole(this.previous, RegionInfoBuilder.UNDEFINED);
        }
      } else {
        if (!this.previous.isNext(ri)) {
          if (this.previous.isOverlap(ri)) {
            addOverlap(this.previous, ri);
          } else if (ri.isOverlap(this.highestEndKeyRegionInfo)) {
            // We may have seen a region a few rows back that overlaps this one.
            addOverlap(this.highestEndKeyRegionInfo, ri);
          } else if (!this.highestEndKeyRegionInfo.isNext(ri)) {
            // Need to check the case if this.highestEndKeyRegionInfo.isNext(ri). If no,
            // report a hole, otherwise, it is ok. For an example,
            // previous: [aa, bb), ri: [cc, dd), highestEndKeyRegionInfo: [a, cc)
            // In this case, it should not report a hole, as highestEndKeyRegionInfo covers
            // the hole between previous and ri.
            addHole(this.previous, ri);
          }
        } else if (ri.isOverlap(this.highestEndKeyRegionInfo)) {
          // We may have seen a region a few rows back that overlaps this one
          // even though it properly 'follows' the region just before.
          addOverlap(this.highestEndKeyRegionInfo, ri);
        }
      }
    }
    this.previous = ri;
    this.highestEndKeyRegionInfo =
      MetaFixer.getRegionInfoWithLargestEndKey(this.highestEndKeyRegionInfo, ri);
    return ri;
  }

  private void addOverlap(RegionInfo a, RegionInfo b) {
    this.report.overlaps.add(new Pair<>(a, b));
  }

  private void addHole(RegionInfo a, RegionInfo b) {
    this.report.holes.add(new Pair<>(a, b));
  }

  /**
   * @return True if table is disabled or disabling; defaults false!
   */
  boolean isTableDisabled(RegionInfo ri) {
    if (ri == null) {
      return false;
    }
    if (this.services == null) {
      return false;
    }
    if (this.services.getTableStateManager() == null) {
      return false;
    }
    TableState state = null;
    try {
      state = this.services.getTableStateManager().getTableState(ri.getTable());
    } catch (IOException e) {
      LOG.warn("Failed getting table state", e);
    }
    return state != null && state.isDisabledOrDisabling();
  }

  /**
   * Run through referenced servers and save off unknown and the dead.
   */
  private void checkServer(RegionLocations locations) {
    if (this.services == null) {
      // Can't do this test if no services.
      return;
    }
    if (locations == null) {
      return;
    }
    if (locations.getRegionLocations() == null) {
      return;
    }
    // Check referenced servers are known/online. Here we are looking
    // at both the default replica -- the main replica -- and then replica
    // locations too.
    for (HRegionLocation location : locations.getRegionLocations()) {
      if (location == null) {
        continue;
      }
      ServerName sn = location.getServerName();
      if (sn == null) {
        continue;
      }
      if (location.getRegion() == null) {
        LOG.warn("Empty RegionInfo in {}", location);
        // This should never happen but if it does, will mess up below.
        continue;
      }
      RegionInfo ri = location.getRegion();
      // Skip split parent region
      if (ri.isSplitParent()) {
        continue;
      }
      // skip the offline regions which belong to disabled table.
      if (isTableDisabled(ri)) {
        continue;
      }
      RegionState rs = this.services.getAssignmentManager().getRegionStates().getRegionState(ri);
      if (rs == null || rs.isClosedOrAbnormallyClosed()) {
        // If closed against an 'Unknown Server', that is should be fine.
        continue;
      }
      ServerManager.ServerLiveState state =
        this.services.getServerManager().isServerKnownAndOnline(sn);
      switch (state) {
        case UNKNOWN:
          this.report.unknownServers.add(new Pair<>(ri, sn));
          break;

        default:
          break;
      }
    }
  }

  /**
   * @return True iff first row in hbase:meta or if we've broached a new table in hbase:meta
   */
  private boolean isTableTransition(RegionInfo ri) {
    return this.previous == null || !this.previous.getTable().equals(ri.getTable());
  }

  @Override
  public void close() throws IOException {
    // This is a table transition... after the last region. Check previous.
    // Should be last region. If not, its a hole on end of laster table.
    if (this.previous != null && !this.previous.isLast()) {
      addHole(this.previous, RegionInfoBuilder.UNDEFINED);
    }
    this.closed = true;
  }
}