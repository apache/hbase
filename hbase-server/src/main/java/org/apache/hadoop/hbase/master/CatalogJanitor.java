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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.GCMultipleMergedRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.GCRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A janitor for the catalog tables. Scans the <code>hbase:meta</code> catalog
 * table on a period. Makes a lastReport on state of hbase:meta. Looks for unused
 * regions to garbage collect. Scan of hbase:meta runs if we are NOT in maintenance
 * mode, if we are NOT shutting down, AND if the assignmentmanager is loaded.
 * Playing it safe, we will garbage collect no-longer needed region references
 * only if there are no regions-in-transition (RIT).
 */
// TODO: Only works with single hbase:meta region currently.  Fix.
// TODO: Should it start over every time? Could it continue if runs into problem? Only if
// problem does not mess up 'results'.
// TODO: Do more by way of 'repair'; see note on unknownServers below.
@InterfaceAudience.Private
public class CatalogJanitor extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogJanitor.class.getName());
  private final AtomicBoolean alreadyRunning = new AtomicBoolean(false);
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private final MasterServices services;

  /**
   * Saved report from last hbase:meta scan to completion. May be stale if having trouble
   * completing scan. Check its date.
   */
  private volatile Report lastReport;

  CatalogJanitor(final MasterServices services) {
    super("CatalogJanitor-" + services.getServerName().toShortString(), services,
      services.getConfiguration().getInt("hbase.catalogjanitor.interval", 300000));
    this.services = services;
  }

  @Override
  protected boolean initialChore() {
    try {
      if (getEnabled()) {
        scan();
      }
    } catch (IOException e) {
      LOG.warn("Failed initial janitorial scan of hbase:meta table", e);
      return false;
    }
    return true;
  }

  boolean setEnabled(final boolean enabled) {
    boolean alreadyEnabled = this.enabled.getAndSet(enabled);
    // If disabling is requested on an already enabled chore, we could have an active
    // scan still going on, callers might not be aware of that and do further action thinkng
    // that no action would be from this chore.  In this case, the right action is to wait for
    // the active scan to complete before exiting this function.
    if (!enabled && alreadyEnabled) {
      while (alreadyRunning.get()) {
        Threads.sleepWithoutInterrupt(100);
      }
    }
    return alreadyEnabled;
  }

  boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  protected void chore() {
    try {
      AssignmentManager am = this.services.getAssignmentManager();
      if (getEnabled() && !this.services.isInMaintenanceMode() &&
          !this.services.getServerManager().isClusterShutdown() &&
          isMetaLoaded(am)) {
        scan();
      } else {
        LOG.warn("CatalogJanitor is disabled! Enabled=" + getEnabled() + 
          ", maintenanceMode=" + this.services.isInMaintenanceMode() + ", am=" + am +
          ", metaLoaded=" + isMetaLoaded(am) + ", hasRIT=" + isRIT(am) +
          " clusterShutDown=" + this.services.getServerManager().isClusterShutdown());
      }
    } catch (IOException e) {
      LOG.warn("Failed janitorial scan of hbase:meta table", e);
    }
  }

  private static boolean isMetaLoaded(AssignmentManager am) {
    return am != null && am.isMetaLoaded();
  }

  private static boolean isRIT(AssignmentManager am) {
    return isMetaLoaded(am) && am.hasRegionsInTransition();
  }

  /**
   * Run janitorial scan of catalog <code>hbase:meta</code> table looking for
   * garbage to collect.
   * @return How many items gc'd whether for merge or split.
   */
  int scan() throws IOException {
    int gcs = 0;
    try {
      if (!alreadyRunning.compareAndSet(false, true)) {
        LOG.debug("CatalogJanitor already running");
        return gcs;
      }
      this.lastReport = scanForReport();
      if (!this.lastReport.isEmpty()) {
        LOG.warn(this.lastReport.toString());
      }

      if (isRIT(this.services.getAssignmentManager())) {
        LOG.warn("Playing-it-safe skipping merge/split gc'ing of regions from hbase:meta while " +
            "regions-in-transition (RIT)");
      }
      Map<RegionInfo, Result> mergedRegions = this.lastReport.mergedRegions;
      for (Map.Entry<RegionInfo, Result> e : mergedRegions.entrySet()) {
        if (this.services.isInMaintenanceMode()) {
          // Stop cleaning if the master is in maintenance mode
          break;
        }

        List<RegionInfo> parents = MetaTableAccessor.getMergeRegions(e.getValue().rawCells());
        if (parents != null && cleanMergeRegion(e.getKey(), parents)) {
          gcs++;
        }
      }
      // Clean split parents
      Map<RegionInfo, Result> splitParents = this.lastReport.splitParents;

      // Now work on our list of found parents. See if any we can clean up.
      HashSet<String> parentNotCleaned = new HashSet<>();
      for (Map.Entry<RegionInfo, Result> e : splitParents.entrySet()) {
        if (this.services.isInMaintenanceMode()) {
          // Stop cleaning if the master is in maintenance mode
          break;
        }

        if (!parentNotCleaned.contains(e.getKey().getEncodedName()) &&
            cleanParent(e.getKey(), e.getValue())) {
          gcs++;
        } else {
          // We could not clean the parent, so it's daughters should not be
          // cleaned either (HBASE-6160)
          PairOfSameType<RegionInfo> daughters =
              MetaTableAccessor.getDaughterRegions(e.getValue());
          parentNotCleaned.add(daughters.getFirst().getEncodedName());
          parentNotCleaned.add(daughters.getSecond().getEncodedName());
        }
      }
      return gcs;
    } finally {
      alreadyRunning.set(false);
    }
  }

  /**
   * Scan hbase:meta.
   * @return Return generated {@link Report}
   */
  Report scanForReport() throws IOException {
    ReportMakingVisitor visitor = new ReportMakingVisitor(this.services);
    // Null tablename means scan all of meta.
    MetaTableAccessor.scanMetaForTableRegions(this.services.getConnection(), visitor, null);
    return visitor.getReport();
  }

  /**
   * @return Returns last published Report that comes of last successful scan
   *   of hbase:meta.
   */
  public Report getLastReport() {
    return this.lastReport;
  }

  /**
   * If merged region no longer holds reference to the merge regions, archive
   * merge region on hdfs and perform deleting references in hbase:meta
   * @return true if we delete references in merged region on hbase:meta and archive
   *   the files on the file system
   */
  private boolean cleanMergeRegion(final RegionInfo mergedRegion, List<RegionInfo> parents)
      throws IOException {
    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, mergedRegion.getTable());
    TableDescriptor htd = getDescriptor(mergedRegion.getTable());
    HRegionFileSystem regionFs = null;
    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(
          this.services.getConfiguration(), fs, tabledir, mergedRegion, true);
    } catch (IOException e) {
      LOG.warn("Merged region does not exist: " + mergedRegion.getEncodedName());
    }
    if (regionFs == null || !regionFs.hasReferences(htd)) {
      LOG.debug("Deleting parents ({}) from fs; merged child {} no longer holds references",
           parents.stream().map(r -> RegionInfo.getShortNameToLog(r)).
              collect(Collectors.joining(", ")),
          mergedRegion);
      ProcedureExecutor<MasterProcedureEnv> pe = this.services.getMasterProcedureExecutor();
      pe.submitProcedure(new GCMultipleMergedRegionsProcedure(pe.getEnvironment(),
          mergedRegion,  parents));
      for (RegionInfo ri:  parents) {
        // The above scheduled GCMultipleMergedRegionsProcedure does the below.
        // Do we need this?
        this.services.getAssignmentManager().getRegionStates().deleteRegion(ri);
        this.services.getServerManager().removeRegion(ri);
      }
      return true;
    }
    return false;
  }

  /**
   * Compare HRegionInfos in a way that has split parents sort BEFORE their daughters.
   */
  static class SplitParentFirstComparator implements Comparator<RegionInfo> {
    Comparator<byte[]> rowEndKeyComparator = new Bytes.RowEndKeyComparator();
    @Override
    public int compare(RegionInfo left, RegionInfo right) {
      // This comparator differs from the one RegionInfo in that it sorts
      // parent before daughters.
      if (left == null) {
        return -1;
      }
      if (right == null) {
        return 1;
      }
      // Same table name.
      int result = left.getTable().compareTo(right.getTable());
      if (result != 0) {
        return result;
      }
      // Compare start keys.
      result = Bytes.compareTo(left.getStartKey(), right.getStartKey());
      if (result != 0) {
        return result;
      }
      // Compare end keys, but flip the operands so parent comes first
      result = rowEndKeyComparator.compare(right.getEndKey(), left.getEndKey());

      return result;
    }
  }

  /**
   * If daughters no longer hold reference to the parents, delete the parent.
   * @param parent RegionInfo of split offlined parent
   * @param rowContent Content of <code>parent</code> row in
   * <code>metaRegionName</code>
   * @return True if we removed <code>parent</code> from meta table and from
   * the filesystem.
   */
  boolean cleanParent(final RegionInfo parent, Result rowContent)
  throws IOException {
    // Check whether it is a merged region and if it is clean of references.
    if (MetaTableAccessor.hasMergeRegions(rowContent.rawCells())) {
      // Wait until clean of merge parent regions first
      return false;
    }
    // Run checks on each daughter split.
    PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(rowContent);
    Pair<Boolean, Boolean> a = checkDaughterInFs(parent, daughters.getFirst());
    Pair<Boolean, Boolean> b = checkDaughterInFs(parent, daughters.getSecond());
    if (hasNoReferences(a) && hasNoReferences(b)) {
      String daughterA = daughters.getFirst() != null?
          daughters.getFirst().getShortNameToLog(): "null";
      String daughterB = daughters.getSecond() != null?
          daughters.getSecond().getShortNameToLog(): "null";
      LOG.debug("Deleting region " + parent.getShortNameToLog() +
        " because daughters -- " + daughterA + ", " + daughterB +
        " -- no longer hold references");
      ProcedureExecutor<MasterProcedureEnv> pe = this.services.getMasterProcedureExecutor();
      pe.submitProcedure(new GCRegionProcedure(pe.getEnvironment(), parent));
      // Remove from in-memory states
      this.services.getAssignmentManager().getRegionStates().deleteRegion(parent);
      this.services.getServerManager().removeRegion(parent);
      return true;
    }
    return false;
  }

  /**
   * @param p A pair where the first boolean says whether or not the daughter
   * region directory exists in the filesystem and then the second boolean says
   * whether the daughter has references to the parent.
   * @return True the passed <code>p</code> signifies no references.
   */
  private boolean hasNoReferences(final Pair<Boolean, Boolean> p) {
    return !p.getFirst() || !p.getSecond();
  }

  /**
   * Checks if a daughter region -- either splitA or splitB -- still holds
   * references to parent.
   * @param parent Parent region
   * @param daughter Daughter region
   * @return A pair where the first boolean says whether or not the daughter
   *   region directory exists in the filesystem and then the second boolean says
   *   whether the daughter has references to the parent.
   */
  private Pair<Boolean, Boolean> checkDaughterInFs(final RegionInfo parent,
    final RegionInfo daughter)
  throws IOException {
    if (daughter == null)  {
      return new Pair<>(Boolean.FALSE, Boolean.FALSE);
    }

    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, daughter.getTable());

    Path daughterRegionDir = new Path(tabledir, daughter.getEncodedName());

    HRegionFileSystem regionFs;

    try {
      if (!FSUtils.isExists(fs, daughterRegionDir)) {
        return new Pair<>(Boolean.FALSE, Boolean.FALSE);
      }
    } catch (IOException ioe) {
      LOG.error("Error trying to determine if daughter region exists, " +
               "assuming exists and has references", ioe);
      return new Pair<>(Boolean.TRUE, Boolean.TRUE);
    }

    boolean references = false;
    TableDescriptor parentDescriptor = getDescriptor(parent.getTable());
    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(
          this.services.getConfiguration(), fs, tabledir, daughter, true);

      for (ColumnFamilyDescriptor family: parentDescriptor.getColumnFamilies()) {
        if ((references = regionFs.hasReferences(family.getNameAsString()))) {
          break;
        }
      }
    } catch (IOException e) {
      LOG.error("Error trying to determine referenced files from : " + daughter.getEncodedName()
          + ", to: " + parent.getEncodedName() + " assuming has references", e);
      return new Pair<>(Boolean.TRUE, Boolean.TRUE);
    }
    return new Pair<>(Boolean.TRUE, references);
  }

  private TableDescriptor getDescriptor(final TableName tableName) throws IOException {
    return this.services.getTableDescriptors().get(tableName);
  }

  /**
   * Checks if the specified region has merge qualifiers, if so, try to clean them.
   * @return true if no info:merge* columns; i.e. the specified region doesn't have
   *   any merge qualifiers.
   */
  public boolean cleanMergeQualifier(final RegionInfo region) throws IOException {
    // Get merge regions if it is a merged region and already has merge qualifier
    List<RegionInfo> parents = MetaTableAccessor.getMergeRegions(this.services.getConnection(),
        region.getRegionName());
    if (parents == null || parents.isEmpty()) {
      // It doesn't have merge qualifier, no need to clean
      return true;
    }
    return cleanMergeRegion(region, parents);
  }

  /**
   * Report made by ReportMakingVisitor
   */
  public static class Report {
    private final long now = EnvironmentEdgeManager.currentTime();

    // Keep Map of found split parents. These are candidates for cleanup.
    // Use a comparator that has split parents come before its daughters.
    final Map<RegionInfo, Result> splitParents = new TreeMap<>(new SplitParentFirstComparator());
    final Map<RegionInfo, Result> mergedRegions = new TreeMap<>(RegionInfo.COMPARATOR);
    int count = 0;

    private final List<Pair<RegionInfo, RegionInfo>> holes = new ArrayList<>();
    private final List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();

    /**
     * TODO: If CatalogJanitor finds an 'Unknown Server', it should 'fix' it by queuing
     * a {@link org.apache.hadoop.hbase.master.procedure.HBCKServerCrashProcedure} for
     * found server for it to clean up meta.
     */
    private final List<Pair<RegionInfo, ServerName>> unknownServers = new ArrayList<>();

    private final List<byte []> emptyRegionInfo = new ArrayList<>();

    @VisibleForTesting
    Report() {}

    public long getCreateTime() {
      return this.now;
    }

    public List<Pair<RegionInfo, RegionInfo>> getHoles() {
      return this.holes;
    }

    /**
     * @return Overlap pairs found as we scanned hbase:meta; ordered by hbase:meta
     *   table sort. Pairs of overlaps may have overlap with subsequent pairs.
     * @see MetaFixer#calculateMerges(int, List) where we aggregate overlaps
     *   for a single 'merge' call.
     */
    public List<Pair<RegionInfo, RegionInfo>> getOverlaps() {
      return this.overlaps;
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
      for (Pair<RegionInfo, RegionInfo> p: this.holes) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append("hole=").append(p.getFirst().getRegionNameAsString()).append("/").
            append(p.getSecond().getRegionNameAsString());
      }
      for (Pair<RegionInfo, RegionInfo> p: this.overlaps) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append("overlap=").append(p.getFirst().getRegionNameAsString()).append("/").
            append(p.getSecond().getRegionNameAsString());
      }
      for (byte [] r: this.emptyRegionInfo) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append("empty=").append(Bytes.toStringBinary(r));
      }
      for (Pair<RegionInfo, ServerName> p: this.unknownServers) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append("unknown_server=").append(p.getSecond()).append("/").
            append(p.getFirst().getRegionNameAsString());
      }
      return sb.toString();
    }
  }

  /**
   * Visitor we use in here in CatalogJanitor to go against hbase:meta table.
   * Generates a Report made of a collection of split parents and counts of rows
   * in the hbase:meta table. Also runs hbase:meta consistency checks to
   * generate more report. Report is NOT ready until after this visitor has been
   * {@link #close()}'d.
   */
  static class ReportMakingVisitor implements MetaTableAccessor.CloseableVisitor {
    private final MasterServices services;
    private volatile boolean closed;

    /**
     * Report is not done until after the close has been called.
     * @see #close()
     * @see #getReport()
     */
    private Report report = new Report();

    /**
     * RegionInfo from previous row.
     */
    private RegionInfo previous = null;

    /**
     * Keep account of the highest end key seen as we move through hbase:meta.
     * Usually, the current RegionInfo has the highest end key but if an overlap,
     * this may no longer hold. An overlap may be a region with startkey 'd' and
     * endkey 'g'. The next region in meta may be 'e' to 'f' and then 'f' to 'g'.
     * Looking at previous and current meta row, we won't know about the 'd' to 'g'
     * overlap unless we keep a running 'highest-endpoint-seen'.
     */
    private RegionInfo highestEndKeyRegionInfo = null;

    ReportMakingVisitor(MasterServices services) {
      this.services = services;
    }

    /**
     * Do not call until after {@link #close()}.
     * Will throw a {@link RuntimeException} if you do.
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
      } catch(Throwable t) {
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
     * @return Returns default regioninfo found in row parse as a convenience to save
     *   on having to do a double-parse of Result.
     */
    private RegionInfo metaTableConsistencyCheck(Result metaTableRow) {
      RegionInfo ri;
      // Locations comes back null if the RegionInfo field is empty.
      // If locations is null, ensure the regioninfo is for sure empty before progressing.
      // If really empty, report as missing regioninfo!  Otherwise, can run server check
      // and get RegionInfo from locations.
      RegionLocations locations = MetaTableAccessor.getRegionLocations(metaTableRow);
      if (locations == null) {
        ri = MetaTableAccessor.getRegionInfo(metaTableRow,
            MetaTableAccessor.getRegionInfoColumn());
      } else {
        ri = locations.getDefaultRegionLocation().getRegion();
        checkServer(locations);
      }

      if (ri == null) {
        this.report.emptyRegionInfo.add(metaTableRow.getRow());
        return ri;
      }

      if (!Bytes.equals(metaTableRow.getRow(), ri.getRegionName())) {
        LOG.warn("INCONSISTENCY: Row name is not equal to serialized info:regioninfo content; " +
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
          // On table transition, look to see if last region was last in table
          // and if this is the first. Report 'hole' if neither is true.
          // HBCK1 used to have a special category for missing start or end keys.
          // We'll just lump them in as 'holes'.
          if ((this.previous != null && !this.previous.isLast()) || !ri.isFirst()) {
            addHole(this.previous == null? RegionInfo.UNDEFINED: this.previous, ri);
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
      for (HRegionLocation location: locations.getRegionLocations()) {
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
        if (rs.isClosedOrAbnormallyClosed()) {
          // If closed against an 'Unknown Server', that is should be fine.
          continue;
        }
        ServerManager.ServerLiveState state = this.services.getServerManager().
            isServerKnownAndOnline(sn);
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
      return this.previous == null ||
          !this.previous.getTable().equals(ri.getTable());
    }

    @Override
    public void close() throws IOException {
      // This is a table transition... after the last region. Check previous.
      // Should be last region. If not, its a hole on end of laster table.
      if (this.previous != null && !this.previous.isLast()) {
        addHole(this.previous, RegionInfo.UNDEFINED);
      }
      this.closed = true;
    }
  }

  private static void checkLog4jProperties() {
    String filename = "log4j.properties";
    try {
      final InputStream inStream =
          CatalogJanitor.class.getClassLoader().getResourceAsStream(filename);
      if (inStream != null) {
        new Properties().load(inStream);
      } else {
        System.out.println("No " + filename + " on classpath; Add one else no logging output!");
      }
    } catch (IOException e) {
      LOG.error("Log4j check failed", e);
    }
  }

  /**
   * For testing against a cluster.
   * Doesn't have a MasterServices context so does not report on good vs bad servers.
   */
  public static void main(String [] args) throws IOException {
    checkLog4jProperties();
    ReportMakingVisitor visitor = new ReportMakingVisitor(null);
    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean("hbase.defaults.for.version.skip", true);
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      /* Used to generate an overlap.
      */
      Get g = new Get(Bytes.toBytes("t2,40,1564119846424.1db8c57d64e0733e0f027aaeae7a0bf0."));
      g.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      try (Table t = connection.getTable(TableName.META_TABLE_NAME)) {
        Result r = t.get(g);
        byte [] row = g.getRow();
        row[row.length - 2] <<= row[row.length - 2];
        Put p = new Put(g.getRow());
        p.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
            r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
        t.put(p);
      }
      MetaTableAccessor.scanMetaForTableRegions(connection, visitor, null);
      Report report = visitor.getReport();
      LOG.info(report != null? report.toString(): "empty");
    }
  }
}
