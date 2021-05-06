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
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
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
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.GCMultipleMergedRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.GCRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A janitor for the catalog tables. Scans the <code>hbase:meta</code> catalog table on a period.
 * Makes a lastReport on state of hbase:meta. Looks for unused regions to garbage collect. Scan of
 * hbase:meta runs if we are NOT in maintenance mode, if we are NOT shutting down, AND if the
 * assignmentmanager is loaded. Playing it safe, we will garbage collect no-longer needed region
 * references only if there are no regions-in-transition (RIT).
 */
// TODO: Only works with single hbase:meta region currently. Fix.
// TODO: Should it start over every time? Could it continue if runs into problem? Only if
// problem does not mess up 'results'.
// TODO: Do more by way of 'repair'; see note on unknownServers below.
@InterfaceAudience.Private
public class CatalogJanitor extends ScheduledChore {

  public static final int DEFAULT_HBASE_CATALOGJANITOR_INTERVAL = 300 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(CatalogJanitor.class.getName());

  private final AtomicBoolean alreadyRunning = new AtomicBoolean(false);
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private final MasterServices services;

  /**
   * Saved report from last hbase:meta scan to completion. May be stale if having trouble completing
   * scan. Check its date.
   */
  private volatile Report lastReport;

  public CatalogJanitor(final MasterServices services) {
    super("CatalogJanitor-" + services.getServerName().toShortString(), services,
      services.getConfiguration().getInt("hbase.catalogjanitor.interval",
        DEFAULT_HBASE_CATALOGJANITOR_INTERVAL));
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

  public boolean setEnabled(final boolean enabled) {
    boolean alreadyEnabled = this.enabled.getAndSet(enabled);
    // If disabling is requested on an already enabled chore, we could have an active
    // scan still going on, callers might not be aware of that and do further action thinkng
    // that no action would be from this chore. In this case, the right action is to wait for
    // the active scan to complete before exiting this function.
    if (!enabled && alreadyEnabled) {
      while (alreadyRunning.get()) {
        Threads.sleepWithoutInterrupt(100);
      }
    }
    return alreadyEnabled;
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  protected void chore() {
    try {
      AssignmentManager am = this.services.getAssignmentManager();
      if (getEnabled() && !this.services.isInMaintenanceMode() &&
        !this.services.getServerManager().isClusterShutdown() && isMetaLoaded(am)) {
        scan();
      } else {
        LOG.warn("CatalogJanitor is disabled! Enabled=" + getEnabled() + ", maintenanceMode=" +
          this.services.isInMaintenanceMode() + ", am=" + am + ", metaLoaded=" + isMetaLoaded(am) +
          ", hasRIT=" + isRIT(am) + " clusterShutDown=" +
          this.services.getServerManager().isClusterShutdown());
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
   * Run janitorial scan of catalog <code>hbase:meta</code> table looking for garbage to collect.
   * @return How many items gc'd whether for merge or split. Returns -1 if previous scan is in
   *         progress.
   */
  public int scan() throws IOException {
    int gcs = 0;
    try {
      if (!alreadyRunning.compareAndSet(false, true)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("CatalogJanitor already running");
        }
        // -1 indicates previous scan is in progress
        return -1;
      }
      this.lastReport = scanForReport();
      if (!this.lastReport.isEmpty()) {
        LOG.warn(this.lastReport.toString());
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(this.lastReport.toString());
        }
      }

      updateAssignmentManagerMetrics();

      Map<RegionInfo, Result> mergedRegions = this.lastReport.mergedRegions;
      for (Map.Entry<RegionInfo, Result> e : mergedRegions.entrySet()) {
        if (this.services.isInMaintenanceMode()) {
          // Stop cleaning if the master is in maintenance mode
          if (LOG.isDebugEnabled()) {
            LOG.debug("In maintenence mode, not cleaning");
          }
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("In maintenence mode, not cleaning");
          }
          break;
        }

        if (!parentNotCleaned.contains(e.getKey().getEncodedName()) &&
          cleanParent(e.getKey(), e.getValue())) {
          gcs++;
        } else {
          // We could not clean the parent, so it's daughters should not be
          // cleaned either (HBASE-6160)
          PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(e.getValue());
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
  // will be override in tests.
  protected Report scanForReport() throws IOException {
    ReportMakingVisitor visitor = new ReportMakingVisitor(this.services);
    // Null tablename means scan all of meta.
    MetaTableAccessor.scanMetaForTableRegions(this.services.getConnection(), visitor, null);
    return visitor.getReport();
  }

  /**
   * @return Returns last published Report that comes of last successful scan of hbase:meta.
   */
  public Report getLastReport() {
    return this.lastReport;
  }

  /**
   * If merged region no longer holds reference to the merge regions, archive merge region on hdfs
   * and perform deleting references in hbase:meta
   * @return true if we delete references in merged region on hbase:meta and archive the files on
   *         the file system
   */
  private boolean cleanMergeRegion(final RegionInfo mergedRegion, List<RegionInfo> parents)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning merged region {}", mergedRegion);
    }
    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = CommonFSUtils.getTableDir(rootdir, mergedRegion.getTable());
    TableDescriptor htd = getDescriptor(mergedRegion.getTable());
    HRegionFileSystem regionFs = null;
    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(this.services.getConfiguration(), fs,
        tabledir, mergedRegion, true);
    } catch (IOException e) {
      LOG.warn("Merged region does not exist: " + mergedRegion.getEncodedName());
    }
    if (regionFs == null || !regionFs.hasReferences(htd)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Deleting parents ({}) from fs; merged child {} no longer holds references", parents
            .stream().map(r -> RegionInfo.getShortNameToLog(r)).collect(Collectors.joining(", ")),
          mergedRegion);
      }
      ProcedureExecutor<MasterProcedureEnv> pe = this.services.getMasterProcedureExecutor();
      GCMultipleMergedRegionsProcedure mergeRegionProcedure =
          new GCMultipleMergedRegionsProcedure(pe.getEnvironment(), mergedRegion, parents);
      pe.submitProcedure(mergeRegionProcedure);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Submitted procedure {} for merged region {}", mergeRegionProcedure,
          mergedRegion);
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

  static boolean cleanParent(MasterServices services, RegionInfo parent, Result rowContent)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning parent region {}", parent);
    }
    // Check whether it is a merged region and if it is clean of references.
    if (MetaTableAccessor.hasMergeRegions(rowContent.rawCells())) {
      // Wait until clean of merge parent regions first
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region {} has merge parents, cleaning them first", parent);
      }
      return false;
    }
    // Run checks on each daughter split.
    PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(rowContent);
    Pair<Boolean, Boolean> a = checkDaughterInFs(services, parent, daughters.getFirst());
    Pair<Boolean, Boolean> b = checkDaughterInFs(services, parent, daughters.getSecond());
    if (hasNoReferences(a) && hasNoReferences(b)) {
      String daughterA =
        daughters.getFirst() != null ? daughters.getFirst().getShortNameToLog() : "null";
      String daughterB =
        daughters.getSecond() != null ? daughters.getSecond().getShortNameToLog() : "null";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting region " + parent.getShortNameToLog() + " because daughters -- " +
          daughterA + ", " + daughterB + " -- no longer hold references");
      }
      ProcedureExecutor<MasterProcedureEnv> pe = services.getMasterProcedureExecutor();
      GCRegionProcedure gcRegionProcedure = new GCRegionProcedure(pe.getEnvironment(), parent);
      pe.submitProcedure(gcRegionProcedure);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Submitted procedure {} for split parent {}", gcRegionProcedure, parent);
      }
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        if (!hasNoReferences(a)) {
          LOG.debug("Deferring removal of region {} because daughter {} still has references",
            parent, daughters.getFirst());
        }
        if (!hasNoReferences(b)) {
          LOG.debug("Deferring removal of region {} because daughter {} still has references",
            parent, daughters.getSecond());
        }
      }
    }
    return false;
  }

  /**
   * If daughters no longer hold reference to the parents, delete the parent.
   * @param parent RegionInfo of split offlined parent
   * @param rowContent Content of <code>parent</code> row in <code>metaRegionName</code>
   * @return True if we removed <code>parent</code> from meta table and from the filesystem.
   */
  private boolean cleanParent(final RegionInfo parent, Result rowContent) throws IOException {
    return cleanParent(services, parent, rowContent);
  }

  /**
   * @param p A pair where the first boolean says whether or not the daughter region directory
   *          exists in the filesystem and then the second boolean says whether the daughter has
   *          references to the parent.
   * @return True the passed <code>p</code> signifies no references.
   */
  private static boolean hasNoReferences(final Pair<Boolean, Boolean> p) {
    return !p.getFirst() || !p.getSecond();
  }

  /**
   * Checks if a daughter region -- either splitA or splitB -- still holds references to parent.
   * @param parent Parent region
   * @param daughter Daughter region
   * @return A pair where the first boolean says whether or not the daughter region directory exists
   *         in the filesystem and then the second boolean says whether the daughter has references
   *         to the parent.
   */
  private static Pair<Boolean, Boolean> checkDaughterInFs(MasterServices services,
    final RegionInfo parent, final RegionInfo daughter) throws IOException {
    if (daughter == null) {
      return new Pair<>(Boolean.FALSE, Boolean.FALSE);
    }

    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    Path rootdir = services.getMasterFileSystem().getRootDir();
    Path tabledir = CommonFSUtils.getTableDir(rootdir, daughter.getTable());

    Path daughterRegionDir = new Path(tabledir, daughter.getEncodedName());

    HRegionFileSystem regionFs;

    try {
      if (!CommonFSUtils.isExists(fs, daughterRegionDir)) {
        return new Pair<>(Boolean.FALSE, Boolean.FALSE);
      }
    } catch (IOException ioe) {
      LOG.error("Error trying to determine if daughter region exists, " +
        "assuming exists and has references", ioe);
      return new Pair<>(Boolean.TRUE, Boolean.TRUE);
    }

    boolean references = false;
    TableDescriptor parentDescriptor = services.getTableDescriptors().get(parent.getTable());
    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(services.getConfiguration(), fs,
        tabledir, daughter, true);

      for (ColumnFamilyDescriptor family : parentDescriptor.getColumnFamilies()) {
        references = regionFs.hasReferences(family.getNameAsString());
        if (references) {
          break;
        }
      }
    } catch (IOException e) {
      LOG.error("Error trying to determine referenced files from : " + daughter.getEncodedName() +
        ", to: " + parent.getEncodedName() + " assuming has references", e);
      return new Pair<>(Boolean.TRUE, Boolean.TRUE);
    }
    return new Pair<>(Boolean.TRUE, references);
  }

  private TableDescriptor getDescriptor(final TableName tableName) throws IOException {
    return this.services.getTableDescriptors().get(tableName);
  }

  private void updateAssignmentManagerMetrics() {
    services.getAssignmentManager().getAssignmentManagerMetrics()
        .updateHoles(lastReport.getHoles().size());
    services.getAssignmentManager().getAssignmentManagerMetrics()
        .updateOverlaps(lastReport.getOverlaps().size());
    services.getAssignmentManager().getAssignmentManagerMetrics()
        .updateUnknownServerRegions(lastReport.getUnknownServers().size());
    services.getAssignmentManager().getAssignmentManagerMetrics()
        .updateEmptyRegionInfoRegions(lastReport.getEmptyRegionInfo().size());
  }

  private static void checkLog4jProperties() {
    String filename = "log4j.properties";
    try (final InputStream inStream =
      CatalogJanitor.class.getClassLoader().getResourceAsStream(filename)) {
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
   * For testing against a cluster. Doesn't have a MasterServices context so does not report on good
   * vs bad servers.
   */
  public static void main(String[] args) throws IOException {
    checkLog4jProperties();
    ReportMakingVisitor visitor = new ReportMakingVisitor(null);
    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean("hbase.defaults.for.version.skip", true);
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      /*
       * Used to generate an overlap.
       */
      Get g = new Get(Bytes.toBytes("t2,40,1564119846424.1db8c57d64e0733e0f027aaeae7a0bf0."));
      g.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      try (Table t = connection.getTable(TableName.META_TABLE_NAME)) {
        Result r = t.get(g);
        byte[] row = g.getRow();
        row[row.length - 2] <<= row[row.length - 2];
        Put p = new Put(g.getRow());
        p.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
        t.put(p);
      }
      MetaTableAccessor.scanMetaForTableRegions(connection, visitor, null);
      Report report = visitor.getReport();
      LOG.info(report != null ? report.toString() : "empty");
    }
  }
}
