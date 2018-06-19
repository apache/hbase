/**
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
package org.apache.hadoop.hbase.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.GCMergedRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.GCRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A janitor for the catalog tables.  Scans the <code>hbase:meta</code> catalog
 * table on a period looking for unused regions to garbage collect.
 */
@InterfaceAudience.Private
public class CatalogJanitor extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogJanitor.class.getName());

  private final AtomicBoolean alreadyRunning = new AtomicBoolean(false);
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private final MasterServices services;
  private final Connection connection;
  // PID of the last Procedure launched herein. Keep around for Tests.

  CatalogJanitor(final MasterServices services) {
    super("CatalogJanitor-" + services.getServerName().toShortString(), services,
      services.getConfiguration().getInt("hbase.catalogjanitor.interval", 300000));
    this.services = services;
    this.connection = services.getConnection();
  }

  @Override
  protected boolean initialChore() {
    try {
      if (this.enabled.get()) scan();
    } catch (IOException e) {
      LOG.warn("Failed initial scan of catalog table", e);
      return false;
    }
    return true;
  }

  /**
   * @param enabled
   */
  public boolean setEnabled(final boolean enabled) {
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
      if (this.enabled.get() && !this.services.isInMaintenanceMode() && am != null &&
        am.isMetaLoaded() && !am.hasRegionsInTransition()) {
        scan();
      } else {
        LOG.warn("CatalogJanitor is disabled! Enabled=" + this.enabled.get() +
          ", maintenanceMode=" + this.services.isInMaintenanceMode() + ", am=" + am +
          ", metaLoaded=" + (am != null && am.isMetaLoaded()) + ", hasRIT=" +
          (am != null && am.hasRegionsInTransition()));
      }
    } catch (IOException e) {
      LOG.warn("Failed scan of catalog table", e);
    }
  }

  /**
   * Scans hbase:meta and returns a number of scanned rows, and a map of merged
   * regions, and an ordered map of split parents.
   * @return triple of scanned rows, map of merged regions and map of split
   *         parent regioninfos
   * @throws IOException
   */
  Triple<Integer, Map<RegionInfo, Result>, Map<RegionInfo, Result>>
    getMergedRegionsAndSplitParents() throws IOException {
    return getMergedRegionsAndSplitParents(null);
  }

  /**
   * Scans hbase:meta and returns a number of scanned rows, and a map of merged
   * regions, and an ordered map of split parents. if the given table name is
   * null, return merged regions and split parents of all tables, else only the
   * specified table
   * @param tableName null represents all tables
   * @return triple of scanned rows, and map of merged regions, and map of split
   *         parent regioninfos
   * @throws IOException
   */
  Triple<Integer, Map<RegionInfo, Result>, Map<RegionInfo, Result>>
    getMergedRegionsAndSplitParents(final TableName tableName) throws IOException {
    final boolean isTableSpecified = (tableName != null);
    // TODO: Only works with single hbase:meta region currently.  Fix.
    final AtomicInteger count = new AtomicInteger(0);
    // Keep Map of found split parents.  There are candidates for cleanup.
    // Use a comparator that has split parents come before its daughters.
    final Map<RegionInfo, Result> splitParents = new TreeMap<>(new SplitParentFirstComparator());
    final Map<RegionInfo, Result> mergedRegions = new TreeMap<>(RegionInfo.COMPARATOR);
    // This visitor collects split parents and counts rows in the hbase:meta table

    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        count.incrementAndGet();
        RegionInfo info = MetaTableAccessor.getRegionInfo(r);
        if (info == null) return true; // Keep scanning
        if (isTableSpecified
            && info.getTable().compareTo(tableName) > 0) {
          // Another table, stop scanning
          return false;
        }
        if (LOG.isTraceEnabled()) LOG.trace("" + info + " IS-SPLIT_PARENT=" + info.isSplitParent());
        if (info.isSplitParent()) splitParents.put(info, r);
        if (r.getValue(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER) != null) {
          mergedRegions.put(info, r);
        }
        // Returning true means "keep scanning"
        return true;
      }
    };

    // Run full scan of hbase:meta catalog table passing in our custom visitor with
    // the start row
    MetaTableAccessor.scanMetaForTableRegions(this.connection, visitor, tableName);

    return new Triple<>(count.get(), mergedRegions, splitParents);
  }

  /**
   * If merged region no longer holds reference to the merge regions, archive
   * merge region on hdfs and perform deleting references in hbase:meta
   * @param mergedRegion
   * @return true if we delete references in merged region on hbase:meta and archive
   *         the files on the file system
   * @throws IOException
   */
  boolean cleanMergeRegion(final RegionInfo mergedRegion,
      final RegionInfo regionA, final RegionInfo regionB) throws IOException {
    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, mergedRegion.getTable());
    TableDescriptor htd = getTableDescriptor(mergedRegion.getTable());
    HRegionFileSystem regionFs = null;
    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(
          this.services.getConfiguration(), fs, tabledir, mergedRegion, true);
    } catch (IOException e) {
      LOG.warn("Merged region does not exist: " + mergedRegion.getEncodedName());
    }
    if (regionFs == null || !regionFs.hasReferences(htd)) {
      LOG.debug("Deleting region " + regionA.getShortNameToLog() + " and "
          + regionB.getShortNameToLog()
          + " from fs because merged region no longer holds references");
      ProcedureExecutor<MasterProcedureEnv> pe = this.services.getMasterProcedureExecutor();
      pe.submitProcedure(new GCMergedRegionsProcedure(pe.getEnvironment(),
          mergedRegion, regionA, regionB));
      // Remove from in-memory states
      this.services.getAssignmentManager().getRegionStates().deleteRegion(regionA);
      this.services.getAssignmentManager().getRegionStates().deleteRegion(regionB);
      this.services.getServerManager().removeRegion(regionA);
      this.services.getServerManager().removeRegion(regionB);
      return true;
    }
    return false;
  }

  /**
   * Run janitorial scan of catalog <code>hbase:meta</code> table looking for
   * garbage to collect.
   * @return number of archiving jobs started.
   * @throws IOException
   */
  int scan() throws IOException {
    int result = 0;

    try {
      if (!alreadyRunning.compareAndSet(false, true)) {
        LOG.debug("CatalogJanitor already running");
        return result;
      }
      Triple<Integer, Map<RegionInfo, Result>, Map<RegionInfo, Result>> scanTriple =
        getMergedRegionsAndSplitParents();
      /**
       * clean merge regions first
       */
      Map<RegionInfo, Result> mergedRegions = scanTriple.getSecond();
      for (Map.Entry<RegionInfo, Result> e : mergedRegions.entrySet()) {
        if (this.services.isInMaintenanceMode()) {
          // Stop cleaning if the master is in maintenance mode
          break;
        }

        PairOfSameType<RegionInfo> p = MetaTableAccessor.getMergeRegions(e.getValue());
        RegionInfo regionA = p.getFirst();
        RegionInfo regionB = p.getSecond();
        if (regionA == null || regionB == null) {
          LOG.warn("Unexpected references regionA="
              + (regionA == null ? "null" : regionA.getShortNameToLog())
              + ",regionB="
              + (regionB == null ? "null" : regionB.getShortNameToLog())
              + " in merged region " + e.getKey().getShortNameToLog());
        } else {
          if (cleanMergeRegion(e.getKey(), regionA, regionB)) {
            result++;
          }
        }
      }
      /**
       * clean split parents
       */
      Map<RegionInfo, Result> splitParents = scanTriple.getThird();

      // Now work on our list of found parents. See if any we can clean up.
      // regions whose parents are still around
      HashSet<String> parentNotCleaned = new HashSet<>();
      for (Map.Entry<RegionInfo, Result> e : splitParents.entrySet()) {
        if (this.services.isInMaintenanceMode()) {
          // Stop cleaning if the master is in maintenance mode
          break;
        }

        if (!parentNotCleaned.contains(e.getKey().getEncodedName()) &&
            cleanParent(e.getKey(), e.getValue())) {
          result++;
        } else {
          // We could not clean the parent, so it's daughters should not be
          // cleaned either (HBASE-6160)
          PairOfSameType<RegionInfo> daughters =
              MetaTableAccessor.getDaughterRegions(e.getValue());
          parentNotCleaned.add(daughters.getFirst().getEncodedName());
          parentNotCleaned.add(daughters.getSecond().getEncodedName());
        }
      }
      return result;
    } finally {
      alreadyRunning.set(false);
    }
  }

  /**
   * Compare HRegionInfos in a way that has split parents sort BEFORE their
   * daughters.
   */
  static class SplitParentFirstComparator implements Comparator<RegionInfo> {
    Comparator<byte[]> rowEndKeyComparator = new Bytes.RowEndKeyComparator();
    @Override
    public int compare(RegionInfo left, RegionInfo right) {
      // This comparator differs from the one RegionInfo in that it sorts
      // parent before daughters.
      if (left == null) return -1;
      if (right == null) return 1;
      // Same table name.
      int result = left.getTable().compareTo(right.getTable());
      if (result != 0) return result;
      // Compare start keys.
      result = Bytes.compareTo(left.getStartKey(), right.getStartKey());
      if (result != 0) return result;
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
   * @throws IOException
   */
  boolean cleanParent(final RegionInfo parent, Result rowContent)
  throws IOException {
    // Check whether it is a merged region and not clean reference
    // No necessary to check MERGEB_QUALIFIER because these two qualifiers will
    // be inserted/deleted together
    if (rowContent.getValue(HConstants.CATALOG_FAMILY, HConstants.MERGEA_QUALIFIER) != null) {
      // wait cleaning merge region first
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
   * region directory exists in the filesystem and then the second boolean says
   * whether the daughter has references to the parent.
   * @throws IOException
   */
  Pair<Boolean, Boolean> checkDaughterInFs(final RegionInfo parent, final RegionInfo daughter)
  throws IOException {
    if (daughter == null)  {
      return new Pair<>(Boolean.FALSE, Boolean.FALSE);
    }

    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, daughter.getTable());

    Path daughterRegionDir = new Path(tabledir, daughter.getEncodedName());

    HRegionFileSystem regionFs = null;

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
    TableDescriptor parentDescriptor = getTableDescriptor(parent.getTable());
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
    return new Pair<>(Boolean.TRUE, Boolean.valueOf(references));
  }

  private TableDescriptor getTableDescriptor(final TableName tableName)
      throws FileNotFoundException, IOException {
    return this.services.getTableDescriptors().get(tableName);
  }

  /**
   * Checks if the specified region has merge qualifiers, if so, try to clean
   * them
   * @param region
   * @return true if the specified region doesn't have merge qualifier now
   * @throws IOException
   */
  public boolean cleanMergeQualifier(final RegionInfo region)
      throws IOException {
    // Get merge regions if it is a merged region and already has merge
    // qualifier
    Pair<RegionInfo, RegionInfo> mergeRegions = MetaTableAccessor
        .getRegionsFromMergeQualifier(this.services.getConnection(),
          region.getRegionName());
    if (mergeRegions == null
        || (mergeRegions.getFirst() == null && mergeRegions.getSecond() == null)) {
      // It doesn't have merge qualifier, no need to clean
      return true;
    }
    // It shouldn't happen, we must insert/delete these two qualifiers together
    if (mergeRegions.getFirst() == null || mergeRegions.getSecond() == null) {
      LOG.error("Merged region " + region.getRegionNameAsString()
          + " has only one merge qualifier in META.");
      return false;
    }
    return cleanMergeRegion(region, mergeRegions.getFirst(),
        mergeRegions.getSecond());
  }
}
