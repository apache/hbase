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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Triple;

/**
 * A janitor for the catalog tables.  Scans the <code>hbase:meta</code> catalog
 * table on a period looking for unused regions to garbage collect.
 */
@InterfaceAudience.Private
public class CatalogJanitor extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(CatalogJanitor.class.getName());
  private final Server server;
  private final MasterServices services;
  private AtomicBoolean enabled = new AtomicBoolean(true);
  private AtomicBoolean alreadyRunning = new AtomicBoolean(false);
  private final Connection connection;

  CatalogJanitor(final Server server, final MasterServices services) {
    super("CatalogJanitor-" + server.getServerName().toShortString(), server, server
        .getConfiguration().getInt("hbase.catalogjanitor.interval", 300000));
    this.server = server;
    this.services = services;
    this.connection = server.getConnection();
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
    return this.enabled.getAndSet(enabled);
  }

  boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  protected void chore() {
    try {
      if (this.enabled.get()) {
        scan();
      } else {
        LOG.warn("CatalogJanitor disabled! Not running scan.");
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
  Triple<Integer, Map<HRegionInfo, Result>, Map<HRegionInfo, Result>>
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
  Triple<Integer, Map<HRegionInfo, Result>, Map<HRegionInfo, Result>>
    getMergedRegionsAndSplitParents(final TableName tableName) throws IOException {
    final boolean isTableSpecified = (tableName != null);
    // TODO: Only works with single hbase:meta region currently.  Fix.
    final AtomicInteger count = new AtomicInteger(0);
    // Keep Map of found split parents.  There are candidates for cleanup.
    // Use a comparator that has split parents come before its daughters.
    final Map<HRegionInfo, Result> splitParents =
      new TreeMap<HRegionInfo, Result>(new SplitParentFirstComparator());
    final Map<HRegionInfo, Result> mergedRegions = new TreeMap<HRegionInfo, Result>();
    // This visitor collects split parents and counts rows in the hbase:meta table

    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        count.incrementAndGet();
        HRegionInfo info = MetaTableAccessor.getHRegionInfo(r);
        if (info == null) return true; // Keep scanning
        if (isTableSpecified
            && info.getTable().compareTo(tableName) > 0) {
          // Another table, stop scanning
          return false;
        }
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

    return new Triple<Integer, Map<HRegionInfo, Result>, Map<HRegionInfo, Result>>(
        count.get(), mergedRegions, splitParents);
  }

  /**
   * If merged region no longer holds reference to the merge regions, archive
   * merge region on hdfs and perform deleting references in hbase:meta
   * @param mergedRegion
   * @param regionA
   * @param regionB
   * @return true if we delete references in merged region on hbase:meta and archive
   *         the files on the file system
   * @throws IOException
   */
  boolean cleanMergeRegion(final HRegionInfo mergedRegion,
      final HRegionInfo regionA, final HRegionInfo regionB) throws IOException {
    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, mergedRegion.getTable());
    HTableDescriptor htd = getTableDescriptor(mergedRegion.getTable());
    HRegionFileSystem regionFs = null;
    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(
          this.services.getConfiguration(), fs, tabledir, mergedRegion, true);
    } catch (IOException e) {
      LOG.warn("Merged region does not exist: " + mergedRegion.getEncodedName());
    }
    if (regionFs == null || !regionFs.hasReferences(htd)) {
      LOG.debug("Deleting region " + regionA.getRegionNameAsString() + " and "
          + regionB.getRegionNameAsString()
          + " from fs because merged region no longer holds references");
      HFileArchiver.archiveRegion(this.services.getConfiguration(), fs, regionA);
      HFileArchiver.archiveRegion(this.services.getConfiguration(), fs, regionB);
      MetaTableAccessor.deleteMergeQualifiers(server.getConnection(),
        mergedRegion);
      return true;
    }
    return false;
  }

  /**
   * Run janitorial scan of catalog <code>hbase:meta</code> table looking for
   * garbage to collect.
   * @return number of cleaned regions
   * @throws IOException
   */
  int scan() throws IOException {
    try {
      if (!alreadyRunning.compareAndSet(false, true)) {
        return 0;
      }
      Triple<Integer, Map<HRegionInfo, Result>, Map<HRegionInfo, Result>> scanTriple =
        getMergedRegionsAndSplitParents();
      int count = scanTriple.getFirst();
      /**
       * clean merge regions first
       */
      int mergeCleaned = 0;
      Map<HRegionInfo, Result> mergedRegions = scanTriple.getSecond();
      for (Map.Entry<HRegionInfo, Result> e : mergedRegions.entrySet()) {
        PairOfSameType<HRegionInfo> p = MetaTableAccessor.getMergeRegions(e.getValue());
        HRegionInfo regionA = p.getFirst();
        HRegionInfo regionB = p.getSecond();
        if (regionA == null || regionB == null) {
          LOG.warn("Unexpected references regionA="
              + (regionA == null ? "null" : regionA.getRegionNameAsString())
              + ",regionB="
              + (regionB == null ? "null" : regionB.getRegionNameAsString())
              + " in merged region " + e.getKey().getRegionNameAsString());
        } else {
          if (cleanMergeRegion(e.getKey(), regionA, regionB)) {
            mergeCleaned++;
          }
        }
      }
      /**
       * clean split parents
       */
      Map<HRegionInfo, Result> splitParents = scanTriple.getThird();

      // Now work on our list of found parents. See if any we can clean up.
      int splitCleaned = 0;
      // regions whose parents are still around
      HashSet<String> parentNotCleaned = new HashSet<String>();
      for (Map.Entry<HRegionInfo, Result> e : splitParents.entrySet()) {
        if (!parentNotCleaned.contains(e.getKey().getEncodedName()) &&
            cleanParent(e.getKey(), e.getValue())) {
          splitCleaned++;
        } else {
          // We could not clean the parent, so it's daughters should not be
          // cleaned either (HBASE-6160)
          PairOfSameType<HRegionInfo> daughters =
              MetaTableAccessor.getDaughterRegions(e.getValue());
          parentNotCleaned.add(daughters.getFirst().getEncodedName());
          parentNotCleaned.add(daughters.getSecond().getEncodedName());
        }
      }
      if ((mergeCleaned + splitCleaned) != 0) {
        LOG.info("Scanned " + count + " catalog row(s), gc'd " + mergeCleaned
            + " unreferenced merged region(s) and " + splitCleaned
            + " unreferenced parent region(s)");
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("Scanned " + count + " catalog row(s), gc'd " + mergeCleaned
            + " unreferenced merged region(s) and " + splitCleaned
            + " unreferenced parent region(s)");
      }
      return mergeCleaned + splitCleaned;
    } finally {
      alreadyRunning.set(false);
    }
  }

  /**
   * Compare HRegionInfos in a way that has split parents sort BEFORE their
   * daughters.
   */
  static class SplitParentFirstComparator implements Comparator<HRegionInfo> {
    Comparator<byte[]> rowEndKeyComparator = new Bytes.RowEndKeyComparator();
    @Override
    public int compare(HRegionInfo left, HRegionInfo right) {
      // This comparator differs from the one HRegionInfo in that it sorts
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
   * @param parent HRegionInfo of split offlined parent
   * @param rowContent Content of <code>parent</code> row in
   * <code>metaRegionName</code>
   * @return True if we removed <code>parent</code> from meta table and from
   * the filesystem.
   * @throws IOException
   */
  boolean cleanParent(final HRegionInfo parent, Result rowContent)
  throws IOException {
    boolean result = false;
    // Check whether it is a merged region and not clean reference
    // No necessary to check MERGEB_QUALIFIER because these two qualifiers will
    // be inserted/deleted together
    if (rowContent.getValue(HConstants.CATALOG_FAMILY,
        HConstants.MERGEA_QUALIFIER) != null) {
      // wait cleaning merge region first
      return result;
    }
    // Run checks on each daughter split.
    PairOfSameType<HRegionInfo> daughters = MetaTableAccessor.getDaughterRegions(rowContent);
    Pair<Boolean, Boolean> a = checkDaughterInFs(parent, daughters.getFirst());
    Pair<Boolean, Boolean> b = checkDaughterInFs(parent, daughters.getSecond());
    if (hasNoReferences(a) && hasNoReferences(b)) {
      LOG.debug("Deleting region " + parent.getRegionNameAsString() +
        " because daughter splits no longer hold references");
      FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
      if (LOG.isTraceEnabled()) LOG.trace("Archiving parent region: " + parent);
      HFileArchiver.archiveRegion(this.services.getConfiguration(), fs, parent);
      MetaTableAccessor.deleteRegion(this.connection, parent);
      result = true;
    }
    return result;
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
  Pair<Boolean, Boolean> checkDaughterInFs(final HRegionInfo parent, final HRegionInfo daughter)
  throws IOException {
    if (daughter == null)  {
      return new Pair<Boolean, Boolean>(Boolean.FALSE, Boolean.FALSE);
    }

    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = FSUtils.getTableDir(rootdir, daughter.getTable());

    Path daughterRegionDir = new Path(tabledir, daughter.getEncodedName());

    HRegionFileSystem regionFs = null;

    try {
      if (!FSUtils.isExists(fs, daughterRegionDir)) {
        return new Pair<Boolean, Boolean>(Boolean.FALSE, Boolean.FALSE);
      }
    } catch (IOException ioe) {
      LOG.warn("Error trying to determine if daughter region exists, " +
               "assuming exists and has references", ioe);
      return new Pair<Boolean, Boolean>(Boolean.TRUE, Boolean.TRUE);
    }

    try {
      regionFs = HRegionFileSystem.openRegionFromFileSystem(
          this.services.getConfiguration(), fs, tabledir, daughter, true);
    } catch (IOException e) {
      LOG.warn("Error trying to determine referenced files from : " + daughter.getEncodedName()
          + ", to: " + parent.getEncodedName() + " assuming has references", e);
      return new Pair<Boolean, Boolean>(Boolean.TRUE, Boolean.TRUE);
    }

    boolean references = false;
    HTableDescriptor parentDescriptor = getTableDescriptor(parent.getTable());
    for (HColumnDescriptor family: parentDescriptor.getFamilies()) {
      if ((references = regionFs.hasReferences(family.getNameAsString()))) {
        break;
      }
    }
    return new Pair<Boolean, Boolean>(Boolean.TRUE, Boolean.valueOf(references));
  }

  private HTableDescriptor getTableDescriptor(final TableName tableName)
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
  public boolean cleanMergeQualifier(final HRegionInfo region)
      throws IOException {
    // Get merge regions if it is a merged region and already has merge
    // qualifier
    Pair<HRegionInfo, HRegionInfo> mergeRegions = MetaTableAccessor
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