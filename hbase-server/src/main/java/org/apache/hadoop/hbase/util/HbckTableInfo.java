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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandler;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandlerImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Ordering;
import org.apache.hbase.thirdparty.com.google.common.collect.TreeMultimap;

/**
 * Maintain information about a particular table.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HbckTableInfo {
  private static final Logger LOG = LoggerFactory.getLogger(HbckTableInfo.class.getName());

  private static final String TO_BE_LOADED = "to_be_loaded";

  TableName tableName;
  TreeSet<ServerName> deployedOn;

  // backwards regions
  final List<HbckRegionInfo> backwards = new ArrayList<>();

  // sidelined big overlapped regions
  final Map<Path, HbckRegionInfo> sidelinedRegions = new HashMap<>();

  // region split calculator
  final RegionSplitCalculator<HbckRegionInfo> sc =
      new RegionSplitCalculator<>(HbckRegionInfo.COMPARATOR);

  // Histogram of different TableDescriptors found.  Ideally there is only one!
  final Set<TableDescriptor> htds = new HashSet<>();

  // key = start split, values = set of splits in problem group
  final Multimap<byte[], HbckRegionInfo> overlapGroups =
      TreeMultimap.create(RegionSplitCalculator.BYTES_COMPARATOR, HbckRegionInfo.COMPARATOR);

  // list of regions derived from meta entries.
  private ImmutableList<RegionInfo> regionsFromMeta = null;

  HBaseFsck hbck;

  HbckTableInfo(TableName name, HBaseFsck hbck) {
    this.tableName = name;
    this.hbck = hbck;
    deployedOn = new TreeSet<>();
  }

  /**
   * @return descriptor common to all regions.  null if are none or multiple!
   */
  TableDescriptor getTableDescriptor() {
    if (htds.size() == 1) {
      return (TableDescriptor)htds.toArray()[0];
    } else {
      LOG.error("None/Multiple table descriptors found for table '"
          + tableName + "' regions: " + htds);
    }
    return null;
  }

  public void addRegionInfo(HbckRegionInfo hir) {
    if (Bytes.equals(hir.getEndKey(), HConstants.EMPTY_END_ROW)) {
      // end key is absolute end key, just add it.
      // ignore replicas other than primary for these checks
      if (hir.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
        sc.add(hir);
      }
      return;
    }

    // if not the absolute end key, check for cycle
    if (Bytes.compareTo(hir.getStartKey(), hir.getEndKey()) > 0) {
      hbck.getErrors().reportError(HbckErrorReporter.ERROR_CODE.REGION_CYCLE, String.format(
          "The endkey for this region comes before the " + "startkey, startkey=%s, endkey=%s",
          Bytes.toStringBinary(hir.getStartKey()), Bytes.toStringBinary(hir.getEndKey())), this,
          hir);
      backwards.add(hir);
      return;
    }

    // main case, add to split calculator
    // ignore replicas other than primary for these checks
    if (hir.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      sc.add(hir);
    }
  }

  public void addServer(ServerName server) {
    this.deployedOn.add(server);
  }

  public TableName getName() {
    return tableName;
  }

  public int getNumRegions() {
    return sc.getStarts().size() + backwards.size();
  }

  public synchronized ImmutableList<RegionInfo> getRegionsFromMeta(
      TreeMap<String, HbckRegionInfo> regionInfoMap) {
    // lazy loaded, synchronized to ensure a single load
    if (regionsFromMeta == null) {
      List<RegionInfo> regions = new ArrayList<>();
      for (HbckRegionInfo h : regionInfoMap.values()) {
        if (tableName.equals(h.getTableName())) {
          if (h.getMetaEntry() != null) {
            regions.add(h.getMetaEntry());
          }
        }
      }
      regionsFromMeta = Ordering.from(RegionInfo.COMPARATOR).immutableSortedCopy(regions);
    }

    return regionsFromMeta;
  }

  class IntegrityFixSuggester extends TableIntegrityErrorHandlerImpl {
    HbckErrorReporter errors;

    IntegrityFixSuggester(HbckTableInfo ti, HbckErrorReporter errors) {
      this.errors = errors;
      setTableInfo(ti);
    }

    @Override
    public void handleRegionStartKeyNotEmpty(HbckRegionInfo hi) throws IOException {
      errors.reportError(HbckErrorReporter.ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY,
          "First region should start with an empty key.  You need to "
              + " create a new region and regioninfo in HDFS to plug the hole.",
          getTableInfo(), hi);
    }

    @Override
    public void handleRegionEndKeyNotEmpty(byte[] curEndKey) throws IOException {
      errors.reportError(HbckErrorReporter.ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY,
          "Last region should end with an empty key. You need to "
              + "create a new region and regioninfo in HDFS to plug the hole.", getTableInfo());
    }

    @Override
    public void handleDegenerateRegion(HbckRegionInfo hi) throws IOException{
      errors.reportError(HbckErrorReporter.ERROR_CODE.DEGENERATE_REGION,
          "Region has the same start and end key.", getTableInfo(), hi);
    }

    @Override
    public void handleDuplicateStartKeys(HbckRegionInfo r1, HbckRegionInfo r2) throws IOException {
      byte[] key = r1.getStartKey();
      // dup start key
      errors.reportError(HbckErrorReporter.ERROR_CODE.DUPE_STARTKEYS,
          "Multiple regions have the same startkey: " + Bytes.toStringBinary(key), getTableInfo(),
          r1);
      errors.reportError(HbckErrorReporter.ERROR_CODE.DUPE_STARTKEYS,
          "Multiple regions have the same startkey: " + Bytes.toStringBinary(key), getTableInfo(),
          r2);
    }

    @Override
    public void handleSplit(HbckRegionInfo r1, HbckRegionInfo r2) throws IOException{
      byte[] key = r1.getStartKey();
      // dup start key
      errors.reportError(HbckErrorReporter.ERROR_CODE.DUPE_ENDKEYS,
          "Multiple regions have the same regionID: "
              + Bytes.toStringBinary(key), getTableInfo(), r1);
      errors.reportError(HbckErrorReporter.ERROR_CODE.DUPE_ENDKEYS,
          "Multiple regions have the same regionID: "
              + Bytes.toStringBinary(key), getTableInfo(), r2);
    }

    @Override
    public void handleOverlapInRegionChain(HbckRegionInfo hi1, HbckRegionInfo hi2)
        throws IOException {
      errors.reportError(HbckErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
          "There is an overlap in the region chain.", getTableInfo(), hi1, hi2);
    }

    @Override
    public void handleHoleInRegionChain(byte[] holeStart, byte[] holeStop) throws IOException {
      errors.reportError(
          HbckErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN,
          "There is a hole in the region chain between "
              + Bytes.toStringBinary(holeStart) + " and "
              + Bytes.toStringBinary(holeStop)
              + ".  You need to create a new .regioninfo and region "
              + "dir in hdfs to plug the hole.");
    }
  }

  /**
   * This handler fixes integrity errors from hdfs information.  There are
   * basically three classes of integrity problems 1) holes, 2) overlaps, and
   * 3) invalid regions.
   *
   * This class overrides methods that fix holes and the overlap group case.
   * Individual cases of particular overlaps are handled by the general
   * overlap group merge repair case.
   *
   * If hbase is online, this forces regions offline before doing merge
   * operations.
   */
  class HDFSIntegrityFixer extends IntegrityFixSuggester {
    Configuration conf;

    boolean fixOverlaps = true;

    HDFSIntegrityFixer(HbckTableInfo ti, HbckErrorReporter errors, Configuration conf,
        boolean fixHoles, boolean fixOverlaps) {
      super(ti, errors);
      this.conf = conf;
      this.fixOverlaps = fixOverlaps;
      // TODO properly use fixHoles
    }

    /**
     * This is a special case hole -- when the first region of a table is
     * missing from META, HBase doesn't acknowledge the existance of the
     * table.
     */
    @Override
    public void handleRegionStartKeyNotEmpty(HbckRegionInfo next) throws IOException {
      errors.reportError(HbckErrorReporter.ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY,
          "First region should start with an empty key.  Creating a new " +
              "region and regioninfo in HDFS to plug the hole.",
          getTableInfo(), next);
      TableDescriptor htd = getTableInfo().getTableDescriptor();
      // from special EMPTY_START_ROW to next region's startKey
      RegionInfo newRegion = RegionInfoBuilder.newBuilder(htd.getTableName())
          .setStartKey(HConstants.EMPTY_START_ROW)
          .setEndKey(next.getStartKey())
          .build();

      // TODO test
      HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
      LOG.info("Table region start key was not empty.  Created new empty region: "
          + newRegion + " " +region);
      hbck.fixes++;
    }

    @Override
    public void handleRegionEndKeyNotEmpty(byte[] curEndKey) throws IOException {
      errors.reportError(HbckErrorReporter.ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY,
          "Last region should end with an empty key.  Creating a new "
              + "region and regioninfo in HDFS to plug the hole.", getTableInfo());
      TableDescriptor htd = getTableInfo().getTableDescriptor();
      // from curEndKey to EMPTY_START_ROW
      RegionInfo newRegion = RegionInfoBuilder.newBuilder(htd.getTableName())
          .setStartKey(curEndKey)
          .setEndKey(HConstants.EMPTY_START_ROW)
          .build();

      HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
      LOG.info("Table region end key was not empty.  Created new empty region: " + newRegion
          + " " + region);
      hbck.fixes++;
    }

    /**
     * There is a hole in the hdfs regions that violates the table integrity
     * rules.  Create a new empty region that patches the hole.
     */
    @Override
    public void handleHoleInRegionChain(byte[] holeStartKey, byte[] holeStopKey)
        throws IOException {
      errors.reportError(HbckErrorReporter.ERROR_CODE.HOLE_IN_REGION_CHAIN,
          "There is a hole in the region chain between " + Bytes.toStringBinary(holeStartKey) +
              " and " + Bytes.toStringBinary(holeStopKey) +
              ".  Creating a new regioninfo and region " + "dir in hdfs to plug the hole.");
      TableDescriptor htd = getTableInfo().getTableDescriptor();
      RegionInfo newRegion =
          RegionInfoBuilder.newBuilder(htd.getTableName()).setStartKey(holeStartKey)
              .setEndKey(holeStopKey).build();
      HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
      LOG.info("Plugged hole by creating new empty region: " + newRegion + " " + region);
      hbck.fixes++;
    }

    /**
     * This takes set of overlapping regions and merges them into a single
     * region.  This covers cases like degenerate regions, shared start key,
     * general overlaps, duplicate ranges, and partial overlapping regions.
     *
     * Cases:
     * - Clean regions that overlap
     * - Only .oldlogs regions (can't find start/stop range, or figure out)
     *
     * This is basically threadsafe, except for the fixer increment in mergeOverlaps.
     */
    @Override
    public void handleOverlapGroup(Collection<HbckRegionInfo> overlap)
        throws IOException {
      Preconditions.checkNotNull(overlap);
      Preconditions.checkArgument(overlap.size() >0);

      if (!this.fixOverlaps) {
        LOG.warn("Not attempting to repair overlaps.");
        return;
      }

      if (overlap.size() > hbck.getMaxMerge()) {
        LOG.warn("Overlap group has " + overlap.size() + " overlapping " +
            "regions which is greater than " + hbck.getMaxMerge() +
            ", the max number of regions to merge");
        if (hbck.shouldSidelineBigOverlaps()) {
          // we only sideline big overlapped groups that exceeds the max number of regions to merge
          sidelineBigOverlaps(overlap);
        }
        return;
      }
      if (hbck.shouldRemoveParents()) {
        removeParentsAndFixSplits(overlap);
      }
      mergeOverlaps(overlap);
    }

    void removeParentsAndFixSplits(Collection<HbckRegionInfo> overlap) throws IOException {
      Pair<byte[], byte[]> range = null;
      HbckRegionInfo parent = null;
      HbckRegionInfo daughterA = null;
      HbckRegionInfo daughterB = null;
      Collection<HbckRegionInfo> daughters = new ArrayList<HbckRegionInfo>(overlap);

      String thread = Thread.currentThread().getName();
      LOG.info("== [" + thread + "] Attempting fix splits in overlap state.");

      // we only can handle a single split per group at the time
      if (overlap.size() > 3) {
        LOG.info("Too many overlaps were found on this group, falling back to regular merge.");
        return;
      }

      for (HbckRegionInfo hi : overlap) {
        if (range == null) {
          range = new Pair<byte[], byte[]>(hi.getStartKey(), hi.getEndKey());
        } else {
          if (RegionSplitCalculator.BYTES_COMPARATOR
              .compare(hi.getStartKey(), range.getFirst()) < 0) {
            range.setFirst(hi.getStartKey());
          }
          if (RegionSplitCalculator.BYTES_COMPARATOR
              .compare(hi.getEndKey(), range.getSecond()) > 0) {
            range.setSecond(hi.getEndKey());
          }
        }
      }

      LOG.info("This group range is [" + Bytes.toStringBinary(range.getFirst()) + ", "
          + Bytes.toStringBinary(range.getSecond()) + "]");

      // attempt to find a possible parent for the edge case of a split
      for (HbckRegionInfo hi : overlap) {
        if (Bytes.compareTo(hi.getHdfsHRI().getStartKey(), range.getFirst()) == 0
            && Bytes.compareTo(hi.getHdfsHRI().getEndKey(), range.getSecond()) == 0) {
          LOG.info("This is a parent for this group: " + hi.toString());
          parent = hi;
        }
      }

      // Remove parent regions from daughters collection
      if (parent != null) {
        daughters.remove(parent);
      }

      // Lets verify that daughters share the regionID at split time and they
      // were created after the parent
      for (HbckRegionInfo hi : daughters) {
        if (Bytes.compareTo(hi.getHdfsHRI().getStartKey(), range.getFirst()) == 0) {
          if (parent.getHdfsHRI().getRegionId() < hi.getHdfsHRI().getRegionId()) {
            daughterA = hi;
          }
        }
        if (Bytes.compareTo(hi.getHdfsHRI().getEndKey(), range.getSecond()) == 0) {
          if (parent.getHdfsHRI().getRegionId() < hi.getHdfsHRI().getRegionId()) {
            daughterB = hi;
          }
        }
      }

      // daughters must share the same regionID and we should have a parent too
      if (daughterA.getHdfsHRI().getRegionId() != daughterB.getHdfsHRI().getRegionId() ||
          parent == null) {
        return;
      }

      FileSystem fs = FileSystem.get(conf);
      LOG.info("Found parent: " + parent.getRegionNameAsString());
      LOG.info("Found potential daughter a: " + daughterA.getRegionNameAsString());
      LOG.info("Found potential daughter b: " + daughterB.getRegionNameAsString());
      LOG.info("Trying to fix parent in overlap by removing the parent.");
      try {
        hbck.closeRegion(parent);
      } catch (IOException ioe) {
        LOG.warn("Parent region could not be closed, continuing with regular merge...", ioe);
        return;
      } catch (InterruptedException ie) {
        LOG.warn("Parent region could not be closed, continuing with regular merge...", ie);
        return;
      }

      try {
        hbck.offline(parent.getRegionName());
      } catch (IOException ioe) {
        LOG.warn("Unable to offline parent region: " + parent.getRegionNameAsString()
            + ".  Just continuing with regular merge... ", ioe);
        return;
      }

      try {
        HBaseFsckRepair.removeParentInMeta(conf, parent.getHdfsHRI());
      } catch (IOException ioe) {
        LOG.warn("Unable to remove parent region in META: " + parent.getRegionNameAsString()
            + ".  Just continuing with regular merge... ", ioe);
        return;
      }

      hbck.sidelineRegionDir(fs, parent);
      LOG.info(
          "[" + thread + "] Sidelined parent region dir " + parent.getHdfsRegionDir() + " into " +
              hbck.getSidelineDir());
      hbck.debugLsr(parent.getHdfsRegionDir());

      // Make sure we don't have the parents and daughters around
      overlap.remove(parent);
      overlap.remove(daughterA);
      overlap.remove(daughterB);

      LOG.info("Done fixing split.");

    }

    void mergeOverlaps(Collection<HbckRegionInfo> overlap)
        throws IOException {
      String thread = Thread.currentThread().getName();
      LOG.info("== [" + thread + "] Merging regions into one region: "
          + Joiner.on(",").join(overlap));
      // get the min / max range and close all concerned regions
      Pair<byte[], byte[]> range = null;
      for (HbckRegionInfo hi : overlap) {
        if (range == null) {
          range = new Pair<>(hi.getStartKey(), hi.getEndKey());
        } else {
          if (RegionSplitCalculator.BYTES_COMPARATOR
              .compare(hi.getStartKey(), range.getFirst()) < 0) {
            range.setFirst(hi.getStartKey());
          }
          if (RegionSplitCalculator.BYTES_COMPARATOR
              .compare(hi.getEndKey(), range.getSecond()) > 0) {
            range.setSecond(hi.getEndKey());
          }
        }
        // need to close files so delete can happen.
        LOG.debug("[" + thread + "] Closing region before moving data around: " +  hi);
        LOG.debug("[" + thread + "] Contained region dir before close");
        hbck.debugLsr(hi.getHdfsRegionDir());
        try {
          LOG.info("[" + thread + "] Closing region: " + hi);
          hbck.closeRegion(hi);
        } catch (IOException ioe) {
          LOG.warn("[" + thread + "] Was unable to close region " + hi
              + ".  Just continuing... ", ioe);
        } catch (InterruptedException e) {
          LOG.warn("[" + thread + "] Was unable to close region " + hi
              + ".  Just continuing... ", e);
        }

        try {
          LOG.info("[" + thread + "] Offlining region: " + hi);
          hbck.offline(hi.getRegionName());
        } catch (IOException ioe) {
          LOG.warn("[" + thread + "] Unable to offline region from master: " + hi
              + ".  Just continuing... ", ioe);
        }
      }

      // create new empty container region.
      TableDescriptor htd = getTableInfo().getTableDescriptor();
      // from start key to end Key
      RegionInfo newRegion = RegionInfoBuilder.newBuilder(htd.getTableName())
          .setStartKey(range.getFirst())
          .setEndKey(range.getSecond())
          .build();
      HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
      LOG.info("[" + thread + "] Created new empty container region: " +
          newRegion + " to contain regions: " + Joiner.on(",").join(overlap));
      hbck.debugLsr(region.getRegionFileSystem().getRegionDir());

      // all target regions are closed, should be able to safely cleanup.
      boolean didFix= false;
      Path target = region.getRegionFileSystem().getRegionDir();
      for (HbckRegionInfo contained : overlap) {
        LOG.info("[" + thread + "] Merging " + contained  + " into " + target);
        int merges = hbck.mergeRegionDirs(target, contained);
        if (merges > 0) {
          didFix = true;
        }
      }
      if (didFix) {
        hbck.fixes++;
      }
    }

    /**
     * Sideline some regions in a big overlap group so that it
     * will have fewer regions, and it is easier to merge them later on.
     *
     * @param bigOverlap the overlapped group with regions more than maxMerge
     */
    void sidelineBigOverlaps(Collection<HbckRegionInfo> bigOverlap) throws IOException {
      int overlapsToSideline = bigOverlap.size() - hbck.getMaxMerge();
      if (overlapsToSideline > hbck.getMaxOverlapsToSideline()) {
        overlapsToSideline = hbck.getMaxOverlapsToSideline();
      }
      List<HbckRegionInfo> regionsToSideline =
          RegionSplitCalculator.findBigRanges(bigOverlap, overlapsToSideline);
      FileSystem fs = FileSystem.get(conf);
      for (HbckRegionInfo regionToSideline: regionsToSideline) {
        try {
          LOG.info("Closing region: " + regionToSideline);
          hbck.closeRegion(regionToSideline);
        } catch (IOException ioe) {
          LOG.warn("Was unable to close region " + regionToSideline
              + ".  Just continuing... ", ioe);
        } catch (InterruptedException e) {
          LOG.warn("Was unable to close region " + regionToSideline
              + ".  Just continuing... ", e);
        }

        try {
          LOG.info("Offlining region: " + regionToSideline);
          hbck.offline(regionToSideline.getRegionName());
        } catch (IOException ioe) {
          LOG.warn("Unable to offline region from master: " + regionToSideline
              + ".  Just continuing... ", ioe);
        }

        LOG.info("Before sideline big overlapped region: " + regionToSideline.toString());
        Path sidelineRegionDir = hbck.sidelineRegionDir(fs, TO_BE_LOADED, regionToSideline);
        if (sidelineRegionDir != null) {
          sidelinedRegions.put(sidelineRegionDir, regionToSideline);
          LOG.info("After sidelined big overlapped region: "
              + regionToSideline.getRegionNameAsString()
              + " to " + sidelineRegionDir.toString());
          hbck.fixes++;
        }
      }
    }
  }

  /**
   * Check the region chain (from META) of this table.  We are looking for
   * holes, overlaps, and cycles.
   * @return false if there are errors
   */
  public boolean checkRegionChain(TableIntegrityErrorHandler handler) throws IOException {
    // When table is disabled no need to check for the region chain. Some of the regions
    // accidently if deployed, this below code might report some issues like missing start
    // or end regions or region hole in chain and may try to fix which is unwanted.
    if (hbck.isTableDisabled(this.tableName)) {
      return true;
    }
    int originalErrorsCount = hbck.getErrors().getErrorList().size();
    Multimap<byte[], HbckRegionInfo> regions = sc.calcCoverage();
    SortedSet<byte[]> splits = sc.getSplits();

    byte[] prevKey = null;
    byte[] problemKey = null;

    if (splits.isEmpty()) {
      // no region for this table
      handler.handleHoleInRegionChain(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    }

    for (byte[] key : splits) {
      Collection<HbckRegionInfo> ranges = regions.get(key);
      if (prevKey == null && !Bytes.equals(key, HConstants.EMPTY_BYTE_ARRAY)) {
        for (HbckRegionInfo rng : ranges) {
          handler.handleRegionStartKeyNotEmpty(rng);
        }
      }

      // check for degenerate ranges
      for (HbckRegionInfo rng : ranges) {
        // special endkey case converts '' to null
        byte[] endKey = rng.getEndKey();
        endKey = (endKey.length == 0) ? null : endKey;
        if (Bytes.equals(rng.getStartKey(),endKey)) {
          handler.handleDegenerateRegion(rng);
        }
      }

      if (ranges.size() == 1) {
        // this split key is ok -- no overlap, not a hole.
        if (problemKey != null) {
          LOG.warn("reached end of problem group: " + Bytes.toStringBinary(key));
        }
        problemKey = null; // fell through, no more problem.
      } else if (ranges.size() > 1) {
        // set the new problem key group name, if already have problem key, just
        // keep using it.
        if (problemKey == null) {
          // only for overlap regions.
          LOG.warn("Naming new problem group: " + Bytes.toStringBinary(key));
          problemKey = key;
        }
        overlapGroups.putAll(problemKey, ranges);

        // record errors
        ArrayList<HbckRegionInfo> subRange = new ArrayList<>(ranges);
        //  this dumb and n^2 but this shouldn't happen often
        for (HbckRegionInfo r1 : ranges) {
          if (r1.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
            continue;
          }
          subRange.remove(r1);
          for (HbckRegionInfo r2 : subRange) {
            if (r2.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
              continue;
            }
            // general case of same start key
            if (Bytes.compareTo(r1.getStartKey(), r2.getStartKey())==0) {
              handler.handleDuplicateStartKeys(r1,r2);
            } else if (Bytes.compareTo(r1.getEndKey(), r2.getStartKey())==0 &&
                r1.getHdfsHRI().getRegionId() == r2.getHdfsHRI().getRegionId()) {
              LOG.info("this is a split, log to splits");
              handler.handleSplit(r1, r2);
            } else {
              // overlap
              handler.handleOverlapInRegionChain(r1, r2);
            }
          }
        }

      } else if (ranges.isEmpty()) {
        if (problemKey != null) {
          LOG.warn("reached end of problem group: " + Bytes.toStringBinary(key));
        }
        problemKey = null;

        byte[] holeStopKey = sc.getSplits().higher(key);
        // if higher key is null we reached the top.
        if (holeStopKey != null) {
          // hole
          handler.handleHoleInRegionChain(key, holeStopKey);
        }
      }
      prevKey = key;
    }

    // When the last region of a table is proper and having an empty end key, 'prevKey'
    // will be null.
    if (prevKey != null) {
      handler.handleRegionEndKeyNotEmpty(prevKey);
    }

    // TODO fold this into the TableIntegrityHandler
    if (hbck.getConf().getBoolean("hbasefsck.overlap.merge.parallel", true)) {
      boolean ok = handleOverlapsParallel(handler, prevKey);
      if (!ok) {
        return false;
      }
    } else {
      for (Collection<HbckRegionInfo> overlap : overlapGroups.asMap().values()) {
        handler.handleOverlapGroup(overlap);
      }
    }

    if (HBaseFsck.shouldDisplayFullReport()) {
      // do full region split map dump
      hbck.getErrors().print("---- Table '"  +  this.tableName
          + "': region split map");
      dump(splits, regions);
      hbck.getErrors().print("---- Table '"  +  this.tableName
          + "': overlap groups");
      dumpOverlapProblems(overlapGroups);
      hbck.getErrors().print("There are " + overlapGroups.keySet().size()
          + " overlap groups with " + overlapGroups.size()
          + " overlapping regions");
    }
    if (!sidelinedRegions.isEmpty()) {
      LOG.warn("Sidelined big overlapped regions, please bulk load them!");
      hbck.getErrors().print("---- Table '"  +  this.tableName
          + "': sidelined big overlapped regions");
      dumpSidelinedRegions(sidelinedRegions);
    }
    return hbck.getErrors().getErrorList().size() == originalErrorsCount;
  }

  private boolean handleOverlapsParallel(TableIntegrityErrorHandler handler, byte[] prevKey)
      throws IOException {
    // we parallelize overlap handler for the case we have lots of groups to fix.  We can
    // safely assume each group is independent.
    List<HBaseFsck.WorkItemOverlapMerge> merges = new ArrayList<>(overlapGroups.size());
    List<Future<Void>> rets;
    for (Collection<HbckRegionInfo> overlap : overlapGroups.asMap().values()) {
      //
      merges.add(new HBaseFsck.WorkItemOverlapMerge(overlap, handler));
    }
    try {
      rets = hbck.executor.invokeAll(merges);
    } catch (InterruptedException e) {
      LOG.error("Overlap merges were interrupted", e);
      return false;
    }
    for(int i=0; i<merges.size(); i++) {
      HBaseFsck.WorkItemOverlapMerge work = merges.get(i);
      Future<Void> f = rets.get(i);
      try {
        f.get();
      } catch(ExecutionException e) {
        LOG.warn("Failed to merge overlap group" + work, e.getCause());
      } catch (InterruptedException e) {
        LOG.error("Waiting for overlap merges was interrupted", e);
        return false;
      }
    }
    return true;
  }

  /**
   * This dumps data in a visually reasonable way for visual debugging
   */
  private void dump(SortedSet<byte[]> splits, Multimap<byte[], HbckRegionInfo> regions) {
    // we display this way because the last end key should be displayed as well.
    StringBuilder sb = new StringBuilder();
    for (byte[] k : splits) {
      sb.setLength(0); // clear out existing buffer, if any.
      sb.append(Bytes.toStringBinary(k) + ":\t");
      for (HbckRegionInfo r : regions.get(k)) {
        sb.append("[ "+ r.toString() + ", "
            + Bytes.toStringBinary(r.getEndKey())+ "]\t");
      }
      hbck.getErrors().print(sb.toString());
    }
  }

  private void dumpOverlapProblems(Multimap<byte[], HbckRegionInfo> regions) {
    // we display this way because the last end key should be displayed as
    // well.
    for (byte[] k : regions.keySet()) {
      hbck.getErrors().print(Bytes.toStringBinary(k) + ":");
      for (HbckRegionInfo r : regions.get(k)) {
        hbck.getErrors().print("[ " + r.toString() + ", "
            + Bytes.toStringBinary(r.getEndKey()) + "]");
      }
      hbck.getErrors().print("----");
    }
  }

  private void dumpSidelinedRegions(Map<Path, HbckRegionInfo> regions) {
    for (Map.Entry<Path, HbckRegionInfo> entry : regions.entrySet()) {
      TableName tableName = entry.getValue().getTableName();
      Path path = entry.getKey();
      hbck.getErrors().print("This sidelined region dir should be bulk loaded: " + path.toString());
      hbck.getErrors().print("Bulk load command looks like: " + BulkLoadHFilesTool.NAME + " " +
          path.toUri().getPath() + " " + tableName);
    }
  }
}
