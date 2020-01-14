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
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.util.Bytes;
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
    // Run the ReplicationBarrierCleaner here; it may clear out rep_barrier rows which
    // can help cleaning up damaged hbase:meta.
    this.masterServices.runReplicationBarrierCleaner();
  }

  /**
   * If hole, it papers it over by adding a region in the filesystem and to hbase:meta.
   * Does not assign.
   */
  void fixHoles(CatalogJanitor.Report report) {
    final List<Pair<RegionInfo, RegionInfo>> holes = report.getHoles();
    if (holes.isEmpty()) {
      LOG.info("CatalogJanitor Report contains no holes to fix. Skipping.");
      return;
    }

    LOG.info("Identified {} region holes to fix. Detailed fixup progress logged at DEBUG.",
      holes.size());

    final List<RegionInfo> newRegionInfos = createRegionInfosForHoles(holes);
    final List<RegionInfo> newMetaEntries = createMetaEntries(masterServices, newRegionInfos);
    final TransitRegionStateProcedure[] assignProcedures = masterServices
      .getAssignmentManager()
      .createRoundRobinAssignProcedures(newMetaEntries);

    masterServices.getMasterProcedureExecutor().submitProcedures(assignProcedures);
    LOG.info(
      "Scheduled {}/{} new regions for assignment.", assignProcedures.length, holes.size());
  }

  /**
   * Create a new {@link RegionInfo} corresponding to each provided "hole" pair.
   */
  private static List<RegionInfo> createRegionInfosForHoles(
    final List<Pair<RegionInfo, RegionInfo>> holes) {
    final List<RegionInfo> newRegionInfos = holes.stream()
      .map(MetaFixer::getHoleCover)
      .filter(Optional::isPresent)
      .map(Optional::get)
      .collect(Collectors.toList());
    LOG.debug("Constructed {}/{} RegionInfo descriptors corresponding to identified holes.",
      newRegionInfos.size(), holes.size());
    return newRegionInfos;
  }

  /**
   * @return Attempts to calculate a new {@link RegionInfo} that covers the region range described
   *   in {@code hole}.
   */
  private static Optional<RegionInfo> getHoleCover(Pair<RegionInfo, RegionInfo> hole) {
    final RegionInfo left = hole.getFirst();
    final RegionInfo right = hole.getSecond();

    if (left.getTable().equals(right.getTable())) {
      // Simple case.
      if (Bytes.compareTo(left.getEndKey(), right.getStartKey()) >= 0) {
        LOG.warn("Skipping hole fix; left-side endKey is not less than right-side startKey;"
          + " left=<{}>, right=<{}>", left, right);
        return Optional.empty();
      }
      return Optional.of(buildRegionInfo(left.getTable(), left.getEndKey(), right.getStartKey()));
    }

    final boolean leftUndefined = left.equals(RegionInfo.UNDEFINED);
    final boolean rightUndefined = right.equals(RegionInfo.UNDEFINED);
    final boolean last = left.isLast();
    final boolean first = right.isFirst();
    if (leftUndefined && rightUndefined) {
      LOG.warn("Skipping hole fix; both the hole left-side and right-side RegionInfos are " +
        "UNDEFINED; left=<{}>, right=<{}>", left, right);
      return Optional.empty();
    }
    if (leftUndefined || last) {
      return Optional.of(
        buildRegionInfo(right.getTable(), HConstants.EMPTY_START_ROW, right.getStartKey()));
    }
    if (rightUndefined || first) {
      return Optional.of(
        buildRegionInfo(left.getTable(), left.getEndKey(), HConstants.EMPTY_END_ROW));
    }
    LOG.warn("Skipping hole fix; don't know what to do with left=<{}>, right=<{}>", left, right);
    return Optional.empty();
  }

  private static RegionInfo buildRegionInfo(TableName tn, byte [] start, byte [] end) {
    return RegionInfoBuilder.newBuilder(tn).setStartKey(start).setEndKey(end).build();
  }

  /**
   * Create entries in the {@code hbase:meta} for each provided {@link RegionInfo}. Best effort.
   * @param masterServices used to connect to {@code hbase:meta}
   * @param newRegionInfos the new {@link RegionInfo} entries to add to the filesystem
   * @return a list of {@link RegionInfo} entries for which {@code hbase:meta} entries were
   *   successfully created
   */
  private static List<RegionInfo> createMetaEntries(final MasterServices masterServices,
    final List<RegionInfo> newRegionInfos) {

    final List<Either<RegionInfo, IOException>> addMetaEntriesResults = newRegionInfos.stream()
      .map(regionInfo -> {
        try {
          MetaTableAccessor.addRegionToMeta(masterServices.getConnection(), regionInfo);
          masterServices.getAssignmentManager()
            .getRegionStates()
            .updateRegionState(regionInfo, RegionState.State.CLOSED);
          return Either.<RegionInfo, IOException>ofLeft(regionInfo);
        } catch (IOException e) {
          return Either.<RegionInfo, IOException>ofRight(e);
        }
      })
      .collect(Collectors.toList());
    final List<RegionInfo> createMetaEntriesSuccesses = addMetaEntriesResults.stream()
      .filter(Either::hasLeft)
      .map(Either::getLeft)
      .collect(Collectors.toList());
    final List<IOException> createMetaEntriesFailures = addMetaEntriesResults.stream()
      .filter(Either::hasRight)
      .map(Either::getRight)
      .collect(Collectors.toList());
    LOG.debug("Added {}/{} entries to hbase:meta",
      createMetaEntriesSuccesses.size(), newRegionInfos.size());

    if (!createMetaEntriesFailures.isEmpty()) {
      LOG.warn("Failed to create entries in hbase:meta for {}/{} RegionInfo descriptors. First"
          + " failure message included; full list of failures with accompanying stack traces is"
          + " available at log level DEBUG. message={}", createMetaEntriesFailures.size(),
        addMetaEntriesResults.size(), createMetaEntriesFailures.get(0).getMessage());
      if (LOG.isDebugEnabled()) {
        createMetaEntriesFailures.forEach(
          ioe -> LOG.debug("Attempt to fix region hole in hbase:meta failed.", ioe));
      }
    }

    return createMetaEntriesSuccesses;
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

  /**
   * A union over {@link L} and {@link R}.
   */
  private static class Either<L, R> {
    private final L left;
    private final R right;

    public static <L, R> Either<L, R> ofLeft(L left) {
      return new Either<>(left, null);
    }

    public static <L, R> Either<L, R> ofRight(R right) {
      return new Either<>(null, right);
    }

    Either(L left, R right) {
      this.left = left;
      this.right = right;
    }

    public boolean hasLeft() {
      return left != null;
    }

    public L getLeft() {
      if (!hasLeft()) {
        throw new IllegalStateException("Either contains no left.");
      }
      return left;
    }

    public boolean hasRight() {
      return right != null;
    }

    public R getRight() {
      if (!hasRight()) {
        throw new IllegalStateException("Either contains no right.");
      }
      return right;
    }
  }
}
