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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.OPEN_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.StripeStoreFlusher;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ImmutableList;

/**
 * Stripe store implementation of compaction policy.
 */
@InterfaceAudience.Private
public class StripeCompactionPolicy extends CompactionPolicy {
  private final static Log LOG = LogFactory.getLog(StripeCompactionPolicy.class);
  // Policy used to compact individual stripes.
  private ExploringCompactionPolicy stripePolicy = null;

  private StripeStoreConfig config;

  public StripeCompactionPolicy(
      Configuration conf, StoreConfigInformation storeConfigInfo, StripeStoreConfig config) {
    super(conf, storeConfigInfo);
    this.config = config;
    stripePolicy = new ExploringCompactionPolicy(conf, storeConfigInfo);
  }

  public List<StoreFile> preSelectFilesForCoprocessor(StripeInformationProvider si,
      List<StoreFile> filesCompacting) {
    // We sincerely hope nobody is messing with us with their coprocessors.
    // If they do, they are very likely to shoot themselves in the foot.
    // We'll just exclude all the filesCompacting from the list.
    ArrayList<StoreFile> candidateFiles = new ArrayList<StoreFile>(si.getStorefiles());
    candidateFiles.removeAll(filesCompacting);
    return candidateFiles;
  }

  public StripeCompactionRequest createEmptyRequest(
      StripeInformationProvider si, CompactionRequest request) {
    // Treat as L0-ish compaction with fixed set of files, and hope for the best.
    if (si.getStripeCount() > 0) {
      return new BoundaryStripeCompactionRequest(request, si.getStripeBoundaries());
    }
    Pair<Long, Integer> targetKvsAndCount = estimateTargetKvs(
        request.getFiles(), this.config.getInitialCount());
    return new SplitStripeCompactionRequest(
        request, OPEN_KEY, OPEN_KEY, targetKvsAndCount.getSecond(), targetKvsAndCount.getFirst());
  }

  public StripeStoreFlusher.StripeFlushRequest selectFlush(KVComparator comparator,
      StripeInformationProvider si, int kvCount) {
    if (this.config.isUsingL0Flush()) {
      // L0 is used, return dumb request.
      return new StripeStoreFlusher.StripeFlushRequest(comparator);
    }
    if (si.getStripeCount() == 0) {
      // No stripes - start with the requisite count, derive KVs per stripe.
      int initialCount = this.config.getInitialCount();
      return new StripeStoreFlusher.SizeStripeFlushRequest(comparator, initialCount,
          kvCount / initialCount);
    }
    // There are stripes - do according to the boundaries.
    return new StripeStoreFlusher.BoundaryStripeFlushRequest(comparator, si.getStripeBoundaries());
  }

  public StripeCompactionRequest selectCompaction(StripeInformationProvider si,
      List<StoreFile> filesCompacting, boolean isOffpeak) throws IOException {
    // TODO: first cut - no parallel compactions. To have more fine grained control we
    //       probably need structure more sophisticated than a list.
    if (!filesCompacting.isEmpty()) {
      LOG.debug("Not selecting compaction: " + filesCompacting.size() + " files compacting");
      return null;
    }

    // We are going to do variations of compaction in strict order of preference.
    // A better/more advanced approach is to use a heuristic to see which one is "more
    // necessary" at current time.

    // This can happen due to region split. We can skip it later; for now preserve
    // compact-all-things behavior.
    Collection<StoreFile> allFiles = si.getStorefiles();
    if (StoreUtils.hasReferences(allFiles)) {
      LOG.debug("There are references in the store; compacting all files");
      long targetKvs = estimateTargetKvs(allFiles, config.getInitialCount()).getFirst();
      SplitStripeCompactionRequest request = new SplitStripeCompactionRequest(
          allFiles, OPEN_KEY, OPEN_KEY, targetKvs);
      request.setMajorRangeFull();
      return request;
    }

    int stripeCount = si.getStripeCount();
    List<StoreFile> l0Files = si.getLevel0Files();

    // See if we need to make new stripes.
    boolean shouldCompactL0 = (this.config.getLevel0MinFiles() <= l0Files.size());
    if (stripeCount == 0) {
      if (!shouldCompactL0) return null; // nothing to do.
      return selectNewStripesCompaction(si);
    }

    boolean canDropDeletesNoL0 = l0Files.size() == 0;
    if (shouldCompactL0) {
      if (!canDropDeletesNoL0) {
        // If we need to compact L0, see if we can add something to it, and drop deletes.
        StripeCompactionRequest result = selectSingleStripeCompaction(
            si, true, canDropDeletesNoL0, isOffpeak);
        if (result != null) return result;
      }
      LOG.debug("Selecting L0 compaction with " + l0Files.size() + " files");
      return new BoundaryStripeCompactionRequest(l0Files, si.getStripeBoundaries());
    }

    // Try to delete fully expired stripes
    StripeCompactionRequest result = selectExpiredMergeCompaction(si, canDropDeletesNoL0);
    if (result != null) return result;

    // Ok, nothing special here, let's see if we need to do a common compaction.
    // This will also split the stripes that are too big if needed.
    return selectSingleStripeCompaction(si, false, canDropDeletesNoL0, isOffpeak);
  }

  public boolean needsCompactions(StripeInformationProvider si, List<StoreFile> filesCompacting) {
    // Approximation on whether we need compaction.
    return filesCompacting.isEmpty()
        && (StoreUtils.hasReferences(si.getStorefiles())
          || (si.getLevel0Files().size() >= this.config.getLevel0MinFiles())
          || needsSingleStripeCompaction(si));
  }

  @Override
  public boolean isMajorCompaction(Collection<StoreFile> filesToCompact) throws IOException {
    return false; // there's never a major compaction!
  }

  @Override
  public boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  /**
   * @param si StoreFileManager.
   * @return Whether any stripe potentially needs compaction.
   */
  protected boolean needsSingleStripeCompaction(StripeInformationProvider si) {
    int minFiles = this.config.getStripeCompactMinFiles();
    for (List<StoreFile> stripe : si.getStripes()) {
      if (stripe.size() >= minFiles) return true;
    }
    return false;
  }

  protected StripeCompactionRequest selectSingleStripeCompaction(StripeInformationProvider si,
      boolean includeL0, boolean canDropDeletesWithoutL0, boolean isOffpeak) throws IOException {
    ArrayList<ImmutableList<StoreFile>> stripes = si.getStripes();

    int bqIndex = -1;
    List<StoreFile> bqSelection = null;
    int stripeCount = stripes.size();
    long bqTotalSize = -1;
    for (int i = 0; i < stripeCount; ++i) {
      // If we want to compact L0 to drop deletes, we only want whole-stripe compactions.
      // So, pass includeL0 as 2nd parameter to indicate that.
      List<StoreFile> selection = selectSimpleCompaction(stripes.get(i),
          !canDropDeletesWithoutL0 && includeL0, isOffpeak);
      if (selection.isEmpty()) continue;
      long size = 0;
      for (StoreFile sf : selection) {
        size += sf.getReader().length();
      }
      if (bqSelection == null || selection.size() > bqSelection.size() ||
          (selection.size() == bqSelection.size() && size < bqTotalSize)) {
        bqSelection = selection;
        bqIndex = i;
        bqTotalSize = size;
      }
    }
    if (bqSelection == null) {
      LOG.debug("No good compaction is possible in any stripe");
      return null;
    }
    List<StoreFile> filesToCompact = new ArrayList<StoreFile>(bqSelection);
    // See if we can, and need to, split this stripe.
    int targetCount = 1;
    long targetKvs = Long.MAX_VALUE;
    boolean hasAllFiles = filesToCompact.size() == stripes.get(bqIndex).size();
    String splitString = "";
    if (hasAllFiles && bqTotalSize >= config.getSplitSize()) {
      if (includeL0) {
        // We want to avoid the scenario where we compact a stripe w/L0 and then split it.
        // So, if we might split, don't compact the stripe with L0.
        return null;
      }
      Pair<Long, Integer> kvsAndCount = estimateTargetKvs(filesToCompact, config.getSplitCount());
      targetKvs = kvsAndCount.getFirst();
      targetCount = kvsAndCount.getSecond();
      splitString = "; the stripe will be split into at most "
          + targetCount + " stripes with " + targetKvs + " target KVs";
    }

    LOG.debug("Found compaction in a stripe with end key ["
        + Bytes.toString(si.getEndRow(bqIndex)) + "], with "
        + filesToCompact.size() + " files of total size " + bqTotalSize + splitString);

    // See if we can drop deletes.
    StripeCompactionRequest req;
    if (includeL0) {
      assert hasAllFiles;
      List<StoreFile> l0Files = si.getLevel0Files();
      LOG.debug("Adding " + l0Files.size() + " files to compaction to be able to drop deletes");
      ConcatenatedLists<StoreFile> sfs = new ConcatenatedLists<StoreFile>();
      sfs.addSublist(filesToCompact);
      sfs.addSublist(l0Files);
      req = new BoundaryStripeCompactionRequest(sfs, si.getStripeBoundaries());
    } else {
      req = new SplitStripeCompactionRequest(
          filesToCompact, si.getStartRow(bqIndex), si.getEndRow(bqIndex), targetCount, targetKvs);
    }
    if (hasAllFiles && (canDropDeletesWithoutL0 || includeL0)) {
      req.setMajorRange(si.getStartRow(bqIndex), si.getEndRow(bqIndex));
    }
    req.getRequest().setOffPeak(isOffpeak);
    return req;
  }

  /**
   * Selects the compaction of a single stripe using default policy.
   * @param sfs Files.
   * @param allFilesOnly Whether a compaction of all-or-none files is needed.
   * @return The resulting selection.
   */
  private List<StoreFile> selectSimpleCompaction(
      List<StoreFile> sfs, boolean allFilesOnly, boolean isOffpeak) {
    int minFilesLocal = Math.max(
        allFilesOnly ? sfs.size() : 0, this.config.getStripeCompactMinFiles());
    int maxFilesLocal = Math.max(this.config.getStripeCompactMaxFiles(), minFilesLocal);
    return stripePolicy.applyCompactionPolicy(sfs, false, isOffpeak, minFilesLocal, maxFilesLocal);
  }

  /**
   * Selects the compaction that compacts all files (to be removed later).
   * @param si StoreFileManager.
   * @param targetStripeCount Target stripe count.
   * @param targetSize Target stripe size.
   * @return The compaction.
   */
  private StripeCompactionRequest selectCompactionOfAllFiles(StripeInformationProvider si,
      int targetStripeCount, long targetSize) {
    Collection<StoreFile> allFiles = si.getStorefiles();
    SplitStripeCompactionRequest request = new SplitStripeCompactionRequest(
        allFiles, OPEN_KEY, OPEN_KEY, targetStripeCount, targetSize);
    request.setMajorRangeFull();
    LOG.debug("Selecting a compaction that includes all " + allFiles.size() + " files");
    return request;
  }

  private StripeCompactionRequest selectNewStripesCompaction(StripeInformationProvider si) {
    List<StoreFile> l0Files = si.getLevel0Files();
    Pair<Long, Integer> kvsAndCount = estimateTargetKvs(l0Files, config.getInitialCount());
    LOG.debug("Creating " + kvsAndCount.getSecond() + " initial stripes with "
        + kvsAndCount.getFirst() + " kvs each via L0 compaction of " + l0Files.size() + " files");
    SplitStripeCompactionRequest request = new SplitStripeCompactionRequest(
        si.getLevel0Files(), OPEN_KEY, OPEN_KEY, kvsAndCount.getSecond(), kvsAndCount.getFirst());
    request.setMajorRangeFull(); // L0 only, can drop deletes.
    return request;
  }

  private StripeCompactionRequest selectExpiredMergeCompaction(
      StripeInformationProvider si, boolean canDropDeletesNoL0) {
    long cfTtl = this.storeConfigInfo.getStoreFileTtl();
    if (cfTtl == Long.MAX_VALUE) {
      return null; // minversion might be set, cannot delete old files
    }
    long timestampCutoff = EnvironmentEdgeManager.currentTimeMillis() - cfTtl;
    // Merge the longest sequence of stripes where all files have expired, if any.
    int start = -1, bestStart = -1, length = 0, bestLength = 0;
    ArrayList<ImmutableList<StoreFile>> stripes = si.getStripes();
    OUTER: for (int i = 0; i < stripes.size(); ++i) {
      for (StoreFile storeFile : stripes.get(i)) {
        if (storeFile.getReader().getMaxTimestamp() < timestampCutoff) continue;
        // Found non-expired file, this stripe has to stay.
        if (length > bestLength) {
          bestStart = start;
          bestLength = length;
        }
        start = -1;
        length = 0;
        continue OUTER;
      }
      if (start == -1) {
        start = i;
      }
      ++length;
    }
    if (length > bestLength) {
      bestStart = start;
      bestLength = length;
    }
    if (bestLength == 0) return null;
    if (bestLength == 1) {
      // This is currently inefficient. If only one stripe expired, we will rewrite some
      // entire stripe just to delete some expired files because we rely on metadata and it
      // cannot simply be updated in an old file. When we either determine stripe dynamically
      // or move metadata to manifest, we can just drop the "expired stripes".
      if (bestStart == (stripes.size() - 1)) return null;
      ++bestLength;
    }
    LOG.debug("Merging " + bestLength + " stripes to delete expired store files");
    int endIndex = bestStart + bestLength - 1;
    ConcatenatedLists<StoreFile> sfs = new ConcatenatedLists<StoreFile>();
    sfs.addAllSublists(stripes.subList(bestStart, endIndex + 1));
    SplitStripeCompactionRequest result = new SplitStripeCompactionRequest(sfs,
        si.getStartRow(bestStart), si.getEndRow(endIndex), 1, Long.MAX_VALUE);
    if (canDropDeletesNoL0) {
      result.setMajorRangeFull();
    }
    return result;
  }

  private static long getTotalKvCount(final Collection<StoreFile> candidates) {
    long totalSize = 0;
    for (StoreFile storeFile : candidates) {
      totalSize += storeFile.getReader().getEntries();
    }
    return totalSize;
  }

  public static long getTotalFileSize(final Collection<StoreFile> candidates) {
    long totalSize = 0;
    for (StoreFile storeFile : candidates) {
      totalSize += storeFile.getReader().length();
    }
    return totalSize;
  }

  private Pair<Long, Integer> estimateTargetKvs(Collection<StoreFile> files, double splitCount) {
    // If the size is larger than what we target, we don't want to split into proportionally
    // larger parts and then have to split again very soon. So, we will increase the multiplier
    // by one until we get small enough parts. E.g. 5Gb stripe that should have been split into
    // 2 parts when it was 3Gb will be split into 3x1.67Gb parts, rather than 2x2.5Gb parts.
    long totalSize = getTotalFileSize(files);
    long targetPartSize = config.getSplitPartSize();
    assert targetPartSize > 0 && splitCount > 0;
    double ratio = totalSize / (splitCount * targetPartSize); // ratio of real to desired size
    while (ratio > 1.0) {
      // Ratio of real to desired size if we increase the multiplier.
      double newRatio = totalSize / ((splitCount + 1.0) * targetPartSize);
      if ((1.0 / newRatio) >= ratio) break; // New ratio is < 1.0, but further than the last one.
      ratio = newRatio;
      splitCount += 1.0;
    }
    long kvCount = (long)(getTotalKvCount(files) / splitCount);
    return new Pair<Long, Integer>(kvCount, (int)Math.ceil(splitCount));
  }

  /** Stripe compaction request wrapper. */
  public abstract static class StripeCompactionRequest {
    protected CompactionRequest request;
    protected byte[] majorRangeFromRow = null, majorRangeToRow = null;

    public List<Path> execute(StripeCompactor compactor,
      CompactionThroughputController throughputController) throws IOException {
      return execute(compactor, throughputController, null);
    }
    /**
     * Executes the request against compactor (essentially, just calls correct overload of
     * compact method), to simulate more dynamic dispatch.
     * @param compactor Compactor.
     * @return result of compact(...)
     */
    public abstract List<Path> execute(StripeCompactor compactor,
        CompactionThroughputController throughputController, User user) throws IOException;

    public StripeCompactionRequest(CompactionRequest request) {
      this.request = request;
    }

    /**
     * Sets compaction "major range". Major range is the key range for which all
     * the files are included, so they can be treated like major-compacted files.
     * @param startRow Left boundary, inclusive.
     * @param endRow Right boundary, exclusive.
     */
    public void setMajorRange(byte[] startRow, byte[] endRow) {
      this.majorRangeFromRow = startRow;
      this.majorRangeToRow = endRow;
    }

    public CompactionRequest getRequest() {
      return this.request;
    }

    public void setRequest(CompactionRequest request) {
      assert request != null;
      this.request = request;
      this.majorRangeFromRow = this.majorRangeToRow = null;
    }
  }

  /**
   * Request for stripe compactor that will cause it to split the source files into several
   * separate files at the provided boundaries.
   */
  private static class BoundaryStripeCompactionRequest extends StripeCompactionRequest {
    private final List<byte[]> targetBoundaries;

    /**
     * @param request Original request.
     * @param targetBoundaries New files should be written with these boundaries.
     */
    public BoundaryStripeCompactionRequest(CompactionRequest request,
        List<byte[]> targetBoundaries) {
      super(request);
      this.targetBoundaries = targetBoundaries;
    }

    public BoundaryStripeCompactionRequest(Collection<StoreFile> files,
        List<byte[]> targetBoundaries) {
      this(new CompactionRequest(files), targetBoundaries);
    }

    @Override
    public List<Path> execute(StripeCompactor compactor,
        CompactionThroughputController throughputController, User user) throws IOException {
      return compactor.compact(this.request, this.targetBoundaries, this.majorRangeFromRow,
        this.majorRangeToRow, throughputController, user);
    }
  }

  /**
   * Request for stripe compactor that will cause it to split the source files into several
   * separate files into based on key-value count, as well as file count limit.
   * Most of the files will be roughly the same size. The last file may be smaller or larger
   * depending on the interplay of the amount of data and maximum number of files allowed.
   */
  private static class SplitStripeCompactionRequest extends StripeCompactionRequest {
    private final byte[] startRow, endRow;
    private final int targetCount;
    private final long targetKvs;

    /**
     * @param request Original request.
     * @param startRow Left boundary of the range to compact, inclusive.
     * @param endRow Right boundary of the range to compact, exclusive.
     * @param targetCount The maximum number of stripe to compact into.
     * @param targetKvs The KV count of each segment. If targetKvs*targetCount is less than
     *                  total number of kvs, all the overflow data goes into the last stripe.
     */
    public SplitStripeCompactionRequest(CompactionRequest request,
        byte[] startRow, byte[] endRow, int targetCount, long targetKvs) {
      super(request);
      this.startRow = startRow;
      this.endRow = endRow;
      this.targetCount = targetCount;
      this.targetKvs = targetKvs;
    }

    public SplitStripeCompactionRequest(
        CompactionRequest request, byte[] startRow, byte[] endRow, long targetKvs) {
      this(request, startRow, endRow, Integer.MAX_VALUE, targetKvs);
    }

    public SplitStripeCompactionRequest(
        Collection<StoreFile> files, byte[] startRow, byte[] endRow, long targetKvs) {
      this(files, startRow, endRow, Integer.MAX_VALUE, targetKvs);
    }

    public SplitStripeCompactionRequest(Collection<StoreFile> files,
        byte[] startRow, byte[] endRow, int targetCount, long targetKvs) {
      this(new CompactionRequest(files), startRow, endRow, targetCount, targetKvs);
    }

    @Override
    public List<Path> execute(StripeCompactor compactor,
        CompactionThroughputController throughputController, User user) throws IOException {
      return compactor.compact(this.request, this.targetCount, this.targetKvs, this.startRow,
        this.endRow, this.majorRangeFromRow, this.majorRangeToRow, throughputController, user);
    }

    /** Set major range of the compaction to the entire compaction range.
     * See {@link #setMajorRange(byte[], byte[])}. */
    public void setMajorRangeFull() {
      setMajorRange(this.startRow, this.endRow);
    }
  }

  /** The information about stripes that the policy needs to do its stuff */
  public static interface StripeInformationProvider {
    public Collection<StoreFile> getStorefiles();

    /**
     * Gets the start row for a given stripe.
     * @param stripeIndex Stripe index.
     * @return Start row. May be an open key.
     */
    public byte[] getStartRow(int stripeIndex);

    /**
     * Gets the end row for a given stripe.
     * @param stripeIndex Stripe index.
     * @return End row. May be an open key.
     */
    public byte[] getEndRow(int stripeIndex);

    /**
     * @return Level 0 files.
     */
    public List<StoreFile> getLevel0Files();

    /**
     * @return All stripe boundaries; including the open ones on both ends.
     */
    public List<byte[]> getStripeBoundaries();

    /**
     * @return The stripes.
     */
    public ArrayList<ImmutableList<StoreFile>> getStripes();

    /**
     * @return Stripe count.
     */
    public int getStripeCount();
  }
}
