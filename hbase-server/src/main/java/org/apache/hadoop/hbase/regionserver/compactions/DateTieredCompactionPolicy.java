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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.math.LongMath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * HBASE-15181 This is a simple implementation of date-based tiered compaction similar to
 * Cassandra's for the following benefits:
 * 1. Improve date-range-based scan by structuring store files in date-based tiered layout.
 * 2. Reduce compaction overhead.
 * 3. Improve TTL efficiency.
 * Perfect fit for the use cases that:
 * 1. has mostly date-based data write and scan and a focus on the most recent data.
 * Out-of-order writes are handled gracefully. Time range overlapping among store files is
 * tolerated and the performance impact is minimized. Configuration can be set at hbase-site
 * or overridden at per-table or per-column-family level by hbase shell. Design spec is at
 * https://docs.google.com/document/d/1_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG_uy8/
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DateTieredCompactionPolicy extends SortedCompactionPolicy {
  private static final Log LOG = LogFactory.getLog(DateTieredCompactionPolicy.class);

  private RatioBasedCompactionPolicy compactionPolicyPerWindow;

  public DateTieredCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo)
      throws IOException {
    super(conf, storeConfigInfo);
    try {
      compactionPolicyPerWindow =
        ReflectionUtils.instantiateWithCustomCtor(comConf.getCompactionPolicyForTieredWindow(),
          new Class[] { Configuration.class, StoreConfigInformation.class }, new Object[] { conf,
            storeConfigInfo });
    } catch (Exception e) {
      throw new IOException("Unable to load configured compaction policy '"
          + comConf.getCompactionPolicyForTieredWindow() + "'", e);
    }
  }

  /**
   * Heuristics for guessing whether we need minor compaction.
   */
  @Override
  @VisibleForTesting
  public boolean needsCompaction(final Collection<StoreFile> storeFiles,
      final List<StoreFile> filesCompacting) {
    ArrayList<StoreFile> candidates = new ArrayList<StoreFile>(storeFiles);
    try {
      return selectMinorCompaction(candidates, false, true) != null;
    } catch (Exception e) {
      LOG.error("Can not check for compaction: ", e);
      return false;
    }
  }

  public boolean shouldPerformMajorCompaction(final Collection<StoreFile> filesToCompact)
    throws IOException {
    long mcTime = getNextMajorCompactTime(filesToCompact);
    if (filesToCompact == null || mcTime == 0) {
      return false;
    }

    // TODO: Use better method for determining stamp of last major (HBASE-2990)
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = EnvironmentEdgeManager.currentTime();
    if (lowTimestamp <= 0L || lowTimestamp >= (now - mcTime)) {
      return false;
    }

    long cfTTL = this.storeConfigInfo.getStoreFileTtl();
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    long oldestToCompact = getOldestToCompact(comConf.getMaxStoreFileAgeMillis(), now);
    List<Long> boundaries = getCompactBoundariesForMajor(filesToCompact, oldestToCompact, now);
    boolean[] filesInWindow = new boolean[boundaries.size()];

    for (StoreFile file: filesToCompact) {
      Long minTimestamp = file.getMinimumTimestamp();
      long oldest = (minTimestamp == null) ? Long.MIN_VALUE : now - minTimestamp.longValue();
      if (cfTTL != Long.MAX_VALUE && oldest >= cfTTL) {
        LOG.debug("Major compaction triggered on store " + this
          + "; for TTL maintenance");
        return true;
      }
      if (!file.isMajorCompaction() || file.isBulkLoadResult()) {
        LOG.debug("Major compaction triggered on store " + this
          + ", because there are new files and time since last major compaction "
          + (now - lowTimestamp) + "ms");
        return true;
      }

      int lowerWindowIndex = Collections.binarySearch(boundaries,
        minTimestamp == null ? (Long)Long.MAX_VALUE : minTimestamp);
      int upperWindowIndex = Collections.binarySearch(boundaries,
        file.getMaximumTimestamp() == null ? (Long)Long.MAX_VALUE : file.getMaximumTimestamp());
      if (lowerWindowIndex != upperWindowIndex) {
        LOG.debug("Major compaction triggered on store " + this + "; because file "
          + file.getPath() + " has data with timestamps cross window boundaries");
        return true;
      } else if (filesInWindow[upperWindowIndex]) {
        LOG.debug("Major compaction triggered on store " + this +
          "; because there are more than one file in some windows");
        return true;
      } else {
        filesInWindow[upperWindowIndex] = true;
      }
      hdfsBlocksDistribution.add(file.getHDFSBlockDistribution());
    }

    float blockLocalityIndex = hdfsBlocksDistribution
        .getBlockLocalityIndex(RSRpcServices.getHostname(comConf.conf, false));
    if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
      LOG.debug("Major compaction triggered on store " + this
        + "; to make hdfs blocks local, current blockLocalityIndex is "
        + blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() + ")");
      return true;
    }

    LOG.debug("Skipping major compaction of " + this +
      ", because the files are already major compacted");
    return false;
  }

  @Override
  protected CompactionRequest createCompactionRequest(ArrayList<StoreFile> candidateSelection,
    boolean tryingMajor, boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    CompactionRequest result = tryingMajor ? selectMajorCompaction(candidateSelection)
      : selectMinorCompaction(candidateSelection, mayUseOffPeak, mayBeStuck);
    LOG.debug("Generated compaction request: " + result);
    return result;
  }

  public CompactionRequest selectMajorCompaction(ArrayList<StoreFile> candidateSelection) {
    long now = EnvironmentEdgeManager.currentTime();
    long oldestToCompact = getOldestToCompact(comConf.getMaxStoreFileAgeMillis(), now);
    return new DateTieredCompactionRequest(candidateSelection,
      this.getCompactBoundariesForMajor(candidateSelection, oldestToCompact, now));
  }

  /**
   * We receive store files sorted in ascending order by seqId then scan the list of files. If the
   * current file has a maxTimestamp older than last known maximum, treat this file as it carries
   * the last known maximum. This way both seqId and timestamp are in the same order. If files carry
   * the same maxTimestamps, they are ordered by seqId. We then reverse the list so they are ordered
   * by seqId and maxTimestamp in descending order and build the time windows. All the out-of-order
   * data into the same compaction windows, guaranteeing contiguous compaction based on sequence id.
   */
  public CompactionRequest selectMinorCompaction(ArrayList<StoreFile> candidateSelection,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    long oldestToCompact = getOldestToCompact(comConf.getMaxStoreFileAgeMillis(), now);

    List<StoreFile> storeFileList = Lists.newArrayList(filterOldStoreFiles(candidateSelection,
      oldestToCompact));

    List<Pair<StoreFile, Long>> storefileMaxTimestampPairs =
        Lists.newArrayListWithCapacity(Iterables.size(storeFileList));
    long maxTimestampSeen = Long.MIN_VALUE;
    for (StoreFile storeFile : storeFileList) {
      // if there is out-of-order data,
      // we put them in the same window as the last file in increasing order
      maxTimestampSeen = Math.max(maxTimestampSeen,
        storeFile.getMaximumTimestamp() == null? Long.MIN_VALUE : storeFile.getMaximumTimestamp());
      storefileMaxTimestampPairs.add(new Pair<StoreFile, Long>(storeFile, maxTimestampSeen));
    }
    Collections.reverse(storefileMaxTimestampPairs);

    Window window = getIncomingWindow(now, comConf.getBaseWindowMillis());
    int minThreshold = comConf.getIncomingWindowMin();
    PeekingIterator<Pair<StoreFile, Long>> it =
        Iterators.peekingIterator(storefileMaxTimestampPairs.iterator());
    while (it.hasNext()) {
      int compResult = window.compareToTimestamp(it.peek().getSecond());
      if (compResult > 0) {
        // If the file is too old for the window, switch to the next window
        window = window.nextWindow(comConf.getWindowsPerTier(),
          oldestToCompact);
        minThreshold = comConf.getMinFilesToCompact();
      } else {
        // The file is within the target window
        ArrayList<StoreFile> fileList = Lists.newArrayList();
        // Add all files in the same window. For incoming window
        // we tolerate files with future data although it is sub-optimal
        while (it.hasNext() && window.compareToTimestamp(it.peek().getSecond()) <= 0) {
          fileList.add(it.next().getFirst());
        }
        if (fileList.size() >= minThreshold) {
          LOG.debug("Processing files: " + fileList + " for window: " + window);
          DateTieredCompactionRequest request = generateCompactionRequest(fileList, window,
            mayUseOffPeak, mayBeStuck, minThreshold);
          if (request != null) {
            return request;
          }
        }
      }
    }
    // A non-null file list is expected by HStore
    return new CompactionRequest(Collections.<StoreFile> emptyList());
  }

  private DateTieredCompactionRequest generateCompactionRequest(ArrayList<StoreFile> storeFiles,
      Window window, boolean mayUseOffPeak, boolean mayBeStuck, int minThreshold)
    throws IOException {
    // The files has to be in ascending order for ratio-based compaction to work right
    // and removeExcessFile to exclude youngest files.
    Collections.reverse(storeFiles);

    // Compact everything in the window if have more files than comConf.maxBlockingFiles
    compactionPolicyPerWindow.setMinThreshold(minThreshold);
    ArrayList<StoreFile> storeFileSelection = mayBeStuck ? storeFiles
      : compactionPolicyPerWindow.applyCompactionPolicy(storeFiles, mayUseOffPeak, false);
    if (storeFileSelection != null && !storeFileSelection.isEmpty()) {
      // If there is any file in the window excluded from compaction,
      // only one file will be output from compaction.
      boolean singleOutput = storeFiles.size() != storeFileSelection.size() ||
        comConf.useSingleOutputForMinorCompaction();
      List<Long> boundaries = getCompactionBoundariesForMinor(window, singleOutput);
      DateTieredCompactionRequest result = new DateTieredCompactionRequest(storeFileSelection,
        boundaries);
      return result;
    }
    return null;
  }

  /**
   * Return a list of boundaries for multiple compaction output
   *   in ascending order.
   */
  private List<Long> getCompactBoundariesForMajor(Collection<StoreFile> filesToCompact,
    long oldestToCompact, long now) {
    long minTimestamp = Long.MAX_VALUE;
    for (StoreFile file : filesToCompact) {
      minTimestamp = Math.min(minTimestamp,
        file.getMinimumTimestamp() == null? Long.MAX_VALUE : file.getMinimumTimestamp());
    }

    List<Long> boundaries = new ArrayList<Long>();

    // Add startMillis of all windows between now and min timestamp
    for (Window window = getIncomingWindow(now, comConf.getBaseWindowMillis());
      window.compareToTimestamp(minTimestamp) > 0;
      window = window.nextWindow(comConf.getWindowsPerTier(), oldestToCompact)) {
      boundaries.add(window.startMillis());
    }
    boundaries.add(Long.MIN_VALUE);
    Collections.reverse(boundaries);
    return boundaries;
  }

  /**
   * @return a list of boundaries for multiple compaction output
   *   from minTimestamp to maxTimestamp.
   */
  private static List<Long> getCompactionBoundariesForMinor(Window window, boolean singleOutput) {
    List<Long> boundaries = new ArrayList<Long>();
    boundaries.add(Long.MIN_VALUE);
    if (!singleOutput) {
      boundaries.add(window.startMillis());
    }
    return boundaries;
  }

  /**
   * Removes all store files with max timestamp older than (current - maxAge).
   * @param storeFiles all store files to consider
   * @param maxAge the age in milliseconds when a store file stops participating in compaction.
   * @return a list of storeFiles with the store file older than maxAge excluded
   */
  private static Iterable<StoreFile> filterOldStoreFiles(List<StoreFile> storeFiles,
    final long cutoff) {
    return Iterables.filter(storeFiles, new Predicate<StoreFile>() {
      @Override
      public boolean apply(StoreFile storeFile) {
        // Known findbugs issue to guava. SuppressWarning or Nonnull annotation don't work.
        if (storeFile == null) {
          return false;
        }
        Long maxTimestamp = storeFile.getMaximumTimestamp();
        return maxTimestamp == null ? true : maxTimestamp >= cutoff;
      }
    });
  }

  private static Window getIncomingWindow(long now, long baseWindowMillis) {
    return new Window(baseWindowMillis, now / baseWindowMillis);
  }

  private static long getOldestToCompact(long maxAgeMillis, long now) {
    try {
      return LongMath.checkedSubtract(now, maxAgeMillis);
    } catch (ArithmeticException ae) {
      LOG.warn("Value for " + CompactionConfiguration.MAX_AGE_MILLIS_KEY + ": " + maxAgeMillis
        + ". All the files will be eligible for minor compaction.");
      return Long.MIN_VALUE;
    }
  }

  /**
   * This is the class we use to partition from epoch time to now into tiers of exponential sizes of
   * windows.
   */
  private static final class Window {
    /**
     * How big a range of timestamps fit inside the window in milliseconds.
     */
    private final long windowMillis;

    /**
     * A timestamp t is within the window iff t / size == divPosition.
     */
    private final long divPosition;

    private Window(long baseWindowMillis, long divPosition) {
      windowMillis = baseWindowMillis;
      this.divPosition = divPosition;
    }

    /**
     * Compares the window to a timestamp.
     * @param timestamp the timestamp to compare.
     * @return a negative integer, zero, or a positive integer as the window lies before, covering,
     *         or after than the timestamp.
     */
    public int compareToTimestamp(long timestamp) {
      if (timestamp < 0) {
        try {
          timestamp = LongMath.checkedSubtract(timestamp, windowMillis - 1);
        } catch (ArithmeticException ae) {
          timestamp = Long.MIN_VALUE;
        }
      }
      long pos = timestamp / windowMillis;
      return divPosition == pos ? 0 : divPosition < pos ? -1 : 1;
    }

    /**
     * Move to the new window of the same tier or of the next tier, which represents an earlier time
     * span.
     * @param windowsPerTier The number of contiguous windows that will have the same size. Windows
     *          following those will be <code>tierBase</code> times as big.
     * @return The next window
     */
    public Window nextWindow(int windowsPerTier, long oldestToCompact) {
      // Don't promote to the next tier if there is not even 1 window at current tier
      // or if the next window crosses the max age.
      if (divPosition % windowsPerTier > 0 ||
          startMillis() - windowMillis * windowsPerTier < oldestToCompact) {
        return new Window(windowMillis, divPosition - 1);
      } else {
        return new Window(windowMillis * windowsPerTier, divPosition / windowsPerTier - 1);
      }
    }

    /**
     * Inclusive lower bound
     */
    public long startMillis() {
      try {
        return LongMath.checkedMultiply(windowMillis, divPosition);
      } catch (ArithmeticException ae) {
        return Long.MIN_VALUE;
      }
    }

    /**
     * Exclusive upper bound
     */
    public long endMillis() {
      try {
        return LongMath.checkedMultiply(windowMillis, (divPosition + 1));
      } catch (ArithmeticException ae) {
        return Long.MAX_VALUE;
      }
    }

    @Override
    public String toString() {
      return "[" + startMillis() + ", " + endMillis() + ")";
    }
  }
}