/**
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
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
 * 2. never or rarely deletes data. Out-of-order writes are handled gracefully. Time range
 * overlapping among store files is tolerated and the performance impact is minimized. Configuration
 * can be set at hbase-site or overriden at per-table or per-column-famly level by hbase shell.
 * Design spec is at
 * https://docs.google.com/document/d/1_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG_uy8/
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DateTieredCompactionPolicy extends RatioBasedCompactionPolicy {
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

  @Override
  public boolean isMajorCompaction(Collection<StoreFile> filesToCompact) throws IOException {
    // Never do major compaction unless forced
    return false;
  }

  @Override
  /**
   * Heuristics for guessing whether we need compaction.
   */
  public boolean needsCompaction(final Collection<StoreFile> storeFiles,
      final List<StoreFile> filesCompacting) {
    return needsCompaction(storeFiles, filesCompacting, EnvironmentEdgeManager.currentTime());
  }

  @VisibleForTesting
  public boolean needsCompaction(final Collection<StoreFile> storeFiles,
      final List<StoreFile> filesCompacting, long now) {
    if (!super.needsCompaction(storeFiles, filesCompacting)) {
      return false;
    }

    ArrayList<StoreFile> candidates = new ArrayList<StoreFile>(storeFiles);
    candidates = filterBulk(candidates);
    candidates = skipLargeFiles(candidates, true);
    try {
      candidates = applyCompactionPolicy(candidates, true, false, now);
    } catch (Exception e) {
      LOG.error("Can not check for compaction: ", e);
      return false;
    }

    return candidates != null && candidates.size() >= comConf.getMinFilesToCompact();
  }

  /**
   * Could return null if no candidates are found
   */
  @Override
  public ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    return applyCompactionPolicy(candidates, mayUseOffPeak, mayBeStuck,
      EnvironmentEdgeManager.currentTime());
  }

  /**
   * Input candidates are sorted from oldest to newest by seqId. Could return null if no candidates
   * are found.
   */
  @VisibleForTesting
  public ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
      boolean mayUseOffPeak, boolean mayBeStuck, long now) throws IOException {
    Iterable<StoreFile> candidatesInWindow =
      filterOldStoreFiles(Lists.newArrayList(candidates), comConf.getMaxStoreFileAgeMillis(), now);

    List<ArrayList<StoreFile>> buckets =
        partitionFilesToBuckets(candidatesInWindow, comConf.getBaseWindowMillis(),
          comConf.getWindowsPerTier(), now);
    LOG.debug("Compaction buckets are: " + buckets);
    if (buckets.size() >= storeConfigInfo.getBlockingFileCount()) {
      LOG.warn("Number of compaction buckets:" +  buckets.size()
        + ", exceeds blocking file count setting: "
        + storeConfigInfo.getBlockingFileCount()
        + ", either increase hbase.hstore.blockingStoreFiles or "
        + "reduce the number of tiered compaction windows");
    }

    return newestBucket(buckets, comConf.getIncomingWindowMin(), now,
      comConf.getBaseWindowMillis(), mayUseOffPeak);
  }

  /**
   * @param buckets the list of buckets, sorted from newest to oldest, from which to return the
   *          newest bucket within thresholds.
   * @param incomingWindowThreshold minimum number of storeFiles in a bucket to qualify.
   * @param maxThreshold maximum number of storeFiles to compact at once (the returned bucket will
   *          be trimmed down to this).
   * @return a bucket (a list of store files within a window to be compacted).
   * @throws IOException error
   */
  private ArrayList<StoreFile> newestBucket(List<ArrayList<StoreFile>> buckets,
      int incomingWindowThreshold, long now, long baseWindowMillis, boolean mayUseOffPeak)
      throws IOException {
    Window incomingWindow = getInitialWindow(now, baseWindowMillis);
    for (ArrayList<StoreFile> bucket : buckets) {
      int minThreshold =
          incomingWindow.compareToTimestamp(bucket.get(0).getMaximumTimestamp()) <= 0 ? comConf
              .getIncomingWindowMin() : comConf.getMinFilesToCompact();
      compactionPolicyPerWindow.setMinThreshold(minThreshold);
      ArrayList<StoreFile> candidates =
          compactionPolicyPerWindow.applyCompactionPolicy(bucket, mayUseOffPeak, false);
      if (candidates != null && !candidates.isEmpty()) {
        return candidates;
      }
    }
    return null;
  }

  /**
   * We receive store files sorted in ascending order by seqId then scan the list of files. If the
   * current file has a maxTimestamp older than last known maximum, treat this file as it carries
   * the last known maximum. This way both seqId and timestamp are in the same order. If files carry
   * the same maxTimestamps, they are ordered by seqId. We then reverse the list so they are ordered
   * by seqId and maxTimestamp in decending order and build the time windows. All the out-of-order
   * data into the same compaction windows, guaranteeing contiguous compaction based on sequence id.
   */
  private static List<ArrayList<StoreFile>> partitionFilesToBuckets(Iterable<StoreFile> storeFiles,
      long baseWindowSizeMillis, int windowsPerTier, long now) {
    List<ArrayList<StoreFile>> buckets = Lists.newArrayList();
    Window window = getInitialWindow(now, baseWindowSizeMillis);

    List<Pair<StoreFile, Long>> storefileMaxTimestampPairs =
        Lists.newArrayListWithCapacity(Iterables.size(storeFiles));
    long maxTimestampSeen = Long.MIN_VALUE;
    for (StoreFile storeFile : storeFiles) {
      // if there is out-of-order data,
      // we put them in the same window as the last file in increasing order
      maxTimestampSeen = Math.max(maxTimestampSeen, storeFile.getMaximumTimestamp());
      storefileMaxTimestampPairs.add(new Pair<StoreFile, Long>(storeFile, maxTimestampSeen));
    }

    Collections.reverse(storefileMaxTimestampPairs);
    PeekingIterator<Pair<StoreFile, Long>> it =
        Iterators.peekingIterator(storefileMaxTimestampPairs.iterator());

    while (it.hasNext()) {
      int compResult = window.compareToTimestamp(it.peek().getSecond());
      if (compResult > 0) {
        // If the file is too old for the window, switch to the next window
        window = window.nextWindow(windowsPerTier);
      } else {
        // The file is within the target window
        ArrayList<StoreFile> bucket = Lists.newArrayList();
        // Add all files in the same window to current bucket. For incoming window
        // we tolerate files with future data although it is sub-optimal
        while (it.hasNext() && window.compareToTimestamp(it.peek().getSecond()) <= 0) {
          bucket.add(it.next().getFirst());
        }
        if (!bucket.isEmpty()) {
          buckets.add(bucket);
        }
      }
    }

    return buckets;
  }

  /**
   * Removes all store files with max timestamp older than (current - maxAge).
   * @param storeFiles all store files to consider
   * @param maxAge the age in milliseconds when a store file stops participating in compaction.
   * @param now current time. store files with max timestamp less than (now - maxAge) are filtered.
   * @return a list of storeFiles with the store file older than maxAge excluded
   */
  private static Iterable<StoreFile> filterOldStoreFiles(List<StoreFile> storeFiles, long maxAge,
      long now) {
    if (maxAge == 0) {
      return ImmutableList.of();
    }
    final long cutoff = now - maxAge;
    return Iterables.filter(storeFiles, new Predicate<StoreFile>() {
      @Override
      public boolean apply(StoreFile storeFile) {
        // Known findbugs issue to guava. SuppressWarning or Nonnull annotation don't work.
        if (storeFile == null) {
          return false;
        }
        return storeFile.getMaximumTimestamp() >= cutoff;
      }
    });
  }

  private static Window getInitialWindow(long now, long timeUnit) {
    return new Window(timeUnit, now / timeUnit);
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
      this.windowMillis = baseWindowMillis;
      this.divPosition = divPosition;
    }

    /**
     * Compares the window to a timestamp.
     * @param timestamp the timestamp to compare.
     * @return a negative integer, zero, or a positive integer as the window lies before, covering,
     *         or after than the timestamp.
     */
    public int compareToTimestamp(long timestamp) {
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
    public Window nextWindow(int windowsPerTier) {
      if (divPosition % windowsPerTier > 0) {
        return new Window(windowMillis, divPosition - 1);
      } else {
        return new Window(windowMillis * windowsPerTier, divPosition / windowsPerTier - 1);
      }
    }
  }
}