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
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * HBASE-15181 This is a simple implementation of date-based tiered compaction similar to
 * Cassandra's for the following benefits:
 * <ol>
 * <li>Improve date-range-based scan by structuring store files in date-based tiered layout.</li>
 * <li>Reduce compaction overhead.</li>
 * <li>Improve TTL efficiency.</li>
 * </ol>
 * Perfect fit for the use cases that:
 * <ol>
 * <li>has mostly date-based data write and scan and a focus on the most recent data.</li>
 * </ol>
 * Out-of-order writes are handled gracefully. Time range overlapping among store files is tolerated
 * and the performance impact is minimized. Configuration can be set at hbase-site or overridden at
 * per-table or per-column-family level by hbase shell. Design spec is at
 * https://docs.google.com/document/d/1_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG_uy8/
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DateTieredCompactionPolicy extends SortedCompactionPolicy {

  private static final Log LOG = LogFactory.getLog(DateTieredCompactionPolicy.class);

  private final RatioBasedCompactionPolicy compactionPolicyPerWindow;

  private final CompactionWindowFactory windowFactory;

  public DateTieredCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo)
      throws IOException {
    super(conf, storeConfigInfo);
    try {
      compactionPolicyPerWindow = ReflectionUtils.instantiateWithCustomCtor(
        comConf.getCompactionPolicyForDateTieredWindow(),
        new Class[] { Configuration.class, StoreConfigInformation.class },
        new Object[] { conf, storeConfigInfo });
    } catch (Exception e) {
      throw new IOException("Unable to load configured compaction policy '"
          + comConf.getCompactionPolicyForDateTieredWindow() + "'", e);
    }
    try {
      windowFactory = ReflectionUtils.instantiateWithCustomCtor(
        comConf.getDateTieredCompactionWindowFactory(),
        new Class[] { CompactionConfiguration.class }, new Object[] { comConf });
    } catch (Exception e) {
      throw new IOException("Unable to load configured window factory '"
          + comConf.getDateTieredCompactionWindowFactory() + "'", e);
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
      return !selectMinorCompaction(candidates, false, true).getFiles().isEmpty();
    } catch (Exception e) {
      LOG.error("Can not check for compaction: ", e);
      return false;
    }
  }

  public boolean shouldPerformMajorCompaction(final Collection<StoreFile> filesToCompact) throws IOException {
    long mcTime = getNextMajorCompactTime(filesToCompact);
    if (filesToCompact == null || mcTime == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("filesToCompact: " + filesToCompact + " mcTime: " + mcTime);
      }
      return false;
    }
    
    // TODO: Use better method for determining stamp of last major (HBASE-2990)
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (lowTimestamp <= 0L || lowTimestamp >= (now - mcTime)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("lowTimestamp: " + lowTimestamp + " lowTimestamp: " + lowTimestamp + " now: " +
            now + " mcTime: " + mcTime); 
      }
      return false;
    }
    
    long cfTtl = this.storeConfigInfo.getStoreFileTtl();
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    List<Long> boundaries = getCompactBoundariesForMajor(filesToCompact, now);
    boolean[] filesInWindow = new boolean[boundaries.size()];
    
    for (StoreFile file: filesToCompact) {
      Long minTimestamp = file.getMinimumTimestamp();
      long oldest = (minTimestamp == null) ? Long.MIN_VALUE : now - minTimestamp.longValue();
      if (cfTtl != Long.MAX_VALUE && oldest >= cfTtl) {
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
        minTimestamp == null ? Long.MAX_VALUE : file.getMinimumTimestamp());
      int upperWindowIndex = Collections.binarySearch(boundaries,
        file.getMaximumTimestamp() == null ? Long.MAX_VALUE : file.getMaximumTimestamp());

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
        .getBlockLocalityIndex(HRegionServer.getHostname(comConf.conf));
    if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
      LOG.debug("Major compaction triggered on store " + this
        + "; to make hdfs blocks local, current blockLocalityIndex is "
        + blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() + ")");
      return true;
    } 

    LOG.debug("Skipping major compaction of " + this + ", because the files are already major compacted");
    return false;
  }
  
  @Override 
  protected CompactionRequest getCompactionRequest(ArrayList<StoreFile> candidateSelection, boolean tryingMajor,
      boolean isUserCompaction, boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    CompactionRequest result = tryingMajor ? selectMajorCompaction(candidateSelection)
      : selectMinorCompaction(candidateSelection, mayUseOffPeak, mayBeStuck);
    ArrayList<StoreFile> filesToCompact = Lists.newArrayList(result.getFiles());
    removeExcessFiles(filesToCompact, isUserCompaction, tryingMajor);
    result.updateFiles(filesToCompact);
    result.setOffPeak(!filesToCompact.isEmpty() && !tryingMajor && mayUseOffPeak);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated compaction request: " + result);
    }
    return result;
  }

  public CompactionRequest selectMajorCompaction(ArrayList<StoreFile> candidateSelection) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    return new DateTieredCompactionRequest(candidateSelection,
      this.getCompactBoundariesForMajor(candidateSelection, now));
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
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long oldestToCompact = getOldestToCompact(comConf.getDateTieredMaxStoreFileAgeMillis(), now);

    List<Pair<StoreFile, Long>> storefileMaxTimestampPairs =
        Lists.newArrayListWithCapacity(candidateSelection.size());
    long maxTimestampSeen = Long.MIN_VALUE;
    for (StoreFile storeFile : candidateSelection) {
      // if there is out-of-order data,
      // we put them in the same window as the last file in increasing order
      maxTimestampSeen = Math.max(maxTimestampSeen,
        storeFile.getMaximumTimestamp() == null? Long.MIN_VALUE : storeFile.getMaximumTimestamp());
      storefileMaxTimestampPairs.add(new Pair<StoreFile, Long>(storeFile, maxTimestampSeen));
    }
    Collections.reverse(storefileMaxTimestampPairs);

    CompactionWindow window = getIncomingWindow(now);
    int minThreshold = comConf.getDateTieredIncomingWindowMin();
    PeekingIterator<Pair<StoreFile, Long>> it =
        Iterators.peekingIterator(storefileMaxTimestampPairs.iterator());
    while (it.hasNext()) {
      if (window.compareToTimestamp(oldestToCompact) < 0) {
        break;
      }
      int compResult = window.compareToTimestamp(it.peek().getSecond());
      if (compResult > 0) {
        // If the file is too old for the window, switch to the next window
        window = window.nextEarlierWindow();
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Processing files: " + fileList + " for window: " + window);
          }
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
      CompactionWindow window, boolean mayUseOffPeak, boolean mayBeStuck, int minThreshold)
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
        comConf.useDateTieredSingleOutputForMinorCompaction();
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
  private List<Long> getCompactBoundariesForMajor(Collection<StoreFile> filesToCompact, long now) {
    long minTimestamp = Long.MAX_VALUE;
    for (StoreFile file : filesToCompact) {
      minTimestamp =
        Math.min(minTimestamp,
          file.getMinimumTimestamp() == null ? Long.MAX_VALUE : file.getMinimumTimestamp());
    }
    
    List<Long> boundaries = new ArrayList<Long>();

    // Add startMillis of all windows between now and min timestamp
    for (CompactionWindow window = getIncomingWindow(now);
        window.compareToTimestamp(minTimestamp) > 0;
        window = window.nextEarlierWindow()) {
      boundaries.add(window.startMillis());
    }
    boundaries.add(Long.MIN_VALUE);
    Collections.reverse(boundaries);
    return boundaries;
  }

  /**
   * @return a list of boundaries for multiple compaction output from minTimestamp to maxTimestamp.
   */
  private static List<Long> getCompactionBoundariesForMinor(CompactionWindow window,
      boolean singleOutput) {
    List<Long> boundaries = new ArrayList<Long>();
    boundaries.add(Long.MIN_VALUE);
    if (!singleOutput) {
      boundaries.add(window.startMillis());
     }
    return boundaries;
  }

  private CompactionWindow getIncomingWindow(long now) {
    return windowFactory.newIncomingWindow(now);
  }

  private static long getOldestToCompact(long maxAgeMillis, long now) {
    try {
      return LongMath.checkedSubtract(now, maxAgeMillis);
    } catch (ArithmeticException ae) {
      LOG.warn("Value for " + CompactionConfiguration.DATE_TIERED_MAX_AGE_MILLIS_KEY + ": "
          + maxAgeMillis + ". All the files will be eligible for minor compaction.");
      return Long.MIN_VALUE;
    }
  }
}
