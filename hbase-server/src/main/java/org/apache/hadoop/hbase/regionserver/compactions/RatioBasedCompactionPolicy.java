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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;

/**
 * The default algorithm for selecting files for compaction. Combines the compaction configuration
 * and the provisional file selection that it's given to produce the list of suitable candidates for
 * compaction.
 */
@InterfaceAudience.Private
public class RatioBasedCompactionPolicy extends SortedCompactionPolicy {
  private static final Log LOG = LogFactory.getLog(RatioBasedCompactionPolicy.class);

  public RatioBasedCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  @Override
  public boolean shouldPerformMajorCompaction(final Collection<StoreFile> filesToCompact)
      throws IOException {
    boolean result = false;
    long mcTime = getNextMajorCompactTime(filesToCompact);
    if (filesToCompact == null || filesToCompact.isEmpty() || mcTime == 0) {
      return result;
    }
    // TODO: Use better method for determining stamp of last major (HBASE-2990)
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = System.currentTimeMillis();
    if (lowTimestamp > 0l && lowTimestamp < (now - mcTime)) {
      // Major compaction time has elapsed.
      long cfTtl = this.storeConfigInfo.getStoreFileTtl();
      if (filesToCompact.size() == 1) {
        // Single file
        StoreFile sf = filesToCompact.iterator().next();
        Long minTimestamp = sf.getMinimumTimestamp();
        long oldest = (minTimestamp == null) ? Long.MIN_VALUE : now - minTimestamp.longValue();
        if (sf.isMajorCompaction() && (cfTtl == Long.MAX_VALUE || oldest < cfTtl)) {
          float blockLocalityIndex =
              sf.getHDFSBlockDistribution().getBlockLocalityIndex(
                HRegionServer.getHostname(comConf.conf));
          if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Major compaction triggered on only store " + this
                  + "; to make hdfs blocks local, current blockLocalityIndex is "
                  + blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() + ")");
            }
            result = true;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skipping major compaction of " + this
                  + " because one (major) compacted file only, oldestTime " + oldest
                  + "ms is < ttl=" + cfTtl + " and blockLocalityIndex is " + blockLocalityIndex
                  + " (min " + comConf.getMinLocalityToForceCompact() + ")");
            }
          }
        } else if (cfTtl != HConstants.FOREVER && oldest > cfTtl) {
          LOG.debug("Major compaction triggered on store " + this
              + ", because keyvalues outdated; time since last major compaction "
              + (now - lowTimestamp) + "ms");
          result = true;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store " + this
              + "; time since last major compaction " + (now - lowTimestamp) + "ms");
        }
        result = true;
      }
    }
    return result;
  }

  @Override
  protected CompactionRequest getCompactionRequest(ArrayList<StoreFile> candidateSelection,
      boolean tryingMajor, boolean isUserCompaction, boolean mayUseOffPeak, boolean mayBeStuck)
      throws IOException {
    if (!tryingMajor) {
      candidateSelection = filterBulk(candidateSelection);
      candidateSelection = applyCompactionPolicy(candidateSelection, mayUseOffPeak, mayBeStuck);
      candidateSelection =
          checkMinFilesCriteria(candidateSelection, comConf.getMinFilesToCompact());
    }
    removeExcessFiles(candidateSelection, isUserCompaction, tryingMajor);
    CompactionRequest result = new CompactionRequest(candidateSelection);
    result.setOffPeak(!candidateSelection.isEmpty() && !tryingMajor && mayUseOffPeak);
    return result;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset -- Default minor compaction selection algorithm: choose
   *         CompactSelection from candidates -- First exclude bulk-load files if indicated in
   *         configuration. Start at the oldest file and stop when you find the first file that
   *         meets compaction criteria: (1) a recently-flushed, small file (i.e. <= minCompactSize)
   *         OR (2) within the compactRatio of sum(newer_files) Given normal skew, any newer files
   *         will also meet this criteria
   *         <p/>
   *         Additional Note: If fileSizes.size() >> maxFilesToCompact, we will recurse on
   *         compact(). Consider the oldest files first to avoid a situation where we always compact
   *         [end-threshold,end). Then, the last file becomes an aggregate of the previous
   *         compactions. normal skew: older ----> newer (increasing seqID) _ | | _ | | | | _ --|-|-
   *         |-|- |-|---_-------_------- minCompactSize | | | | | | | | _ | | | | | | | | | | | | |
   *         | | | | | | | | | | | | |
   */
  protected ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    if (candidates.isEmpty()) {
      return candidates;
    }

    // we're doing a minor compaction, let's see what files are applicable
    int start = 0;
    double ratio = comConf.getCompactionRatio();
    if (mayUseOffPeak) {
      ratio = comConf.getCompactionRatioOffPeak();
      LOG.info("Running an off-peak compaction, selection ratio = " + ratio);
    }

    // get store file sizes for incremental compacting selection.
    final int countOfFiles = candidates.size();
    long[] fileSizes = new long[countOfFiles];
    long[] sumSize = new long[countOfFiles];
    for (int i = countOfFiles - 1; i >= 0; --i) {
      StoreFile file = candidates.get(i);
      fileSizes[i] = file.getReader().length();
      // calculate the sum of fileSizes[i,i+maxFilesToCompact-1) for algo
      int tooFar = i + comConf.getMaxFilesToCompact() - 1;
      sumSize[i] =
          fileSizes[i] + ((i + 1 < countOfFiles) ? sumSize[i + 1] : 0)
              - ((tooFar < countOfFiles) ? fileSizes[tooFar] : 0);
    }

    while (countOfFiles - start >= comConf.getMinFilesToCompact()
        && fileSizes[start] > Math.max(comConf.getMinCompactSize(),
          (long) (sumSize[start + 1] * ratio))) {
      ++start;
    }
    if (start < countOfFiles) {
      LOG.info("Default compaction algorithm has selected " + (countOfFiles - start)
          + " files from " + countOfFiles + " candidates");
    } else if (mayBeStuck) {
      // We may be stuck. Compact the latest files if we can.
      int filesToLeave = candidates.size() - comConf.getMinFilesToCompact();
      if (filesToLeave >= 0) {
        start = filesToLeave;
      }
    }
    candidates.subList(0, start).clear();
    return candidates;
  }

  /**
   * Overwrite min threshold for compaction
   * @param minThreshold
   */
  public void setMinThreshold(int minThreshold) {
    comConf.setMinFilesToCompact(minThreshold);
  }
}
