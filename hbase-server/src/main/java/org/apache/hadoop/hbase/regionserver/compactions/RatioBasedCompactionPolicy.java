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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * The default algorithm for selecting files for compaction.
 * Combines the compaction configuration and the provisional file selection that
 * it's given to produce the list of suitable candidates for compaction.
 */
@InterfaceAudience.Private
public class RatioBasedCompactionPolicy extends CompactionPolicy {
  private static final Log LOG = LogFactory.getLog(RatioBasedCompactionPolicy.class);

  public RatioBasedCompactionPolicy(Configuration conf,
                                    StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  private ArrayList<StoreFile> getCurrentEligibleFiles(
      ArrayList<StoreFile> candidateFiles, final List<StoreFile> filesCompacting) {
    // candidates = all storefiles not already in compaction queue
    if (!filesCompacting.isEmpty()) {
      // exclude all files older than the newest file we're currently
      // compacting. this allows us to preserve contiguity (HBASE-2856)
      StoreFile last = filesCompacting.get(filesCompacting.size() - 1);
      int idx = candidateFiles.indexOf(last);
      Preconditions.checkArgument(idx != -1);
      candidateFiles.subList(0, idx + 1).clear();
    }
    return candidateFiles;
  }

  public List<StoreFile> preSelectCompactionForCoprocessor(
      final Collection<StoreFile> candidates, final List<StoreFile> filesCompacting) {
    return getCurrentEligibleFiles(new ArrayList<StoreFile>(candidates), filesCompacting);
  }

  /**
   * @param candidateFiles candidate files, ordered from oldest to newest. All files in store.
   * @return subset copy of candidate list that meets compaction criteria
   * @throws java.io.IOException
   */
  public CompactionRequest selectCompaction(Collection<StoreFile> candidateFiles,
      final List<StoreFile> filesCompacting, final boolean isUserCompaction,
      final boolean mayUseOffPeak, final boolean forceMajor) throws IOException {
    // Preliminary compaction subject to filters
    ArrayList<StoreFile> candidateSelection = new ArrayList<StoreFile>(candidateFiles);
    // Stuck and not compacting enough (estimate). It is not guaranteed that we will be
    // able to compact more if stuck and compacting, because ratio policy excludes some
    // non-compacting files from consideration during compaction (see getCurrentEligibleFiles).
    int futureFiles = filesCompacting.isEmpty() ? 0 : 1;
    boolean mayBeStuck = (candidateFiles.size() - filesCompacting.size() + futureFiles)
        >= storeConfigInfo.getBlockingFileCount();
    candidateSelection = getCurrentEligibleFiles(candidateSelection, filesCompacting);
    LOG.debug("Selecting compaction from " + candidateFiles.size() + " store files, " +
        filesCompacting.size() + " compacting, " + candidateSelection.size() +
        " eligible, " + storeConfigInfo.getBlockingFileCount() + " blocking");

    // If we can't have all files, we cannot do major anyway
    boolean isAllFiles = candidateFiles.size() == candidateSelection.size();
    if (!(forceMajor && isAllFiles)) {
      candidateSelection = skipLargeFiles(candidateSelection);
      isAllFiles = candidateFiles.size() == candidateSelection.size();
    }

    // Try a major compaction if this is a user-requested major compaction,
    // or if we do not have too many files to compact and this was requested as a major compaction
    boolean isTryingMajor = (forceMajor && isAllFiles && isUserCompaction)
        || (((forceMajor && isAllFiles) || isMajorCompaction(candidateSelection))
          && (candidateSelection.size() < comConf.getMaxFilesToCompact()));
    // Or, if there are any references among the candidates.
    boolean isAfterSplit = StoreUtils.hasReferences(candidateSelection);
    if (!isTryingMajor && !isAfterSplit) {
      // We're are not compacting all files, let's see what files are applicable
      candidateSelection = filterBulk(candidateSelection);
      candidateSelection = applyCompactionPolicy(candidateSelection, mayUseOffPeak, mayBeStuck);
      candidateSelection = checkMinFilesCriteria(candidateSelection);
    }
    candidateSelection = removeExcessFiles(candidateSelection, isUserCompaction, isTryingMajor);
    // Now we have the final file list, so we can determine if we can do major/all files.
    isAllFiles = (candidateFiles.size() == candidateSelection.size());
    CompactionRequest result = new CompactionRequest(candidateSelection);
    result.setOffPeak(!candidateSelection.isEmpty() && !isAllFiles && mayUseOffPeak);
    result.setIsMajor(isTryingMajor && isAllFiles, isAllFiles);
    return result;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * exclude all files above maxCompactSize
   * Also save all references. We MUST compact them
   */
  private ArrayList<StoreFile> skipLargeFiles(ArrayList<StoreFile> candidates) {
    int pos = 0;
    while (pos < candidates.size() && !candidates.get(pos).isReference()
      && (candidates.get(pos).getReader().length() > comConf.getMaxCompactSize())) {
      ++pos;
    }
    if (pos > 0) {
      LOG.debug("Some files are too large. Excluding " + pos
          + " files from compaction candidates");
      candidates.subList(0, pos).clear();
    }
    return candidates;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * exclude all bulk load files if configured
   */
  private ArrayList<StoreFile> filterBulk(ArrayList<StoreFile> candidates) {
    candidates.removeAll(Collections2.filter(candidates,
        new Predicate<StoreFile>() {
          @Override
          public boolean apply(StoreFile input) {
            return input.excludeFromMinorCompaction();
          }
        }));
    return candidates;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * take upto maxFilesToCompact from the start
   */
  private ArrayList<StoreFile> removeExcessFiles(ArrayList<StoreFile> candidates,
      boolean isUserCompaction, boolean isMajorCompaction) {
    int excess = candidates.size() - comConf.getMaxFilesToCompact();
    if (excess > 0) {
      if (isMajorCompaction && isUserCompaction) {
        LOG.debug("Warning, compacting more than " + comConf.getMaxFilesToCompact() +
            " files because of a user-requested major compaction");
      } else {
        LOG.debug("Too many admissible files. Excluding " + excess
          + " files from compaction candidates");
        candidates.subList(comConf.getMaxFilesToCompact(), candidates.size()).clear();
      }
    }
    return candidates;
  }
  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * forget the compactionSelection if we don't have enough files
   */
  private ArrayList<StoreFile> checkMinFilesCriteria(ArrayList<StoreFile> candidates) {
    int minFiles = comConf.getMinFilesToCompact();
    if (candidates.size() < minFiles) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Not compacting files because we only have " + candidates.size() +
          " files ready for compaction. Need " + minFiles + " to initiate.");
      }
      candidates.clear();
    }
    return candidates;
  }

  /**
    * @param candidates pre-filtrate
    * @return filtered subset
    * -- Default minor compaction selection algorithm:
    * choose CompactSelection from candidates --
    * First exclude bulk-load files if indicated in configuration.
    * Start at the oldest file and stop when you find the first file that
    * meets compaction criteria:
    * (1) a recently-flushed, small file (i.e. <= minCompactSize)
    * OR
    * (2) within the compactRatio of sum(newer_files)
    * Given normal skew, any newer files will also meet this criteria
    * <p/>
    * Additional Note:
    * If fileSizes.size() >> maxFilesToCompact, we will recurse on
    * compact().  Consider the oldest files first to avoid a
    * situation where we always compact [end-threshold,end).  Then, the
    * last file becomes an aggregate of the previous compactions.
    *
    * normal skew:
    *
    *         older ----> newer (increasing seqID)
    *     _
    *    | |   _
    *    | |  | |   _
    *  --|-|- |-|- |-|---_-------_-------  minCompactSize
    *    | |  | |  | |  | |  _  | |
    *    | |  | |  | |  | | | | | |
    *    | |  | |  | |  | | | | | |
    */
  ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
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
      sumSize[i] = fileSizes[i]
        + ((i + 1 < countOfFiles) ? sumSize[i + 1] : 0)
        - ((tooFar < countOfFiles) ? fileSizes[tooFar] : 0);
    }


    while (countOfFiles - start >= comConf.getMinFilesToCompact() &&
      fileSizes[start] > Math.max(comConf.getMinCompactSize(),
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

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  public boolean isMajorCompaction(final Collection<StoreFile> filesToCompact)
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
        long oldest = (minTimestamp == null)
            ? Long.MIN_VALUE
            : now - minTimestamp.longValue();
        if (sf.isMajorCompaction() &&
            (cfTtl == HConstants.FOREVER || oldest < cfTtl)) {
          float blockLocalityIndex = sf.getHDFSBlockDistribution().getBlockLocalityIndex(
              RSRpcServices.getHostname(comConf.conf)
          );
          if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Major compaction triggered on only store " + this +
                  "; to make hdfs blocks local, current blockLocalityIndex is " +
                  blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() +
                  ")");
            }
            result = true;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skipping major compaction of " + this +
                  " because one (major) compacted file only, oldestTime " +
                  oldest + "ms is < ttl=" + cfTtl + " and blockLocalityIndex is " +
                  blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() +
                  ")");
            }
          }
        } else if (cfTtl != HConstants.FOREVER && oldest > cfTtl) {
          LOG.debug("Major compaction triggered on store " + this +
            ", because keyvalues outdated; time since last major compaction " +
            (now - lowTimestamp) + "ms");
          result = true;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store " + this +
              "; time since last major compaction " + (now - lowTimestamp) + "ms");
        }
        result = true;
      }
    }
    return result;
  }

  /**
   * Used calculation jitter
   */
  private final Random random = new Random();

  /**
   * @param filesToCompact
   * @return When to run next major compaction
   */
  public long getNextMajorCompactTime(final Collection<StoreFile> filesToCompact) {
    // default = 24hrs
    long ret = comConf.getMajorCompactionPeriod();
    if (ret > 0) {
      // default = 20% = +/- 4.8 hrs
      double jitterPct = comConf.getMajorCompactionJitter();
      if (jitterPct > 0) {
        long jitter = Math.round(ret * jitterPct);
        // deterministic jitter avoids a major compaction storm on restart
        Integer seed = StoreUtils.getDeterministicRandomSeed(filesToCompact);
        if (seed != null) {
          // Synchronized to ensure one user of random instance at a time.
          double rnd = -1;
          synchronized (this) {
            this.random.setSeed(seed);
            rnd = this.random.nextDouble();
          }
          ret += jitter - Math.round(2L * jitter * rnd);
        } else {
          ret = 0; // If seed is null, then no storefiles == no major compaction
        }
      }
    }
    return ret;
  }

  /**
   * @param compactionSize Total size of some compaction
   * @return whether this should be a large or small compaction
   */
  public boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  public boolean needsCompaction(final Collection<StoreFile> storeFiles,
      final List<StoreFile> filesCompacting) {
    int numCandidates = storeFiles.size() - filesCompacting.size();
    return numCandidates >= comConf.getMinFilesToCompact();
  }
}
