/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * An abstract compaction policy that select files on seq id order.
 */
@InterfaceAudience.Private
public abstract class SortedCompactionPolicy extends CompactionPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(SortedCompactionPolicy.class);

  private static final Random RNG = new Random();

  public SortedCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  public List<HStoreFile> preSelectCompactionForCoprocessor(Collection<HStoreFile> candidates,
      List<HStoreFile> filesCompacting) {
    return getCurrentEligibleFiles(new ArrayList<>(candidates), filesCompacting);
  }

  /**
   * @param candidateFiles candidate files, ordered from oldest to newest by seqId. We rely on
   *   DefaultStoreFileManager to sort the files by seqId to guarantee contiguous compaction based
   *   on seqId for data consistency.
   * @return subset copy of candidate list that meets compaction criteria
   */
  public CompactionRequestImpl selectCompaction(Collection<HStoreFile> candidateFiles,
      List<HStoreFile> filesCompacting, boolean isUserCompaction, boolean mayUseOffPeak,
      boolean forceMajor) throws IOException {
    // Preliminary compaction subject to filters
    ArrayList<HStoreFile> candidateSelection = new ArrayList<>(candidateFiles);
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
      candidateSelection = skipLargeFiles(candidateSelection, mayUseOffPeak);
      isAllFiles = candidateFiles.size() == candidateSelection.size();
    }

    // Try a major compaction if this is a user-requested major compaction,
    // or if we do not have too many files to compact and this was requested as a major compaction
    boolean isTryingMajor = (forceMajor && isAllFiles && isUserCompaction)
        || (((forceMajor && isAllFiles) || shouldPerformMajorCompaction(candidateSelection))
          && (candidateSelection.size() < comConf.getMaxFilesToCompact()));
    // Or, if there are any references among the candidates.
    boolean isAfterSplit = StoreUtils.hasReferences(candidateSelection);

    CompactionRequestImpl result = createCompactionRequest(candidateSelection,
      isTryingMajor || isAfterSplit, mayUseOffPeak, mayBeStuck);
    result.setAfterSplit(isAfterSplit);

    ArrayList<HStoreFile> filesToCompact = Lists.newArrayList(result.getFiles());
    removeExcessFiles(filesToCompact, isUserCompaction, isTryingMajor);
    result.updateFiles(filesToCompact);

    isAllFiles = (candidateFiles.size() == filesToCompact.size());
    result.setOffPeak(!filesToCompact.isEmpty() && !isAllFiles && mayUseOffPeak);
    result.setIsMajor(isTryingMajor && isAllFiles, isAllFiles);

    return result;
  }

  protected abstract CompactionRequestImpl createCompactionRequest(
      ArrayList<HStoreFile> candidateSelection, boolean tryingMajor, boolean mayUseOffPeak,
      boolean mayBeStuck) throws IOException;

  /**
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  @Override
  public abstract boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
      throws IOException;

  /**
   * @param filesToCompact
   * @return When to run next major compaction
   */
  public long getNextMajorCompactTime(Collection<HStoreFile> filesToCompact) {
    /** Default to {@link org.apache.hadoop.hbase.HConstants#DEFAULT_MAJOR_COMPACTION_PERIOD}. */
    long period = comConf.getMajorCompactionPeriod();
    if (period <= 0) {
      return period;
    }

    /**
     * Default to {@link org.apache.hadoop.hbase.HConstants#DEFAULT_MAJOR_COMPACTION_JITTER},
     * that is, +/- 3.5 days (7 days * 0.5).
     */
    double jitterPct = comConf.getMajorCompactionJitter();
    if (jitterPct <= 0) {
      return period;
    }

    // deterministic jitter avoids a major compaction storm on restart
    OptionalInt seed = StoreUtils.getDeterministicRandomSeed(filesToCompact);
    if (seed.isPresent()) {
      long jitter = Math.round(period * jitterPct);
      // Synchronized to ensure one user of random instance at a time.
      synchronized (RNG) {
        RNG.setSeed(seed.getAsInt());
        return period + jitter - Math.round(2L * jitter * RNG.nextDouble());
      }
    } else {
      return 0L;
    }
  }

  /**
   * @param compactionSize Total size of some compaction
   * @return whether this should be a large or small compaction
   */
  @Override
  public boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  public abstract boolean needsCompaction(Collection<HStoreFile> storeFiles,
      List<HStoreFile> filesCompacting);

  protected ArrayList<HStoreFile> getCurrentEligibleFiles(ArrayList<HStoreFile> candidateFiles,
      final List<HStoreFile> filesCompacting) {
    // candidates = all storefiles not already in compaction queue
    if (!filesCompacting.isEmpty()) {
      // exclude all files older than the newest file we're currently
      // compacting. this allows us to preserve contiguity (HBASE-2856)
      HStoreFile last = filesCompacting.get(filesCompacting.size() - 1);
      int idx = candidateFiles.indexOf(last);
      Preconditions.checkArgument(idx != -1);
      candidateFiles.subList(0, idx + 1).clear();
    }
    return candidateFiles;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset exclude all files above maxCompactSize
   *   Also save all references. We MUST compact them
   */
  protected ArrayList<HStoreFile> skipLargeFiles(ArrayList<HStoreFile> candidates,
    boolean mayUseOffpeak) {
    int pos = 0;
    while (pos < candidates.size() && !candidates.get(pos).isReference()
      && (candidates.get(pos).getReader().length() > comConf.getMaxCompactSize(mayUseOffpeak))) {
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
   */
  protected void filterBulk(ArrayList<HStoreFile> candidates) {
    candidates.removeIf(HStoreFile::excludeFromMinorCompaction);
  }

  /**
   * @param candidates pre-filtrate
   */
  protected void removeExcessFiles(ArrayList<HStoreFile> candidates,
      boolean isUserCompaction, boolean isMajorCompaction) {
    int excess = candidates.size() - comConf.getMaxFilesToCompact();
    if (excess > 0) {
      if (isMajorCompaction && isUserCompaction) {
        LOG.debug("Warning, compacting more than " + comConf.getMaxFilesToCompact()
            + " files because of a user-requested major compaction");
      } else {
        LOG.debug("Too many admissible files. Excluding " + excess
            + " files from compaction candidates");
        candidates.subList(comConf.getMaxFilesToCompact(), candidates.size()).clear();
      }
    }
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset forget the compactionSelection if we don't have enough files
   */
  protected ArrayList<HStoreFile> checkMinFilesCriteria(ArrayList<HStoreFile> candidates,
      int minFiles) {
    if (candidates.size() < minFiles) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not compacting files because we only have " + candidates.size() +
            " files ready for compaction. Need " + minFiles + " to initiate.");
      }
      candidates.clear();
    }
    return candidates;
  }
}
