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

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.StoreConfiguration;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

/**
 * The default (and only, as of now) algorithm for selecting files for compaction.
 * Combines the compaction configuration and the provisional file selection that
 * it's given to produce the list of suitable candidates for compaction.
 */
@InterfaceAudience.Private
public class CompactionPolicy {

  private static final Log LOG = LogFactory.getLog(CompactionPolicy.class);
  private final static Calendar calendar = new GregorianCalendar();

  CompactionConfiguration comConf;
  StoreConfiguration storeConfig;

  public CompactionPolicy(Configuration configuration, StoreConfiguration storeConfig) {
    updateConfiguration(configuration, storeConfig);
  }

  /**
   * @param candidateFiles candidate files, ordered from oldest to newest
   * @return subset copy of candidate list that meets compaction criteria
   * @throws java.io.IOException
   */
  public CompactSelection selectCompaction(List<StoreFile> candidateFiles,
      boolean isUserCompaction, boolean forceMajor)
    throws IOException {
    // Prelimanry compaction subject to filters
    CompactSelection candidateSelection = new CompactSelection(candidateFiles);
    long cfTtl = this.storeConfig.getStoreFileTtl();
    if (!forceMajor) {
      // If there are expired files, only select them so that compaction deletes them
      if (comConf.shouldDeleteExpired() && (cfTtl != Long.MAX_VALUE)) {
        CompactSelection expiredSelection = selectExpiredStoreFiles(
          candidateSelection, EnvironmentEdgeManager.currentTimeMillis() - cfTtl);
        if (expiredSelection != null) {
          return expiredSelection;
        }
      }
      candidateSelection = skipLargeFiles(candidateSelection);
    }

    // Force a major compaction if this is a user-requested major compaction,
    // or if we do not have too many files to compact and this was requested
    // as a major compaction.
    // Or, if there are any references among the candidates.
    boolean majorCompaction = (
      (forceMajor && isUserCompaction)
      || ((forceMajor || isMajorCompaction(candidateSelection.getFilesToCompact()))
          && (candidateSelection.getFilesToCompact().size() < comConf.getMaxFilesToCompact()))
      || StoreUtils.hasReferences(candidateSelection.getFilesToCompact())
      );

    if (!majorCompaction) {
      // we're doing a minor compaction, let's see what files are applicable
      candidateSelection = filterBulk(candidateSelection);
      candidateSelection = applyCompactionPolicy(candidateSelection);
      candidateSelection = checkMinFilesCriteria(candidateSelection);
    }
    candidateSelection =
        removeExcessFiles(candidateSelection, isUserCompaction, majorCompaction);
    return candidateSelection;
  }

  /**
   * Updates the compaction configuration. Used for tests.
   * TODO: replace when HBASE-3909 is completed in some form.
   */
  public void updateConfiguration(Configuration configuration,
      StoreConfiguration storeConfig) {
    this.comConf = new CompactionConfiguration(configuration, storeConfig);
    this.storeConfig = storeConfig;
  }

  /**
   * Select the expired store files to compact
   *
   * @param candidates the initial set of storeFiles
   * @param maxExpiredTimeStamp
   *          The store file will be marked as expired if its max time stamp is
   *          less than this maxExpiredTimeStamp.
   * @return A CompactSelection contains the expired store files as
   *         filesToCompact
   */
  private CompactSelection selectExpiredStoreFiles(
      CompactSelection candidates, long maxExpiredTimeStamp) {
    List<StoreFile> filesToCompact = candidates.getFilesToCompact();
    if (filesToCompact == null || filesToCompact.size() == 0)
      return null;
    ArrayList<StoreFile> expiredStoreFiles = null;
    boolean hasExpiredStoreFiles = false;
    CompactSelection expiredSFSelection = null;

    for (StoreFile storeFile : filesToCompact) {
      if (storeFile.getReader().getMaxTimestamp() < maxExpiredTimeStamp) {
        LOG.info("Deleting the expired store file by compaction: "
            + storeFile.getPath() + " whose maxTimeStamp is "
            + storeFile.getReader().getMaxTimestamp()
            + " while the max expired timestamp is " + maxExpiredTimeStamp);
        if (!hasExpiredStoreFiles) {
          expiredStoreFiles = new ArrayList<StoreFile>();
          hasExpiredStoreFiles = true;
        }
        expiredStoreFiles.add(storeFile);
      }
    }

    if (hasExpiredStoreFiles) {
      expiredSFSelection = new CompactSelection(expiredStoreFiles);
    }
    return expiredSFSelection;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * exclude all files above maxCompactSize
   * Also save all references. We MUST compact them
   */
  private CompactSelection skipLargeFiles(CompactSelection candidates) {
    int pos = 0;
    while (pos < candidates.getFilesToCompact().size() &&
      candidates.getFilesToCompact().get(pos).getReader().length() >
        comConf.getMaxCompactSize() &&
      !candidates.getFilesToCompact().get(pos).isReference()) {
      ++pos;
    }
    if (pos > 0) {
      LOG.debug("Some files are too large. Excluding " + pos
          + " files from compaction candidates");
      candidates.clearSubList(0, pos);
    }
    return candidates;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * exclude all bulk load files if configured
   */
  private CompactSelection filterBulk(CompactSelection candidates) {
    candidates.getFilesToCompact().removeAll(Collections2.filter(
        candidates.getFilesToCompact(),
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
  private CompactSelection removeExcessFiles(CompactSelection candidates,
      boolean isUserCompaction, boolean isMajorCompaction) {
    int excess = candidates.getFilesToCompact().size() - comConf.getMaxFilesToCompact();
    if (excess > 0) {
      if (isMajorCompaction && isUserCompaction) {
        LOG.debug("Warning, compacting more than " + comConf.getMaxFilesToCompact() +
            " files because of a user-requested major compaction");
      } else {
        LOG.debug("Too many admissible files. Excluding " + excess
          + " files from compaction candidates");
        candidates.clearSubList(comConf.getMaxFilesToCompact(),
          candidates.getFilesToCompact().size());
      }
    }
    return candidates;
  }
  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * forget the compactionSelection if we don't have enough files
   */
  private CompactSelection checkMinFilesCriteria(CompactSelection candidates) {
    int minFiles = comConf.getMinFilesToCompact();
    if (candidates.getFilesToCompact().size() < minFiles) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Not compacting files because we only have " +
            candidates.getFilesToCompact().size() +
          " files ready for compaction.  Need " + minFiles + " to initiate.");
      }
      candidates.emptyFileList();
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
  CompactSelection applyCompactionPolicy(CompactSelection candidates) throws IOException {
    if (candidates.getFilesToCompact().isEmpty()) {
      return candidates;
    }

    // we're doing a minor compaction, let's see what files are applicable
    int start = 0;
    double ratio = comConf.getCompactionRatio();
    if (isOffPeakHour() && candidates.trySetOffpeak()) {
      ratio = comConf.getCompactionRatioOffPeak();
      LOG.info("Running an off-peak compaction, selection ratio = " + ratio
          + ", numOutstandingOffPeakCompactions is now "
          + CompactSelection.getNumOutStandingOffPeakCompactions());
    }

    // get store file sizes for incremental compacting selection.
    int countOfFiles = candidates.getFilesToCompact().size();
    long[] fileSizes = new long[countOfFiles];
    long[] sumSize = new long[countOfFiles];
    for (int i = countOfFiles - 1; i >= 0; --i) {
      StoreFile file = candidates.getFilesToCompact().get(i);
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
    }

    candidates = candidates.getSubList(start, countOfFiles);

    return candidates;
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  public boolean isMajorCompaction(final List<StoreFile> filesToCompact)
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
      long cfTtl = this.storeConfig.getStoreFileTtl();
      if (filesToCompact.size() == 1) {
        // Single file
        StoreFile sf = filesToCompact.get(0);
        Long minTimestamp = sf.getMinimumTimestamp();
        long oldest = (minTimestamp == null)
            ? Long.MIN_VALUE
            : now - minTimestamp.longValue();
        if (sf.isMajorCompaction() &&
            (cfTtl == HConstants.FOREVER || oldest < cfTtl)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping major compaction of " + this +
                " because one (major) compacted file only and oldestTime " +
                oldest + "ms is < ttl=" + cfTtl);
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

  public long getNextMajorCompactTime(final List<StoreFile> filesToCompact) {
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
          double rnd = (new Random(seed)).nextDouble();
          ret += jitter - Math.round(2L * jitter * rnd);
        } else {
          ret = 0; // no storefiles == no major compaction
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

  /**
   * @param numCandidates Number of candidate store files
   * @return whether a compactionSelection is possible
   */
  public boolean needsCompaction(int numCandidates) {
    return numCandidates > comConf.getMinFilesToCompact();
  }

  /**
   * @return whether this is off-peak hour
   */
  private boolean isOffPeakHour() {
    int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
    int startHour = comConf.getOffPeakStartHour();
    int endHour = comConf.getOffPeakEndHour();
    // If offpeak time checking is disabled just return false.
    if (startHour == endHour) {
      return false;
    }
    if (startHour < endHour) {
      return (currentHour >= startHour && currentHour < endHour);
    }
    return (currentHour >= startHour || currentHour < endHour);
  }
}