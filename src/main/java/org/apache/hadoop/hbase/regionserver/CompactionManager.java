/**
 * Copyright 2012 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

public class CompactionManager {

  private static final Log LOG = LogFactory.getLog(CompactionManager.class);
  private final static Calendar calendar = new GregorianCalendar();

  private Store store;
  CompactionConfiguration comConf;

  CompactionManager(Configuration configuration, Store store) {
    this.store = store;
    comConf = new CompactionConfiguration(configuration, store);
  }

  /**
   * Update the configuration when it changes on-the-fly.
   * @param conf
   */
  protected void updateConfiguration(Configuration conf) {
    comConf.updateConfiguration(conf);
  }

  /**
   * @param candidateFiles candidate files, ordered from oldest to newest
   * @return subset copy of candidate list that meets compaction criteria
   * @throws java.io.IOException
   */
  CompactSelection selectCompaction(List<StoreFile> candidateFiles, boolean forceMajor)
    throws IOException {

    // Prelimanry compaction subject to filters
    CompactSelection candidateSelection = new CompactSelection(candidateFiles);

    if (!forceMajor) {
      // If there are expired files, only select them so that compaction deletes them
      if (comConf.shouldDeleteExpired() && (store.ttl != Long.MAX_VALUE)) {
        CompactSelection expiredSelection = selectExpiredSFs(
          candidateSelection, EnvironmentEdgeManager.currentTimeMillis() - store.ttl);
        if (expiredSelection != null) {
          return expiredSelection;
        }
      }
      candidateSelection = skipLargeFiles(candidateSelection);
    }

    // major compact on user action or age (caveat: we have too many files)
    boolean majorCompaction = (
      (forceMajor || isMajorCompaction(candidateSelection.getFilesToCompact())) &&
        candidateSelection.getFilesToCompact().size() < comConf.getMaxFilesToCompact()) ||
        store.hasReferences(candidateSelection.getFilesToCompact());

    if (!majorCompaction) {
      // we're doing a minor compaction, let's see what files are applicable
      candidateSelection = filterBulk(candidateSelection);
      candidateSelection = applyCompactionPolicy(candidateSelection);
      candidateSelection = checkMinFilesCriteria(candidateSelection);
    }
    candidateSelection = removeExcessFiles(candidateSelection);
    return candidateSelection;
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
  private CompactSelection selectExpiredSFs
      (CompactSelection candidates, long maxExpiredTimeStamp) {
    if (candidates.filesToCompact == null
        || candidates.filesToCompact.isEmpty()) {
      return null;
    }

    ArrayList<StoreFile> expiredStoreFiles = null;

    for (StoreFile storeFile : candidates.filesToCompact) {
      if (storeFile.getReader().getMaxTimestamp() < maxExpiredTimeStamp) {
        LOG.info("Deleting the expired store file by compaction: "
            + storeFile.getPath() + " whose maxTimeStamp is "
            + storeFile.getReader().getMaxTimestamp()
            + " while the max expired timestamp is " + maxExpiredTimeStamp);
        if (expiredStoreFiles == null) {
          expiredStoreFiles = new ArrayList<StoreFile>();
        }
        expiredStoreFiles.add(storeFile);
      }
    }

    if (expiredStoreFiles == null) {
      return null;
    }

    return new CompactSelection(expiredStoreFiles);
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
      LOG.debug("Some files are too large. Excluding " + pos + " files from compaction candidates");
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
    if (comConf.shouldExcludeBulk()) {
      int previous = candidates.getFilesToCompact().size();
      candidates.getFilesToCompact().removeAll(Collections2.filter(candidates.getFilesToCompact(),
          new Predicate<StoreFile>() {
            @Override
            public boolean apply(StoreFile input) {
               // If we have assigned a sequenceId to the hfile, we won't skip the file.
              return input.isBulkLoadResult() && input.getMaxSequenceId() <= 0;
            }
          }));
      LOG.info("Exclude " + (candidates.getFilesToCompact().size() - previous) +
          " bulk imported store files from compaction candidates");
    }
    return candidates;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * take upto maxFilesToCompact from the start
   */
  private CompactSelection removeExcessFiles(CompactSelection candidates) {
    int excess = candidates.getFilesToCompact().size() - comConf.getMaxFilesToCompact();
    if (excess > 0) {
      LOG.debug("Too many admissible files. Excluding " + excess
        + " files from compaction candidates");
      candidates.clearSubList(comConf.getMaxFilesToCompact(),
        candidates.getFilesToCompact().size());
    }
    return candidates;
  }
  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * forget the compactionSelection if we don't have enough files
   */
  private CompactSelection checkMinFilesCriteria(CompactSelection candidates) {
    if (candidates.getFilesToCompact().size() < comConf.getMinFilesToCompact()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipped minor compaction of " + store + ". No admissible set of files found.");
      }
      candidates.emptyFileList();
    }
    return candidates;
  }

  /**
    * @param candidates pre-filtrate
    * @return filtered subset
    * -- Default minor compaction selection algorithm: Choose CompactSelection from candidates --
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
    double r = comConf.getCompactionRatio();

    if (isOffPeakHour()
        && !(CompactSelection.getNumOutStandingOffPeakCompactions() > 0)) {
      r = comConf.getCompactionRatioOffPeak();
      candidates.setOffPeak();
      LOG.info("Running an off-peak compaction, selection ratio = " + r
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
      fileSizes[start] > Math.max(comConf.getMinCompactSize(), (long) (sumSize[start + 1] * r))) {
      ++start;
    }
    if (start < countOfFiles) {
      LOG.info("Default compaction algorithm has selected " + (countOfFiles - start)
        + " files from " + countOfFiles + " candidates");
    }

    candidates = candidates.getSubList(start, countOfFiles);

    return candidates;
  }

  /**
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  boolean isMajorCompaction(final List<StoreFile> filesToCompact) throws IOException {

    boolean result = false;
    long mcTime = getNextMajorCompactTime();
    if (filesToCompact == null || filesToCompact.isEmpty() || mcTime == 0) {
      return result;
    }
    long lowTimestamp = getLowestTimestamp(filesToCompact);
    long now = System.currentTimeMillis();
    if (lowTimestamp > 0l && lowTimestamp < (now - mcTime)) {
      // Major compaction time has elapsed.
      long elapsedTime = now - lowTimestamp;
      if (filesToCompact.size() == 1 &&
        filesToCompact.get(0).isMajorCompaction() &&
        (store.ttl == HConstants.FOREVER || elapsedTime < store.ttl)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping major compaction of " + store
            + " because one (major) compacted file only and elapsedTime "
            + elapsedTime + "ms is < ttl=" + store.ttl);
        }
      } else if (store.isPeakTime(calendar.get(Calendar.HOUR_OF_DAY))) {
        LOG.debug("Peak traffic time for HBase, not scheduling any major "
          + "compactions. Peak hour period is : " + comConf.getOffPeakStartHour() + " - "
          + comConf.getOffPeakEndHour() + " current hour is : "
          + calendar.get(Calendar.HOUR_OF_DAY));
        result = false;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Major compaction triggered on store " + store + "; "
            + "time since last major compaction " + StringUtils.formatTimeDiff(now, lowTimestamp));
        }
        result = true;
      }
    }
    return result;
  }

  long getNextMajorCompactTime() {
    // default = 24hrs
    long ret = comConf.getMajorCompactionPeriod();
    String strCompactionTime = store.getFamily().getValue(HConstants.MAJOR_COMPACTION_PERIOD);
    if (strCompactionTime != null) {
      ret = (new Long(strCompactionTime)).longValue();
    }

    if (ret > 0) {
      // default = 20% = +/- 4.8 hrs
      double jitterPct = comConf.getMajorCompactionJitter();
      if (jitterPct > 0) {
        long jitter = Math.round(ret * jitterPct);
        // deterministic jitter avoids a major compaction storm on restart
        Integer seed = store.getDeterministicRandomSeed();
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

  /*
   * Gets lowest timestamp from candidate StoreFiles
   *
   * @param fs
   * @param dir
   * @throws IOException
   */
  static long getLowestTimestamp(final List<StoreFile> candidates)
    throws IOException {
    long minTs = Long.MAX_VALUE;
    for (StoreFile storeFile : candidates) {
      minTs = Math.min(minTs, storeFile.getModificationTimeStamp());
    }
    return minTs;
  }

  /**
   * @param compactionSize Total size of some compaction
   * @return whether this should be a large or small compaction
   */
  boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  /**
   * @param numCandidates Number of candidate store files
   * @return whether a compactionSelection is possible
   */
  boolean needsCompaction(int numCandidates) {
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
