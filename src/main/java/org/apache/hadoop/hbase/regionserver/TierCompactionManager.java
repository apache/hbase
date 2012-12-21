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
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;
public class TierCompactionManager extends CompactionManager {

  private static final Log LOG = LogFactory.getLog(TierCompactionManager.class);

  private int[] endInTier;
  private int[] tierOf;

  private TierCompactionConfiguration tierConf;

  TierCompactionManager(Configuration configuration, Store store) {
    super(configuration, store);
    comConf = new TierCompactionConfiguration(configuration, store);
    tierConf = (TierCompactionConfiguration) comConf;
  }

  /**
   * @param candidates pre-filtrate
   * @return filtered subset
   * -- Tier Based minor compaction selection algorithm: Choose CompactSelection from candidates --
   * <p/>
   * First exclude bulk-load files if indicated in configuration.
   * Arrange files from oldest to newest then select an appropriate ['start','end') pair
   * try 'start' from oldest to newest (smallest to largest fileIndex)
   * for each value, identify the 'end' fileIndex
   * stop when the range ['start','end') is an admissible compaction
   * <p/>
   * Notes:
   * <p/>
   * a compaction is admissible if
   * - file fileSize[start] is at most maxCompactSize AND
   * - number of files is at least currentTier.minFilesToCompact AND
   * - (fileSize[start] is at most ratio times the rest of the files in the compaction OR
   * - fileSize[start] is at most minCompactSize)
   * <p/>
   * end is endInTier[tierOf[start].endingInclusionTier]
   * By default currentTier.endingIndexForTier = currentTier, so in the default
   * case 'end' is always 1 + the last fileIndex in currentTier, making sure
   * files from different tiers are never selected together in the default case
   * normal skew:
   *
   *         older ----> newer (increasing seqID, increasing minFlushTime)
   *
   * Tier 2  |  Tier 1   |  Tier 0
   *        |          |
   *     _  |          |
   *    | | |  _       |
   *    | | | | |   _  |
   *  --|-|-|-|-|- |-|-|--_-------_-------  minCompactSize
   *    | | | | |  | | | | |  _  | |
   *    | | | | |  | | | | | | | | |
   *    | | | | |  | | | | | | | | |
   */
  @Override
  CompactSelection applyCompactionPolicy(CompactSelection candidates) throws IOException {
    // we're doing a minor compaction, let's see what files are applicable
    int start = -1;
    int end = -1;

    // skip selection algorithm if we don't have enough files
    if (candidates.getFilesToCompact().isEmpty()) {
      candidates.emptyFileList();
      return candidates;
    }

    // get store file sizes for incremental compacting selection.
    int countOfFiles = candidates.getFilesToCompact().size();
    long[] fileSizes = new long[countOfFiles];
    StoreFile file;
    long[] sumSize = new long[countOfFiles + 1];
    sumSize[countOfFiles] = 0;
    for (int i = countOfFiles - 1; i >= 0; --i) {
      file = candidates.getFilesToCompact().get(i);
      fileSizes[i] = file.getReader().length();
      // calculate the sum of fileSizes[i,i+maxFilesToCompact-1) for algo
      sumSize[i] = fileSizes[i] + sumSize[i + 1];
    }

    /**
     * divide into tiers:
     * assign tierOf[fileIndex] = tierIndex
     * assign endInTier[tierIndex] = 1 + index of the last file in tierIndex
     */
    // Backward compatibility - if files with indices < i don't have minFlushTime field, then
    //    all of them get tierOf[i]. If no file has minFlushTime all gets tier zero.
    int numTiers = tierConf.getNumCompactionTiers();
    TierCompactionConfiguration.CompactionTier tier;
    tierOf = new int[countOfFiles];
    endInTier = new int[numTiers + 1];
    endInTier[numTiers] = 0;

    LOG.info("Applying TierCompactionPolicy with " + countOfFiles + " files");

    int i;
    int j = countOfFiles;
    //
    Calendar tierBoundary = tierConf.isTierBoundaryFixed() ? Calendar
        .getInstance() : null;
    for (i = 0; i < numTiers; i++) {
      tier = tierConf.getCompactionTier(i);
      // assign the current tier as the start point of the next tier
      if (tierConf.isTierBoundaryFixed()) {
        tierBoundary = tier.getTierBoundary(tierBoundary);
      }
      endInTier[i] = j;
      while (j > 0) {
        file = candidates.getFilesToCompact().get(j - 1);
        if (!isInTier(file, tier, tierBoundary)) {
          break;
        }
        j--;
        tierOf[j] = i;
      }
    }

    long restSize;
    double ratio;

    //Main algorithm
    for (j = 0; j < countOfFiles; j++) {
      start = next(start);
      tier = tierConf.getCompactionTier(tierOf[start]);
      end = endInTier[tier.getEndingIndexForTier()];
      restSize = sumSize[start + 1] - sumSize[end];
      ratio = tier.getCompactionRatio();
      if (fileSizes[start] <= tierConf.getMaxCompactSize() &&
        end - start >= tier.getMinFilesToCompact() &&
        (fileSizes[start] <= tierConf.getMinCompactSize() ||
          (fileSizes[start] <= restSize * ratio))) {
        break;
      }
    }
    String tab = "    ";
    for (i = 0; i < numTiers; i++) {
      LOG.info("Tier " + i + " : " + tierConf.getCompactionTier(i).getDescription());
      if (endInTier[i] == endInTier[i+1]) {
        LOG.info(tab + "No file is assigned to this tier.");
      } else {
        LOG.info(tab + (endInTier[i] - endInTier[i+1])
          + " file(s) are assigned to this tier with serial number(s) "
          + endInTier[i + 1] + " to " + (endInTier[i] - 1));
      }
      for (j = endInTier[i + 1]; j < endInTier[i]; j++) {
        file = candidates.getFilesToCompact().get(j);
        LOG.info(tab + tab + "SeqID = " + file.getMaxSequenceId()
          + ", Age = " + StringUtils.formatTimeDiff(
              EnvironmentEdgeManager.currentTimeMillis(), file.getMinFlushTime())
          + ", Size = " + StringUtils.humanReadableInt(fileSizes[j])
          + ", Path = " + file.getPath());
      }
    }
    if (start < countOfFiles) {
      end = Math.min(end, start
        + tierConf.getCompactionTier(tierOf[start]).getMaxFilesToCompact());
    }
    if (start < end) {
      String strTier = String.valueOf(tierOf[start]);
      if (tierOf[end - 1] != tierOf[start]) {
        strTier += " to " + tierOf[end - 1];
      }
      LOG.info("Tier Based compaction algorithm has selected " + (end - start)
        + " files from tier " + strTier + " out of " + countOfFiles + " candidates");
    }

    candidates = candidates.getSubList(start, end);
    return candidates;
  }

  // Either we use age-based tier compaction or boundary-based tier compaction
  private boolean isInTier(StoreFile file, TierCompactionConfiguration.CompactionTier tier, 
      Calendar tierBoundary) {
    if (tierConf.isTierBoundaryFixed()) {
      return file.getMinFlushTime() >= tierBoundary.getTimeInMillis();
    } else {
      return file.getReader().length() <= tier.getMaxSize()
          && EnvironmentEdgeManager.currentTimeMillis()
          - file.getMinFlushTime() <= tier.getMaxAgeInDisk();
    }
  }
  
  /**
   * This function iterates over the start values in order.
   * Whenever an admissible compaction is found, we return the selection.
   * Hence the order is important if there are more than one admissible compaction.
   * @param start current Value
   * @return next Value
   */
  private int next(int start) {
    if (tierConf.isRecentFirstOrder()) {
      return backNext(start);
    }
    return fwdNext(start);
  }

  /**
   * This function iterates over the start values in newer-first order of tiers,
   * but older-first order of files within a tier.
   * For example, suppose the tiers are:
   * Tier 3 - files 0,1,2
   * Tier 2 - files 3,4
   * Tier 1 - no files
   * Tier 0 - files 5,6,7
   * Then the order of 'start' files will be:
   * 5,6,7,3,4,0,1,2
   * @param start current Value
   * @return next Value
   */
  private int backNext(int start) {
    int tier = 0;
    if (start == -1) {
      while (endInTier[tier] >= endInTier[0]) {
        tier++;
      }
      return endInTier[tier];
    }
    tier = tierOf[start];
    if (endInTier[tier] == start + 1) {
      tier++;
      start = endInTier[tier];
      while (endInTier[tier] >= start) {
        tier++;
      }
      return endInTier[tier];
    }
    return start + 1;
  }

  /**
   * This function iterates over the start values in older-first order of files.
   * @param start current Value
   * @return next Value
   */
  private int fwdNext(int start) {
    return start + 1;
  }

}