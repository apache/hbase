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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to pick which files if any to compact together.
 *
 * This class will search all possibilities for different and if it gets stuck it will choose
 * the smallest set of files to compact.
 */
@InterfaceAudience.Private
public class ExploringCompactionPolicy extends RatioBasedCompactionPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(ExploringCompactionPolicy.class);

  /**
   * Constructor for ExploringCompactionPolicy.
   * @param conf The configuration object
   * @param storeConfigInfo An object to provide info about the store.
   */
  public ExploringCompactionPolicy(final Configuration conf,
                                   final StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  protected final ArrayList<HStoreFile> applyCompactionPolicy(ArrayList<HStoreFile> candidates,
      boolean mayUseOffPeak, boolean mightBeStuck) throws IOException {
    return new ArrayList<>(applyCompactionPolicy(candidates, mightBeStuck, mayUseOffPeak,
      comConf.getMinFilesToCompact(), comConf.getMaxFilesToCompact()));
  }

  public List<HStoreFile> applyCompactionPolicy(List<HStoreFile> candidates, boolean mightBeStuck,
      boolean mayUseOffPeak, int minFiles, int maxFiles) {
    final double currentRatio = mayUseOffPeak
        ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();

    // Start off choosing nothing.
    List<HStoreFile> bestSelection = new ArrayList<>(0);
    List<HStoreFile> smallest = mightBeStuck ? new ArrayList<>(0) : null;
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    int opts = 0, optsInRatio = 0, bestStart = -1; // for debug logging
    // Consider every starting place.
    for (int start = 0; start < candidates.size(); start++) {
      // Consider every different sub list permutation in between start and end with min files.
      for (int currentEnd = start + minFiles - 1;
          currentEnd < candidates.size(); currentEnd++) {
        List<HStoreFile> potentialMatchFiles = candidates.subList(start, currentEnd + 1);

        // Sanity checks
        if (potentialMatchFiles.size() < minFiles) {
          continue;
        }
        if (potentialMatchFiles.size() > maxFiles) {
          continue;
        }

        // Compute the total size of files that will
        // have to be read if this set of files is compacted.
        long size = getTotalStoreSize(potentialMatchFiles);

        // Store the smallest set of files.  This stored set of files will be used
        // if it looks like the algorithm is stuck.
        if (mightBeStuck && size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }

        if (size > comConf.getMaxCompactSize(mayUseOffPeak)) {
          continue;
        }

        ++opts;
        if (size >= comConf.getMinCompactSize()
            && !filesInRatio(potentialMatchFiles, currentRatio)) {
          continue;
        }

        ++optsInRatio;
        if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
          bestStart = start;
        }
      }
    }
    if (bestSelection.isEmpty() && mightBeStuck) {
      LOG.debug("Exploring compaction algorithm has selected " + smallest.size()
          + " files of size "+ smallestSize + " because the store might be stuck");
      return new ArrayList<>(smallest);
    }
    LOG.debug("Exploring compaction algorithm has selected {}  files of size {} starting at " +
      "candidate #{} after considering {} permutations with {} in ratio", bestSelection.size(),
      bestSize, bestStart, opts, optsInRatio);
    return new ArrayList<>(bestSelection);
  }

  /**
   * Select at least one file in the candidates list to compact, through choosing files
   * from the head to the index that the accumulation length larger the max compaction size.
   * This method is a supplementary of the selectSimpleCompaction() method, aims to make sure
   * at least one file can be selected to compact, for compactions like L0 files, which need to
   * compact all files and as soon as possible.
   */
  public List<HStoreFile> selectCompactFiles(final List<HStoreFile> candidates, int maxFiles,
      boolean isOffpeak) {
    long selectedSize = 0L;
    for (int end = 0; end < Math.min(candidates.size(), maxFiles); end++) {
      selectedSize += candidates.get(end).getReader().length();
      if (selectedSize >= comConf.getMaxCompactSize(isOffpeak)) {
        return candidates.subList(0, end + 1);
      }
    }
    return candidates;
  }

  private boolean isBetterSelection(List<HStoreFile> bestSelection, long bestSize,
      List<HStoreFile> selection, long size, boolean mightBeStuck) {
    if (mightBeStuck && bestSize > 0 && size > 0) {
      // Keep the selection that removes most files for least size. That penaltizes adding
      // large files to compaction, but not small files, so we don't become totally inefficient
      // (might want to tweak that in future). Also, given the current order of looking at
      // permutations, prefer earlier files and smaller selection if the difference is small.
      final double REPLACE_IF_BETTER_BY = 1.05;
      double thresholdQuality = ((double)bestSelection.size() / bestSize) * REPLACE_IF_BETTER_BY;
      return thresholdQuality < ((double)selection.size() / size);
    }
    // Keep if this gets rid of more files.  Or the same number of files for less io.
    return selection.size() > bestSelection.size()
      || (selection.size() == bestSelection.size() && size < bestSize);
  }

  /**
   * Find the total size of a list of store files.
   * @param potentialMatchFiles StoreFile list.
   * @return Sum of StoreFile.getReader().length();
   */
  private long getTotalStoreSize(List<HStoreFile> potentialMatchFiles) {
    return potentialMatchFiles.stream().mapToLong(sf -> sf.getReader().length()).sum();
  }

  /**
   * Check that all files satisfy the constraint
   *      FileSize(i) <= ( Sum(0,N,FileSize(_)) - FileSize(i) ) * Ratio.
   *
   * @param files List of store files to consider as a compaction candidate.
   * @param currentRatio The ratio to use.
   * @return a boolean if these files satisfy the ratio constraints.
   */
  private boolean filesInRatio(List<HStoreFile> files, double currentRatio) {
    if (files.size() < 2) {
      return true;
    }

    long totalFileSize = getTotalStoreSize(files);

    for (HStoreFile file : files) {
      long singleFileSize = file.getReader().length();
      long sumAllOtherFileSizes = totalFileSize - singleFileSize;

      if (singleFileSize > sumAllOtherFileSizes * currentRatio) {
        return false;
      }
    }
    return true;
  }
}
