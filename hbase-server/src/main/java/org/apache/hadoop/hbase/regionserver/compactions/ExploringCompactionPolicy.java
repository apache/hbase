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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Class to pick which files if any to compact together.
 *
 * This class will search all possibilities for different and if it gets stuck it will choose
 * the smallest set of files to compact.
 */
@InterfaceAudience.Private
public class ExploringCompactionPolicy extends RatioBasedCompactionPolicy {

  /** Computed number of files that are needed to assume compactions are stuck. */
  private final long filesNeededToForce;

  /**
   * Constructor for ExploringCompactionPolicy.
   * @param conf The configuration object
   * @param storeConfigInfo An object to provide info about the store.
   */
  public ExploringCompactionPolicy(final Configuration conf,
                                   final StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
    filesNeededToForce = storeConfigInfo.getBlockingFileCount();
  }

  @Override
  final ArrayList<StoreFile> applyCompactionPolicy(final ArrayList<StoreFile> candidates,
                                             final boolean mayUseOffPeak) throws IOException {
    // Start off choosing nothing.
    List<StoreFile> bestSelection = new ArrayList<StoreFile>(0);
    List<StoreFile> smallest = new ArrayList<StoreFile>(0);
    long bestSize = 0;
    long smallestSize = Long.MAX_VALUE;

    boolean mightBeStuck = candidates.size() >= filesNeededToForce;

    // Consider every starting place.
    for (int start = 0; start < candidates.size(); start++) {
      // Consider every different sub list permutation in between start and end with min files.
      for (int currentEnd = start + comConf.getMinFilesToCompact() - 1;
          currentEnd < candidates.size(); currentEnd++) {
        List<StoreFile> potentialMatchFiles = candidates.subList(start, currentEnd + 1);

        // Sanity checks
        if (potentialMatchFiles.size() < comConf.getMinFilesToCompact()) {
          continue;
        }
        if (potentialMatchFiles.size() > comConf.getMaxFilesToCompact()) {
          continue;
        }

        // Compute the total size of files that will
        // have to be read if this set of files is compacted.
        long size = getTotalStoreSize(potentialMatchFiles);

        // Store the smallest set of files.  This stored set of files will be used
        // if it looks like the algorithm is stuck.
        if (size < smallestSize) {
          smallest = potentialMatchFiles;
          smallestSize = size;
        }

        if (size >= comConf.getMinCompactSize()
            && !filesInRatio(potentialMatchFiles, mayUseOffPeak)) {
          continue;
        }

        if (size > comConf.getMaxCompactSize()) {
          continue;
        }

        // Keep if this gets rid of more files.  Or the same number of files for less io.
        if (potentialMatchFiles.size() > bestSelection.size()
            || (potentialMatchFiles.size() == bestSelection.size() && size < bestSize)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
        }
      }
    }
    if (bestSelection.size() == 0 && mightBeStuck) {
      return new ArrayList<StoreFile>(smallest);
    }
    return new ArrayList<StoreFile>(bestSelection);
  }

  /**
   * Find the total size of a list of store files.
   * @param potentialMatchFiles StoreFile list.
   * @return Sum of StoreFile.getReader().length();
   */
  private long getTotalStoreSize(final List<StoreFile> potentialMatchFiles) {
    long size = 0;

    for (StoreFile s:potentialMatchFiles) {
      size += s.getReader().length();
    }
    return size;
  }

  /**
   * Check that all files satisfy the constraint
   *      FileSize(i) <= ( Sum(0,N,FileSize(_)) - FileSize(i) ) * Ratio.
   *
   * @param files List of store files to consider as a compaction candidate.
   * @param isOffPeak should the offPeak compaction ratio be used ?
   * @return a boolean if these files satisfy the ratio constraints.
   */
  private boolean filesInRatio(final List<StoreFile> files, final boolean isOffPeak) {
    if (files.size() < 2) {
      return  true;
    }
    final double currentRatio =
        isOffPeak ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();

    long totalFileSize = getTotalStoreSize(files);

    for (StoreFile file : files) {
      long singleFileSize = file.getReader().length();
      long sumAllOtherFileSizes = totalFileSize - singleFileSize;

      if (singleFileSize > sumAllOtherFileSizes * currentRatio) {
        return false;
      }
    }
    return true;
  }
}
