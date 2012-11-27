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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.Private
public class CompactSelection {
  private static final long serialVersionUID = 1L;
  static final Log LOG = LogFactory.getLog(CompactSelection.class);
  // the actual list - this is needed to handle methods like "sublist"
  // correctly
  List<StoreFile> filesToCompact = new ArrayList<StoreFile>();

  /**
   * Number of off peak compactions either in the compaction queue or
   * happening now. Please lock compactionCountLock before modifying.
   */
  static long numOutstandingOffPeakCompactions = 0;

  /**
   * Lock object for numOutstandingOffPeakCompactions
   */
  private final static Object compactionCountLock = new Object();

  // was this compaction promoted to an off-peak
  boolean isOffPeakCompaction = false;
  // CompactSelection object creation time.
  private final long selectionTime;

  public CompactSelection(List<StoreFile> filesToCompact) {
    this.selectionTime = EnvironmentEdgeManager.currentTimeMillis();
    this.filesToCompact = filesToCompact;
    this.isOffPeakCompaction = false;
  }

  /**
   * Select the expired store files to compact
   * 
   * @param maxExpiredTimeStamp
   *          The store file will be marked as expired if its max time stamp is
   *          less than this maxExpiredTimeStamp.
   * @return A CompactSelection contains the expired store files as
   *         filesToCompact
   */
  public CompactSelection selectExpiredStoreFilesToCompact(
      long maxExpiredTimeStamp) {
    if (filesToCompact == null || filesToCompact.size() == 0)
      return null;
    ArrayList<StoreFile> expiredStoreFiles = null;
    boolean hasExpiredStoreFiles = false;
    CompactSelection expiredSFSelection = null;

    for (StoreFile storeFile : this.filesToCompact) {
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
   * The current compaction finished, so reset the off peak compactions count
   * if this was an off peak compaction.
   */
  public void finishRequest() {
    if (isOffPeakCompaction) {
      long newValueToLog = -1;
      synchronized(compactionCountLock) {
        assert !isOffPeakCompaction : "Double-counting off-peak count for compaction";
        newValueToLog = --numOutstandingOffPeakCompactions;
        isOffPeakCompaction = false;
      }
      LOG.info("Compaction done, numOutstandingOffPeakCompactions is now " +
          newValueToLog);
    }
  }

  public List<StoreFile> getFilesToCompact() {
    return filesToCompact;
  }

  /**
   * Removes all files from the current compaction list, and resets off peak
   * compactions is set.
   */
  public void emptyFileList() {
    filesToCompact.clear();
    if (isOffPeakCompaction) {
      long newValueToLog = -1;
      synchronized(compactionCountLock) {
        // reset the off peak count
        newValueToLog = --numOutstandingOffPeakCompactions;
        isOffPeakCompaction = false;
      }
      LOG.info("Nothing to compact, numOutstandingOffPeakCompactions is now " +
          newValueToLog);
    }
  }

  public boolean isOffPeakCompaction() {
    return this.isOffPeakCompaction;
  }

  public static long getNumOutStandingOffPeakCompactions() {
    synchronized(compactionCountLock) {
      return numOutstandingOffPeakCompactions;
    }
  }

  /**
   * Tries making the compaction off-peak.
   * Only checks internal compaction constraints, not timing.
   * @return Eventual value of isOffPeakCompaction.
   */
  public boolean trySetOffpeak() {
    assert !isOffPeakCompaction : "Double-setting off-peak for compaction " + this;
    synchronized(compactionCountLock) {
      if (numOutstandingOffPeakCompactions == 0) {
         numOutstandingOffPeakCompactions++;
         isOffPeakCompaction = true;
      }
    }
    return isOffPeakCompaction;
  }

  public long getSelectionTime() {
    return selectionTime;
  }

  public CompactSelection subList(int start, int end) {
    throw new UnsupportedOperationException();
  }

  public CompactSelection getSubList(int start, int end) {
    filesToCompact = filesToCompact.subList(start, end);
    return this;
  }

  public void clearSubList(int start, int end) {
    filesToCompact.subList(start, end).clear();
  }

  private boolean isValidHour(int hour) {
    return (hour >= 0 && hour <= 23);
  }
}
