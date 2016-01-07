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
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreFile;

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

  // HBase conf object
  Configuration conf;
  // was this compaction promoted to an off-peak
  boolean isOffPeakCompaction = false;
  // compactRatio: double on purpose!  Float.MAX < Long.MAX < Double.MAX
  // With float, java will downcast your long to float for comparisons (bad)
  private double compactRatio;
  // compaction ratio off-peak
  private double compactRatioOffPeak;
  // offpeak start time
  private int offPeakStartHour = -1;
  // off peak end time
  private int offPeakEndHour = -1;

  public CompactSelection(Configuration conf, List<StoreFile> filesToCompact) {
    this.filesToCompact = filesToCompact;
    this.conf = conf;
    this.compactRatio = conf.getFloat("hbase.hstore.compaction.ratio", 1.2F);
    this.compactRatioOffPeak = conf.getFloat("hbase.hstore.compaction.ratio.offpeak", 5.0F);

    // Peak time is from [offPeakStartHour, offPeakEndHour). Valid numbers are [0, 23]
    this.offPeakStartHour = conf.getInt("hbase.offpeak.start.hour", -1);
    this.offPeakEndHour = conf.getInt("hbase.offpeak.end.hour", -1);
    if (!isValidHour(this.offPeakStartHour) || !isValidHour(this.offPeakEndHour)) {
      if (!(this.offPeakStartHour == -1 && this.offPeakEndHour == -1)) {
        LOG.warn("Invalid start/end hour for peak hour : start = " +
            this.offPeakStartHour + " end = " + this.offPeakEndHour +
            ". Valid numbers are [0-23]");
      }
      this.offPeakStartHour = this.offPeakEndHour = -1;
    }
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
      if (expiredStoreFiles.size() == 1
          && expiredStoreFiles.get(0).getReader().getEntries() == 0) {
        // If just one empty store file, do not select for compaction.
        return expiredSFSelection;
      }
      expiredSFSelection = new CompactSelection(conf, expiredStoreFiles);
    }
    return expiredSFSelection;
  }

  /**
   * If the current hour falls in the off peak times and there are no 
   * outstanding off peak compactions, the current compaction is 
   * promoted to an off peak compaction. Currently only one off peak 
   * compaction is present in the compaction queue.
   *
   * @param currentHour
   * @return
   */
  public double getCompactSelectionRatio() {
    double r = this.compactRatio;
    synchronized(compactionCountLock) {
      if (isOffPeakHour() && numOutstandingOffPeakCompactions == 0) {
        r = this.compactRatioOffPeak;
        numOutstandingOffPeakCompactions++;
        isOffPeakCompaction = true;
      }
    }
    if(isOffPeakCompaction) {
      LOG.info("Running an off-peak compaction, selection ratio = " +
          compactRatioOffPeak + ", numOutstandingOffPeakCompactions is now " +
          numOutstandingOffPeakCompactions);
    }
    return r;
  }

  /**
   * The current compaction finished, so reset the off peak compactions count
   * if this was an off peak compaction.
   */
  public void finishRequest() {
    if (isOffPeakCompaction) {
      synchronized(compactionCountLock) {
        numOutstandingOffPeakCompactions--;
        isOffPeakCompaction = false;
      }
      LOG.info("Compaction done, numOutstandingOffPeakCompactions is now " +
          numOutstandingOffPeakCompactions);
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
      synchronized(compactionCountLock) {
        // reset the off peak count
        numOutstandingOffPeakCompactions--;
        isOffPeakCompaction = false;
      }
      LOG.info("Nothing to compact, numOutstandingOffPeakCompactions is now " +
          numOutstandingOffPeakCompactions);
    }
  }

  public boolean isOffPeakCompaction() {
    return this.isOffPeakCompaction;
  }

  private boolean isOffPeakHour() {
    int currentHour = (new GregorianCalendar()).get(Calendar.HOUR_OF_DAY);
    // If offpeak time checking is disabled just return false.
    if (this.offPeakStartHour == this.offPeakEndHour) {
      return false;
    }
    if (this.offPeakStartHour < this.offPeakEndHour) {
      return (currentHour >= this.offPeakStartHour && currentHour < this.offPeakEndHour);
    }
    return (currentHour >= this.offPeakStartHour || currentHour < this.offPeakEndHour);
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
