/**
 * Copyright 2011 The Apache Software Foundation
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

/**
 * This class is NOT thread-safe.
 *
 * It does support concurrent access by multiple threads only if each object
 * is accessed by only one thread.
 *
 * The synchronisation is done so that the static fields of the class are
 * consistents.
 */

package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class CompactSelection {

  private static final long serialVersionUID = 1L;

  /** an unique ID for each compaction for log */
  private static long counter = 0;
  private final long compactSelectionID;
  private final long selectionTime;

  static final Log LOG = LogFactory.getLog(CompactSelection.class);
  // the actual list - this is needed to handle methods like "sublist" 
  // correctly
  List<StoreFile> filesToCompact = new ArrayList<StoreFile>();
  // number of off peak compactions either in the compaction queue or  
  // happening now
  private static AtomicLong numOutstandingOffPeakCompactions = new AtomicLong(0);
  // was this compaction promoted to an off-peak
  boolean isOffPeakCompaction = false;
  // Remember the set of expired storeFiles


  public CompactSelection(List<StoreFile> filesToCompact) {
    this.filesToCompact = filesToCompact;
    this.compactSelectionID = counter;
    this.selectionTime = EnvironmentEdgeManager.currentTimeMillis();
    counter++;
  }

  long getCompactSelectionID() {
    return compactSelectionID;
  }


  long getSelectionTime() {
    return selectionTime;
  }

  /**
   * The current compaction finished, so reset the off peak compactions count 
   * if this was an off peak compaction.
   */
  public void finishRequest() {
    if (isOffPeakCompaction) {
      numOutstandingOffPeakCompactions.decrementAndGet();
      isOffPeakCompaction = false;
    }
    LOG.info("Compaction done, numOutstandingOffPeakCompactions is now " +
        numOutstandingOffPeakCompactions.get());
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
      // reset the off peak count
      numOutstandingOffPeakCompactions.decrementAndGet();
      isOffPeakCompaction = false;
    }
    LOG.info("Nothing to compact, numOutstandingOffPeakCompactions is now " +
        numOutstandingOffPeakCompactions.get());
  }
  
  public boolean isOffPeakCompaction() {
    return this.isOffPeakCompaction;
  }

  void setOffPeak() {
    numOutstandingOffPeakCompactions.incrementAndGet();
    isOffPeakCompaction = true;
  }

 static long getNumOutStandingOffPeakCompactions() {
   return numOutstandingOffPeakCompactions.get();
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

}
