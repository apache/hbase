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

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds information relevant for tracking the progress of a
 * compaction.
 *
 * <p>The metrics tracked allow one to calculate the percent completion of the
 * compaction based on the number of Key/Value pairs already compacted vs.
 * total amount scheduled to be compacted.
 *
 */
@InterfaceAudience.Private
public class CompactionProgress {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionProgress.class);

  /** the total compacting key values in currently running compaction */
  public long totalCompactingKVs;
  /** the completed count of key values in currently running compaction */
  public long currentCompactedKVs = 0;
  /** the total size of data processed by the currently running compaction, in bytes */
  public long totalCompactedSize = 0;

  /** Constructor
   * @param totalCompactingKVs the total Key/Value pairs to be compacted
   */
  public CompactionProgress(long totalCompactingKVs) {
    this.totalCompactingKVs = totalCompactingKVs;
  }

  /** getter for calculated percent complete
   * @return float
   */
  public float getProgressPct() {
    return (float)currentCompactedKVs / getTotalCompactingKVs();
  }

  /**
   * Cancels the compaction progress, setting things to 0.
   */
  public void cancel() {
    this.currentCompactedKVs = this.totalCompactingKVs = 0;
  }

  /**
   * Marks the compaction as complete by setting total to current KV count;
   * Total KV count is an estimate, so there might be a discrepancy otherwise.
   */
  public void complete() {
    this.totalCompactingKVs = this.currentCompactedKVs;
  }

  /**
   * @return the total compacting key values in currently running compaction
   */
  public long getTotalCompactingKVs() {
    if (totalCompactingKVs < currentCompactedKVs) {
      LOG.debug("totalCompactingKVs={} less than currentCompactedKVs={}",
        totalCompactingKVs, currentCompactedKVs);
      return currentCompactedKVs;
    }
    return totalCompactingKVs;
  }

  /**
   * @return the completed count of key values in currently running compaction
   */
  public long getCurrentCompactedKvs() {
    return currentCompactedKVs;
  }

  /**
   * @return the total data size processed by the currently running compaction, in bytes
   */
  public long getTotalCompactedSize() {
    return totalCompactedSize;
  }

  @Override
  public String toString() {
    return String.format("%d/%d (%.2f%%)", currentCompactedKVs, getTotalCompactingKVs(),
      100 * getProgressPct());
  }
}
