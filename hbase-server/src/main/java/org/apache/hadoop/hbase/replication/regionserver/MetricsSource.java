/**
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

package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This class is for maintaining the various replication statistics for a source and publishing them
 * through the metrics interfaces.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class MetricsSource {

  public static final String SOURCE_SIZE_OF_LOG_QUEUE = "source.sizeOfLogQueue";
  public static final String SOURCE_AGE_OF_LAST_SHIPPED_OP = "source.ageOfLastShippedOp";
  public static final String SOURCE_LOG_EDITS_READ = "source.logEditsRead";
  public static final String SOURCE_LOG_EDITS_FILTERED = "source.logEditsFiltered";
  public static final String SOURCE_SHIPPED_BATCHES = "source.shippedBatches";
  public static final String SOURCE_SHIPPED_KBS = "source.shippedKBs";
  public static final String SOURCE_SHIPPED_OPS = "source.shippedOps";
  public static final String SOURCE_LOG_READ_IN_BYTES = "source.logReadInBytes";

  public static final Log LOG = LogFactory.getLog(MetricsSource.class);
  private String id;

  private long lastTimestamp = 0;
  private int lastQueueSize = 0;

  private String sizeOfLogQueKey;
  private String ageOfLastShippedOpKey;
  private String logEditsReadKey;
  private String logEditsFilteredKey;
  private final String shippedBatchesKey;
  private final String shippedOpsKey;
  private final String shippedKBsKey;
  private final String logReadInBytesKey;

  private MetricsReplicationSource rms;

  /**
   * Constructor used to register the metrics
   *
   * @param id Name of the source this class is monitoring
   */
  public MetricsSource(String id) {
    this.id = id;

    sizeOfLogQueKey = "source." + id + ".sizeOfLogQueue";
    ageOfLastShippedOpKey = "source." + id + ".ageOfLastShippedOp";
    logEditsReadKey = "source." + id + ".logEditsRead";
    logEditsFilteredKey = "source." + id + ".logEditsFiltered";
    shippedBatchesKey = "source." + this.id + ".shippedBatches";
    shippedOpsKey = "source." + this.id + ".shippedOps";
    shippedKBsKey = "source." + this.id + ".shippedKBs";
    logReadInBytesKey = "source." + this.id + ".logReadInBytes";
    rms = CompatibilitySingletonFactory.getInstance(MetricsReplicationSource.class);
  }

  /**
   * Set the age of the last edit that was shipped
   *
   * @param timestamp write time of the edit
   */
  public void setAgeOfLastShippedOp(long timestamp) {
    long age = EnvironmentEdgeManager.currentTimeMillis() - timestamp;
    rms.setGauge(ageOfLastShippedOpKey, age);
    rms.setGauge(SOURCE_AGE_OF_LAST_SHIPPED_OP, age);
    this.lastTimestamp = timestamp;
  }

  /**
   * Convenience method to use the last given timestamp to refresh the age of the last edit. Used
   * when replication fails and need to keep that metric accurate.
   */
  public void refreshAgeOfLastShippedOp() {
    if (this.lastTimestamp > 0) {
      setAgeOfLastShippedOp(this.lastTimestamp);
    }
  }

  /**
   * Set the size of the log queue
   *
   * @param size the size.
   */
  public void setSizeOfLogQueue(int size) {
    rms.setGauge(sizeOfLogQueKey, size);
    rms.incGauge(SOURCE_SIZE_OF_LOG_QUEUE, size - lastQueueSize);
    lastQueueSize = size;
  }

  /**
   * Add on the the number of log edits read
   *
   * @param delta the number of log edits read.
   */
  private void incrLogEditsRead(long delta) {
    rms.incCounters(logEditsReadKey, delta);
    rms.incCounters(SOURCE_LOG_EDITS_READ, delta);
  }

  /** Increment the number of log edits read by one. */
  public void incrLogEditsRead() {
    incrLogEditsRead(1);
  }

  /**
   * Add on the number of log edits filtered
   *
   * @param delta the number filtered.
   */
  private void incrLogEditsFiltered(long delta) {
    rms.incCounters(logEditsFilteredKey, delta);
    rms.incCounters(SOURCE_LOG_EDITS_FILTERED, delta);
  }

  /** The number of log edits filtered out. */
  public void incrLogEditsFiltered() {
    incrLogEditsFiltered(1);
  }

  /**
   * Convience method to apply changes to metrics do to shipping a batch of logs.
   *
   * @param batchSize the size of the batch that was shipped to sinks.
   */
  public void shipBatch(long batchSize, int sizeInKB) {
    rms.incCounters(shippedBatchesKey, 1);
    rms.incCounters(SOURCE_SHIPPED_BATCHES, 1);
    rms.incCounters(shippedOpsKey, batchSize);
    rms.incCounters(SOURCE_SHIPPED_OPS, batchSize);
    rms.incCounters(shippedKBsKey, sizeInKB);
    rms.incCounters(SOURCE_SHIPPED_KBS, sizeInKB);
  }

  /** increase the byte number read by source from log file */
  public void incrLogReadInBytes(long readInBytes) {
    rms.incCounters(logReadInBytesKey, readInBytes);
    rms.incCounters(SOURCE_LOG_READ_IN_BYTES, readInBytes);
  }

  /** Removes all metrics about this Source. */
  public void clear() {
    rms.removeMetric(sizeOfLogQueKey);
    rms.decGauge(SOURCE_SIZE_OF_LOG_QUEUE, lastQueueSize);
    lastQueueSize = 0;
    rms.removeMetric(ageOfLastShippedOpKey);

    rms.removeMetric(logEditsFilteredKey);
    rms.removeMetric(logEditsReadKey);

  }
}
