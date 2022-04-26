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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for maintaining the various replication statistics for a source and publishing them
 * through the metrics interfaces.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class MetricsSource implements BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsSource.class);

  // tracks last shipped timestamp for each wal group
  private Map<String, Long> lastShippedTimeStamps = new HashMap<String, Long>();
  private Map<String, Long> ageOfLastShippedOp = new HashMap<>();
  private long lastHFileRefsQueueSize = 0;
  private String id;
  private long timeStampNextToReplicate;

  private final MetricsReplicationSourceSource singleSourceSource;
  private final MetricsReplicationGlobalSourceSource globalSourceSource;
  private Map<String, MetricsReplicationTableSource> singleSourceSourceByTable;

  /**
   * Constructor used to register the metrics
   *
   * @param id Name of the source this class is monitoring
   */
  public MetricsSource(String id) {
    this.id = id;
    singleSourceSource =
        CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class)
            .getSource(id);
    globalSourceSource = CompatibilitySingletonFactory
      .getInstance(MetricsReplicationSourceFactory.class).getGlobalSource();
    singleSourceSourceByTable = new HashMap<>();
  }

  /**
   * Constructor for injecting custom (or test) MetricsReplicationSourceSources
   * @param id Name of the source this class is monitoring
   * @param singleSourceSource Class to monitor id-scoped metrics
   * @param globalSourceSource Class to monitor global-scoped metrics
   */
  public MetricsSource(String id, MetricsReplicationSourceSource singleSourceSource,
                       MetricsReplicationGlobalSourceSource globalSourceSource,
                       Map<String, MetricsReplicationTableSource> singleSourceSourceByTable) {
    this.id = id;
    this.singleSourceSource = singleSourceSource;
    this.globalSourceSource = globalSourceSource;
    this.singleSourceSourceByTable = singleSourceSourceByTable;
  }

  /**
   * Set the age of the last edit that was shipped
   * @param timestamp target write time of the edit
   * @param walGroup which group we are setting
   */
  public void setAgeOfLastShippedOp(long timestamp, String walGroup) {
    long age = EnvironmentEdgeManager.currentTime() - timestamp;
    singleSourceSource.setLastShippedAge(age);
    globalSourceSource.setLastShippedAge(age);
    this.ageOfLastShippedOp.put(walGroup, age);
    this.lastShippedTimeStamps.put(walGroup, timestamp);
  }

  /**
   * Update the table level replication metrics per table
   *
   * @param walEntries List of pairs of WAL entry and it's size
   */
  public void updateTableLevelMetrics(List<Pair<Entry, Long>> walEntries) {
    for (Pair<Entry, Long> walEntryWithSize : walEntries) {
      Entry entry = walEntryWithSize.getFirst();
      long entrySize = walEntryWithSize.getSecond();
      String tableName = entry.getKey().getTableName().getNameAsString();
      long writeTime = entry.getKey().getWriteTime();
      long age = EnvironmentEdgeManager.currentTime() - writeTime;

      // get the replication metrics source for table at the run time
      MetricsReplicationTableSource tableSource = this.getSingleSourceSourceByTable()
        .computeIfAbsent(tableName,
          t -> CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class)
            .getTableSource(t));
      tableSource.setLastShippedAge(age);
      tableSource.incrShippedBytes(entrySize);
    }
  }

  /**
   * Set the age of the last edit that was shipped group by table
   * @param timestamp write time of the edit
   * @param tableName String as group and tableName
   */
  public void setAgeOfLastShippedOpByTable(long timestamp, String tableName) {
    long age = EnvironmentEdgeManager.currentTime() - timestamp;
    this.getSingleSourceSourceByTable().computeIfAbsent(
        tableName, t -> CompatibilitySingletonFactory
            .getInstance(MetricsReplicationSourceFactory.class).getTableSource(t))
            .setLastShippedAge(age);
  }

  /**
   * get age of last shipped op of given wal group. If the walGroup is null, return 0
   * @param walGroup which group we are getting
   * @return age
   */
  public long getAgeOfLastShippedOp(String walGroup) {
    return this.ageOfLastShippedOp.get(walGroup) == null ? 0 : ageOfLastShippedOp.get(walGroup);
  }

  /**
   * Convenience method to use the last given timestamp to refresh the age of the last edit. Used
   * when replication fails and need to keep that metric accurate.
   * @param walGroupId id of the group to update
   */
  public void refreshAgeOfLastShippedOp(String walGroupId) {
    Long lastTimestamp = this.lastShippedTimeStamps.get(walGroupId);
    if (lastTimestamp == null) {
      this.lastShippedTimeStamps.put(walGroupId, 0L);
      lastTimestamp = 0L;
    }
    if (lastTimestamp > 0) {
      setAgeOfLastShippedOp(lastTimestamp, walGroupId);
    }
  }

  /**
   * Increment size of the log queue.
   */
  public void incrSizeOfLogQueue() {
    singleSourceSource.incrSizeOfLogQueue(1);
    globalSourceSource.incrSizeOfLogQueue(1);
  }

  public void decrSizeOfLogQueue() {
    singleSourceSource.decrSizeOfLogQueue(1);
    globalSourceSource.decrSizeOfLogQueue(1);
  }

  /**
   * Increment the count for initializing sources
   */
  public void incrSourceInitializing() {
    singleSourceSource.incrSourceInitializing();
    globalSourceSource.incrSourceInitializing();
  }

  /**
   * Decrement the count for initializing sources
   */
  public void decrSourceInitializing() {
    singleSourceSource.decrSourceInitializing();
    globalSourceSource.decrSourceInitializing();
  }

  /**
   * Add on the the number of log edits read
   *
   * @param delta the number of log edits read.
   */
  private void incrLogEditsRead(long delta) {
    singleSourceSource.incrLogReadInEdits(delta);
    globalSourceSource.incrLogReadInEdits(delta);
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
  public void incrLogEditsFiltered(long delta) {
    singleSourceSource.incrLogEditsFiltered(delta);
    globalSourceSource.incrLogEditsFiltered(delta);
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
  public void shipBatch(long batchSize, int sizeInBytes) {
    singleSourceSource.incrBatchesShipped(1);
    globalSourceSource.incrBatchesShipped(1);

    singleSourceSource.incrOpsShipped(batchSize);
    globalSourceSource.incrOpsShipped(batchSize);

    singleSourceSource.incrShippedBytes(sizeInBytes);
    globalSourceSource.incrShippedBytes(sizeInBytes);
  }

  /**
   * Convenience method to update metrics when batch of operations has failed.
   */
  public void incrementFailedBatches(){
    singleSourceSource.incrFailedBatches();
    globalSourceSource.incrFailedBatches();
  }


  /**
   * Gets the number of edits not eligible for replication this source queue logs so far.
   * @return logEditsFiltered non-replicable edits filtered from this queue logs.
   */
  public long getEditsFiltered(){
    return this.singleSourceSource.getEditsFiltered();
  }

  /**
   * Gets the number of edits eligible for replication read from this source queue logs so far.
   * @return replicableEdits total number of replicable edits read from this queue logs.
   */
  public long getReplicableEdits(){
    return this.singleSourceSource.getWALEditsRead() - this.singleSourceSource.getEditsFiltered();
  }

  /**
   * Gets the number of OPs shipped by this source queue to target cluster.
   * @return oPsShipped total number of OPs shipped by this source.
   */
  public long getOpsShipped() {
    return this.singleSourceSource.getShippedOps();
  }

  /**
   * Convience method to apply changes to metrics do to shipping a batch of logs.
   *
   * @param batchSize the size of the batch that was shipped to sinks.
   * @param hfiles total number of hfiles shipped to sinks.
   */
  public void shipBatch(long batchSize, int sizeInBytes, long hfiles) {
    shipBatch(batchSize, sizeInBytes);
    singleSourceSource.incrHFilesShipped(hfiles);
    globalSourceSource.incrHFilesShipped(hfiles);
  }

  /** increase the byte number read by source from log file */
  public void incrLogReadInBytes(long readInBytes) {
    singleSourceSource.incrLogReadInBytes(readInBytes);
    globalSourceSource.incrLogReadInBytes(readInBytes);
  }

  /** Removes all metrics about this Source. */
  public void clear() {
    int lastQueueSize = singleSourceSource.getSizeOfLogQueue();
    globalSourceSource.decrSizeOfLogQueue(lastQueueSize);
    singleSourceSource.decrSizeOfLogQueue(lastQueueSize);
    singleSourceSource.clear();
    globalSourceSource.decrSizeOfHFileRefsQueue(lastHFileRefsQueueSize);
    lastShippedTimeStamps.clear();
    lastHFileRefsQueueSize = 0;
    timeStampNextToReplicate = 0;
  }

  /**
   * Get AgeOfLastShippedOp
   * @return AgeOfLastShippedOp
   */
  public Long getAgeOfLastShippedOp() {
    return singleSourceSource.getLastShippedAge();
  }

  /**
   * Get the sizeOfLogQueue
   * @return sizeOfLogQueue
   */
  public int getSizeOfLogQueue() {
    return singleSourceSource.getSizeOfLogQueue();
  }

  /**
   * Get the timeStampsOfLastShippedOp, if there are multiple groups, return the latest one
   * @return lastTimestampForAge
   * @deprecated Since 2.0.0. Removed in 3.0.0.
   * @see #getTimestampOfLastShippedOp()
   */
  @Deprecated
  public long getTimeStampOfLastShippedOp() {
    return getTimestampOfLastShippedOp();
  }


  /**
   * Get the value of uncleanlyClosedWAL counter
   * @return uncleanlyClosedWAL
   */
  public long getUncleanlyClosedWALs() {
    return singleSourceSource.getUncleanlyClosedWALs();
  }

  /**
   * Get the timestampsOfLastShippedOp, if there are multiple groups, return the latest one
   * @return lastTimestampForAge
   */
  public long getTimestampOfLastShippedOp() {
    long lastTimestamp = 0L;
    for (long ts : lastShippedTimeStamps.values()) {
      if (ts > lastTimestamp) {
        lastTimestamp = ts;
      }
    }
    return lastTimestamp;
  }

  /**
   * TimeStamp of next edit to be replicated.
   * @return timeStampNextToReplicate - TimeStamp of next edit to be replicated.
   */
  public long getTimeStampNextToReplicate() {
    return timeStampNextToReplicate;
  }

  /**
   * TimeStamp of next edit targeted for replication. Used for calculating lag,
   * as if this timestamp is greater than timestamp of last shipped, it means there's
   * at least one edit pending replication.
   * @param timeStampNextToReplicate timestamp of next edit in the queue that should be replicated.
   */
  public void setTimeStampNextToReplicate(long timeStampNextToReplicate) {
    this.timeStampNextToReplicate = timeStampNextToReplicate;
  }

  public long getReplicationDelay() {
    if(getTimestampOfLastShippedOp()>=timeStampNextToReplicate){
      return 0;
    }else{
      return EnvironmentEdgeManager.currentTime() - timeStampNextToReplicate;
    }
  }

  /**
   * Get the source initializing counts
   * @return number of replication sources getting initialized
   */
  public int getSourceInitializing() {
    return singleSourceSource.getSourceInitializing();
  }

  /**
   * Get the slave peer ID
   * @return peerID
   */
  public String getPeerID() {
    return id;
  }

  public void incrSizeOfHFileRefsQueue(long size) {
    singleSourceSource.incrSizeOfHFileRefsQueue(size);
    globalSourceSource.incrSizeOfHFileRefsQueue(size);
    lastHFileRefsQueueSize = size;
  }

  public void decrSizeOfHFileRefsQueue(int size) {
    singleSourceSource.decrSizeOfHFileRefsQueue(size);
    globalSourceSource.decrSizeOfHFileRefsQueue(size);
    lastHFileRefsQueueSize -= size;
    if (lastHFileRefsQueueSize < 0) {
      lastHFileRefsQueueSize = 0;
    }
  }

  public void incrUnknownFileLengthForClosedWAL() {
    singleSourceSource.incrUnknownFileLengthForClosedWAL();
    globalSourceSource.incrUnknownFileLengthForClosedWAL();
  }

  public void incrUncleanlyClosedWALs() {
    singleSourceSource.incrUncleanlyClosedWALs();
    globalSourceSource.incrUncleanlyClosedWALs();
  }

  public void incrBytesSkippedInUncleanlyClosedWALs(final long bytes) {
    singleSourceSource.incrBytesSkippedInUncleanlyClosedWALs(bytes);
    globalSourceSource.incrBytesSkippedInUncleanlyClosedWALs(bytes);
  }

  public void incrRestartedWALReading() {
    singleSourceSource.incrRestartedWALReading();
    globalSourceSource.incrRestartedWALReading();
  }

  public void incrRepeatedFileBytes(final long bytes) {
    singleSourceSource.incrRepeatedFileBytes(bytes);
    globalSourceSource.incrRepeatedFileBytes(bytes);
  }

  public void incrCompletedWAL() {
    singleSourceSource.incrCompletedWAL();
    globalSourceSource.incrCompletedWAL();
  }

  public void incrCompletedRecoveryQueue() {
    singleSourceSource.incrCompletedRecoveryQueue();
    globalSourceSource.incrCompletedRecoveryQueue();
  }

  public void incrFailedRecoveryQueue() {
    globalSourceSource.incrFailedRecoveryQueue();
  }

  /*
   Sets the age of oldest log file just for source.
  */
  public void setOldestWalAge(long age) {
    singleSourceSource.setOldestWalAge(age);
  }

  public long getOldestWalAge() {
    return singleSourceSource.getOldestWalAge();
  }

  @Override
  public void init() {
    singleSourceSource.init();
    globalSourceSource.init();
  }

  @Override
  public void setGauge(String gaugeName, long value) {
    singleSourceSource.setGauge(gaugeName, value);
    globalSourceSource.setGauge(gaugeName, value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    singleSourceSource.incGauge(gaugeName, delta);
    globalSourceSource.incGauge(gaugeName, delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    singleSourceSource.decGauge(gaugeName, delta);
    globalSourceSource.decGauge(gaugeName, delta);
  }

  @Override
  public void removeMetric(String key) {
    singleSourceSource.removeMetric(key);
    globalSourceSource.removeMetric(key);
  }

  @Override
  public void incCounters(String counterName, long delta) {
    singleSourceSource.incCounters(counterName, delta);
    globalSourceSource.incCounters(counterName, delta);
  }

  @Override
  public void updateHistogram(String name, long value) {
    singleSourceSource.updateHistogram(name, value);
    globalSourceSource.updateHistogram(name, value);
  }

  @Override
  public String getMetricsContext() {
    return globalSourceSource.getMetricsContext();
  }

  @Override
  public String getMetricsDescription() {
    return globalSourceSource.getMetricsDescription();
  }

  @Override
  public String getMetricsJmxContext() {
    return globalSourceSource.getMetricsJmxContext();
  }

  @Override
  public String getMetricsName() {
    return globalSourceSource.getMetricsName();
  }

  @InterfaceAudience.Private
  public Map<String, MetricsReplicationTableSource> getSingleSourceSourceByTable() {
    return singleSourceSourceByTable;
  }

  /**
   * Sets the amount of memory in bytes used in this RegionServer by edits pending replication.
   */
  public void setWALReaderEditsBufferUsage(long usageInBytes) {
    globalSourceSource.setWALReaderEditsBufferBytes(usageInBytes);
  }

  /**
   * Returns the amount of memory in bytes used in this RegionServer by edits pending replication.
   * @return
   */
  public long getWALReaderEditsBufferUsage() {
    return globalSourceSource.getWALReaderEditsBufferBytes();
  }
}
