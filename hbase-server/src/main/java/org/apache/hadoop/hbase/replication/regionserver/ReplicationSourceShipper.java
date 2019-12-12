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

import static org.apache.hadoop.hbase.replication.ReplicationUtils.getAdaptiveTimeout;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.sleepForRetries;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * This thread reads entries from a queue and ships them. Entries are placed onto the queue by
 * ReplicationSourceWALReaderThread
 */
@InterfaceAudience.Private
public class ReplicationSourceShipper implements Callable<ReplicationSourceShipper.WorkerState> {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceShipper.class);

  // Hold the state of a replication worker thread
  public enum WorkerState {
    RUNNING,
    STOPPED,
    FINISHED,  // The worker is done processing a queue
  }

  private final Configuration conf;
  protected final String walGroupId;
  protected final PriorityBlockingQueue<Path> queue;
  private final ReplicationSource source;

  // Last position in the log that we sent to ZooKeeper
  // It will be accessed by the stats thread so make it volatile
  private volatile long currentPosition = -1;
  // Path of the current log
  private Path currentPath;
  // Current state of the worker thread
  private volatile WorkerState state;
  protected ReplicationSourceWALReader entryReader;

  // How long should we sleep for each retry
  protected final long sleepForRetries;
  // Maximum number of retries before taking bold actions
  protected final int maxRetriesMultiplier;
  private final int DEFAULT_TIMEOUT = 20000;
  private final int getEntriesTimeout;
  private final int shipEditsTimeout;

  public ReplicationSourceShipper(Configuration conf, String walGroupId,
      PriorityBlockingQueue<Path> queue, ReplicationSource source) {
    this.conf = conf;
    this.walGroupId = walGroupId;
    this.queue = queue;
    this.source = Objects.requireNonNull(source);
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
    this.getEntriesTimeout =
        this.conf.getInt("replication.source.getEntries.timeout", DEFAULT_TIMEOUT); // 20 seconds
    this.shipEditsTimeout = this.conf.getInt(HConstants.REPLICATION_SOURCE_SHIPEDITS_TIMEOUT,
        HConstants.REPLICATION_SOURCE_SHIPEDITS_TIMEOUT_DFAULT);
  }

  @Override
  public WorkerState call() throws Exception {
    Objects.requireNonNull(this.entryReader);

    setWorkerState(WorkerState.RUNNING);
    LOG.info("Running ReplicationSourceShipper Thread for wal group: {}", this.walGroupId);
    // Loop until we close down
    while (isActive()) {
      // Sleep until replication is enabled again
      if (!source.isPeerEnabled()) {
        // The peer enabled check is in memory, not expensive, so do not need to increase the
        // sleep interval as it may cause a long lag when we enable the peer.
        sleepForRetries("Replication is disabled", sleepForRetries, 1, maxRetriesMultiplier);
        continue;
      }
      try {
        WALEntryBatch entryBatch = this.entryReader.poll(getEntriesTimeout);
        LOG.debug("Shipper from source {} got entry batch from reader: {}",
            source.getQueueId(), entryBatch);
        if (entryBatch == null) {
          continue;
        }
        // the NO_MORE_DATA instance has no path so do not call shipEdits
        if (entryBatch == WALEntryBatch.NO_MORE_DATA) {
          noMoreData();
        } else {
          shipEdits(entryBatch);
        }
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while waiting for next replication entry batch", e);
        break;
      }
    }

    // If the worker exits run loop without finishing its task, mark it as stopped.
    if (!isFinished()) {
      setWorkerState(WorkerState.STOPPED);
    }

    this.entryReader.stopReaderRunning();

    return this.state;
  }

  private void noMoreData() {
    if (source.isRecovered()) {
      LOG.debug("Finished recovering queue for group {} of peer {}", walGroupId,
        source.getQueueId());
      source.getSourceMetrics().incrCompletedRecoveryQueue();
    } else {
      LOG.debug("Finished queue for group {} of peer {}", walGroupId, source.getQueueId());
    }
    setWorkerState(WorkerState.FINISHED);
  }

  /**
   * get batchEntry size excludes bulk load file sizes.
   * Uses ReplicationSourceWALReader's static method.
   */
  private int getBatchEntrySizeExcludeBulkLoad(WALEntryBatch entryBatch) {
    int totalSize = 0;
    for (Entry entry : entryBatch.getWalEntries()) {
      totalSize += ReplicationSourceWALReader.getEntrySizeExcludeBulkLoad(entry);
    }
    return totalSize;
  }

  /**
   * Do the shipping logic
   */
  private void shipEdits(WALEntryBatch entryBatch) {
    List<Entry> entries = entryBatch.getWalEntries();
    int sleepMultiplier = 0;
    if (entries.isEmpty()) {
      updateLogPosition(entryBatch);
      return;
    }
    int currentSize = (int) entryBatch.getHeapSize();
    int sizeExcludeBulkLoad = getBatchEntrySizeExcludeBulkLoad(entryBatch);
    source.getSourceMetrics().setTimeStampNextToReplicate(entries.get(entries.size() - 1)
        .getKey().getWriteTime());
    while (isActive()) {
      try {
        try {
          source.tryThrottle(currentSize);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while sleeping for throttling control");
          Thread.currentThread().interrupt();
          // current thread might be interrupted to terminate
          // directly go back to while() for confirm this
          continue;
        }
        // create replicateContext here, so the entries can be GC'd upon return from this call
        // stack
        ReplicationEndpoint.ReplicateContext replicateContext =
            new ReplicationEndpoint.ReplicateContext();
        replicateContext.setEntries(entries).setSize(currentSize);
        replicateContext.setWalGroupId(walGroupId);
        replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, sleepMultiplier));

        long startTimeNs = System.nanoTime();
        // send the edits to the endpoint. Will block until the edits are shipped and acknowledged
        boolean replicated = source.getReplicationEndpoint().replicate(replicateContext);
        long endTimeNs = System.nanoTime();

        if (!replicated) {
          continue;
        } else {
          sleepMultiplier = Math.max(sleepMultiplier - 1, 0);
        }
        // Clean up hfile references
        for (Entry entry : entries) {
          cleanUpHFileRefs(entry.getEdit());
          LOG.trace("shipped entry {}: ", entry);
          TableName tableName = entry.getKey().getTableName();
          source.getSourceMetrics().setAgeOfLastShippedOpByTable(entry.getKey().getWriteTime(),
              tableName.getNameAsString());
        }
        // Log and clean up WAL logs
        updateLogPosition(entryBatch);

        //offsets totalBufferUsed by deducting shipped batchSize (excludes bulk load size)
        //this sizeExcludeBulkLoad has to use same calculation that when calling
        //acquireBufferQuota() in ReplicatinoSourceWALReader because they maintain
        //same variable: totalBufferUsed
        source.postShipEdits(entries, sizeExcludeBulkLoad);
        // FIXME check relationship between wal group and overall
        source.getSourceMetrics().shipBatch(entryBatch.getNbOperations(), currentSize,
          entryBatch.getNbHFiles());
        source.getSourceMetrics().setAgeOfLastShippedOp(
          entries.get(entries.size() - 1).getKey().getWriteTime(), walGroupId);
        if (LOG.isTraceEnabled()) {
          LOG.debug("Replicated {} entries or {} operations in {} ms",
              entries.size(), entryBatch.getNbOperations(), (endTimeNs - startTimeNs) / 1000000);
        }
        break;
      } catch (Exception ex) {
        LOG.warn("{} threw unknown exception:",
          source.getReplicationEndpoint().getClass().getName(), ex);
        if (sleepForRetries("ReplicationEndpoint threw exception", sleepForRetries, sleepMultiplier,
          maxRetriesMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }

  private void cleanUpHFileRefs(WALEdit edit) throws IOException {
    String peerId = source.getPeerId();
    if (peerId.contains("-")) {
      // peerClusterZnode will be in the form peerId + "-" + rsZNode.
      // A peerId will not have "-" in its name, see HBASE-11394
      peerId = peerId.split("-")[0];
    }
    List<Cell> cells = edit.getCells();
    int totalCells = cells.size();
    for (int i = 0; i < totalCells; i++) {
      Cell cell = cells.get(i);
      if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
        BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
        List<StoreDescriptor> stores = bld.getStoresList();
        int totalStores = stores.size();
        for (int j = 0; j < totalStores; j++) {
          List<String> storeFileList = stores.get(j).getStoreFileList();
          source.getSourceManager().cleanUpHFileRefs(peerId, storeFileList);
          source.getSourceMetrics().decrSizeOfHFileRefsQueue(storeFileList.size());
        }
      }
    }
  }

  private boolean updateLogPosition(WALEntryBatch batch) {
    boolean updated = false;
    // if end of file is true, then the logPositionAndCleanOldLogs method will remove the file
    // record on zk, so let's call it. The last wal position maybe zero if end of file is true and
    // there is no entry in the batch. It is OK because that the queue storage will ignore the zero
    // position and the file will be removed soon in cleanOldLogs.
    if (batch.isEndOfFile() || !batch.getLastWalPath().equals(currentPath) ||
      batch.getLastWalPosition() != currentPosition) {
      source.getSourceManager().logPositionAndCleanOldLogs(source, batch);
      updated = true;
    }
    // if end of file is true, then we can just skip to the next file in queue.
    // the only exception is for recovered queue, if we reach the end of the queue, then there will
    // no more files so here the currentPath may be null.
    if (batch.isEndOfFile()) {
      currentPath = entryReader.getCurrentPath();
      currentPosition = 0L;
    } else {
      currentPath = batch.getLastWalPath();
      currentPosition = batch.getLastWalPosition();
    }
    return updated;
  }

  Path getCurrentPath() {
    return entryReader.getCurrentPath();
  }

  long getCurrentPosition() {
    return currentPosition;
  }

  void setWALReader(ReplicationSourceWALReader entryReader) {
    this.entryReader = entryReader;
  }

  long getStartPosition() {
    return 0;
  }

  protected boolean isActive() {
    return source.isSourceActive() && state == WorkerState.RUNNING;
  }

  protected final void setWorkerState(WorkerState state) {
    this.state = state;
  }

  void stopWorker() {
    setWorkerState(WorkerState.STOPPED);
  }

  public boolean isFinished() {
    return state == WorkerState.FINISHED;
  }

}
