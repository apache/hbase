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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Threads;
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
public class ReplicationSourceShipper extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceShipper.class);

  // Hold the state of a replication worker thread
  public enum WorkerState {
    RUNNING,
    STOPPED,
    FINISHED,  // The worker is done processing a recovered queue
  }

  private final Configuration conf;
  protected final String walGroupId;
  protected final ReplicationSourceLogQueue logQueue;
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
      ReplicationSourceLogQueue logQueue, ReplicationSource source) {
    this.conf = conf;
    this.walGroupId = walGroupId;
    this.logQueue = logQueue;
    this.source = source;
    // 1 second
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    // 5 minutes @ 1 sec per
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    // 20 seconds
    this.getEntriesTimeout =
      this.conf.getInt("replication.source.getEntries.timeout", DEFAULT_TIMEOUT);
    this.shipEditsTimeout = this.conf.getInt(HConstants.REPLICATION_SOURCE_SHIPEDITS_TIMEOUT,
      HConstants.REPLICATION_SOURCE_SHIPEDITS_TIMEOUT_DFAULT);
  }

  @Override
  public final void run() {
    setWorkerState(WorkerState.RUNNING);
    LOG.info("Running ReplicationSourceShipper Thread for wal group: {}", this.walGroupId);
    // Loop until we close down
    while (isActive()) {
      // Sleep until replication is enabled again
      if (!source.isPeerEnabled()) {
        // The peer enabled check is in memory, not expensive, so do not need to increase the
        // sleep interval as it may cause a long lag when we enable the peer.
        sleepForRetries("Replication is disabled", 1);
        continue;
      }
      try {
        WALEntryBatch entryBatch = entryReader.poll(getEntriesTimeout);
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
      } catch (InterruptedException | ReplicationRuntimeException e) {
        // It is interrupted and needs to quit.
        LOG.warn("Interrupted while waiting for next replication entry batch", e);
        Thread.currentThread().interrupt();
      }
    }
    // If the worker exits run loop without finishing its task, mark it as stopped.
    if (!isFinished()) {
      setWorkerState(WorkerState.STOPPED);
    } else {
      source.workerThreads.remove(this.walGroupId);
      postFinish();
    }
  }

  // To be implemented by recovered shipper
  protected void noMoreData() {
  }

  // To be implemented by recovered shipper
  protected void postFinish() {
  }

  /**
   * get batchEntry size excludes bulk load file sizes.
   * Uses ReplicationSourceWALReader's static method.
   */
  private int getBatchEntrySizeExcludeBulkLoad(WALEntryBatch entryBatch) {
    int totalSize = 0;
    for(Entry entry : entryBatch.getWalEntries()) {
      totalSize += ReplicationSourceWALReader.getEntrySizeExcludeBulkLoad(entry);
    }
    return  totalSize;
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
        }
        // Log and clean up WAL logs
        updateLogPosition(entryBatch);

        //offsets totalBufferUsed by deducting shipped batchSize (excludes bulk load size)
        //this sizeExcludeBulkLoad has to use same calculation that when calling
        //acquireBufferQuota() in ReplicationSourceWALReader because they maintain
        //same variable: totalBufferUsed
        source.postShipEdits(entries, sizeExcludeBulkLoad);
        // FIXME check relationship between wal group and overall
        source.getSourceMetrics().shipBatch(entryBatch.getNbOperations(), currentSize,
          entryBatch.getNbHFiles());
        source.getSourceMetrics().setAgeOfLastShippedOp(
          entries.get(entries.size() - 1).getKey().getWriteTime(), walGroupId);
        source.getSourceMetrics().updateTableLevelMetrics(entryBatch.getWalEntriesWithSize());

        if (LOG.isTraceEnabled()) {
          LOG.debug("Replicated {} entries or {} operations in {} ms",
              entries.size(), entryBatch.getNbOperations(), (endTimeNs - startTimeNs) / 1000000);
        }
        break;
      } catch (Exception ex) {
        source.getSourceMetrics().incrementFailedBatches();
        LOG.warn("{} threw unknown exception:",
          source.getReplicationEndpoint().getClass().getName(), ex);
        if (sleepForRetries("ReplicationEndpoint threw exception", sleepMultiplier)) {
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
      source.logPositionAndCleanOldLogs(batch);
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

  public void startup(UncaughtExceptionHandler handler) {
    String name = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(this,
      name + ".replicationSource.shipper" + walGroupId + "," + source.getQueueId(),
      handler::uncaughtException);
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
    return source.isSourceActive() && state == WorkerState.RUNNING && !isInterrupted();
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

  /**
   * Do the sleeping logic
   * @param msg Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  public boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      LOG.trace("{}, sleeping {} times {}", msg, sleepForRetries, sleepMultiplier);
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  /**
   * Attempts to properly update <code>ReplicationSourceManager.totalBufferUser</code>,
   * in case there were unprocessed entries batched by the reader to the shipper,
   * but the shipper didn't manage to ship those because the replication source is being terminated.
   * In that case, it iterates through the batched entries and decrease the pending
   * entries size from <code>ReplicationSourceManager.totalBufferUser</code>
   * <p/>
   * <b>NOTES</b>
   * 1) This method should only be called upon replication source termination.
   * It blocks waiting for both shipper and reader threads termination,
   * to make sure no race conditions
   * when updating <code>ReplicationSourceManager.totalBufferUser</code>.
   *
   * 2) It <b>does not</b> attempt to terminate reader and shipper threads. Those <b>must</b>
   * have been triggered interruption/termination prior to calling this method.
   */
  void clearWALEntryBatch() {
    long timeout = System.currentTimeMillis() + this.shipEditsTimeout;
    while(this.isAlive() || this.entryReader.isAlive()){
      try {
        if (System.currentTimeMillis() >= timeout) {
          LOG.warn("Shipper clearWALEntryBatch method timed out whilst waiting reader/shipper "
            + "thread to stop. Not cleaning buffer usage. Shipper alive: {}; Reader alive: {}",
            this.source.getPeerId(), this.isAlive(), this.entryReader.isAlive());
          return;
        } else {
          // Wait both shipper and reader threads to stop
          Thread.sleep(this.sleepForRetries);
        }
      } catch (InterruptedException e) {
        LOG.warn("{} Interrupted while waiting {} to stop on clearWALEntryBatch. "
            + "Not cleaning buffer usage: {}", this.source.getPeerId(), this.getName(), e);
        return;
      }
    }
    LongAccumulator totalToDecrement = new LongAccumulator((a,b) -> a + b, 0);
    entryReader.entryBatchQueue.forEach(w -> {
      entryReader.entryBatchQueue.remove(w);
      w.getWalEntries().forEach(e -> {
        long entrySizeExcludeBulkLoad = ReplicationSourceWALReader.getEntrySizeExcludeBulkLoad(e);
        totalToDecrement.accumulate(entrySizeExcludeBulkLoad);
      });
    });
    if( LOG.isTraceEnabled()) {
      LOG.trace("Decrementing totalBufferUsed by {}B while stopping Replication WAL Readers.",
        totalToDecrement.longValue());
    }
    long newBufferUsed = source.getSourceManager().getTotalBufferUsed()
      .addAndGet(-totalToDecrement.longValue());
    source.getSourceManager().getGlobalMetrics().setWALReaderEditsBufferBytes(newBufferUsed);
  }
}
