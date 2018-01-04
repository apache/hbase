/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceWALReader.WALEntryBatch;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

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

  protected final Configuration conf;
  protected final String walGroupId;
  protected final PriorityBlockingQueue<Path> queue;
  protected final ReplicationSourceInterface source;

  // Last position in the log that we sent to ZooKeeper
  protected long lastLoggedPosition = -1;
  // Path of the current log
  protected volatile Path currentPath;
  // Current state of the worker thread
  private WorkerState state;
  protected ReplicationSourceWALReader entryReader;

  // How long should we sleep for each retry
  protected final long sleepForRetries;
  // Maximum number of retries before taking bold actions
  protected final int maxRetriesMultiplier;

  // Use guava cache to set ttl for each key
  private final LoadingCache<String, Boolean> canSkipWaitingSet = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.DAYS).build(
      new CacheLoader<String, Boolean>() {
        @Override
        public Boolean load(String key) throws Exception {
          return false;
        }
      }
  );

  public ReplicationSourceShipper(Configuration conf, String walGroupId,
      PriorityBlockingQueue<Path> queue, ReplicationSourceInterface source) {
    this.conf = conf;
    this.walGroupId = walGroupId;
    this.queue = queue;
    this.source = source;
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
  }

  @Override
  public void run() {
    setWorkerState(WorkerState.RUNNING);
    // Loop until we close down
    while (isActive()) {
      int sleepMultiplier = 1;
      // Sleep until replication is enabled again
      if (!source.isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }

      while (entryReader == null) {
        if (sleepForRetries("Replication WAL entry reader thread not initialized",
          sleepMultiplier)) {
          sleepMultiplier++;
        }
      }

      try {
        WALEntryBatch entryBatch = entryReader.take();
        for (Map.Entry<String, Long> entry : entryBatch.getLastSeqIds().entrySet()) {
          waitingUntilCanPush(entry);
        }
        shipEdits(entryBatch);
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while waiting for next replication entry batch", e);
        Thread.currentThread().interrupt();
      }
    }
    // If the worker exits run loop without finishing its task, mark it as stopped.
    if (state != WorkerState.FINISHED) {
      setWorkerState(WorkerState.STOPPED);
    }
  }

  /**
   * Do the shipping logic
   */
  protected void shipEdits(WALEntryBatch entryBatch) {
    List<Entry> entries = entryBatch.getWalEntries();
    long lastReadPosition = entryBatch.getLastWalPosition();
    currentPath = entryBatch.getLastWalPath();
    int sleepMultiplier = 0;
    if (entries.isEmpty()) {
      if (lastLoggedPosition != lastReadPosition) {
        // Save positions to meta table before zk.
        updateSerialRepPositions(entryBatch.getLastSeqIds());
        updateLogPosition(lastReadPosition);
        // if there was nothing to ship and it's not an error
        // set "ageOfLastShippedOp" to <now> to indicate that we're current
        source.getSourceMetrics().setAgeOfLastShippedOp(EnvironmentEdgeManager.currentTime(),
          walGroupId);
      }
      return;
    }
    int currentSize = (int) entryBatch.getHeapSize();
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

        long startTimeNs = System.nanoTime();
        // send the edits to the endpoint. Will block until the edits are shipped and acknowledged
        boolean replicated = source.getReplicationEndpoint().replicate(replicateContext);
        long endTimeNs = System.nanoTime();

        if (!replicated) {
          continue;
        } else {
          sleepMultiplier = Math.max(sleepMultiplier - 1, 0);
        }

        if (this.lastLoggedPosition != lastReadPosition) {
          //Clean up hfile references
          int size = entries.size();
          for (int i = 0; i < size; i++) {
            cleanUpHFileRefs(entries.get(i).getEdit());
          }

          // Save positions to meta table before zk.
          updateSerialRepPositions(entryBatch.getLastSeqIds());
          //Log and clean up WAL logs
          updateLogPosition(lastReadPosition);
        }

        source.postShipEdits(entries, currentSize);
        // FIXME check relationship between wal group and overall
        source.getSourceMetrics().shipBatch(entryBatch.getNbOperations(), currentSize,
          entryBatch.getNbHFiles());
        source.getSourceMetrics().setAgeOfLastShippedOp(
          entries.get(entries.size() - 1).getKey().getWriteTime(), walGroupId);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Replicated " + entries.size() + " entries or " + entryBatch.getNbOperations()
              + " operations in " + ((endTimeNs - startTimeNs) / 1000000) + " ms");
        }
        break;
      } catch (Exception ex) {
        LOG.warn(source.getReplicationEndpoint().getClass().getName() + " threw unknown exception:"
            + org.apache.hadoop.util.StringUtils.stringifyException(ex));
        if (sleepForRetries("ReplicationEndpoint threw exception", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }

  private void waitingUntilCanPush(Map.Entry<String, Long> entry) {
    String key = entry.getKey();
    long seq = entry.getValue();
    boolean deleteKey = false;
    if (seq <= 0) {
      // There is a REGION_CLOSE marker, we can not continue skipping after this entry.
      deleteKey = true;
      seq = -seq;
    }

    if (!canSkipWaitingSet.getUnchecked(key)) {
      try {
        source.getSourceManager().waitUntilCanBePushed(Bytes.toBytes(key), seq, source.getPeerId());
      } catch (IOException e) {
        LOG.error("waitUntilCanBePushed fail", e);
        throw new RuntimeException("waitUntilCanBePushed fail");
      } catch (InterruptedException e) {
        LOG.warn("waitUntilCanBePushed interrupted", e);
        Thread.currentThread().interrupt();
      }
      canSkipWaitingSet.put(key, true);
    }
    if (deleteKey) {
      canSkipWaitingSet.invalidate(key);
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

  protected void updateLogPosition(long lastReadPosition) {
    source.getSourceManager().logPositionAndCleanOldLogs(currentPath, source.getQueueId(),
      lastReadPosition, false);
    lastLoggedPosition = lastReadPosition;
  }

  private void updateSerialRepPositions(Map<String, Long> lastPositionsForSerialScope) {
    try {
      MetaTableAccessor.updateReplicationPositions(source.getSourceManager().getConnection(),
        source.getPeerId(), lastPositionsForSerialScope);
    } catch (IOException e) {
      LOG.error("updateReplicationPositions fail", e);
      throw new RuntimeException("updateReplicationPositions fail");
    }
  }

  public void startup(UncaughtExceptionHandler handler) {
    String name = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(this, name + ".replicationSource." + walGroupId + ","
        + source.getQueueId(), handler);
  }

  public PriorityBlockingQueue<Path> getLogQueue() {
    return this.queue;
  }

  public Path getCurrentPath() {
    return this.entryReader.getCurrentPath();
  }

  public long getCurrentPosition() {
    return this.lastLoggedPosition;
  }

  public void setWALReader(ReplicationSourceWALReader entryReader) {
    this.entryReader = entryReader;
  }

  public long getStartPosition() {
    return 0;
  }

  protected boolean isActive() {
    return source.isSourceActive() && state == WorkerState.RUNNING && !isInterrupted();
  }

  public void setWorkerState(WorkerState state) {
    this.state = state;
  }

  public WorkerState getWorkerState() {
    return state;
  }

  public void stopWorker() {
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
      if (LOG.isTraceEnabled()) {
        LOG.trace(msg + ", sleeping " + sleepForRetries + " times " + sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }
}
