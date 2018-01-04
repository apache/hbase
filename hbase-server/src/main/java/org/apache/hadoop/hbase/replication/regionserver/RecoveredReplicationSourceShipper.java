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
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceWALReader.WALEntryBatch;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Used by a {@link RecoveredReplicationSource}.
 */
@InterfaceAudience.Private
public class RecoveredReplicationSourceShipper extends ReplicationSourceShipper {
  private static final Logger LOG =
      LoggerFactory.getLogger(RecoveredReplicationSourceShipper.class);

  protected final RecoveredReplicationSource source;
  private final ReplicationQueueStorage replicationQueues;

  public RecoveredReplicationSourceShipper(Configuration conf, String walGroupId,
      PriorityBlockingQueue<Path> queue, RecoveredReplicationSource source,
      ReplicationQueueStorage queueStorage) {
    super(conf, walGroupId, queue, source);
    this.source = source;
    this.replicationQueues = queueStorage;
  }

  @Override
  public void run() {
    setWorkerState(WorkerState.RUNNING);
    // Loop until we close down
    while (isActive()) {
      int sleepMultiplier = 1;
      // Sleep until replication is enabled again
      if (!source.isPeerEnabled()) {
        if (source.sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }

      while (entryReader == null) {
        if (source.sleepForRetries("Replication WAL entry reader thread not initialized",
          sleepMultiplier)) {
          sleepMultiplier++;
        }
      }

      try {
        WALEntryBatch entryBatch = entryReader.take();
        shipEdits(entryBatch);
        if (entryBatch.getWalEntries().isEmpty()
            && entryBatch.getLastSeqIds().isEmpty()) {
          LOG.debug("Finished recovering queue for group " + walGroupId + " of peer "
              + source.getQueueId());
          source.getSourceMetrics().incrCompletedRecoveryQueue();
          setWorkerState(WorkerState.FINISHED);
          continue;
        }
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while waiting for next replication entry batch", e);
        Thread.currentThread().interrupt();
      }
    }
    source.tryFinish();
    // If the worker exits run loop without finishing its task, mark it as stopped.
    if (!isFinished()) {
      setWorkerState(WorkerState.STOPPED);
    }
  }

  @Override
  public long getStartPosition() {
    long startPosition = getRecoveredQueueStartPos();
    int numRetries = 0;
    while (numRetries <= maxRetriesMultiplier) {
      try {
        source.locateRecoveredPaths(queue);
        break;
      } catch (IOException e) {
        LOG.error("Error while locating recovered queue paths, attempt #" + numRetries);
        numRetries++;
      }
    }
    return startPosition;
  }

  // If this is a recovered queue, the queue is already full and the first log
  // normally has a position (unless the RS failed between 2 logs)
  private long getRecoveredQueueStartPos() {
    long startPosition = 0;
    String peerClusterZnode = source.getQueueId();
    try {
      startPosition = this.replicationQueues.getWALPosition(source.getServerWALsBelongTo(),
        peerClusterZnode, this.queue.peek().getName());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Recovered queue started with log " + this.queue.peek() + " at position " +
          startPosition);
      }
    } catch (ReplicationException e) {
      terminate("Couldn't get the position of this recovered queue " + peerClusterZnode, e);
    }
    return startPosition;
  }

  @Override
  protected void updateLogPosition(long lastReadPosition) {
    source.getSourceManager().logPositionAndCleanOldLogs(currentPath, source.getQueueId(),
      lastReadPosition, true);
    lastLoggedPosition = lastReadPosition;
  }

  private void terminate(String reason, Exception cause) {
    if (cause == null) {
      LOG.info("Closing worker for wal group " + this.walGroupId + " because: " + reason);

    } else {
      LOG.error("Closing worker for wal group " + this.walGroupId
          + " because an error occurred: " + reason, cause);
    }
    entryReader.interrupt();
    Threads.shutdown(entryReader, sleepForRetries);
    this.interrupt();
    Threads.shutdown(this, sleepForRetries);
    LOG.info("ReplicationSourceWorker " + this.getName() + " terminated");
  }
}
