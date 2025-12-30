/*
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * Reads and filters WAL entries, groups the filtered entries into batches, and puts the batches
 * onto a queue
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class ReplicationSourceWALReader extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceWALReader.class);

  private final ReplicationSourceLogQueue logQueue;
  private final FileSystem fs;
  private final Configuration conf;
  private final WALEntryFilter filter;
  private final ReplicationSource source;

  @InterfaceAudience.Private
  final BlockingQueue<WALEntryBatch> entryBatchQueue;
  // max (heap) size of each batch - multiply by number of batches in queue to get total
  private final long replicationBatchSizeCapacity;
  // max count of each batch - multiply by number of batches in queue to get total
  private final int replicationBatchCountCapacity;
  // position in the WAL to start reading at
  private long currentPosition;
  private final long sleepForRetries;
  private final int maxRetriesMultiplier;

  // Indicates whether this particular worker is running
  private boolean isReaderRunning = true;
  private final String walGroupId;

  AtomicBoolean waitingPeerEnabled = new AtomicBoolean(false);

  /**
   * Creates a reader worker for a given WAL queue. Reads WAL entries off a given queue, batches the
   * entries, and puts them on a batch queue.
   * @param fs            the files system to use
   * @param conf          configuration to use
   * @param logQueue      The WAL queue to read off of
   * @param startPosition position in the first WAL to start reading from
   * @param filter        The filter to use while reading
   * @param source        replication source
   */
  public ReplicationSourceWALReader(FileSystem fs, Configuration conf,
    ReplicationSourceLogQueue logQueue, long startPosition, WALEntryFilter filter,
    ReplicationSource source, String walGroupId) {
    this.logQueue = logQueue;
    this.currentPosition = startPosition;
    this.fs = fs;
    this.conf = conf;
    this.filter = filter;
    this.source = source;
    this.replicationBatchSizeCapacity =
      this.conf.getLong("replication.source.size.capacity", 1024 * 1024 * 64);
    this.replicationBatchCountCapacity = this.conf.getInt("replication.source.nb.capacity", 25000);
    // memory used will be batchSizeCapacity * (nb.batches + 1)
    // the +1 is for the current thread reading before placing onto the queue
    int batchCount = conf.getInt("replication.source.nb.batches", 1);
    // 1 second
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    // 5 minutes @ 1 sec per
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.entryBatchQueue = new LinkedBlockingQueue<>(batchCount);
    this.walGroupId = walGroupId;
    LOG.info("peerClusterZnode=" + source.getQueueId() + ", ReplicationSourceWALReaderThread : "
      + source.getPeerId() + " inited, replicationBatchSizeCapacity=" + replicationBatchSizeCapacity
      + ", replicationBatchCountCapacity=" + replicationBatchCountCapacity
      + ", replicationBatchQueueCapacity=" + batchCount);
  }

  private void replicationDone() throws InterruptedException {
    // we're done with current queue, either this is a recovered queue, or it is the special
    // group for a sync replication peer and the peer has been transited to DA or S state.
    LOG.debug("Stopping the replication source wal reader");
    setReaderRunning(false);
    // shuts down shipper thread immediately
    entryBatchQueue.put(WALEntryBatch.NO_MORE_DATA);
  }

  protected final int sleep(int sleepMultiplier) {
    if (sleepMultiplier < maxRetriesMultiplier) {
      sleepMultiplier++;
    }
    Threads.sleep(sleepForRetries * sleepMultiplier);
    return sleepMultiplier;
  }

  @Override
  public void run() {
    int sleepMultiplier = 1;
    while (isReaderRunning()) { // we only loop back here if something fatal happened to our stream
      try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, conf, currentPosition,
        source.getWALFileLengthProvider(), source.getSourceMetrics(), walGroupId)) {
        while (isReaderRunning()) { // loop here to keep reusing stream while we can
          if (!source.isPeerEnabled()) {
            waitingPeerEnabled.set(true);
            Threads.sleep(sleepForRetries);
            continue;
          } else {
            waitingPeerEnabled.set(false);
          }
          if (!checkBufferQuota()) {
            continue;
          }
          Path currentPath = entryStream.getCurrentPath();
          WALEntryStream.HasNext hasNext = entryStream.hasNext();
          if (hasNext == WALEntryStream.HasNext.NO) {
            replicationDone();
            return;
          }
          // first, check if we have switched a file, if so, we need to manually add an EOF entry
          // batch to the queue
          if (currentPath != null && switched(entryStream, currentPath)) {
            entryBatchQueue.put(WALEntryBatch.endOfFile(currentPath));
            continue;
          }
          if (hasNext == WALEntryStream.HasNext.RETRY) {
            // sleep and retry
            sleepMultiplier = sleep(sleepMultiplier);
            continue;
          }
          if (hasNext == WALEntryStream.HasNext.RETRY_IMMEDIATELY) {
            // retry immediately, this usually means we have switched a file
            continue;
          }
          // below are all for hasNext == YES
          WALEntryBatch batch = createBatch(entryStream);
          boolean successAddToQueue = false;
          try {
            readWALEntries(entryStream, batch);
            currentPosition = entryStream.getPosition();
            // need to propagate the batch even it has no entries since it may carry the last
            // sequence id information for serial replication.
            LOG.debug("Read {} WAL entries eligible for replication", batch.getNbEntries());
            entryBatchQueue.put(batch);
            successAddToQueue = true;
            sleepMultiplier = 1;
          } finally {
            if (!successAddToQueue) {
              // batch is not put to ReplicationSourceWALReader#entryBatchQueue,so we should
              // decrease ReplicationSourceWALReader.totalBufferUsed by the byte size which
              // acquired in ReplicationSourceWALReader.acquireBufferQuota.
              this.getSourceManager().releaseWALEntryBatchBufferQuota(batch);
            }
          }
        }
      } catch (WALEntryFilterRetryableException e) {
        // here we have to recreate the WALEntryStream, as when filtering, we have already called
        // next to get the WAL entry and advanced the WALEntryStream, at WALEntryStream layer, it
        // just considers everything is fine,that's why the catch block is not in the inner block
        LOG.warn("Failed to filter WAL entries and the filter let us retry later", e);
        sleepMultiplier = sleep(sleepMultiplier);
      } catch (InterruptedException e) {
        // this usually means we want to quit
        LOG.warn("Interrupted while sleeping between WAL reads or adding WAL batch to ship queue",
          e);
        Thread.currentThread().interrupt();
      }
    }
  }

  // returns true if we reach the size limit for batch, i.e, we need to finish the batch and return.
  protected final boolean addEntryToBatch(WALEntryBatch batch, Entry entry) {
    WALEdit edit = entry.getEdit();
    if (edit == null || edit.isEmpty()) {
      LOG.trace("Edit null or empty for entry {} ", entry);
      return false;
    }
    LOG.trace("updating TimeStampOfLastAttempted to {}, from entry {}, for source queue: {}",
      entry.getKey().getWriteTime(), entry.getKey(), this.source.getQueueId());
    updateReplicationMarkerEdit(entry, batch.getLastWalPosition());
    long entrySize = getEntrySizeIncludeBulkLoad(entry);
    batch.addEntry(entry, entrySize);
    updateBatchStats(batch, entry, entrySize);
    boolean totalBufferTooLarge = this.getSourceManager().acquireWALEntryBufferQuota(batch, entry);

    // Stop if too many entries or too big
    return totalBufferTooLarge || batch.getHeapSize() >= replicationBatchSizeCapacity
      || batch.getNbEntries() >= replicationBatchCountCapacity;
  }

  protected static final boolean switched(WALEntryStream entryStream, Path path) {
    Path newPath = entryStream.getCurrentPath();
    return newPath == null || !path.getName().equals(newPath.getName());
  }

  // We need to get the WALEntryBatch from the caller so we can add entries in there
  // This is required in case there is any exception in while reading entries
  // we do not want to loss the existing entries in the batch
  protected void readWALEntries(WALEntryStream entryStream, WALEntryBatch batch)
    throws InterruptedException {
    Path currentPath = entryStream.getCurrentPath();
    for (;;) {
      Entry entry = entryStream.next();
      batch.setLastWalPosition(entryStream.getPosition());
      entry = filterEntry(entry);
      if (entry != null) {
        if (addEntryToBatch(batch, entry)) {
          break;
        }
      }
      WALEntryStream.HasNext hasNext = entryStream.hasNext();
      // always return if we have switched to a new file
      if (switched(entryStream, currentPath)) {
        batch.setEndOfFile(true);
        break;
      }
      if (hasNext != WALEntryStream.HasNext.YES) {
        // For hasNext other than YES, it is OK to just retry.
        // As for RETRY and RETRY_IMMEDIATELY, the correct action is to retry, and for NO, it will
        // return NO again when you call the method next time, so it is OK to just return here and
        // let the loop in the upper layer to call hasNext again.
        break;
      }
    }
  }

  public Path getCurrentPath() {
    // if we've read some WAL entries, get the Path we read from
    WALEntryBatch batchQueueHead = entryBatchQueue.peek();
    if (batchQueueHead != null) {
      return batchQueueHead.getLastWalPath();
    }
    // otherwise, we must be currently reading from the head of the log queue
    return logQueue.getQueue(walGroupId).peek();
  }

  // returns false if we've already exceeded the global quota
  private boolean checkBufferQuota() {
    // try not to go over total quota
    if (!this.getSourceManager().checkBufferQuota(this.source.getPeerId())) {
      Threads.sleep(sleepForRetries);
      return false;
    }
    return true;
  }

  private WALEntryBatch createBatch(WALEntryStream entryStream) {
    return new WALEntryBatch(replicationBatchCountCapacity, entryStream.getCurrentPath());
  }

  protected final Entry filterEntry(Entry entry) {
    // Always replicate if this edit is Replication Marker edit.
    if (entry != null && WALEdit.isReplicationMarkerEdit(entry.getEdit())) {
      return entry;
    }
    Entry filtered = filter.filter(entry);
    if (entry != null && (filtered == null || filtered.getEdit().size() == 0)) {
      LOG.trace("Filtered entry for replication: {}", entry);
      source.getSourceMetrics().incrLogEditsFiltered();
    }
    return filtered;
  }

  /**
   * Retrieves the next batch of WAL entries from the queue, waiting up to the specified time for a
   * batch to become available
   * @return A batch of entries, along with the position in the log after reading the batch
   * @throws InterruptedException if interrupted while waiting
   */
  public WALEntryBatch take() throws InterruptedException {
    return entryBatchQueue.take();
  }

  public WALEntryBatch poll(long timeout) throws InterruptedException {
    return entryBatchQueue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  private long getEntrySizeIncludeBulkLoad(Entry entry) {
    WALEdit edit = entry.getEdit();
    return WALEntryBatch.getEntrySizeExcludeBulkLoad(entry) + sizeOfStoreFilesIncludeBulkLoad(edit);
  }

  private void updateBatchStats(WALEntryBatch batch, Entry entry, long entrySize) {
    WALEdit edit = entry.getEdit();
    batch.incrementHeapSize(entrySize);
    Pair<Integer, Integer> nbRowsAndHFiles = countDistinctRowKeysAndHFiles(edit);
    batch.incrementNbRowKeys(nbRowsAndHFiles.getFirst());
    batch.incrementNbHFiles(nbRowsAndHFiles.getSecond());
  }

  /**
   * Count the number of different row keys in the given edit because of mini-batching. We assume
   * that there's at least one Cell in the WALEdit.
   * @param edit edit to count row keys from
   * @return number of different row keys and HFiles
   */
  private Pair<Integer, Integer> countDistinctRowKeysAndHFiles(WALEdit edit) {
    List<Cell> cells = edit.getCells();
    int distinctRowKeys = 1;
    int totalHFileEntries = 0;
    Cell lastCell = cells.get(0);

    int totalCells = edit.size();
    for (int i = 0; i < totalCells; i++) {
      // Count HFiles to be replicated
      if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
        try {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            totalHFileEntries += stores.get(j).getStoreFileList().size();
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
            + "Then its hfiles count will not be added into metric.", e);
        }
      }

      if (!CellUtil.matchingRows(cells.get(i), lastCell)) {
        distinctRowKeys++;
      }
      lastCell = cells.get(i);
    }

    Pair<Integer, Integer> result = new Pair<>(distinctRowKeys, totalHFileEntries);
    return result;
  }

  /**
   * Calculate the total size of all the store files
   * @param edit edit to count row keys from
   * @return the total size of the store files
   */
  private int sizeOfStoreFilesIncludeBulkLoad(WALEdit edit) {
    List<Cell> cells = edit.getCells();
    int totalStoreFilesSize = 0;

    int totalCells = edit.size();
    for (int i = 0; i < totalCells; i++) {
      if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
        try {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            totalStoreFilesSize =
              (int) (totalStoreFilesSize + stores.get(j).getStoreFileSizeBytes());
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
            + "Size of HFiles part of cell will not be considered in replication "
            + "request size calculation.", e);
        }
      }
    }
    return totalStoreFilesSize;
  }

  /*
   * Create @ReplicationMarkerDescriptor with region_server_name, wal_name and offset and set to
   * cell's value.
   */
  private void updateReplicationMarkerEdit(Entry entry, long offset) {
    WALEdit edit = entry.getEdit();
    // Return early if it is not ReplicationMarker edit.
    if (!WALEdit.isReplicationMarkerEdit(edit)) {
      return;
    }
    List<Cell> cells = edit.getCells();
    Preconditions.checkArgument(cells.size() == 1, "ReplicationMarker should have only 1 cell");
    Cell cell = cells.get(0);
    // Create a descriptor with region_server_name, wal_name and offset
    WALProtos.ReplicationMarkerDescriptor.Builder builder =
      WALProtos.ReplicationMarkerDescriptor.newBuilder();
    builder.setRegionServerName(this.source.getServer().getServerName().getHostname());
    builder.setWalName(getCurrentPath().getName());
    builder.setOffset(offset);
    WALProtos.ReplicationMarkerDescriptor descriptor = builder.build();

    // Create a new KeyValue
    KeyValue kv = new KeyValue(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
      CellUtil.cloneQualifier(cell), cell.getTimestamp(), descriptor.toByteArray());
    ArrayList<Cell> newCells = new ArrayList<>();
    newCells.add(kv);
    // Update edit with new cell.
    edit.setCells(newCells);
  }

  /** Returns whether the reader thread is running */
  public boolean isReaderRunning() {
    return isReaderRunning && !isInterrupted();
  }

  /**
   * @param readerRunning the readerRunning to set
   */
  public void setReaderRunning(boolean readerRunning) {
    this.isReaderRunning = readerRunning;
  }

  private ReplicationSourceManager getSourceManager() {
    return this.source.getSourceManager();
  }
}
