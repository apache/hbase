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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final boolean eofAutoRecovery;

  //Indicates whether this particular worker is running
  private boolean isReaderRunning = true;

  private AtomicLong totalBufferUsed;
  private long totalBufferQuota;
  private final String walGroupId;

  /**
   * Creates a reader worker for a given WAL queue. Reads WAL entries off a given queue, batches the
   * entries, and puts them on a batch queue.
   * @param fs the files system to use
   * @param conf configuration to use
   * @param logQueue The WAL queue to read off of
   * @param startPosition position in the first WAL to start reading from
   * @param filter The filter to use while reading
   * @param source replication source
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
    this.totalBufferUsed = source.getSourceManager().getTotalBufferUsed();
    this.totalBufferQuota = source.getSourceManager().getTotalBufferLimit();
    // 1 second
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    // 5 minutes @ 1 sec per
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.eofAutoRecovery = conf.getBoolean("replication.source.eof.autorecovery", false);
    this.entryBatchQueue = new LinkedBlockingQueue<>(batchCount);
    this.walGroupId = walGroupId;
    LOG.info("peerClusterZnode=" + source.getQueueId()
        + ", ReplicationSourceWALReaderThread : " + source.getPeerId()
        + " inited, replicationBatchSizeCapacity=" + replicationBatchSizeCapacity
        + ", replicationBatchCountCapacity=" + replicationBatchCountCapacity
        + ", replicationBatchQueueCapacity=" + batchCount);
  }

  @Override
  public void run() {
    int sleepMultiplier = 1;
    while (isReaderRunning()) { // we only loop back here if something fatal happened to our stream
      WALEntryBatch batch = null;
      try (WALEntryStream entryStream =
          new WALEntryStream(logQueue, conf, currentPosition,
              source.getWALFileLengthProvider(), source.getServerWALsBelongTo(),
              source.getSourceMetrics(), walGroupId)) {
        while (isReaderRunning()) { // loop here to keep reusing stream while we can
          batch = null;
          if (!source.isPeerEnabled()) {
            Threads.sleep(sleepForRetries);
            continue;
          }
          if (!checkQuota()) {
            continue;
          }
          batch = tryAdvanceStreamAndCreateWALBatch(entryStream);
          if (batch == null) {
            // got no entries and didn't advance position in WAL
            handleEmptyWALEntryBatch();
            entryStream.reset(); // reuse stream
            continue;
          }
          // if we have already switched a file, skip reading and put it directly to the ship queue
          if (!batch.isEndOfFile()) {
            readWALEntries(entryStream, batch);
            currentPosition = entryStream.getPosition();
          }
          // need to propagate the batch even it has no entries since it may carry the last
          // sequence id information for serial replication.
          LOG.debug("Read {} WAL entries eligible for replication", batch.getNbEntries());
          entryBatchQueue.put(batch);
          sleepMultiplier = 1;
        }
      } catch (WALEntryFilterRetryableException | IOException e) { // stream related
        if (!handleEofException(e, batch)) {
          LOG.warn("Failed to read stream of replication entries", e);
          if (sleepMultiplier < maxRetriesMultiplier) {
            sleepMultiplier++;
          }
          Threads.sleep(sleepForRetries * sleepMultiplier);
        }
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while sleeping between WAL reads or adding WAL batch to ship queue");
        Thread.currentThread().interrupt();
      }
    }
  }

  // returns true if we reach the size limit for batch, i.e, we need to finish the batch and return.
  protected final boolean addEntryToBatch(WALEntryBatch batch, Entry entry) {
    WALEdit edit = entry.getEdit();
    if (edit == null || edit.isEmpty()) {
      LOG.debug("Edit null or empty for entry {} ", entry);
      return false;
    }
    LOG.debug("updating TimeStampOfLastAttempted to {}, from entry {}, for source queue: {}",
        entry.getKey().getWriteTime(), entry.getKey(), this.source.getQueueId());
    long entrySize = getEntrySizeIncludeBulkLoad(entry);
    long entrySizeExcludeBulkLoad = getEntrySizeExcludeBulkLoad(entry);
    batch.addEntry(entry, entrySize);
    updateBatchStats(batch, entry, entrySize);
    boolean totalBufferTooLarge = acquireBufferQuota(entrySizeExcludeBulkLoad);

    // Stop if too many entries or too big
    return totalBufferTooLarge || batch.getHeapSize() >= replicationBatchSizeCapacity ||
      batch.getNbEntries() >= replicationBatchCountCapacity;
  }

  protected static final boolean switched(WALEntryStream entryStream, Path path) {
    Path newPath = entryStream.getCurrentPath();
    return newPath == null || !path.getName().equals(newPath.getName());
  }

  // We need to get the WALEntryBatch from the caller so we can add entries in there
  // This is required in case there is any exception in while reading entries
  // we do not want to loss the existing entries in the batch
  protected void readWALEntries(WALEntryStream entryStream, WALEntryBatch batch)
    throws IOException, InterruptedException {
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
      boolean hasNext = entryStream.hasNext();
      // always return if we have switched to a new file
      if (switched(entryStream, currentPath)) {
        batch.setEndOfFile(true);
        break;
      }
      if (!hasNext) {
        break;
      }
    }
  }

  private void handleEmptyWALEntryBatch() throws InterruptedException {
    LOG.trace("Didn't read any new entries from WAL");
    if (logQueue.getQueue(walGroupId).isEmpty()) {
      // we're done with current queue, either this is a recovered queue, or it is the special group
      // for a sync replication peer and the peer has been transited to DA or S state.
      LOG.debug("Stopping the replication source wal reader");
      setReaderRunning(false);
      // shuts down shipper thread immediately
      entryBatchQueue.put(WALEntryBatch.NO_MORE_DATA);
    } else {
      Thread.sleep(sleepForRetries);
    }
  }

  private WALEntryBatch tryAdvanceStreamAndCreateWALBatch(WALEntryStream entryStream)
    throws IOException {
    Path currentPath = entryStream.getCurrentPath();
    if (!entryStream.hasNext()) {
      // check whether we have switched a file
      if (currentPath != null && switched(entryStream, currentPath)) {
        return WALEntryBatch.endOfFile(currentPath);
      } else {
        return null;
      }
    }
    if (currentPath != null) {
      if (switched(entryStream, currentPath)) {
        return WALEntryBatch.endOfFile(currentPath);
      }
    }
    return createBatch(entryStream);
  }

  /**
   * This is to handle the EOFException from the WAL entry stream. EOFException should
   * be handled carefully because there are chances of data loss because of never replicating
   * the data. Thus we should always try to ship existing batch of entries here.
   * If there was only one log in the queue before EOF, we ship the empty batch here
   * and since reader is still active, in the next iteration of reader we will
   * stop the reader.
   * <p/>
   * If there was more than one log in the queue before EOF, we ship the existing batch
   * and reset the wal patch and position to the log with EOF, so shipper can remove
   * logs from replication queue
   * @return true only the IOE can be handled
   */
  private boolean handleEofException(Exception e, WALEntryBatch batch) {
    PriorityBlockingQueue<Path> queue = logQueue.getQueue(walGroupId);
    // Dump the log even if logQueue size is 1 if the source is from recovered Source
    // since we don't add current log to recovered source queue so it is safe to remove.
    if ((e instanceof EOFException || e.getCause() instanceof EOFException) &&
      (source.isRecovered() || queue.size() > 1) && this.eofAutoRecovery) {
      Path path = queue.peek();
      try {
        if (!fs.exists(path)) {
          // There is a chance that wal has moved to oldWALs directory, so look there also.
          path = AbstractFSWALProvider.findArchivedLog(path, conf);
          // path can be null if unable to locate in archiveDir.
        }
        if (path != null && fs.getFileStatus(path).getLen() == 0) {
          LOG.warn("Forcing removal of 0 length log in queue: {}", path);
          logQueue.remove(walGroupId);
          currentPosition = 0;
          if (batch != null) {
            // After we removed the WAL from the queue, we should try shipping the existing batch of
            // entries
            addBatchToShippingQueue(batch);
          }
          return true;
        }
      } catch (IOException ioe) {
        LOG.warn("Couldn't get file length information about log " + path, ioe);
      } catch (InterruptedException ie) {
        LOG.trace("Interrupted while adding WAL batch to ship queue");
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  /**
   * Update the batch try to ship and return true if shipped
   * @param batch Batch of entries to ship
   * @throws InterruptedException throws interrupted exception
   */
  private void addBatchToShippingQueue(WALEntryBatch batch) throws InterruptedException {
    // need to propagate the batch even it has no entries since it may carry the last
    // sequence id information for serial replication.
    LOG.debug("Read {} WAL entries eligible for replication", batch.getNbEntries());
    entryBatchQueue.put(batch);
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

  //returns false if we've already exceeded the global quota
  private boolean checkQuota() {
    // try not to go over total quota
    if (totalBufferUsed.get() > totalBufferQuota) {
      LOG.warn("peer={}, can't read more edits from WAL as buffer usage {}B exceeds limit {}B",
          this.source.getPeerId(), totalBufferUsed.get(), totalBufferQuota);
      Threads.sleep(sleepForRetries);
      return false;
    }
    return true;
  }

  private WALEntryBatch createBatch(WALEntryStream entryStream) {
    return new WALEntryBatch(replicationBatchCountCapacity, entryStream.getCurrentPath());
  }

  protected final Entry filterEntry(Entry entry) {
    Entry filtered = filter.filter(entry);
    if (entry != null && (filtered == null || filtered.getEdit().size() == 0)) {
      LOG.debug("Filtered entry for replication: {}", entry);
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
    return  getEntrySizeExcludeBulkLoad(entry) + sizeOfStoreFilesIncludeBulkLoad(edit);
  }

  public static long getEntrySizeExcludeBulkLoad(Entry entry) {
    WALEdit edit = entry.getEdit();
    WALKey key = entry.getKey();
    return edit.heapSize() + key.estimatedSerializedSizeOf();
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

  /**
   * @param size delta size for grown buffer
   * @return true if we should clear buffer and push all
   */
  private boolean acquireBufferQuota(long size) {
    long newBufferUsed = totalBufferUsed.addAndGet(size);
    // Record the new buffer usage
    this.source.getSourceManager().getGlobalMetrics().setWALReaderEditsBufferBytes(newBufferUsed);
    return newBufferUsed >= totalBufferQuota;
  }

  /**
   * @return whether the reader thread is running
   */
  public boolean isReaderRunning() {
    return isReaderRunning && !isInterrupted();
  }

  /**
   * @param readerRunning the readerRunning to set
   */
  public void setReaderRunning(boolean readerRunning) {
    this.isReaderRunning = readerRunning;
  }
}
