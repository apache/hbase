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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream.WALEntryStreamRuntimeException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Reads and filters WAL entries, groups the filtered entries into batches, and puts the batches onto a queue
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicationSourceWALReaderThread extends Thread {
  private static final Log LOG = LogFactory.getLog(ReplicationSourceWALReaderThread.class);

  private PriorityBlockingQueue<Path> logQueue;
  private FileSystem fs;
  private Configuration conf;
  private BlockingQueue<WALEntryBatch> entryBatchQueue;
  // max (heap) size of each batch - multiply by number of batches in queue to get total
  private long replicationBatchSizeCapacity;
  // max count of each batch - multiply by number of batches in queue to get total
  private int replicationBatchCountCapacity;
  // position in the WAL to start reading at
  private long currentPosition;
  private WALEntryFilter filter;
  private long sleepForRetries;
  //Indicates whether this particular worker is running
  private boolean isReaderRunning = true;
  private ReplicationQueueInfo replicationQueueInfo;
  private int maxRetriesMultiplier;
  private MetricsSource metrics;

  /**
   * Creates a reader worker for a given WAL queue. Reads WAL entries off a given queue, batches the
   * entries, and puts them on a batch queue.
   * @param manager replication manager
   * @param replicationQueueInfo
   * @param logQueue The WAL queue to read off of
   * @param startPosition position in the first WAL to start reading from
   * @param fs the files system to use
   * @param conf configuration to use
   * @param filter The filter to use while reading
   * @param metrics replication metrics
   */
  public ReplicationSourceWALReaderThread(ReplicationSourceManager manager,
      ReplicationQueueInfo replicationQueueInfo, PriorityBlockingQueue<Path> logQueue,
      long startPosition,
      FileSystem fs, Configuration conf, WALEntryFilter filter, MetricsSource metrics) {
    this.replicationQueueInfo = replicationQueueInfo;
    this.logQueue = logQueue;
    this.currentPosition = startPosition;
    this.fs = fs;
    this.conf = conf;
    this.filter = filter;
    this.replicationBatchSizeCapacity =
        this.conf.getLong("replication.source.size.capacity", 1024 * 1024 * 64);
    this.replicationBatchCountCapacity = this.conf.getInt("replication.source.nb.capacity", 25000);
    // memory used will be batchSizeCapacity * (nb.batches + 1)
    // the +1 is for the current thread reading before placing onto the queue
    int batchCount = conf.getInt("replication.source.nb.batches", 1);
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
    this.metrics = metrics;
    this.entryBatchQueue = new LinkedBlockingQueue<>(batchCount);
    LOG.info("peerClusterZnode=" + replicationQueueInfo.getPeerClusterZnode()
        + ", ReplicationSourceWALReaderThread : " + replicationQueueInfo.getPeerId()
        + " inited, replicationBatchSizeCapacity=" + replicationBatchSizeCapacity
        + ", replicationBatchCountCapacity=" + replicationBatchCountCapacity
        + ", replicationBatchQueueCapacity=" + batchCount);
  }

  @Override
  public void run() {
    int sleepMultiplier = 1;
    while (isReaderRunning()) { // we only loop back here if something fatal happened to our stream
      try (WALEntryStream entryStream =
          new WALEntryStream(logQueue, fs, conf, currentPosition, metrics)) {
        while (isReaderRunning()) { // loop here to keep reusing stream while we can
          WALEntryBatch batch = null;
          while (entryStream.hasNext()) {
            if (batch == null) {
              batch = new WALEntryBatch(replicationBatchCountCapacity, entryStream.getCurrentPath());
            }
            Entry entry = entryStream.next();
            entry = filterEntry(entry);
            if (entry != null) {
              WALEdit edit = entry.getEdit();
              if (edit != null && !edit.isEmpty()) {
                long entrySize = getEntrySize(entry);
                batch.addEntry(entry);
                updateBatchStats(batch, entry, entryStream.getPosition(), entrySize);
                // Stop if too many entries or too big
                if (batch.getHeapSize() >= replicationBatchSizeCapacity
                    || batch.getNbEntries() >= replicationBatchCountCapacity) {
                  break;
                }
              }
            }
          }
          if (batch != null && (!batch.getLastSeqIds().isEmpty() || batch.getNbEntries() > 0)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(String.format("Read %s WAL entries eligible for replication",
                batch.getNbEntries()));
            }
            entryBatchQueue.put(batch);
            sleepMultiplier = 1;
          } else { // got no entries and didn't advance position in WAL
            LOG.trace("Didn't read any new entries from WAL");
            if (replicationQueueInfo.isQueueRecovered()) {
              // we're done with queue recovery, shut ourself down
              setReaderRunning(false);
              // shuts down shipper thread immediately
              entryBatchQueue.put(batch != null ? batch
                  : new WALEntryBatch(replicationBatchCountCapacity, entryStream.getCurrentPath()));
            } else {
              Thread.sleep(sleepForRetries);
            }
          }
          currentPosition = entryStream.getPosition();
          entryStream.reset(); // reuse stream
        }
      } catch (IOException | WALEntryStreamRuntimeException e) { // stream related
        if (sleepMultiplier < maxRetriesMultiplier) {
          LOG.debug("Failed to read stream of replication entries: " + e);
          sleepMultiplier++;
        } else {
          LOG.error("Failed to read stream of replication entries", e);
          handleEofException(e);
        }
        Threads.sleep(sleepForRetries * sleepMultiplier);
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while sleeping between WAL reads");
        Thread.currentThread().interrupt();
      }
    }
  }

  // if we get an EOF due to a zero-length log, and there are other logs in queue
  // (highly likely we've closed the current log), we've hit the max retries, and autorecovery is
  // enabled, then dump the log
  private void handleEofException(Exception e) {
    if (e.getCause() instanceof EOFException && logQueue.size() > 1
        && conf.getBoolean("replication.source.eof.autorecovery", false)) {
      try {
        if (fs.getFileStatus(logQueue.peek()).getLen() == 0) {
          LOG.warn("Forcing removal of 0 length log in queue: " + logQueue.peek());
          logQueue.remove();
          currentPosition = 0;
        }
      } catch (IOException ioe) {
        LOG.warn("Couldn't get file length information about log " + logQueue.peek());
      }
    }
  }

  public Path getCurrentPath() {
    // if we've read some WAL entries, get the Path we read from
    WALEntryBatch batchQueueHead = entryBatchQueue.peek();
    if (batchQueueHead != null) {
      return batchQueueHead.lastWalPath;
    }
    // otherwise, we must be currently reading from the head of the log queue
    return logQueue.peek();
  }

  private Entry filterEntry(Entry entry) {
    Entry filtered = filter.filter(entry);
    if (entry != null && filtered == null) {
      metrics.incrLogEditsFiltered();
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

  private long getEntrySize(Entry entry) {
    WALEdit edit = entry.getEdit();
    return edit.heapSize() + calculateTotalSizeOfStoreFiles(edit);
  }

  private void updateBatchStats(WALEntryBatch batch, Entry entry, long entryPosition, long entrySize) {
    WALEdit edit = entry.getEdit();
    if (edit != null && !edit.isEmpty()) {
      batch.incrementHeapSize(entrySize);
      Pair<Integer, Integer> nbRowsAndHFiles = countDistinctRowKeysAndHFiles(edit);
      batch.incrementNbRowKeys(nbRowsAndHFiles.getFirst());
      batch.incrementNbHFiles(nbRowsAndHFiles.getSecond());
    }
    batch.lastWalPosition = entryPosition;
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
              + "Then its hfiles count will not be added into metric.");
        }
      }

      if (!CellUtil.matchingRow(cells.get(i), lastCell)) {
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
  private int calculateTotalSizeOfStoreFiles(WALEdit edit) {
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
            totalStoreFilesSize += stores.get(j).getStoreFileSizeBytes();
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
              + "Size of HFiles part of cell will not be considered in replication "
              + "request size calculation.",
            e);
        }
      }
    }
    return totalStoreFilesSize;
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

  /**
   * Holds a batch of WAL entries to replicate, along with some statistics
   *
   */
  static class WALEntryBatch {
    private List<Entry> walEntries;
    // last WAL that was read
    private Path lastWalPath;
    // position in WAL of last entry in this batch
    private long lastWalPosition = 0;
    // number of distinct row keys in this batch
    private int nbRowKeys = 0;
    // number of HFiles
    private int nbHFiles = 0;
    // heap size of data we need to replicate
    private long heapSize = 0;
    // save the last sequenceid for each region if the table has serial-replication scope
    private Map<String, Long> lastSeqIds = new HashMap<>();

    /**
     * @param walEntries
     * @param lastWalPath Path of the WAL the last entry in this batch was read from
     * @param lastWalPosition Position in the WAL the last entry in this batch was read from
     */
    private WALEntryBatch(int maxNbEntries, Path lastWalPath) {
      this.walEntries = new ArrayList<>(maxNbEntries);
      this.lastWalPath = lastWalPath;
    }

    public void addEntry(Entry entry) {
      walEntries.add(entry);
    }

    /**
     * @return the WAL Entries.
     */
    public List<Entry> getWalEntries() {
      return walEntries;
    }

    /**
     * @return the path of the last WAL that was read.
     */
    public Path getLastWalPath() {
      return lastWalPath;
    }

    /**
     * @return the position in the last WAL that was read.
     */
    public long getLastWalPosition() {
      return lastWalPosition;
    }

    public int getNbEntries() {
      return walEntries.size();
    }

    /**
     * @return the number of distinct row keys in this batch
     */
    public int getNbRowKeys() {
      return nbRowKeys;
    }

    /**
     * @return the number of HFiles in this batch
     */
    public int getNbHFiles() {
      return nbHFiles;
    }

    /**
     * @return total number of operations in this batch
     */
    public int getNbOperations() {
      return getNbRowKeys() + getNbHFiles();
    }

    /**
     * @return the heap size of this batch
     */
    public long getHeapSize() {
      return heapSize;
    }

    /**
     * @return the last sequenceid for each region if the table has serial-replication scope
     */
    public Map<String, Long> getLastSeqIds() {
      return lastSeqIds;
    }

    private void incrementNbRowKeys(int increment) {
      nbRowKeys += increment;
    }

    private void incrementNbHFiles(int increment) {
      nbHFiles += increment;
    }

    private void incrementHeapSize(long increment) {
      heapSize += increment;
    }

    private void setLastPosition(String region, Long sequenceId) {
      getLastSeqIds().put(region, sequenceId);
    }
  }
}
