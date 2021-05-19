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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Holds a batch of WAL entries to replicate, along with some statistics
 */
@InterfaceAudience.Private
class WALEntryBatch {

  // used by recovered replication queue to indicate that all the entries have been read.
  public static final WALEntryBatch NO_MORE_DATA = new WALEntryBatch(0, null);

  private List<Pair<Entry, Long>> walEntriesWithSize;

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
  // indicate that this is the end of the current file
  private boolean endOfFile;

  /**
   * @param lastWalPath Path of the WAL the last entry in this batch was read from
   */
  WALEntryBatch(int maxNbEntries, Path lastWalPath) {
    this.walEntriesWithSize = new ArrayList<>(maxNbEntries);
    this.lastWalPath = lastWalPath;
  }


  static WALEntryBatch endOfFile(Path lastWalPath) {
    WALEntryBatch batch = new WALEntryBatch(0, lastWalPath);
    batch.setLastWalPosition(-1L);
    batch.setEndOfFile(true);
    return batch;
  }

  public void addEntry(Entry entry, long entrySize) {
    walEntriesWithSize.add(new Pair<>(entry, entrySize));
  }

  /**
   * @return the WAL Entries.
   */
  public List<Entry> getWalEntries() {
    return walEntriesWithSize.stream().map(Pair::getFirst).collect(Collectors.toList());
  }

  /**
   * @return the WAL Entries.
   */
  public List<Pair<Entry, Long>> getWalEntriesWithSize() {
    return walEntriesWithSize;
  }

  /**
   * @return the path of the last WAL that was read.
   */
  public Path getLastWalPath() {
    return lastWalPath;
  }

  public void setLastWalPath(Path lastWalPath) {
    this.lastWalPath = lastWalPath;
  }

  /**
   * @return the position in the last WAL that was read.
   */
  public long getLastWalPosition() {
    return lastWalPosition;
  }

  public void setLastWalPosition(long lastWalPosition) {
    this.lastWalPosition = lastWalPosition;
  }

  public int getNbEntries() {
    return walEntriesWithSize.size();
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

  public boolean isEndOfFile() {
    return endOfFile;
  }

  public void setEndOfFile(boolean endOfFile) {
    this.endOfFile = endOfFile;
  }

  public void incrementNbRowKeys(int increment) {
    nbRowKeys += increment;
  }

  public void incrementNbHFiles(int increment) {
    nbHFiles += increment;
  }

  public void incrementHeapSize(long increment) {
    heapSize += increment;
  }

  public void setLastSeqId(String region, long sequenceId) {
    lastSeqIds.put(region, sequenceId);
  }

  @Override
  public String toString() {
    return "WALEntryBatch [walEntries=" + walEntriesWithSize + ", lastWalPath=" + lastWalPath +
      ", lastWalPosition=" + lastWalPosition + ", nbRowKeys=" + nbRowKeys + ", nbHFiles=" +
      nbHFiles + ", heapSize=" + heapSize + ", lastSeqIds=" + lastSeqIds + ", endOfFile=" +
      endOfFile + "]";
  }
}
