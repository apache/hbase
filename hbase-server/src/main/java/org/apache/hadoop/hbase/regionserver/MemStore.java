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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;

/**
 * The MemStore holds in-memory modifications to the Store. Modifications are {@link Cell}s.
 * <p>
 * The MemStore functions should not be called in parallel. Callers should hold write and read
 * locks. This is done in {@link HStore}.
 * </p>
 */
@InterfaceAudience.Private
public interface MemStore extends HeapSize {

  /**
   * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
   * {@link #clearSnapshot(long)}.
   * @return {@link MemStoreSnapshot}
   */
  MemStoreSnapshot snapshot();

  /**
   * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
   * {@link #clearSnapshot(long)}.
   * @param flushOpSeqId the current sequence number of the wal; to be attached to the flushed
   *                     segment
   * @return {@link MemStoreSnapshot}
   */
  MemStoreSnapshot snapshot(long flushOpSeqId);

  /**
   * Clears the current snapshot of the Memstore.
   * @param id
   * @throws UnexpectedStateException
   * @see #snapshot(long)
   */
  void clearSnapshot(long id) throws UnexpectedStateException;

  /**
   * On flush, how much memory we will clear.
   * Flush will first clear out the data in snapshot if any (It will take a second flush
   * invocation to clear the current Cell set). If snapshot is empty, current
   * Cell set will be flushed.
   *
   * @return size of data that is going to be flushed
   */
  long getFlushableSize();

  /**
   * Return the size of the snapshot(s) if any
   * @return size of the memstore snapshot
   */
  long getSnapshotSize();

  /**
   * Write an update
   * @param cell
   * @return approximate size of the passed cell.
   */
  long add(final Cell cell);

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  long timeOfOldestEdit();

  /**
   * Remove n key from the memstore. Only kvs that have the same key and the same memstoreTS are
   * removed. It is ok to not update timeRangeTracker in this call.
   * @param cell
   */
  void rollback(final Cell cell);

  /**
   * Write a delete
   * @param deleteCell
   * @return approximate size of the passed key and value.
   */
  long delete(final Cell deleteCell);

  /**
   * Given the specs of a column, update it, first by inserting a new record,
   * then removing the old one.  Since there is only 1 KeyValue involved, the memstoreTS
   * will be set to 0, thus ensuring that they instantly appear to anyone. The underlying
   * store will ensure that the insert/delete each are atomic. A scanner/reader will either
   * get the new value, or the old value and all readers will eventually only see the new
   * value after the old was removed.
   *
   * @param row
   * @param family
   * @param qualifier
   * @param newValue
   * @param now
   * @return Timestamp
   */
  long updateColumnValue(byte[] row, byte[] family, byte[] qualifier, long newValue, long now);

  /**
   * Update or insert the specified cells.
   * <p>
   * For each Cell, insert into MemStore. This will atomically upsert the value for that
   * row/family/qualifier. If a Cell did already exist, it will then be removed.
   * <p>
   * Currently the memstoreTS is kept at 0 so as each insert happens, it will be immediately
   * visible. May want to change this so it is atomic across all KeyValues.
   * <p>
   * This is called under row lock, so Get operations will still see updates atomically. Scans will
   * only see each KeyValue update as atomic.
   * @param cells
   * @param readpoint readpoint below which we can safely remove duplicate Cells.
   * @return change in memstore size
   */
  long upsert(Iterable<Cell> cells, long readpoint);

  /**
   * @return scanner over the memstore. This might include scanner over the snapshot when one is
   * present.
   */
  List<KeyValueScanner> getScanners(long readPt) throws IOException;

  /**
   * @return Total memory occupied by this MemStore.
   */
  long size();

  /**
   * This method is called when it is clear that the flush to disk is completed.
   * The store may do any post-flush actions at this point.
   * One example is to update the wal with sequence number that is known only at the store level.
   */
  void finalizeFlush();
}
