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
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;

/**
 * The MemStore holds in-memory modifications to the Store. Modifications are {@link Cell}s.
 * <p>
 * The MemStore functions should not be called in parallel. Callers should hold write and read
 * locks. This is done in {@link HStore}.
 * </p>
 */
@InterfaceAudience.Private
public interface MemStore {

  /**
   * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
   * {@link #clearSnapshot(long)}.
   * @return {@link MemStoreSnapshot}
   */
  MemStoreSnapshot snapshot();

  /**
   * Clears the current snapshot of the Memstore.
   * @param id
   * @throws UnexpectedStateException
   * @see #snapshot()
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
  MemstoreSize getFlushableSize();

  /**
   * Return the size of the snapshot(s) if any
   * @return size of the memstore snapshot
   */
  MemstoreSize getSnapshotSize();

  /**
   * Write an update
   * @param cell
   * @param memstoreSize The delta in memstore size will be passed back via this.
   *        This will include both data size and heap overhead delta.
   */
  void add(final Cell cell, MemstoreSize memstoreSize);

  /**
   * Write the updates
   * @param cells
   * @param memstoreSize The delta in memstore size will be passed back via this.
   *        This will include both data size and heap overhead delta.
   */
  void add(Iterable<Cell> cells, MemstoreSize memstoreSize);

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  long timeOfOldestEdit();

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
   * @param memstoreSize The delta in memstore size will be passed back via this.
   *        This will include both data size and heap overhead delta.
   */
  void upsert(Iterable<Cell> cells, long readpoint, MemstoreSize memstoreSize);

  /**
   * @return scanner over the memstore. This might include scanner over the snapshot when one is
   * present.
   */
  List<KeyValueScanner> getScanners(long readPt) throws IOException;

  /**
   * @return Total memory occupied by this MemStore. This won't include any size occupied by the
   *         snapshot. We assume the snapshot will get cleared soon. This is not thread safe and
   *         the memstore may be changed while computing its size. It is the responsibility of the
   *         caller to make sure this doesn't happen.
   */
  MemstoreSize size();

  /**
   * This method is called when it is clear that the flush to disk is completed.
   * The store may do any post-flush actions at this point.
   * One example is to update the wal with sequence number that is known only at the store level.
   */
  void finalizeFlush();

  /* Return true if the memstore may need some extra memory space*/
  boolean isSloppy();
}
