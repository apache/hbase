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
package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The MemStore holds in-memory modifications to the Store. Modifications are {@link Cell}s.
 * <p>
 * The MemStore functions should not be called in parallel. Callers should hold write and read
 * locks. This is done in {@link HStore}.
 * </p>
 */
@InterfaceAudience.Private
public interface MemStore extends Closeable {

  /**
   * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
   * {@link #clearSnapshot(long)}.
   * @return {@link MemStoreSnapshot}
   */
  MemStoreSnapshot snapshot();

  /**
   * Clears the current snapshot of the Memstore.
   * @see #snapshot()
   */
  void clearSnapshot(long id) throws UnexpectedStateException;

  /**
   * Flush will first clear out the data in snapshot if any (It will take a second flush invocation
   * to clear the current Cell set). If snapshot is empty, current Cell set will be flushed.
   * @return On flush, how much memory we will clear.
   */
  MemStoreSize getFlushableSize();

  /**
   * Return the size of the snapshot(s) if any
   * @return size of the memstore snapshot
   */
  MemStoreSize getSnapshotSize();

  /**
   * Write an update
   * @param memstoreSizing The delta in memstore size will be passed back via this. This will
   *                       include both data size and heap overhead delta.
   */
  void add(final ExtendedCell cell, MemStoreSizing memstoreSizing);

  /**
   * Write the updates
   * @param memstoreSizing The delta in memstore size will be passed back via this. This will
   *                       include both data size and heap overhead delta.
   */
  void add(Iterable<ExtendedCell> cells, MemStoreSizing memstoreSizing);

  /** Returns Oldest timestamp of all the Cells in the MemStore */
  long timeOfOldestEdit();

  /**
   * @return scanner over the memstore. This might include scanner over the snapshot when one is
   *         present.
   */
  List<KeyValueScanner> getScanners(long readPt) throws IOException;

  /**
   * @return Total memory occupied by this MemStore. This won't include any size occupied by the
   *         snapshot. We assume the snapshot will get cleared soon. This is not thread safe and the
   *         memstore may be changed while computing its size. It is the responsibility of the
   *         caller to make sure this doesn't happen.
   */
  MemStoreSize size();

  /**
   * This method is called before the flush is executed.
   * @return an estimation (lower bound) of the unflushed sequence id in memstore after the flush is
   *         executed. if memstore will be cleared returns {@code HConstants.NO_SEQNUM}.
   */
  long preFlushSeqIDEstimation();

  /* Return true if the memstore may use some extra memory space */
  boolean isSloppy();

  /**
   * This message intends to inform the MemStore that next coming updates are going to be part of
   * the replaying edits from WAL
   */
  default void startReplayingFromWAL() {
    return;
  }

  /**
   * This message intends to inform the MemStore that the replaying edits from WAL are done
   */
  default void stopReplayingFromWAL() {
    return;
  }

  /**
   * Close the memstore.
   * <p>
   * Usually this should only be called when there is nothing in the memstore, unless we are going
   * to abort ourselves.
   * <p>
   * For normal cases, this method is only used to fix the reference counting, see HBASE-27941.
   */
  @Override
  void close();
}
