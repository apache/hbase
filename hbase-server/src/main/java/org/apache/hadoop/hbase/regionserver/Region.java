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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Region is a subset of HRegion with operations required for the {@link RegionCoprocessor
 * Coprocessors}. The operations include ability to do mutations, requesting compaction, getting
 * different counters/sizes, locking rows and getting access to {@linkplain Store}s.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Region extends ConfigurationObserver {

  ///////////////////////////////////////////////////////////////////////////
  // Region state

  /** @return region information for this region */
  RegionInfo getRegionInfo();

  /** @return table descriptor for this region */
  TableDescriptor getTableDescriptor();

  /** @return true if region is available (not closed and not closing) */
  boolean isAvailable();

  /** @return true if region is closed */
  boolean isClosed();

  /** @return True if closing process has started */
  boolean isClosing();

  /** @return True if region is read only */
  boolean isReadOnly();

  /** @return true if region is splittable */
  boolean isSplittable();

  /**
   * @return true if region is mergeable
   */
  boolean isMergeable();

  /**
   * Return the list of Stores managed by this region
   * <p>Use with caution.  Exposed for use of fixup utilities.
   * @return a list of the Stores managed by this region
   */
  List<? extends Store> getStores();

  /**
   * Return the Store for the given family
   * <p>Use with caution.  Exposed for use of fixup utilities.
   * @return the Store for the given family
   */
  Store getStore(byte[] family);

  /** @return list of store file names for the given families */
  List<String> getStoreFileList(byte[][] columns);

  /**
   * Check the region's underlying store files, open the files that have not
   * been opened yet, and remove the store file readers for store files no
   * longer available.
   * @throws IOException
   */
  boolean refreshStoreFiles() throws IOException;

  /** @return the max sequence id of flushed data on this region; no edit in memory will have
   * a sequence id that is less that what is returned here.
   */
  long getMaxFlushedSeqId();

  /**
   * This can be used to determine the last time all files of this region were major compacted.
   * @param majorCompactionOnly Only consider HFile that are the result of major compaction
   * @return the timestamp of the oldest HFile for all stores of this region
   */
  long getOldestHfileTs(boolean majorCompactionOnly) throws IOException;

  /**
   * @return map of column family names to max sequence id that was read from storage when this
   * region was opened
   */
  public Map<byte[], Long> getMaxStoreSeqId();

  /**
   * @return The earliest time a store in the region was flushed. All
   *         other stores in the region would have been flushed either at, or
   *         after this time.
   */
  long getEarliestFlushTimeForAllStores();

  ///////////////////////////////////////////////////////////////////////////
  // Metrics

  /** @return read requests count for this region */
  long getReadRequestsCount();

  /** @return filtered read requests count for this region */
  long getFilteredReadRequestsCount();

  /** @return write request count for this region */
  long getWriteRequestsCount();

  /**
   * @return memstore size for this region, in bytes. It just accounts data size of cells added to
   *         the memstores of this Region. Means size in bytes for key, value and tags within Cells.
   *         It wont consider any java heap overhead for the cell objects or any other.
   */
  long getMemStoreDataSize();

  /**
   * @return memstore heap size for this region, in bytes. It accounts data size of cells
   *         added to the memstores of this Region, as well as java heap overhead for the cell
   *         objects or any other.
   */
  long getMemStoreHeapSize();

  /**
   * @return memstore off-heap size for this region, in bytes. It accounts data size of cells
   *         added to the memstores of this Region, as well as overhead for the cell
   *         objects or any other that is allocated off-heap.
   */
  long getMemStoreOffHeapSize();

  /** @return the number of mutations processed bypassing the WAL */
  long getNumMutationsWithoutWAL();

  /** @return the size of data processed bypassing the WAL, in bytes */
  long getDataInMemoryWithoutWAL();

  /** @return the number of blocked requests */
  long getBlockedRequestsCount();

  /** @return the number of checkAndMutate guards that passed */
  long getCheckAndMutateChecksPassed();

  /** @return the number of failed checkAndMutate guards */
  long getCheckAndMutateChecksFailed();

  ///////////////////////////////////////////////////////////////////////////
  // Locking

  // Region read locks

  /**
   * Operation enum is used in {@link Region#startRegionOperation} and elsewhere to provide
   * context for various checks.
   */
  enum Operation {
    ANY, GET, PUT, DELETE, SCAN, APPEND, INCREMENT, SPLIT_REGION, MERGE_REGION, BATCH_MUTATE,
    REPLAY_BATCH_MUTATE, COMPACT_REGION, REPLAY_EVENT, SNAPSHOT, COMPACT_SWITCH,
    CHECK_AND_MUTATE
  }

  /**
   * This method needs to be called before any public call that reads or
   * modifies data.
   * Acquires a read lock and checks if the region is closing or closed.
   * <p>{@link #closeRegionOperation} MUST then always be called after
   * the operation has completed, whether it succeeded or failed.
   * @throws IOException
   */
  // TODO Exposing this and closeRegionOperation() as we have getRowLock() exposed.
  // Remove if we get rid of exposing getRowLock().
  void startRegionOperation() throws IOException;

  /**
   * This method needs to be called before any public call that reads or
   * modifies data.
   * Acquires a read lock and checks if the region is closing or closed.
   * <p>{@link #closeRegionOperation} MUST then always be called after
   * the operation has completed, whether it succeeded or failed.
   * @param op The operation is about to be taken on the region
   * @throws IOException
   */
  void startRegionOperation(Operation op) throws IOException;

  /**
   * Closes the region operation lock.
   * @throws IOException
   */
  void closeRegionOperation() throws IOException;

  /**
   * Closes the region operation lock. This needs to be called in the finally block corresponding
   * to the try block of {@link #startRegionOperation(Operation)}
   * @throws IOException
   */
  void closeRegionOperation(Operation op) throws IOException;

  // Row write locks

  /**
   * Row lock held by a given thread.
   * One thread may acquire multiple locks on the same row simultaneously.
   * The locks must be released by calling release() from the same thread.
   */
  public interface RowLock {
    /**
     * Release the given lock.  If there are no remaining locks held by the current thread
     * then unlock the row and allow other threads to acquire the lock.
     * @throws IllegalArgumentException if called by a different thread than the lock owning
     *     thread
     */
    void release();
  }

  /**
   *
   * Get a row lock for the specified row. All locks are reentrant.
   *
   * Before calling this function make sure that a region operation has already been
   * started (the calling thread has already acquired the region-close-guard lock).
   * <p>
   * The obtained locks should be released after use by {@link RowLock#release()}
   * <p>
   * NOTE: the boolean passed here has changed. It used to be a boolean that
   * stated whether or not to wait on the lock. Now it is whether it an exclusive
   * lock is requested.
   *
   * @param row The row actions will be performed against
   * @param readLock is the lock reader or writer. True indicates that a non-exclusive
   * lock is requested
   * @see #startRegionOperation()
   * @see #startRegionOperation(Operation)
   */
  // TODO this needs to be exposed as we have RowProcessor now. If RowProcessor is removed, we can
  // remove this too..
  RowLock getRowLock(byte[] row, boolean readLock) throws IOException;

  ///////////////////////////////////////////////////////////////////////////
  // Region operations

  /**
   * Perform one or more append operations on a row.
   * @param append
   * @return result of the operation
   * @throws IOException
   */
  Result append(Append append) throws IOException;

  /**
   * Perform a batch of mutations.
   * <p>
   * Please do not operate on a same column of a single row in a batch, we will not consider the
   * previous operation in the same batch when performing the operations in the batch.
   *
   * @param mutations the list of mutations
   * @return an array of OperationStatus which internally contains the
   *         OperationStatusCode and the exceptionMessage if any.
   * @throws IOException
   */
  OperationStatus[] batchMutate(Mutation[] mutations)
      throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value and if it does,
   * it performs the mutation. If the passed value is null, the lack of column value
   * (ie: non-existence) is used. See checkAndRowMutate to do many checkAndPuts at a time on a
   * single row.
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op the comparison operator
   * @param comparator the expected value
   * @param mutation data to put if check succeeds
   * @return true if mutation was applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  default boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier, CompareOperator op,
    ByteArrayComparable comparator, Mutation mutation) throws IOException {
    return checkAndMutate(row, family, qualifier, op, comparator, TimeRange.allTime(), mutation);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value and if it does,
   * it performs the mutation. If the passed value is null, the lack of column value
   * (ie: non-existence) is used. See checkAndRowMutate to do many checkAndPuts at a time on a
   * single row.
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op the comparison operator
   * @param comparator the expected value
   * @param mutation data to put if check succeeds
   * @param timeRange time range to check
   * @return true if mutation was applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier, CompareOperator op,
      ByteArrayComparable comparator, TimeRange timeRange, Mutation mutation) throws IOException;

  /**
   * Atomically checks if a row matches the filter and if it does, it performs the mutation. See
   * checkAndRowMutate to do many checkAndPuts at a time on a single row.
   * @param row to check
   * @param filter the filter
   * @param mutation data to put if check succeeds
   * @return true if mutation was applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  default boolean checkAndMutate(byte [] row, Filter filter, Mutation mutation)
    throws IOException {
    return checkAndMutate(row, filter, TimeRange.allTime(), mutation);
  }

  /**
   * Atomically checks if a row value matches the filter and if it does, it performs the mutation.
   * See checkAndRowMutate to do many checkAndPuts at a time on a single row.
   * @param row to check
   * @param filter the filter
   * @param mutation data to put if check succeeds
   * @param timeRange time range to check
   * @return true if mutation was applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  boolean checkAndMutate(byte [] row, Filter filter, TimeRange timeRange, Mutation mutation)
    throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected values and if it does,
   * it performs the row mutations. If the passed value is null, the lack of column value
   * (ie: non-existence) is used. Use to do many mutations on a single row. Use checkAndMutate
   * to do one checkAndMutate at a time.
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op the comparison operator
   * @param comparator the expected value
   * @param mutations data to put if check succeeds
   * @return true if mutations were applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  default boolean checkAndRowMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
    ByteArrayComparable comparator, RowMutations mutations) throws IOException {
    return checkAndRowMutate(row, family, qualifier, op, comparator, TimeRange.allTime(),
      mutations);
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected values and if it does,
   * it performs the row mutations. If the passed value is null, the lack of column value
   * (ie: non-existence) is used. Use to do many mutations on a single row. Use checkAndMutate
   * to do one checkAndMutate at a time.
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param op the comparison operator
   * @param comparator the expected value
   * @param mutations data to put if check succeeds
   * @param timeRange time range to check
   * @return true if mutations were applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  boolean checkAndRowMutate(byte [] row, byte [] family, byte [] qualifier, CompareOperator op,
      ByteArrayComparable comparator, TimeRange timeRange, RowMutations mutations)
      throws IOException;

  /**
   * Atomically checks if a row matches the filter and if it does, it performs the row mutations.
   * Use to do many mutations on a single row. Use checkAndMutate to do one checkAndMutate at a
   * time.
   * @param row to check
   * @param filter the filter
   * @param mutations data to put if check succeeds
   * @return true if mutations were applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  default boolean checkAndRowMutate(byte[] row, Filter filter, RowMutations mutations)
    throws IOException {
    return checkAndRowMutate(row, filter, TimeRange.allTime(), mutations);
  }

  /**
   * Atomically checks if a row matches the filter and if it does, it performs the row mutations.
   * Use to do many mutations on a single row. Use checkAndMutate to do one checkAndMutate at a
   * time.
   * @param row to check
   * @param filter the filter
   * @param mutations data to put if check succeeds
   * @param timeRange time range to check
   * @return true if mutations were applied, false otherwise
   *
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *   {@link #checkAndMutate(CheckAndMutate)}  instead.
   */
  @Deprecated
  boolean checkAndRowMutate(byte [] row, Filter filter, TimeRange timeRange,
    RowMutations mutations) throws IOException;

  /**
   * Atomically checks if a row matches the conditions and if it does, it performs the actions.
   * Use to do many mutations on a single row. Use checkAndMutate to do one checkAndMutate at a
   * time.
   * @param checkAndMutate the CheckAndMutate object
   * @return true if mutations were applied, false otherwise
   * @throws IOException if an error occurred in this method
   */
  CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException;

  /**
   * Deletes the specified cells/row.
   * @param delete
   * @throws IOException
   */
  void delete(Delete delete) throws IOException;

  /**
   * Do a get based on the get parameter.
   * @param get query parameters
   * @return result of the operation
   */
  Result get(Get get) throws IOException;

  /**
   * Do a get based on the get parameter.
   * @param get query parameters
   * @param withCoprocessor invoke coprocessor or not. We don't want to
   * always invoke cp.
   * @return list of cells resulting from the operation
   */
  List<Cell> get(Get get, boolean withCoprocessor) throws IOException;

  /**
   * Return an iterator that scans over the HRegion, returning the indicated
   * columns and rows specified by the {@link Scan}.
   * <p>
   * This Iterator must be closed by the caller.
   *
   * @param scan configured {@link Scan}
   * @return RegionScanner
   * @throws IOException read exceptions
   */
  RegionScanner getScanner(Scan scan) throws IOException;

  /**
   * Return an iterator that scans over the HRegion, returning the indicated columns and rows
   * specified by the {@link Scan}. The scanner will also include the additional scanners passed
   * along with the scanners for the specified Scan instance. Should be careful with the usage to
   * pass additional scanners only within this Region
   * <p>
   * This Iterator must be closed by the caller.
   *
   * @param scan configured {@link Scan}
   * @param additionalScanners Any additional scanners to be used
   * @return RegionScanner
   * @throws IOException read exceptions
   */
  RegionScanner getScanner(Scan scan, List<KeyValueScanner> additionalScanners) throws IOException;

  /** The comparator to be used with the region */
  CellComparator getCellComparator();

  /**
   * Perform one or more increment operations on a row.
   * @param increment
   * @return result of the operation
   * @throws IOException
   */
  Result increment(Increment increment) throws IOException;

  /**
   * Performs multiple mutations atomically on a single row.
   *
   * @param mutations object that specifies the set of mutations to perform atomically
   * @return results of Increment/Append operations. If no Increment/Append operations, it returns
   *   null
   * @throws IOException
   */
  Result mutateRow(RowMutations mutations) throws IOException;

  /**
   * Perform atomic mutations within the region.
   *
   * @param mutations The list of mutations to perform.
   * <code>mutations</code> can contain operations for multiple rows.
   * Caller has to ensure that all rows are contained in this region.
   * @param rowsToLock Rows to lock
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   * If multiple rows are locked care should be taken that
   * <code>rowsToLock</code> is sorted in order to avoid deadlocks.
   * @throws IOException
   */
  // TODO Should not be exposing with params nonceGroup, nonce. Change when doing the jira for
  // Changing processRowsWithLocks and RowProcessor
  void mutateRowsWithLocks(Collection<Mutation> mutations, Collection<byte[]> rowsToLock,
      long nonceGroup, long nonce) throws IOException;

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. For customization, use
   * Coprocessors instead.
   */
  @Deprecated
  void processRowsWithLocks(RowProcessor<?,?> processor) throws IOException;

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. For customization, use
   * Coprocessors instead.
   */
  // TODO Should not be exposing with params nonceGroup, nonce. Change when doing the jira for
  // Changing processRowsWithLocks and RowProcessor
  @Deprecated
  void processRowsWithLocks(RowProcessor<?,?> processor, long nonceGroup, long nonce)
      throws IOException;

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   * @param timeout The timeout of the processor.process() execution
   *                Use a negative number to switch off the time bound
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. For customization, use
   * Coprocessors instead.
   */
  // TODO Should not be exposing with params nonceGroup, nonce. Change when doing the jira for
  // Changing processRowsWithLocks and RowProcessor
  @Deprecated
  void processRowsWithLocks(RowProcessor<?,?> processor, long timeout, long nonceGroup, long nonce)
      throws IOException;

  /**
   * Puts some data in the table.
   * @param put
   * @throws IOException
   */
  void put(Put put) throws IOException;

  ///////////////////////////////////////////////////////////////////////////
  // Flushes, compactions, splits, etc.
  // Wizards only, please

  /**
   * @return if a given region is in compaction now.
   */
  CompactionState getCompactionState();

  /**
   * Request compaction on this region.
   */
  void requestCompaction(String why, int priority, boolean major,
      CompactionLifeCycleTracker tracker) throws IOException;

  /**
   * Request compaction for the given family
   */
  void requestCompaction(byte[] family, String why, int priority, boolean major,
      CompactionLifeCycleTracker tracker) throws IOException;

  /**
   * Request flush on this region.
   */
  void requestFlush(FlushLifeCycleTracker tracker) throws IOException;

  /**
   * Wait for all current flushes of the region to complete
   *
   * @param timeout The maximum time to wait in milliseconds.
   * @return False when timeout elapsed but flushes are not over. True when flushes are over within
   * max wait time period.
   */
  boolean waitForFlushes(long timeout);

  /**
   * @return a read only configuration of this region; throws {@link UnsupportedOperationException}
   *         if you try to set a configuration.
   */
  Configuration getReadOnlyConfiguration();
}
