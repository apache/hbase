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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALSplitter.MutationReplay;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Regions store data for a certain region of a table.  It stores all columns
 * for each row. A given table consists of one or more Regions.
 *
 * <p>An Region is defined by its table and its key extent.
 *
 * <p>Locking at the Region level serves only one purpose: preventing the
 * region from being closed (and consequently split) while other operations
 * are ongoing. Each row level operation obtains both a row lock and a region
 * read lock for the duration of the operation. While a scanner is being
 * constructed, getScanner holds a read lock. If the scanner is successfully
 * constructed, it holds a read lock until it is closed. A close takes out a
 * write lock and consequently will block for ongoing operations and will block
 * new operations from starting while the close is in progress.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Region extends ConfigurationObserver {

  ///////////////////////////////////////////////////////////////////////////
  // Region state

  /** @return region information for this region */
  HRegionInfo getRegionInfo();

  /** @return table descriptor for this region */
  HTableDescriptor getTableDesc();

  /** @return true if region is available (not closed and not closing) */
  boolean isAvailable();

  /** @return true if region is closed */
  boolean isClosed();

  /** @return True if closing process has started */
  boolean isClosing();

  /** @return True if region is in recovering state */
  boolean isRecovering();

  /** @return True if region is read only */
  boolean isReadOnly();

  /**
   * Return the list of Stores managed by this region
   * <p>Use with caution.  Exposed for use of fixup utilities.
   * @return a list of the Stores managed by this region
   */
  List<Store> getStores();

  /**
   * Return the Store for the given family
   * <p>Use with caution.  Exposed for use of fixup utilities.
   * @return the Store for the given family
   */
  Store getStore(byte[] family);

  /** @return list of store file names for the given families */
  List<String> getStoreFileList(byte [][] columns);

  /**
   * Check the region's underlying store files, open the files that have not
   * been opened yet, and remove the store file readers for store files no
   * longer available.
   * @throws IOException
   */
  boolean refreshStoreFiles() throws IOException;

  /** @return the latest sequence number that was read from storage when this region was opened */
  long getOpenSeqNum();

  /** @return the max sequence id of flushed data on this region; no edit in memory will have
   * a sequence id that is less that what is returned here.
   */
  long getMaxFlushedSeqId();

  /** @return the oldest flushed sequence id for the given family; can be beyond
   * {@link #getMaxFlushedSeqId()} in case where we've flushed a subset of a regions column
   * families
   * @deprecated Since version 1.2.0. Exposes too much about our internals; shutting it down.
   * Do not use.
   */
  @VisibleForTesting
  @Deprecated
  public long getOldestSeqIdOfStore(byte[] familyName);

  /**
   * This can be used to determine the last time all files of this region were major compacted.
   * @param majorCompactioOnly Only consider HFile that are the result of major compaction
   * @return the timestamp of the oldest HFile for all stores of this region
   */
  long getOldestHfileTs(boolean majorCompactioOnly) throws IOException;

  /**
   * @return map of column family names to max sequence id that was read from storage when this
   * region was opened
   */
  public Map<byte[], Long> getMaxStoreSeqId();

  /** @return true if loading column families on demand by default */
  boolean isLoadingCfsOnDemandDefault();

  /** @return readpoint considering given IsolationLevel */
  long getReadpoint(IsolationLevel isolationLevel);

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

  /**
   * Update the read request count for this region
   * @param i increment
   */
  void updateReadRequestsCount(long i);

  /** @return write request count for this region */
  long getWriteRequestsCount();

  /**
   * Update the write request count for this region
   * @param i increment
   */
  void updateWriteRequestsCount(long i);

  /** @return memstore size for this region, in bytes */
  long getMemstoreSize();

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

  /** @return the MetricsRegion for this region */
  MetricsRegion getMetrics();

  /** @return the block distribution for all Stores managed by this region */
  HDFSBlocksDistribution getHDFSBlocksDistribution();

  ///////////////////////////////////////////////////////////////////////////
  // Locking

  // Region read locks

  /**
   * Operation enum is used in {@link Region#startRegionOperation} to provide context for
   * various checks before any region operation begins.
   */
  enum Operation {
    ANY, GET, PUT, DELETE, SCAN, APPEND, INCREMENT, SPLIT_REGION, MERGE_REGION, BATCH_MUTATE,
    REPLAY_BATCH_MUTATE, COMPACT_REGION, REPLAY_EVENT
  }

  /**
   * This method needs to be called before any public call that reads or
   * modifies data.
   * Acquires a read lock and checks if the region is closing or closed.
   * <p>{@link #closeRegionOperation} MUST then always be called after
   * the operation has completed, whether it succeeded or failed.
   * @throws IOException
   */
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
   * Tries to acquire a lock on the given row.
   * @param waitForLock if true, will block until the lock is available.
   *        Otherwise, just tries to obtain the lock and returns
   *        false if unavailable.
   * @return the row lock if acquired,
   *   null if waitForLock was false and the lock was not acquired
   * @throws IOException if waitForLock was true and the lock could not be acquired after waiting
   */
  RowLock getRowLock(byte[] row, boolean waitForLock) throws IOException;

  /**
   * If the given list of row locks is not null, releases all locks.
   */
  void releaseRowLocks(List<RowLock> rowLocks);

  ///////////////////////////////////////////////////////////////////////////
  // Region operations

  /**
   * Perform one or more append operations on a row.
   * @param append
   * @param nonceGroup
   * @param nonce
   * @return result of the operation
   * @throws IOException
   */
  Result append(Append append, long nonceGroup, long nonce) throws IOException;

  /**
   * Perform a batch of mutations.
   * <p>
   * Note this supports only Put and Delete mutations and will ignore other types passed.
   * @param mutations the list of mutations
   * @param nonceGroup
   * @param nonce
   * @return an array of OperationStatus which internally contains the
   *         OperationStatusCode and the exceptionMessage if any.
   * @throws IOException
   */
  OperationStatus[] batchMutate(Mutation[] mutations, long nonceGroup, long nonce)
      throws IOException;

  /**
   * Replay a batch of mutations.
   * @param mutations mutations to replay.
   * @param replaySeqId
   * @return an array of OperationStatus which internally contains the
   *         OperationStatusCode and the exceptionMessage if any.
   * @throws IOException
   */
   OperationStatus[] batchReplay(MutationReplay[] mutations, long replaySeqId) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected val
   * If it does, it performs the row mutations.  If the passed value is null, t
   * is for the lack of column (ie: non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp the comparison operator
   * @param comparator
   * @param mutation
   * @param writeToWAL
   * @return true if mutation was applied, false otherwise
   * @throws IOException
   */
  boolean checkAndMutate(byte [] row, byte [] family, byte [] qualifier, CompareOp compareOp,
      ByteArrayComparable comparator, Mutation mutation, boolean writeToWAL) throws IOException;

  /**
   * Atomically checks if a row/family/qualifier value matches the expected val
   * If it does, it performs the row mutations.  If the passed value is null, t
   * is for the lack of column (ie: non-existence)
   * @param row to check
   * @param family column family to check
   * @param qualifier column qualifier to check
   * @param compareOp the comparison operator
   * @param comparator
   * @param mutations
   * @param writeToWAL
   * @return true if mutation was applied, false otherwise
   * @throws IOException
   */
  boolean checkAndRowMutate(byte [] row, byte [] family, byte [] qualifier, CompareOp compareOp,
      ByteArrayComparable comparator, RowMutations mutations, boolean writeToWAL)
      throws IOException;

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
   * Return all the data for the row that matches <i>row</i> exactly,
   * or the one that immediately preceeds it, at or immediately before
   * <i>ts</i>.
   * @param row
   * @param family
   * @return result of the operation
   * @throws IOException
   */
  Result getClosestRowBefore(byte[] row, byte[] family) throws IOException;

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

  /** The comparator to be used with the region */
  CellComparator getCellCompartor();

  /**
   * Perform one or more increment operations on a row.
   * @param increment
   * @param nonceGroup
   * @param nonce
   * @return result of the operation
   * @throws IOException
   */
  Result increment(Increment increment, long nonceGroup, long nonce) throws IOException;

  /**
   * Performs multiple mutations atomically on a single row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param mutations object that specifies the set of mutations to perform atomically
   * @throws IOException
   */
  void mutateRow(RowMutations mutations) throws IOException;

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
  void mutateRowsWithLocks(Collection<Mutation> mutations, Collection<byte[]> rowsToLock,
      long nonceGroup, long nonce) throws IOException;

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   */
  void processRowsWithLocks(RowProcessor<?,?> processor) throws IOException;

  /**
   * Performs atomic multiple reads and writes on a given row.
   *
   * @param processor The object defines the reads and writes to a row.
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce Optional nonce of the operation (unique random id to ensure "more idempotence")
   */
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
   */
  void processRowsWithLocks(RowProcessor<?,?> processor, long timeout, long nonceGroup, long nonce)
      throws IOException;

  /**
   * Puts some data in the table.
   * @param put
   * @throws IOException
   */
  void put(Put put) throws IOException;

  /**
   * Listener class to enable callers of
   * bulkLoadHFile() to perform any necessary
   * pre/post processing of a given bulkload call
   */
  interface BulkLoadListener {

    /**
     * Called before an HFile is actually loaded
     * @param family family being loaded to
     * @param srcPath path of HFile
     * @return final path to be used for actual loading
     * @throws IOException
     */
    String prepareBulkLoad(byte[] family, String srcPath) throws IOException;

    /**
     * Called after a successful HFile load
     * @param family family being loaded to
     * @param srcPath path of HFile
     * @throws IOException
     */
    void doneBulkLoad(byte[] family, String srcPath) throws IOException;

    /**
     * Called after a failed HFile load
     * @param family family being loaded to
     * @param srcPath path of HFile
     * @throws IOException
     */
    void failedBulkLoad(byte[] family, String srcPath) throws IOException;
  }

  /**
   * Attempts to atomically load a group of hfiles.  This is critical for loading
   * rows with multiple column families atomically.
   *
   * @param familyPaths List of Pair&lt;byte[] column family, String hfilePath&gt;
   * @param bulkLoadListener Internal hooks enabling massaging/preparation of a
   * file about to be bulk loaded
   * @param assignSeqId
   * @return true if successful, false if failed recoverably
   * @throws IOException if failed unrecoverably.
   */
  boolean bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths, boolean assignSeqId,
      BulkLoadListener bulkLoadListener) throws IOException;

  ///////////////////////////////////////////////////////////////////////////
  // Coprocessors

  /** @return the coprocessor host */
  RegionCoprocessorHost getCoprocessorHost();

  /**
   * Executes a single protocol buffer coprocessor endpoint {@link Service} method using
   * the registered protocol handlers.  {@link Service} implementations must be registered via the
   * {@link Region#registerService(com.google.protobuf.Service)}
   * method before they are available.
   *
   * @param controller an {@code RpcContoller} implementation to pass to the invoked service
   * @param call a {@code CoprocessorServiceCall} instance identifying the service, method,
   *     and parameters for the method invocation
   * @return a protocol buffer {@code Message} instance containing the method's result
   * @throws IOException if no registered service handler is found or an error
   *     occurs during the invocation
   * @see org.apache.hadoop.hbase.regionserver.Region#registerService(com.google.protobuf.Service)
   */
  Message execService(RpcController controller, CoprocessorServiceCall call) throws IOException;

  /**
   * Registers a new protocol buffer {@link Service} subclass as a coprocessor endpoint to
   * be available for handling
   * {@link Region#execService(com.google.protobuf.RpcController,
   *    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall)}} calls.
   *
   * <p>
   * Only a single instance may be registered per region for a given {@link Service} subclass (the
   * instances are keyed on {@link com.google.protobuf.Descriptors.ServiceDescriptor#getFullName()}.
   * After the first registration, subsequent calls with the same service name will fail with
   * a return value of {@code false}.
   * </p>
   * @param instance the {@code Service} subclass instance to expose as a coprocessor endpoint
   * @return {@code true} if the registration was successful, {@code false}
   * otherwise
   */
  boolean registerService(Service instance);

  ///////////////////////////////////////////////////////////////////////////
  // RowMutation processor support

  /**
   * Check the collection of families for validity.
   * @param families
   * @throws NoSuchColumnFamilyException
   */
  void checkFamilies(Collection<byte[]> families) throws NoSuchColumnFamilyException;

  /**
   * Check the collection of families for valid timestamps
   * @param familyMap
   * @param now current timestamp
   * @throws FailedSanityCheckException
   */
  void checkTimestamps(Map<byte[], List<Cell>> familyMap, long now)
      throws FailedSanityCheckException;

  /**
   * Prepare a delete for a row mutation processor
   * @param delete The passed delete is modified by this method. WARNING!
   * @throws IOException
   */
  void prepareDelete(Delete delete) throws IOException;

  /**
   * Set up correct timestamps in the KVs in Delete object.
   * <p>Caller should have the row and region locks.
   * @param mutation
   * @param familyCellMap
   * @param now
   * @throws IOException
   */
  void prepareDeleteTimestamps(Mutation mutation, Map<byte[], List<Cell>> familyCellMap,
      byte[] now) throws IOException;

  /**
   * Replace any cell timestamps set to {@link org.apache.hadoop.hbase.HConstants#LATEST_TIMESTAMP}
   * provided current timestamp.
   * @param values
   * @param now
   */
  void updateCellTimestamps(final Iterable<List<Cell>> values, final byte[] now)
      throws IOException;

  ///////////////////////////////////////////////////////////////////////////
  // Flushes, compactions, splits, etc.
  // Wizards only, please

  interface FlushResult {
    enum Result {
      FLUSHED_NO_COMPACTION_NEEDED,
      FLUSHED_COMPACTION_NEEDED,
      // Special case where a flush didn't run because there's nothing in the memstores. Used when
      // bulk loading to know when we can still load even if a flush didn't happen.
      CANNOT_FLUSH_MEMSTORE_EMPTY,
      CANNOT_FLUSH
    }

    /** @return the detailed result code */
    Result getResult();

    /** @return true if the memstores were flushed, else false */
    boolean isFlushSucceeded();

    /** @return True if the flush requested a compaction, else false */
    boolean isCompactionNeeded();
  }

  /**
   * Flush the cache.
   *
   * <p>When this method is called the cache will be flushed unless:
   * <ol>
   *   <li>the cache is empty</li>
   *   <li>the region is closed.</li>
   *   <li>a flush is already in progress</li>
   *   <li>writes are disabled</li>
   * </ol>
   *
   * <p>This method may block for some time, so it should not be called from a
   * time-sensitive thread.
   * @param force whether we want to force a flush of all stores
   * @return FlushResult indicating whether the flush was successful or not and if
   * the region needs compacting
   *
   * @throws IOException general io exceptions
   * because a snapshot was not properly persisted.
   * @throws DroppedSnapshotException Thrown when abort is required. The caller MUST catch this
   * exception and MUST abort. Any further operation to the region may cause data loss.
   */
  FlushResult flush(boolean force) throws IOException;

  /**
   * Synchronously compact all stores in the region.
   * <p>This operation could block for a long time, so don't call it from a
   * time-sensitive thread.
   * <p>Note that no locks are taken to prevent possible conflicts between
   * compaction and splitting activities. The regionserver does not normally compact
   * and split in parallel. However by calling this method you may introduce
   * unexpected and unhandled concurrency. Don't do this unless you know what
   * you are doing.
   *
   * @param majorCompaction True to force a major compaction regardless of thresholds
   * @throws IOException
   */
  void compact(final boolean majorCompaction) throws IOException;

  /**
   * Trigger major compaction on all stores in the region.
   * <p>
   * Compaction will be performed asynchronously to this call by the RegionServer's
   * CompactSplitThread. See also {@link Store#triggerMajorCompaction()}
   * @throws IOException
   */
  void triggerMajorCompaction() throws IOException;

  /**
   * @return if a given region is in compaction now.
   */
  CompactionState getCompactionState();

  /** Wait for all current flushes and compactions of the region to complete */
  void waitForFlushesAndCompactions();

}
