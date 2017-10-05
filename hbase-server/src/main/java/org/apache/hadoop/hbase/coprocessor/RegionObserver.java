/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Coprocessors implement this interface to observe and mediate client actions on the region.
 * <p>
 * Since most implementations will be interested in only a subset of hooks, this class uses
 * 'default' functions to avoid having to add unnecessary overrides. When the functions are
 * non-empty, it's simply to satisfy the compiler by returning value of expected (non-void) type. It
 * is done in a way that these default definitions act as no-op. So our suggestion to implementation
 * would be to not call these 'default' methods from overrides.
 * <p>
 * <h3>Exception Handling</h3><br>
 * For all functions, exception handling is done as follows:
 * <ul>
 * <li>Exceptions of type {@link IOException} are reported back to client.</li>
 * <li>For any other kind of exception:
 * <ul>
 * <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then the
 * server aborts.</li>
 * <li>Otherwise, coprocessor is removed from the server and
 * {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 * </ul>
 * </li>
 * </ul>
 * <p>
 * <h3>For Split Related Hooks</h3> <br>
 * In hbase2/AMv2, master runs splits, so the split related hooks are moved to
 * {@link MasterObserver}.
 * <p>
 * <h3>Increment Column Value</h3><br>
 * We do not call this hook anymore.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
// TODO as method signatures need to break, update to
// ObserverContext<? extends RegionCoprocessorEnvironment>
// so we can use additional environment state that isn't exposed to coprocessors.
public interface RegionObserver {
  /** Mutation type for postMutationBeforeWAL hook */
  enum MutationType {
    APPEND, INCREMENT
  }

  /**
   * Called before the region is reported as open to the master.
   * @param c the environment provided by the region server
   */
  default void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {}

  /**
   * Called after the region is reported as open to the master.
   * @param c the environment provided by the region server
   */
  default void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {}

  /**
   * Called after the log replay on the region is over.
   * @param c the environment provided by the region server
   */
  default void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> c) {}

  /**
   * Called before a memstore is flushed to disk and prior to creating the scanner to read from
   * the memstore.  To override or modify how a memstore is flushed,
   * implementing classes can return a new scanner to provide the KeyValues to be
   * stored into the new {@code StoreFile} or null to perform the default processing.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param c the environment provided by the region server
   * @param store the store being flushed
   * @param scanners the scanners for the memstore that is flushed
   * @param s the base scanner, if not {@code null}, from previous RegionObserver in the chain
   * @param readPoint the readpoint to create scanner
   * @return the scanner to use during the flush.  {@code null} if the default implementation
   * is to be used.
   */
  default InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<KeyValueScanner> scanners, InternalScanner s, long readPoint)
      throws IOException {
    return s;
  }

  /**
   * Called before the memstore is flushed to disk.
   * @param c the environment provided by the region server
   */
  default void preFlush(final ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {}

  /**
   * Called before a Store's memstore is flushed to disk.
   * @param c the environment provided by the region server
   * @param store the store where compaction is being requested
   * @param scanner the scanner over existing data used in the store file
   * @return the scanner to use during compaction.  Should not be {@code null}
   * unless the implementation is writing new store files on its own.
   */
  default InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner) throws IOException {
    return scanner;
  }

  /**
   * Called after the memstore is flushed to disk.
   * @param c the environment provided by the region server
   * @throws IOException if an error occurred on the coprocessor
   */
  default void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {}

  /**
   * Called after a Store's memstore is flushed to disk.
   * @param c the environment provided by the region server
   * @param store the store being flushed
   * @param resultFile the new store file written out during compaction
   */
  default void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      StoreFile resultFile) throws IOException {}

  /**
   * Called prior to selecting the {@link StoreFile StoreFiles} to compact from the list of
   * available candidates. To alter the files used for compaction, you may mutate the passed in list
   * of candidates.
   * @param c the environment provided by the region server
   * @param store the store where compaction is being requested
   * @param candidates the store files currently available for compaction
   * @param tracker tracker used to track the life cycle of a compaction
   */
  default void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker)
      throws IOException {}

  /**
   * Called after the {@link StoreFile}s to compact have been selected from the available
   * candidates.
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param selected the store files selected to compact
   * @param tracker tracker used to track the life cycle of a compaction
   * @param request the requested compaction
   */
  default void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      ImmutableList<? extends StoreFile> selected, CompactionLifeCycleTracker tracker,
      CompactionRequest request) {}

  /**
   * Called prior to writing the {@link StoreFile}s selected for compaction into a new
   * {@code StoreFile}. To override or modify the compaction process, implementing classes have two
   * options:
   * <ul>
   * <li>Wrap the provided {@link InternalScanner} with a custom implementation that is returned
   * from this method. The custom scanner can then inspect
   *  {@link org.apache.hadoop.hbase.KeyValue}s from the wrapped scanner, applying its own
   *   policy to what gets written.</li>
   * <li>Call {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} and provide a
   * custom implementation for writing of new {@link StoreFile}s. <strong>Note: any implementations
   * bypassing core compaction using this approach must write out new store files themselves or the
   * existing data will no longer be available after compaction.</strong></li>
   * </ul>
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param scanner the scanner over existing data used in the store file rewriting
   * @param scanType type of Scan
   * @param tracker tracker used to track the life cycle of a compaction
   * @param request the requested compaction
   * @return the scanner to use during compaction. Should not be {@code null} unless the
   *         implementation is writing new store files on its own.
   */
  default InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
      CompactionRequest request)
      throws IOException {
    return scanner;
  }

  /**
   * Called prior to writing the {@link StoreFile}s selected for compaction into a new
   * {@code StoreFile} and prior to creating the scanner used to read the input files. To override
   * or modify the compaction process, implementing classes can return a new scanner to provide the
   * KeyValues to be stored into the new {@code StoreFile} or null to perform the default
   * processing. Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param scanners the list of store file scanners to be read from
   * @param scanType the {@link ScanType} indicating whether this is a major or minor compaction
   * @param earliestPutTs timestamp of the earliest put that was found in any of the involved store
   *          files
   * @param s the base scanner, if not {@code null}, from previous RegionObserver in the chain
   * @param tracker used to track the life cycle of a compaction
   * @param request the requested compaction
   * @param readPoint the readpoint to create scanner
   * @return the scanner to use during compaction. {@code null} if the default implementation is to
   *          be used.
   */
  default InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s, CompactionLifeCycleTracker tracker, CompactionRequest request,
      long readPoint) throws IOException {
    return s;
  }

  /**
   * Called after compaction has completed and the new store file has been moved in to place.
   * @param c the environment provided by the region server
   * @param store the store being compacted
   * @param resultFile the new store file written out during compaction
   * @param tracker used to track the life cycle of a compaction
   * @param request the requested compaction
   */
  default void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request)
      throws IOException {}

  /**
   * Called before the region is reported as closed to the master.
   * @param c the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  default void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
      throws IOException {}

  /**
   * Called after the region is reported as closed to the master.
   * @param c the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  default void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {}

  /**
   * Called before the client performs a Get
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   */
  default void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result)
      throws IOException {}

  /**
   * Called after the client performs a Get
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'result' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param result the result to return to the client, modify as necessary
   */
  default void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {}

  /**
   * Called before the client tests for existence using a Get.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the value to return to the client if bypassing default processing
   */
  default boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
      boolean exists) throws IOException {
    return exists;
  }

  /**
   * Called after the client tests for existence using a Get.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   */
  default boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
      boolean exists) throws IOException {
    return exists;
  }

  /**
   * Called before the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'put' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param put The Put object
   * @param edit The WALEdit object that will be written to the wal
   * @param durability Persistence guarantee for this Put
   */
  default void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      Durability durability) throws IOException {}

  /**
   * Called after the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'put' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param put The Put object
   * @param edit The WALEdit object for the wal
   * @param durability Persistence guarantee for this Put
   */
  default void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      Durability durability) throws IOException {}

  /**
   * Called before the client deletes a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'delete' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param delete The Delete object
   * @param edit The WALEdit object for the wal
   * @param durability Persistence guarantee for this Delete
   */
  default void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
      WALEdit edit, Durability durability) throws IOException {}

  /**
   * Called before the server updates the timestamp for version delete with latest timestamp.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained coprocessors
   * @param c the environment provided by the region server
   * @param mutation - the parent mutation associated with this delete cell
   * @param cell - The deleteColumn with latest version cell
   * @param byteNow - timestamp bytes
   * @param get - the get formed using the current cell's row. Note that the get does not specify
   *          the family and qualifier
   */
  default void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> c,
      Mutation mutation, Cell cell, byte[] byteNow, Get get) throws IOException {}

  /**
   * Called after the client deletes a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'delete' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param delete The Delete object
   * @param edit The WALEdit object for the wal
   * @param durability Persistence guarantee for this Delete
   */
  default void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
      WALEdit edit, Durability durability) throws IOException {}

  /**
   * This will be called for every batch mutation operation happening at the server. This will be
   * called after acquiring the locks on the mutating rows and after applying the proper timestamp
   * for each Mutation at the server. The batch may contain Put/Delete. By setting OperationStatus
   * of Mutations ({@link MiniBatchOperationInProgress#setOperationStatus(int, OperationStatus)}),
   * {@link RegionObserver} can make Region to skip these Mutations.
   * <p>
   * Note: Do not retain references to any Cells in Mutations beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param miniBatchOp batch of Mutations getting applied to region.
   */
  default void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {}

  /**
   * This will be called after applying a batch of Mutations on a region. The Mutations are added to
   * memstore and WAL. The difference of this one with
   * {@link #postPut(ObserverContext, Put, WALEdit, Durability) }
   * and {@link #postDelete(ObserverContext, Delete, WALEdit, Durability) } is
   * this hook will be executed before the mvcc transaction completion.
   * <p>
   * Note: Do not retain references to any Cells in Mutations beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param miniBatchOp batch of Mutations applied to region.
   */
  default void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {}

  /**
   * This will be called for region operations where read lock is acquired in
   * {@link Region#startRegionOperation()}.
   * @param ctx
   * @param operation The operation is about to be taken on the region
   */
  default void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Operation operation) throws IOException {}

  /**
   * Called after releasing read lock in {@link Region#closeRegionOperation()}.
   * @param ctx
   * @param operation
   */
  default void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Operation operation) throws IOException {}

  /**
   * Called after the completion of batch put/delete and will be called even if the batch operation
   * fails.
   * <p>
   * Note: Do not retain references to any Cells in Mutations beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param ctx
   * @param miniBatchOp
   * @param success true if batch operation is successful otherwise false.
   */
  default void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx,
      MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {}

  /**
   * Called before checkAndPut.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'put' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param op the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @param result
   * @return the return value to return to client if bypassing default
   * processing
   */
  default boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put,
      boolean result) throws IOException {
    return result;
  }

  /**
   * Called before checkAndPut but after acquiring rowlock.
   * <p>
   * <b>Note:</b> Caution to be taken for not doing any long time operation in this hook.
   * Row will be locked for longer time. Trying to acquire lock on another row, within this,
   * can lead to potential deadlock.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'put' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param op the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @param result
   * @return the return value to return to client if bypassing default
   * processing
   */
  default boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      ByteArrayComparable comparator, Put put, boolean result) throws IOException {
    return result;
  }

  /**
   * Called after checkAndPut
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'put' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param op the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @param result from the checkAndPut
   * @return the possibly transformed return value to return to client
   */
  default boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put,
      boolean result) throws IOException {
    return result;
  }

  /**
   * Called before checkAndDelete.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'delete' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param op the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @param result
   * @return the value to return to client if bypassing default processing
   */
  default boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
      Delete delete, boolean result) throws IOException {
    return result;
  }

  /**
   * Called before checkAndDelete but after acquiring rowock.
   * <p>
   * <b>Note:</b> Caution to be taken for not doing any long time operation in this hook.
   * Row will be locked for longer time. Trying to acquire lock on another row, within this,
   * can lead to potential deadlock.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'delete' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param op the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @param result
   * @return the value to return to client if bypassing default processing
   */
  default boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
    return result;
  }

  /**
   * Called after checkAndDelete
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'delete' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param op the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @param result from the CheckAndDelete
   * @return the possibly transformed returned value to return to client
   */
  default boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
      Delete delete, boolean result) throws IOException {
    return result;
  }

  /**
   * Called before Append.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'append' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param append Append object
   * @return result to return to the client if bypassing default processing
   */
  default Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    return null;
  }

  /**
   * Called before Append but after acquiring rowlock.
   * <p>
   * <b>Note:</b> Caution to be taken for not doing any long time operation in this hook.
   * Row will be locked for longer time. Trying to acquire lock on another row, within this,
   * can lead to potential deadlock.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'append' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param append Append object
   * @return result to return to the client if bypassing default processing
   */
  default Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
      Append append) throws IOException {
    return null;
  }

  /**
   * Called after Append
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'append' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param append Append object
   * @param result the result returned by increment
   * @return the result to return to the client
   */
  default Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append,
      Result result) throws IOException {
    return result;
  }

  /**
   * Called before Increment.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'increment' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param increment increment object
   * @return result to return to the client if bypassing default processing
   */
  default Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
      throws IOException {
    return null;
  }

  /**
   * Called before Increment but after acquiring rowlock.
   * <p>
   * <b>Note:</b> Caution to be taken for not doing any long time operation in this hook.
   * Row will be locked for longer time. Trying to acquire lock on another row, within this,
   * can lead to potential deadlock.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'increment' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   *
   * @param c
   *          the environment provided by the region server
   * @param increment
   *          increment object
   * @return result to return to the client if bypassing default processing
   *           if an error occurred on the coprocessor
   */
  default Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
      Increment increment) throws IOException {
    return null;
  }

  /**
   * Called after increment
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells in 'increment' beyond the life of this invocation.
   * If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param increment increment object
   * @param result the result returned by increment
   * @return the result to return to the client
   */
  default Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment,
      Result result) throws IOException {
    return result;
  }

  /**
   * Called before the client opens a new scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells returned by scanner, beyond the life of this
   * invocation. If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param scan the Scan specification
   * @param s if not null, the base scanner
   * @return an RegionScanner instance to use instead of the base scanner if
   * overriding default behavior, null otherwise
   */
  default RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
      RegionScanner s) throws IOException {
    return s;
  }

  /**
   * Called before a store opens a new scanner.
   * This hook is called when a "user" scanner is opened.
   * <p>
   * See {@link #preFlushScannerOpen(ObserverContext, Store, List, InternalScanner, long)}
   * and {@link #preCompactScannerOpen(ObserverContext, Store, List, ScanType, long,
   * InternalScanner, CompactionLifeCycleTracker, CompactionRequest, long)} to override scanners
   * created for flushes or compactions, resp.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained coprocessors.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * <p>
   * Note: Do not retain references to any Cells returned by scanner, beyond the life of this
   * invocation. If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param store the store being scanned
   * @param scan the Scan specification
   * @param targetCols columns to be used in the scanner
   * @param s the base scanner, if not {@code null}, from previous RegionObserver in the chain
   * @param readPt the read point
   * @return a KeyValueScanner instance to use or {@code null} to use the default implementation
   */
  default KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s, long readPt)
      throws IOException {
    return s;
  }

  /**
   * Called after the client opens a new scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells returned by scanner, beyond the life of this
   * invocation. If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param scan the Scan specification
   * @param s if not null, the base scanner
   * @return the scanner instance to use
   */
  default RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
      RegionScanner s) throws IOException {
    return s;
  }

  /**
   * Called before the client asks for the next row on a scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells returned by scanner, beyond the life of this
   * invocation. If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param s the scanner
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be returned if default processing
   * is not bypassed.
   * @param limit the maximum number of results to return
   * @param hasNext the 'has more' indication
   * @return 'has more' indication that should be sent to client
   */
  default boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s,
      List<Result> result, int limit, boolean hasNext) throws IOException {
    return hasNext;
  }

  /**
   * Called after the client asks for the next row on a scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * <p>
   * Note: Do not retain references to any Cells returned by scanner, beyond the life of this
   * invocation. If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param s the scanner
   * @param result the result to return to the client, can be modified
   * @param limit the maximum number of results to return
   * @param hasNext the 'has more' indication
   * @return 'has more' indication that should be sent to client
   */
  default boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
    return hasNext;
  }

  /**
   * This will be called by the scan flow when the current scanned row is being filtered out by the
   * filter. The filter may be filtering out the row via any of the below scenarios
   * <ol>
   * <li>
   * <code>boolean filterRowKey(byte [] buffer, int offset, int length)</code> returning true</li>
   * <li>
   * <code>boolean filterRow()</code> returning true</li>
   * <li>
   * <code>default void filterRow(List&lt;KeyValue&gt; kvs)</code> removing all the kvs from
   * the passed List</li>
   * </ol>
   * <p>
   * Note: Do not retain references to any Cells returned by scanner, beyond the life of this
   * invocation. If need a Cell reference for later use, copy the cell and use that.
   * @param c the environment provided by the region server
   * @param s the scanner
   * @param curRowCell The cell in the current row which got filtered out
   * @param hasMore the 'has more' indication
   * @return whether more rows are available for the scanner or not
   */
  default boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner s, Cell curRowCell, boolean hasMore) throws IOException {
    return hasMore;
  }

  /**
   * Called before the client closes a scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param c the environment provided by the region server
   * @param s the scanner
   */
  default void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s)
      throws IOException {}

  /**
   * Called after the client closes a scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param ctx the environment provided by the region server
   * @param s the scanner
   */
  default void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> ctx,
      InternalScanner s) throws IOException {}

  /**
   * Called before replaying WALs for this region.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param ctx the environment provided by the region server
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   */
  default void preReplayWALs(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    RegionInfo info, Path edits) throws IOException {}

  /**
   * Called after replaying WALs for this region.
   * @param ctx the environment provided by the region server
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   */
  default void postReplayWALs(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    RegionInfo info, Path edits) throws IOException {}

  /**
   * Called before a {@link WALEdit}
   * replayed for this region.
   * @param ctx the environment provided by the region server
   */
  default void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {}

  /**
   * Called after a {@link WALEdit}
   * replayed for this region.
   * @param ctx the environment provided by the region server
   */
  default void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {}

  /**
   * Called before bulkLoadHFile. Users can create a StoreFile instance to
   * access the contents of a HFile.
   *
   * @param ctx the environment provided by the region server
   * @param familyPaths pairs of { CF, HFile path } submitted for bulk load. Adding
   * or removing from this list will add or remove HFiles to be bulk loaded.
   */
  default void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
    List<Pair<byte[], String>> familyPaths) throws IOException {}

  /**
   * Called before moving bulk loaded hfile to region directory.
   *
   * @param ctx the environment provided by the region server
   * @param family column family
   * @param pairs List of pairs of { HFile location in staging dir, HFile path in region dir }
   * Each pair are for the same hfile.
   */
  default void preCommitStoreFile(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] family,
      List<Pair<Path, Path>> pairs) throws IOException {}

  /**
   * Called after moving bulk loaded hfile to region directory.
   *
   * @param ctx the environment provided by the region server
   * @param family column family
   * @param srcPath Path to file before the move
   * @param dstPath Path to file after the move
   */
  default void postCommitStoreFile(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] family,
      Path srcPath, Path dstPath) throws IOException {}

  /**
   * Called after bulkLoadHFile.
   *
   * @param ctx the environment provided by the region server
   * @param stagingFamilyPaths pairs of { CF, HFile path } submitted for bulk load
   * @param finalPaths Map of CF to List of file paths for the loaded files
   * @param hasLoaded whether the bulkLoad was successful
   * @return the new value of hasLoaded
   */
  default boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> stagingFamilyPaths, Map<byte[], List<Path>> finalPaths,
      boolean hasLoaded) throws IOException {
    return hasLoaded;
  }

  /**
   * Called before creation of Reader for a store file.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   *
   * @param ctx the environment provided by the region server
   * @param fs fileystem to read from
   * @param p path to the file
   * @param in {@link FSDataInputStreamWrapper}
   * @param size Full size of the file
   * @param cacheConf
   * @param r original reference file. This will be not null only when reading a split file.
   * @param reader the base reader, if not {@code null}, from previous RegionObserver in the chain
   * @return a Reader instance to use instead of the base reader if overriding
   * default behavior, null otherwise
   * @deprecated For Phoenix only, StoreFileReader is not a stable interface.
   */
  @Deprecated
  default StoreFileReader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
      FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
      Reference r, StoreFileReader reader) throws IOException {
    return reader;
  }

  /**
   * Called after the creation of Reader for a store file.
   *
   * @param ctx the environment provided by the region server
   * @param fs fileystem to read from
   * @param p path to the file
   * @param in {@link FSDataInputStreamWrapper}
   * @param size Full size of the file
   * @param cacheConf
   * @param r original reference file. This will be not null only when reading a split file.
   * @param reader the base reader instance
   * @return The reader to use
   * @deprecated For Phoenix only, StoreFileReader is not a stable interface.
   */
  @Deprecated
  default StoreFileReader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
      FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
      Reference r, StoreFileReader reader) throws IOException {
    return reader;
  }

  /**
   * Called after a new cell has been created during an increment operation, but before
   * it is committed to the WAL or memstore.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param ctx the environment provided by the region server
   * @param opType the operation type
   * @param mutation the current mutation
   * @param oldCell old cell containing previous value
   * @param newCell the new cell containing the computed value
   * @return the new cell, possibly changed
   */
  default Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
      MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
    return newCell;
  }

  /**
   * Called after the ScanQueryMatcher creates ScanDeleteTracker. Implementing
   * this hook would help in creating customised DeleteTracker and returning
   * the newly created DeleteTracker
   *
   * @param ctx the environment provided by the region server
   * @param delTracker the deleteTracker that is created by the QueryMatcher
   * @return the Delete Tracker
   */
  default DeleteTracker postInstantiateDeleteTracker(
      ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker)
      throws IOException {
    return delTracker;
  }
}
