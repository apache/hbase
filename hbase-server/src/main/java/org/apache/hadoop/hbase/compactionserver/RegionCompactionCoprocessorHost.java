/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.RawCellBuilderFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SharedConnection;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorService;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

/**
 * 1.Inherited from {@link RegionCoprocessorHost}, only be used on CompactionServer. This host only
 * load coprocessor involves compaction.
 * 2.Other methods of the host, like preFlush,postFlush,prePut, postPut, etc will be not supported.
 * 3.Four methods: preOpen, postOpen, preClose, postClose are overridden as blank implementations.
 * 4.The methods preCompactSelection, postCompactSelection, preCompactScannerOpen, preCompact,
 * postCompact, preStoreFileReaderOpen, postStoreFileReaderOpen, postInstantiateDeleteTracker
 * will be retained.
 */
@InterfaceAudience.Private
public class RegionCompactionCoprocessorHost extends RegionCoprocessorHost {
  private static final Logger LOG = LoggerFactory.getLogger(RegionCompactionCoprocessorHost.class);
  // postCompact will be executed on HRegionServer, so we don't check here
  private static final Set<String> compactionCoprocessor = ImmutableSet.of("preCompactSelection",
    "postCompactSelection", "preCompactScannerOpen", "preCompact", "preStoreFileReaderOpen",
    "postStoreFileReaderOpen", "postInstantiateDeleteTracker");

  private boolean IsCompactionRelatedCoprocessor(Class<?> cpClass) {
    while (cpClass != null) {
      for (Method method : cpClass.getDeclaredMethods()) {
        if (compactionCoprocessor.contains(method.getName())) {
          return true;
        }
      }
      cpClass = cpClass.getSuperclass();
    }
    return false;
  }

  /**
   * The environment will only be used on compactionServer for NotCoreCoprocessor, and the method
   * getRegion, getOnlineRegions, getSharedData will not be supported.
   */
  private static class RegionCompactionEnvironment extends RegionCoprocessorHost.RegionEnvironment {

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public RegionCompactionEnvironment(final RegionCoprocessor impl, final int priority,
        final int seq, final Configuration conf, final Region region,
        final RegionCoprocessorService services) {
      super(impl, priority, seq, conf, region, services, null);
    }

    /** @return the region */
    @Override
    public Region getRegion() {
      throw new UnsupportedOperationException(
          "Method getRegion is not supported when loaded CP on compactionServer");
    }

    @Override
    public OnlineRegions getOnlineRegions() {
      throw new UnsupportedOperationException(
        "Method getOnlineRegions is not supported when loaded CP on compactionServer");
    }

    @Override
    public Connection getConnection() {
      // Mocks may have services as null at test time.
      return services != null ? new SharedConnection(services.getConnection()) : null;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return services != null ? this.services.createConnection(conf) : null;
    }

    @Override
    public ServerName getServerName() {
      return services != null? services.getServerName(): null;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
      throw new UnsupportedOperationException(
        "Method getSharedData is not supported when loaded CP on compactionServer");
    }

    @Override
    public RegionInfo getRegionInfo() {
      return region.getRegionInfo();
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
    }

    @Override
    public RawCellBuilder getCellBuilder() {
      // We always do a DEEP_COPY only
      return RawCellBuilderFactory.create();
    }
  }

  /**
   * Constructor
   * @param region the region
   * @param coprocessorService interface to available RegionServer/CompactionServer functionality
   * @param conf the configuration
   */
  public RegionCompactionCoprocessorHost(final HRegion region,
      final RegionCoprocessorService coprocessorService, final Configuration conf) {
    super(region, coprocessorService, conf);
  }

  @Override
  public RegionCompactionEnvironment createEnvironment(RegionCoprocessor instance, int priority,
      int seq, Configuration conf) {
    if (instance.getClass().isAnnotationPresent(CoreCoprocessor.class)) {
      LOG.info("skip load core coprocessor {} on CompactionServer", instance.getClass().getName());
      return null;
    }
    if (!IsCompactionRelatedCoprocessor(instance.getClass())) {
      LOG.info("skip load compaction no-related coprocessor {} on CompactionServer",
        instance.getClass().getName());
      return null;
    }
    // If coprocessor exposes any services, register them.
    for (Service service : instance.getServices()) {
      region.registerService(service);
    }
    return new RegionCompactionEnvironment(instance, priority, seq, conf, region, rsServices);
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Observer operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Invoked before a region open.
   * For invoke compact, we will open fake region on compaction server.
   * So implement preOpen as blank
   *
   */
  public void preOpen() throws IOException {
  }

  /**
   * Invoked after a region open
   * For invoke compact, we will open fake region on compaction server.
   * So implement postOpen as blank
   */
  public void postOpen() {
  }

  /**
   * Invoked before a region is closed
   * For invoke compact, we will open fake region on compaction server.
   * So implement preClose as blank
   */
  public void preClose(final boolean abortRequested) throws IOException {
  }

  /**
   * Invoked after a region is closed
   * For invoke compact, we will open fake region on compaction server.
   * So implement postClose as blank
   */
  public void postClose(final boolean abortRequested) {
  }

  /**
   * Invoked before create StoreScanner for flush.
   */
  public ScanInfo preFlushScannerOpen(HStore store, FlushLifeCycleTracker tracker)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method preFlushScannerOpen is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked before a memstore flush
   * @return Scanner to use (cannot be null!)
   * @throws IOException
   */
  public InternalScanner preFlush(HStore store, InternalScanner scanner,
      FlushLifeCycleTracker tracker) throws IOException {
    throw new UnsupportedOperationException(
      "Method preFlush is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked before a memstore flush
   * @throws IOException
   */
  public void preFlush(FlushLifeCycleTracker tracker) throws IOException {
    throw new UnsupportedOperationException(
      "Method preFlush is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked after a memstore flush
   * @throws IOException
   */
  public void postFlush(FlushLifeCycleTracker tracker) throws IOException {
    throw new UnsupportedOperationException(
      "Method postFlush is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked before in memory compaction.
   */
  public void preMemStoreCompaction(HStore store) throws IOException {
    throw new UnsupportedOperationException(
      "Method preMemStoreCompaction is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked before create StoreScanner for in memory compaction.
   */
  public ScanInfo preMemStoreCompactionCompactScannerOpen(HStore store) throws IOException {
    throw new UnsupportedOperationException("Method preMemStoreCompactionCompactScannerOpen "
        + "is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked before compacting memstore.
   */
  public InternalScanner preMemStoreCompactionCompact(HStore store, InternalScanner scanner)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method preMemStoreCompactionCompact is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked after in memory compaction.
   */
  public void postMemStoreCompaction(HStore store) throws IOException {
    throw new UnsupportedOperationException(
      "Method postMemStoreCompaction is not supported when loaded CP on compactionServer");
  }

  /**
   * Invoked after a memstore flush
   * @throws IOException
   */
  public void postFlush(HStore store, HStoreFile storeFile, FlushLifeCycleTracker tracker)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method postFlush is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param get the Get request
   * @param results What to return if return is true/'bypass'.
   * @return true if default processing should be bypassed.
   * @exception IOException Exception
   */
  public boolean preGet(final Get get, final List<Cell> results) throws IOException {
    throw new UnsupportedOperationException(
      "Method preGet is not supported when loaded CP on compactionServer");
  }

  /**
   * @param get the Get request
   * @param results the result set
   * @exception IOException Exception
   */
  public void postGet(final Get get, final List<Cell> results) throws IOException {
    throw new UnsupportedOperationException(
        "Method postGet is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param get the Get request
   * @return true or false to return to client if bypassing normal operation, or null otherwise
   * @exception IOException Exception
   */
  public Boolean preExists(final Get get) throws IOException {
    throw new UnsupportedOperationException(
        "Method preExists is not supported when loaded CP on compactionServer");
  }

  /**
   * @param get the Get request
   * @param result the result returned by the region server
   * @return the result to return to the client
   * @exception IOException Exception
   */
  public boolean postExists(final Get get, boolean result)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method postExists is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param put The Put object
   * @param edit The WALEdit object.
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(final Put put, final WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
      "Method prePut is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param mutation - the current mutation
   * @param kv - the current cell
   * @param byteNow - current timestamp in bytes
   * @param get - the get that could be used
   * Note that the get only does not specify the family and qualifier that should be used
   * @return true if default processing should be bypassed
   * @deprecated In hbase-2.0.0. Will be removed in hbase-3.0.0. Added explicitly for a single
   * Coprocessor for its needs only. Will be removed.
   */
  @Deprecated
  public boolean prePrepareTimeStampForDeleteVersion(final Mutation mutation, final Cell kv,
      final byte[] byteNow, final Get get) throws IOException {
    throw new UnsupportedOperationException("Method prePrepareTimeStampForDeleteVersion is "
        + "not supported when loaded CP on compactionServer");
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @exception IOException Exception
   */
  public void postPut(final Put put, final WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
        "Method postPut is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preDelete(final Delete delete, final WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
      "Method preDelete is not supported when loaded CP on compactionServer");
  }

  /**
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @exception IOException Exception
   */
  public void postDelete(final Delete delete, final WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
      "Method postDelete is not supported when loaded CP on compactionServer");
  }

  public void preBatchMutate(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    throw new UnsupportedOperationException(
      "Method preBatchMutate is not supported when loaded CP on compactionServer");
  }

  public void postBatchMutate(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    throw new UnsupportedOperationException(
      "Method postBatchMutate is not supported when loaded CP on compactionServer");
  }

  public void postBatchMutateIndispensably(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method postBatchMutateIndispensably is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param checkAndMutate the CheckAndMutate object
   * @return true or false to return to client if default processing should be bypassed, or null
   *   otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult preCheckAndMutate(CheckAndMutate checkAndMutate)
    throws IOException {
    throw new UnsupportedOperationException(
      "Method preCheckAndMutate is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param checkAndMutate the CheckAndMutate object
   * @return true or false to return to client if default processing should be bypassed, or null
   *   otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult preCheckAndMutateAfterRowLock(CheckAndMutate checkAndMutate)
    throws IOException {
    throw new UnsupportedOperationException(
      "Method preCheckAndMutateAfterRowLock is not supported when loaded CP on compactionServer");
  }

  /**
   * @param checkAndMutate the CheckAndMutate object
   * @param result the result returned by the checkAndMutate
   * @return true or false to return to client if default processing should be bypassed, or null
   *   otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult postCheckAndMutate(CheckAndMutate checkAndMutate,
    CheckAndMutateResult result) throws IOException {
    throw new UnsupportedOperationException(
      "Method postCheckAndMutate is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param append append object
   * @param edit The WALEdit object.
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppend(final Append append, final WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
      "Method preAppend is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param append append object
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppendAfterRowLock(final Append append) throws IOException {
    throw new UnsupportedOperationException(
      "Method preAppendAfterRowLock is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param increment increment object
   * @param edit The WALEdit object.
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrement(final Increment increment, final WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
      "Method preIncrement is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param increment increment object
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrementAfterRowLock(final Increment increment) throws IOException {
    throw new UnsupportedOperationException(
      "Method preIncrementAfterRowLock is not supported when loaded CP on compactionServer");
  }

  /**
   * @param append Append object
   * @param result the result returned by the append
   * @param edit The WALEdit object.
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postAppend(final Append append, final Result result, final WALEdit edit)
    throws IOException {
    throw new UnsupportedOperationException(
      "Method postAppend is not supported when loaded CP on compactionServer");
  }

  /**
   * @param increment increment object
   * @param result the result returned by postIncrement
   * @param edit The WALEdit object.
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postIncrement(final Increment increment, Result result, final WALEdit edit)
    throws IOException {
    throw new UnsupportedOperationException(
      "Method postIncrement is not supported when loaded CP on compactionServer");
  }

  /**
   * @param scan the Scan specification
   * @exception IOException Exception
   */
  public void preScannerOpen(final Scan scan) throws IOException {
    throw new UnsupportedOperationException(
      "Method preScannerOpen is not supported when loaded CP on compactionServer");
  }

  /**
   * @param scan the Scan specification
   * @param s the scanner
   * @return the scanner instance to use
   * @exception IOException Exception
   */
  public RegionScanner postScannerOpen(final Scan scan, RegionScanner s) throws IOException {
    throw new UnsupportedOperationException(
        "Method postScannerOpen is not supported when loaded CP on compactionServer");
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @return 'has next' indication to client if bypassing default behavior, or null otherwise
   * @exception IOException Exception
   */
  public Boolean preScannerNext(final InternalScanner s,
      final List<Result> results, final int limit) throws IOException {
    throw new UnsupportedOperationException(
      "Method preScannerNext is not supported when loaded CP on compactionServer");
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @param hasMore
   * @return 'has more' indication to give to client
   * @exception IOException Exception
   */
  public boolean postScannerNext(final InternalScanner s,
      final List<Result> results, final int limit, boolean hasMore)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method postScannerNext is not supported when loaded CP on compactionServer");
  }

  /**
   * This will be called by the scan flow when the current scanned row is being filtered out by the
   * filter.
   * @param s the scanner
   * @param curRowCell The cell in the current row which got filtered out
   * @return whether more rows are available for the scanner or not
   * @throws IOException
   */
  public boolean postScannerFilterRow(final InternalScanner s, final Cell curRowCell)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method postScannerFilterRow is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param s the scanner
   * @return true if default behavior should be bypassed, false otherwise
   * @exception IOException Exception
   */
  public boolean preScannerClose(final InternalScanner s) throws IOException {
    throw new UnsupportedOperationException(
      "Method preScannerClose is not supported when loaded CP on compactionServer");
  }

  /**
   * @exception IOException Exception
   */
  public void postScannerClose(final InternalScanner s) throws IOException {
    throw new UnsupportedOperationException(
      "Method postScannerClose is not supported when loaded CP on compactionServer");
  }

  /**
   * Called before open store scanner for user scan.
   */
  public ScanInfo preStoreScannerOpen(HStore store, Scan scan) throws IOException {
    throw new UnsupportedOperationException(
      "Method postScannerClose is not supported when loaded CP on compactionServer");
  }

  /**
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   */
  public void preReplayWALs(final RegionInfo info, final Path edits) throws IOException {
    throw new UnsupportedOperationException(
      "Method preReplayWALs is not supported when loaded CP on compactionServer");
  }

  /**
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   * @throws IOException Exception
   */
  public void postReplayWALs(final RegionInfo info, final Path edits) throws IOException {
    throw new UnsupportedOperationException(
      "Method postReplayWALs is not supported when loaded CP on compactionServer");
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @return true if default behavior should be bypassed, false otherwise
   * @deprecated Since hbase-2.0.0. No replacement. To be removed in hbase-3.0.0 and replaced
   * with something that doesn't expose IntefaceAudience.Private classes.
   */
  @Deprecated
  public boolean preWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method preWALRestore is not supported when loaded CP on compactionServer");
  }

  /**
   * @deprecated Since hbase-2.0.0. No replacement. To be removed in hbase-3.0.0 and replaced
   * with something that doesn't expose IntefaceAudience.Private classes.
   */
  @Deprecated
  public void postWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method postWALRestore is not supported when loaded CP on compactionServer");
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   */
  public void preBulkLoadHFile(final List<Pair<byte[], String>> familyPaths) throws IOException {
    throw new UnsupportedOperationException(
      "Method preBulkLoadHFile is not supported when loaded CP on compactionServer");
  }

  public boolean preCommitStoreFile(final byte[] family, final List<Pair<Path, Path>> pairs)
      throws IOException {
    throw new UnsupportedOperationException(
      "Method preCommitStoreFile is not supported when loaded CP on compactionServer");
  }

  public void postCommitStoreFile(final byte[] family, Path srcPath, Path dstPath) throws IOException {
    throw new UnsupportedOperationException(
      "Method postCommitStoreFile is not supported when loaded CP on compactionServer");
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   * @param map Map of CF to List of file paths for the final loaded files
   * @throws IOException
   */
  public void postBulkLoadHFile(final List<Pair<byte[], String>> familyPaths,
      Map<byte[], List<Path>> map) throws IOException {
    throw new UnsupportedOperationException(
      "Method postBulkLoadHFile is not supported when loaded CP on compactionServer");
  }

  public void postStartRegionOperation(final Operation op) throws IOException {
    throw new UnsupportedOperationException(
      "Method postStartRegionOperation is not supported when loaded CP on compactionServer");
  }

  public void postCloseRegionOperation(final Operation op) throws IOException {
    throw new UnsupportedOperationException(
      "Method postCloseRegionOperation is not supported when loaded CP on compactionServer");
  }

  public List<Pair<Cell, Cell>> postIncrementBeforeWAL(final Mutation mutation,
      final List<Pair<Cell, Cell>> cellPairs) throws IOException {
    throw new UnsupportedOperationException(
      "Method postIncrementBeforeWAL is not supported when loaded CP on compactionServer");
  }

  public List<Pair<Cell, Cell>> postAppendBeforeWAL(final Mutation mutation,
      final List<Pair<Cell, Cell>> cellPairs) throws IOException {
    throw new UnsupportedOperationException(
      "Method postAppendBeforeWAL is not supported when loaded CP on compactionServer");
  }

  public void preWALAppend(WALKey key, WALEdit edit) throws IOException {
    throw new UnsupportedOperationException(
      "Method preWALAppend is not supported when loaded CP on compactionServer");
  }

  public Message preEndpointInvocation(final Service service, final String methodName,
      Message request) throws IOException {
    throw new UnsupportedOperationException(
      "Method preEndpointInvocation is not supported when loaded CP on compactionServer");
  }

  public void postEndpointInvocation(final Service service, final String methodName,
      final Message request, final Message.Builder responseBuilder) throws IOException {
    throw new UnsupportedOperationException(
      "Method postEndpointInvocation is not supported when loaded CP on compactionServer");
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // BulkLoadObserver hooks
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public void prePrepareBulkLoad(User user) throws IOException {
    throw new UnsupportedOperationException(
      "Method prePrepareBulkLoad is not supported when loaded CP on compactionServer");
  }

  public void preCleanupBulkLoad(User user) throws IOException {
    throw new UnsupportedOperationException(
      "Method preCleanupBulkLoad is not supported when loaded CP on compactionServer");
  }
}
