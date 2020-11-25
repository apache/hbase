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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.ConnectionUtils.calcEstimatedSize;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createScanResultCache;
import static org.apache.hadoop.hbase.client.ConnectionUtils.incRegionCountMetrics;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.ScannerCallable.MoreResults;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.LeaseException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the scanner interface for the HBase client. If there are multiple regions in a table,
 * this scanner will iterate through them all.
 */
@InterfaceAudience.Private
public abstract class ClientScanner extends AbstractClientScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ClientScanner.class);

  protected final Scan scan;
  protected boolean closed = false;
  // Current region scanner is against. Gets cleared if current region goes
  // wonky: e.g. if it splits on us.
  protected HRegionInfo currentRegion = null;
  protected ScannerCallableWithReplicas callable = null;
  protected Queue<Result> cache;
  private final ScanResultCache scanResultCache;
  protected final int caching;
  protected long lastNext;
  // Keep lastResult returned successfully in case we have to reset scanner.
  protected Result lastResult = null;
  protected final long maxScannerResultSize;
  private final ClusterConnection connection;
  protected final TableName tableName;
  protected final int scannerTimeout;
  protected boolean scanMetricsPublished = false;
  protected RpcRetryingCaller<Result[]> caller;
  protected RpcControllerFactory rpcControllerFactory;
  protected Configuration conf;
  // The timeout on the primary. Applicable if there are multiple replicas for a region
  // In that case, we will only wait for this much timeout on the primary before going
  // to the replicas and trying the same scan. Note that the retries will still happen
  // on each replica and the first successful results will be taken. A timeout of 0 is
  // disallowed.
  protected final int primaryOperationTimeout;
  private int retries;
  protected final ExecutorService pool;

  /**
   * Create a new ClientScanner for the specified table Note that the passed {@link Scan}'s start
   * row maybe changed changed.
   * @param conf The {@link Configuration} to use.
   * @param scan {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   * @param connection Connection identifying the cluster
   * @throws IOException
   */
  public ClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
      ClusterConnection connection, RpcRetryingCallerFactory rpcFactory,
      RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Scan table=" + tableName + ", startRow=" + Bytes.toStringBinary(scan.getStartRow()));
    }
    this.scan = scan;
    this.tableName = tableName;
    this.lastNext = System.currentTimeMillis();
    this.connection = connection;
    this.pool = pool;
    this.primaryOperationTimeout = primaryOperationTimeout;
    this.retries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (scan.getMaxResultSize() > 0) {
      this.maxScannerResultSize = scan.getMaxResultSize();
    } else {
      this.maxScannerResultSize = conf.getLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    }
    this.scannerTimeout = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

    // check if application wants to collect scan metrics
    initScanMetrics(scan);

    // Use the caching from the Scan. If not set, use the default cache setting for this table.
    if (this.scan.getCaching() > 0) {
      this.caching = this.scan.getCaching();
    } else {
      this.caching = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
    }

    this.caller = rpcFactory.<Result[]> newCaller();
    this.rpcControllerFactory = controllerFactory;

    this.conf = conf;

    this.scanResultCache = createScanResultCache(scan);
    initCache();
  }

  protected final int getScanReplicaId() {
    return scan.getReplicaId() >= RegionReplicaUtil.DEFAULT_REPLICA_ID ? scan.getReplicaId() :
      RegionReplicaUtil.DEFAULT_REPLICA_ID;
  }

  protected ClusterConnection getConnection() {
    return this.connection;
  }

  protected TableName getTable() {
    return this.tableName;
  }

  protected int getRetries() {
    return this.retries;
  }

  protected int getScannerTimeout() {
    return this.scannerTimeout;
  }

  protected Configuration getConf() {
    return this.conf;
  }

  protected Scan getScan() {
    return scan;
  }

  protected ExecutorService getPool() {
    return pool;
  }

  protected int getPrimaryOperationTimeout() {
    return primaryOperationTimeout;
  }

  protected int getCaching() {
    return caching;
  }

  protected long getTimestamp() {
    return lastNext;
  }

  protected long getMaxResultSize() {
    return maxScannerResultSize;
  }

  private void closeScanner() throws IOException {
    if (this.callable != null) {
      this.callable.setClose();
      call(callable, caller, scannerTimeout, false);
      this.callable = null;
    }
  }

  /**
   * Will be called in moveToNextRegion when currentRegion is null. Abstract because for normal
   * scan, we will start next scan from the endKey of the currentRegion, and for reversed scan, we
   * will start next scan from the startKey of the currentRegion.
   * @return {@code false} if we have reached the stop row. Otherwise {@code true}.
   */
  protected abstract boolean setNewStartKey();

  /**
   * Will be called in moveToNextRegion to create ScannerCallable. Abstract because for reversed
   * scan we need to create a ReversedScannerCallable.
   */
  protected abstract ScannerCallable createScannerCallable();

  /**
   * Close the previous scanner and create a new ScannerCallable for the next scanner.
   * <p>
   * Marked as protected only because TestClientScanner need to override this method.
   * @return false if we should terminate the scan. Otherwise
   */
  protected boolean moveToNextRegion() {
    // Close the previous scanner if it's open
    try {
      closeScanner();
    } catch (IOException e) {
      // not a big deal continue
      if (LOG.isDebugEnabled()) {
        LOG.debug("close scanner for " + currentRegion + " failed", e);
      }
    }
    if (currentRegion != null) {
      if (!setNewStartKey()) {
        return false;
      }
      scan.resetMvccReadPoint();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Finished " + this.currentRegion);
      }
    }
    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug(
        "Advancing internal scanner to startKey at '" + Bytes.toStringBinary(scan.getStartRow()) +
            "', " + (scan.includeStartRow() ? "inclusive" : "exclusive"));
    }
    // clear the current region, we will set a new value to it after the first call of the new
    // callable.
    this.currentRegion = null;
    this.callable =
        new ScannerCallableWithReplicas(getTable(), getConnection(), createScannerCallable(), pool,
            primaryOperationTimeout, scan, getRetries(), scannerTimeout, caching, conf, caller);
    this.callable.setCaching(this.caching);
    incRegionCountMetrics(scanMetrics);
    return true;
  }

  boolean isAnyRPCcancelled() {
    return callable.isAnyRPCcancelled();
  }

  private Result[] call(ScannerCallableWithReplicas callable, RpcRetryingCaller<Result[]> caller,
      int scannerTimeout, boolean updateCurrentRegion) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // callWithoutRetries is at this layer. Within the ScannerCallableWithReplicas,
    // we do a callWithRetries
    Result[] rrs = caller.callWithoutRetries(callable, scannerTimeout);
    if (currentRegion == null && updateCurrentRegion) {
      currentRegion = callable.getHRegionInfo();
    }
    return rrs;
  }

  /**
   * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
   * application or TableInputFormat.Later, we could push it to other systems. We don't use metrics
   * framework because it doesn't support multi-instances of the same metrics on the same machine;
   * for scan/map reduce scenarios, we will have multiple scans running at the same time. By
   * default, scan metrics are disabled; if the application wants to collect them, this behavior can
   * be turned on by calling calling {@link Scan#setScanMetricsEnabled(boolean)}
   */
  protected void writeScanMetrics() {
    if (this.scanMetrics == null || scanMetricsPublished) {
      return;
    }
    // Publish ScanMetrics to the Scan Object.
    // As we have claimed in the comment of Scan.getScanMetrics, this relies on that user will not
    // call ResultScanner.getScanMetrics and reset the ScanMetrics. Otherwise the metrics published
    // to Scan will be messed up.
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA,
      ProtobufUtil.toScanMetrics(scanMetrics, false).toByteArray());
    scanMetricsPublished = true;
  }

  protected void initSyncCache() {
    cache = new ArrayDeque<>();
  }

  protected Result nextWithSyncCache() throws IOException {
    Result result = cache.poll();
    if (result != null) {
      return result;
    }
    // If there is nothing left in the cache and the scanner is closed,
    // return a no-op
    if (this.closed) {
      return null;
    }

    loadCache();

    // try again to load from cache
    result = cache.poll();

    // if we exhausted this scanner before calling close, write out the scan metrics
    if (result == null) {
      writeScanMetrics();
    }
    return result;
  }

  public int getCacheSize() {
    return cache != null ? cache.size() : 0;
  }

  private boolean scanExhausted(Result[] values) {
    return callable.moreResultsForScan() == MoreResults.NO;
  }

  private boolean regionExhausted(Result[] values) {
    // 1. Not a heartbeat message and we get nothing, this means the region is exhausted. And in the
    // old time we always return empty result for a open scanner operation so we add a check here to
    // keep compatible with the old logic. Should remove the isOpenScanner in the future.
    // 2. Server tells us that it has no more results for this region.
    return (values.length == 0 && !callable.isHeartbeatMessage()) ||
        callable.moreResultsInRegion() == MoreResults.NO;
  }

  private void closeScannerIfExhausted(boolean exhausted) throws IOException {
    if (exhausted) {
      closeScanner();
    }
  }

  private void handleScanError(DoNotRetryIOException e,
      MutableBoolean retryAfterOutOfOrderException, int retriesLeft) throws DoNotRetryIOException {
    // An exception was thrown which makes any partial results that we were collecting
    // invalid. The scanner will need to be reset to the beginning of a row.
    scanResultCache.clear();

    // Unfortunately, DNRIOE is used in two different semantics.
    // (1) The first is to close the client scanner and bubble up the exception all the way
    // to the application. This is preferred when the exception is really un-recoverable
    // (like CorruptHFileException, etc). Plain DoNotRetryIOException also falls into this
    // bucket usually.
    // (2) Second semantics is to close the current region scanner only, but continue the
    // client scanner by overriding the exception. This is usually UnknownScannerException,
    // OutOfOrderScannerNextException, etc where the region scanner has to be closed, but the
    // application-level ClientScanner has to continue without bubbling up the exception to
    // the client. See RSRpcServices to see how it throws DNRIOE's.
    // See also: HBASE-16604, HBASE-17187

    // If exception is any but the list below throw it back to the client; else setup
    // the scanner and retry.
    Throwable cause = e.getCause();
    if ((cause != null && cause instanceof NotServingRegionException) ||
        (cause != null && cause instanceof RegionServerStoppedException) ||
        e instanceof OutOfOrderScannerNextException || e instanceof UnknownScannerException ||
        e instanceof ScannerResetException || e instanceof LeaseException) {
      // Pass. It is easier writing the if loop test as list of what is allowed rather than
      // as a list of what is not allowed... so if in here, it means we do not throw.
      if (retriesLeft <= 0) {
        throw e; // no more retries
      }
    } else {
      throw e;
    }

    // Else, its signal from depths of ScannerCallable that we need to reset the scanner.
    if (this.lastResult != null) {
      // The region has moved. We need to open a brand new scanner at the new location.
      // Reset the startRow to the row we've seen last so that the new scanner starts at
      // the correct row. Otherwise we may see previously returned rows again.
      // If the lastRow is not partial, then we should start from the next row. As now we can
      // exclude the start row, the logic here is the same for both normal scan and reversed scan.
      // If lastResult is partial then include it, otherwise exclude it.
      scan.withStartRow(lastResult.getRow(), lastResult.mayHaveMoreCellsInRow());
    }
    if (e instanceof OutOfOrderScannerNextException) {
      if (retryAfterOutOfOrderException.isTrue()) {
        retryAfterOutOfOrderException.setValue(false);
      } else {
        // TODO: Why wrap this in a DNRIOE when it already is a DNRIOE?
        throw new DoNotRetryIOException(
            "Failed after retry of OutOfOrderScannerNextException: was there a rpc timeout?", e);
      }
    }
    // Clear region.
    this.currentRegion = null;
    // Set this to zero so we don't try and do an rpc and close on remote server when
    // the exception we got was UnknownScanner or the Server is going down.
    callable = null;
  }

  /**
   * Contact the servers to load more {@link Result}s in the cache.
   */
  protected void loadCache() throws IOException {
    // check if scanner was closed during previous prefetch
    if (closed) {
      return;
    }
    long remainingResultSize = maxScannerResultSize;
    int countdown = this.caching;
    // This is possible if we just stopped at the boundary of a region in the previous call.
    if (callable == null && !moveToNextRegion()) {
      closed = true;
      return;
    }
    // This flag is set when we want to skip the result returned. We do
    // this when we reset scanner because it split under us.
    MutableBoolean retryAfterOutOfOrderException = new MutableBoolean(true);
    // Even if we are retrying due to UnknownScannerException, ScannerResetException, etc. we should
    // make sure that we are not retrying indefinitely.
    int retriesLeft = getRetries();
    for (;;) {
      Result[] values;
      try {
        // Server returns a null values if scanning is to stop. Else,
        // returns an empty array if scanning is to go on and we've just
        // exhausted current region.
        // now we will also fetch data when openScanner, so do not make a next call again if values
        // is already non-null.
        values = call(callable, caller, scannerTimeout, true);
        // When the replica switch happens, we need to do certain operations again.
        // The callable will openScanner with the right startkey but we need to pick up
        // from there. Bypass the rest of the loop and let the catch-up happen in the beginning
        // of the loop as it happens for the cases where we see exceptions.
        if (callable.switchedToADifferentReplica()) {
          // Any accumulated partial results are no longer valid since the callable will
          // openScanner with the correct startkey and we must pick up from there
          scanResultCache.clear();
          this.currentRegion = callable.getHRegionInfo();
        }
        retryAfterOutOfOrderException.setValue(true);
      } catch (DoNotRetryIOException e) {
        handleScanError(e, retryAfterOutOfOrderException, retriesLeft--);
        // reopen the scanner
        if (!moveToNextRegion()) {
          break;
        }
        continue;
      }
      long currentTime = System.currentTimeMillis();
      if (this.scanMetrics != null) {
        this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime - lastNext);
      }
      lastNext = currentTime;
      // Groom the array of Results that we received back from the server before adding that
      // Results to the scanner's cache. If partial results are not allowed to be seen by the
      // caller, all book keeping will be performed within this method.
      int numberOfCompleteRowsBefore = scanResultCache.numberOfCompleteRows();
      Result[] resultsToAddToCache =
          scanResultCache.addAndGet(values, callable.isHeartbeatMessage());
      int numberOfCompleteRows =
          scanResultCache.numberOfCompleteRows() - numberOfCompleteRowsBefore;
      for (Result rs : resultsToAddToCache) {
        cache.add(rs);
        long estimatedHeapSizeOfResult = calcEstimatedSize(rs);
        countdown--;
        remainingResultSize -= estimatedHeapSizeOfResult;
        addEstimatedSize(estimatedHeapSizeOfResult);
        this.lastResult = rs;
      }

      if (scan.getLimit() > 0) {
        int newLimit = scan.getLimit() - numberOfCompleteRows;
        assert newLimit >= 0;
        scan.setLimit(newLimit);
      }
      if (scan.getLimit() == 0 || scanExhausted(values)) {
        closeScanner();
        closed = true;
        break;
      }
      boolean regionExhausted = regionExhausted(values);
      if (callable.isHeartbeatMessage()) {
        if (!cache.isEmpty()) {
          // Caller of this method just wants a Result. If we see a heartbeat message, it means
          // processing of the scan is taking a long time server side. Rather than continue to
          // loop until a limit (e.g. size or caching) is reached, break out early to avoid causing
          // unnecesary delays to the caller
          LOG.trace("Heartbeat message received and cache contains Results. " +
            "Breaking out of scan loop");
          // we know that the region has not been exhausted yet so just break without calling
          // closeScannerIfExhausted
          break;
        }
      }
      if (cache.isEmpty() && !closed && scan.isNeedCursorResult()) {
        if (callable.isHeartbeatMessage() && callable.getCursor() != null) {
          // Use cursor row key from server
          cache.add(Result.createCursorResult(callable.getCursor()));
          break;
        }
        if (values.length > 0) {
          // It is size limit exceed and we need return the last Result's row.
          // When user setBatch and the scanner is reopened, the server may return Results that
          // user has seen and the last Result can not be seen because the number is not enough.
          // So the row keys of results may not be same, we must use the last one.
          cache.add(Result.createCursorResult(new Cursor(values[values.length - 1].getRow())));
          break;
        }
      }
      if (countdown <= 0) {
        // we have enough result.
        closeScannerIfExhausted(regionExhausted);
        break;
      }
      if (remainingResultSize <= 0) {
        if (!cache.isEmpty()) {
          closeScannerIfExhausted(regionExhausted);
          break;
        } else {
          // we have reached the max result size but we still can not find anything to return to the
          // user. Reset the maxResultSize and try again.
          remainingResultSize = maxScannerResultSize;
        }
      }
      // we are done with the current region
      if (regionExhausted) {
        if (!moveToNextRegion()) {
          closed = true;
          break;
        }
      }
    }
  }

  protected void addEstimatedSize(long estimatedHeapSizeOfResult) {
    return;
  }

  public int getCacheCount() {
    return cache != null ? cache.size() : 0;
  }

  @Override
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    if (callable != null) {
      callable.setClose();
      try {
        call(callable, caller, scannerTimeout, false);
      } catch (UnknownScannerException e) {
        // We used to catch this error, interpret, and rethrow. However, we
        // have since decided that it's not nice for a scanner's close to
        // throw exceptions. Chances are it was just due to lease time out.
        LOG.debug("scanner failed to close", e);
      } catch (IOException e) {
        /* An exception other than UnknownScanner is unexpected. */
        LOG.warn("scanner failed to close.", e);
      }
      callable = null;
    }
    closed = true;
  }

  @Override
  public boolean renewLease() {
    if (callable == null) {
      return false;
    }
    // do not return any rows, do not advance the scanner
    callable.setRenew(true);
    try {
      this.caller.callWithoutRetries(callable, this.scannerTimeout);
      return true;
    } catch (Exception e) {
      LOG.debug("scanner failed to renew lease", e);
      return false;
    } finally {
      callable.setRenew(false);
    }
  }

  protected void initCache() {
    initSyncCache();
  }

  @Override
  public Result next() throws IOException {
    return nextWithSyncCache();
  }
}
