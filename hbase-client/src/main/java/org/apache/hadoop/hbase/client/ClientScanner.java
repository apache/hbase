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
import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowAfter;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowBefore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ScannerCallable.MoreResults;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implements the scanner interface for the HBase client. If there are multiple regions in a table,
 * this scanner will iterate through them all.
 */
@InterfaceAudience.Private
public abstract class ClientScanner extends AbstractClientScanner {

  private static final Log LOG = LogFactory.getLog(ClientScanner.class);

  protected Scan scan;
  protected boolean closed = false;
  // Current region scanner is against. Gets cleared if current region goes
  // wonky: e.g. if it splits on us.
  protected HRegionInfo currentRegion = null;
  protected ScannerCallableWithReplicas callable = null;
  protected Queue<Result> cache;
  /**
   * A list of partial results that have been returned from the server. This list should only
   * contain results if this scanner does not have enough partial results to form the complete
   * result.
   */
  protected final LinkedList<Result> partialResults = new LinkedList<Result>();
  /**
   * The row for which we are accumulating partial Results (i.e. the row of the Results stored
   * inside partialResults). Changes to partialResultsRow and partialResults are kept in sync via
   * the methods {@link #addToPartialResults(Result)} and {@link #clearPartialResults()}
   */
  protected byte[] partialResultsRow = null;
  /**
   * The last cell from a not full Row which is added to cache
   */
  protected Cell lastCellLoadedToCache = null;
  protected final int caching;
  protected long lastNext;
  // Keep lastResult returned successfully in case we have to reset scanner.
  protected Result lastResult = null;
  protected final long maxScannerResultSize;
  private final ClusterConnection connection;
  private final TableName tableName;
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
    this.scannerTimeout =
        HBaseConfiguration.getInt(conf, HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
          HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
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
    initCache();
  }

  protected abstract void initCache();

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

  @VisibleForTesting
  protected long getMaxResultSize() {
    return maxScannerResultSize;
  }

  // returns true if the passed region endKey
  protected boolean checkScanStopRow(final byte[] endKey) {
    if (this.scan.getStopRow().length > 0) {
      // there is a stop row, check to see if we are past it.
      byte[] stopRow = scan.getStopRow();
      int cmp = Bytes.compareTo(stopRow, 0, stopRow.length, endKey, 0, endKey.length);
      if (cmp <= 0) {
        // stopRow <= endKey (endKey is equals to or larger than stopRow)
        // This is a stop.
        return true;
      }
    }
    return false; // unlikely.
  }

  protected final void closeScanner() throws IOException {
    if (this.callable != null) {
      this.callable.setClose();
      call(callable, caller, scannerTimeout);
      this.callable = null;
    }
  }

  /**
   * Gets a scanner for the next region. If this.currentRegion != null, then we will move to the
   * endrow of this.currentRegion. Else we will get scanner at the scan.getStartRow().
   * @param nbRows the caching option of the scan
   * @return the results fetched when open scanner, or null which means terminate the scan.
   */
  protected Result[] nextScanner(int nbRows) throws IOException {
    // Close the previous scanner if it's open
    closeScanner();

    // Where to start the next scanner
    byte[] localStartKey;

    // if we're at end of table, close and return null to stop iterating
    if (this.currentRegion != null) {
      byte[] endKey = this.currentRegion.getEndKey();
      if (endKey == null || Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
          checkScanStopRow(endKey)) {
        close();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Finished " + this.currentRegion);
        }
        return null;
      }
      localStartKey = endKey;
      // clear mvcc read point if we are going to switch regions
      scan.resetMvccReadPoint();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Finished " + this.currentRegion);
      }
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug(
        "Advancing internal scanner to startKey at '" + Bytes.toStringBinary(localStartKey) + "'");
    }
    try {
      callable = getScannerCallable(localStartKey, nbRows);
      // Open a scanner on the region server starting at the
      // beginning of the region
      Result[] rrs = call(callable, caller, scannerTimeout);
      this.currentRegion = callable.getHRegionInfo();
      if (this.scanMetrics != null) {
        this.scanMetrics.countOfRegions.incrementAndGet();
      }
      if (rrs != null && rrs.length == 0 && callable.moreResultsForScan() == MoreResults.NO) {
        // no results for the scan, return null to terminate the scan.
        closed = true;
        callable = null;
        currentRegion = null;
        return null;
      }
      return rrs;
    } catch (IOException e) {
      closeScanner();
      throw e;
    }
  }

  @VisibleForTesting
  boolean isAnyRPCcancelled() {
    return callable.isAnyRPCcancelled();
  }

  private Result[] call(ScannerCallableWithReplicas callable, RpcRetryingCaller<Result[]> caller,
      int scannerTimeout) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // callWithoutRetries is at this layer. Within the ScannerCallableWithReplicas,
    // we do a callWithRetries
    return caller.callWithoutRetries(callable, scannerTimeout);
  }

  @InterfaceAudience.Private
  protected ScannerCallableWithReplicas getScannerCallable(byte[] localStartKey, int nbRows) {
    scan.setStartRow(localStartKey);
    ScannerCallable s = new ScannerCallable(getConnection(), getTable(), scan, this.scanMetrics,
        this.rpcControllerFactory);
    s.setCaching(nbRows);
    ScannerCallableWithReplicas sr = new ScannerCallableWithReplicas(tableName, getConnection(), s,
        pool, primaryOperationTimeout, scan, retries, scannerTimeout, caching, conf, caller);
    return sr;
  }

  /**
   * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
   * application or TableInputFormat.Later, we could push it to other systems. We don't use metrics
   * framework because it doesn't support multi-instances of the same metrics on the same machine;
   * for scan/map reduce scenarios, we will have multiple scans running at the same time. By
   * default, scan metrics are disabled; if the application wants to collect them, this behavior can
   * be turned on by calling calling {@link Scan#setScanMetricsEnabled(boolean)}
   * <p>
   * This invocation clears the scan metrics. Metrics are aggregated in the Scan instance.
   */
  protected void writeScanMetrics() {
    if (this.scanMetrics == null || scanMetricsPublished) {
      return;
    }
    MapReduceProtos.ScanMetrics pScanMetrics = ProtobufUtil.toScanMetrics(scanMetrics);
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA, pScanMetrics.toByteArray());
    scanMetricsPublished = true;
  }

  protected void initSyncCache() {
    cache = new LinkedList<Result>();
  }

  protected Result nextWithSyncCache() throws IOException {
    // If the scanner is closed and there's nothing left in the cache, next is a no-op.
    if (cache.isEmpty() && this.closed) {
      return null;
    }
    if (cache.isEmpty()) {
      loadCache();
    }

    if (cache.size() > 0) {
      return cache.poll();
    }

    // if we exhausted this scanner before calling close, write out the scan metrics
    writeScanMetrics();
    return null;
  }

  @VisibleForTesting
  public int getCacheSize() {
    return cache != null ? cache.size() : 0;
  }

  private boolean scanExhausted(Result[] values) {
    // This means the server tells us the whole scan operation is done. Usually decided by filter or
    // limit.
    return values == null || callable.moreResultsForScan() == MoreResults.NO;
  }

  private boolean regionExhausted(Result[] values) {
    // 1. Not a heartbeat message and we get nothing, this means the region is exhausted. And in the
    // old time we always return empty result for a open scanner operation so we add a check here to
    // keep compatible with the old logic. Should remove the isOpenScanner in the future.
    // 2. Server tells us that it has no more results for this region.
    return (values.length == 0 && !callable.isHeartbeatMessage() && !callable.isOpenScanner())
        || callable.moreResultsInRegion() == MoreResults.NO;
  }

  private void closeScannerIfExhausted(boolean exhausted) throws IOException {
    if (exhausted) {
      if (!partialResults.isEmpty()) {
        // XXX: continue if there are partial results. But in fact server should not set
        // hasMoreResults to false if there are partial results.
        LOG.warn("Server tells us there is no more results for this region but we still have"
            + " partialResults, this should not happen, retry on the current scanner anyway");
      } else {
        closeScanner();
      }
    }
  }

  private Result[] nextScannerWithRetries(int nbRows) throws IOException {
    int retriesLeft = getRetries();
    for (;;) {
      try {
        return nextScanner(nbRows);
      } catch (DoNotRetryIOException e) {
        handleScanError(e, null, retriesLeft--);
      }
    }
  }

  private void handleScanError(DoNotRetryIOException e,
      MutableBoolean retryAfterOutOfOrderException, int retriesLeft) throws DoNotRetryIOException {
    // An exception was thrown which makes any partial results that we were collecting
    // invalid. The scanner will need to be reset to the beginning of a row.
    clearPartialResults();

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
        e instanceof ScannerResetException) {
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
      // (ScannerCallable by now has "relocated" the correct region)
      if (!this.lastResult.isPartial() && scan.getBatch() < 0) {
        if (scan.isReversed()) {
          scan.setStartRow(createClosestRowBefore(lastResult.getRow()));
        } else {
          scan.setStartRow(createClosestRowAfter(lastResult.getRow()));
        }
      } else {
        // we need rescan this row because we only loaded partial row before
        scan.setStartRow(lastResult.getRow());
      }
    }
    if (e instanceof OutOfOrderScannerNextException) {
      if (retryAfterOutOfOrderException != null) {
        if (retryAfterOutOfOrderException.isTrue()) {
          retryAfterOutOfOrderException.setValue(false);
        } else {
          // TODO: Why wrap this in a DNRIOE when it already is a DNRIOE?
          throw new DoNotRetryIOException(
              "Failed after retry of OutOfOrderScannerNextException: was there a rpc timeout?", e);
        }
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
    Result[] values = null;
    long remainingResultSize = maxScannerResultSize;
    int countdown = this.caching;
    // This is possible if we just stopped at the boundary of a region in the previous call.
    if (callable == null) {
      values = nextScannerWithRetries(countdown);
      if (values == null) {
        return;
      }
    }
    // We need to reset it if it's a new callable that was created with a countdown in nextScanner
    callable.setCaching(this.caching);
    // This flag is set when we want to skip the result returned. We do
    // this when we reset scanner because it split under us.
    MutableBoolean retryAfterOutOfOrderException = new MutableBoolean(true);
    // Even if we are retrying due to UnknownScannerException, ScannerResetException, etc. we should
    // make sure that we are not retrying indefinitely.
    int retriesLeft = getRetries();
    for (;;) {
      try {
        // Server returns a null values if scanning is to stop. Else,
        // returns an empty array if scanning is to go on and we've just
        // exhausted current region.
        // now we will also fetch data when openScanner, so do not make a next call again if values
        // is already non-null.
        if (values == null) {
          values = call(callable, caller, scannerTimeout);
        }
        // When the replica switch happens, we need to do certain operations again.
        // The callable will openScanner with the right startkey but we need to pick up
        // from there. Bypass the rest of the loop and let the catch-up happen in the beginning
        // of the loop as it happens for the cases where we see exceptions.
        if (callable.switchedToADifferentReplica()) {
          // Any accumulated partial results are no longer valid since the callable will
          // openScanner with the correct startkey and we must pick up from there
          clearPartialResults();
          this.currentRegion = callable.getHRegionInfo();
          // Now we will also fetch data when openScanner so usually we should not get a null
          // result, but at some places we still use null to indicate the scan is terminated, so add
          // a sanity check here. Should be removed later.
          if (values == null) {
            continue;
          }
        }
        retryAfterOutOfOrderException.setValue(true);
      } catch (DoNotRetryIOException e) {
        handleScanError(e, retryAfterOutOfOrderException, retriesLeft--);
        // reopen the scanner
        values = nextScannerWithRetries(countdown);
        if (values == null) {
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
      List<Result> resultsToAddToCache =
          getResultsToAddToCache(values, callable.isHeartbeatMessage());
      if (!resultsToAddToCache.isEmpty()) {
        for (Result rs : resultsToAddToCache) {
          rs = filterLoadedCell(rs);
          if (rs == null) {
            continue;
          }

          cache.add(rs);
          long estimatedHeapSizeOfResult = calcEstimatedSize(rs);
          countdown--;
          remainingResultSize -= estimatedHeapSizeOfResult;
          addEstimatedSize(estimatedHeapSizeOfResult);
          this.lastResult = rs;
          if (this.lastResult.isPartial() || scan.getBatch() > 0) {
            updateLastCellLoadedToCache(this.lastResult);
          } else {
            this.lastCellLoadedToCache = null;
          }
        }
        if (scan.getLimit() > 0) {
          int limit = scan.getLimit() - resultsToAddToCache.size();
          assert limit >= 0;
          scan.setLimit(limit);
        }
      }
      if (scanExhausted(values)) {
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
          if (LOG.isTraceEnabled()) {
            LOG.trace("Heartbeat message received and cache contains Results."
                + " Breaking out of scan loop");
          }
          // we know that the region has not been exhausted yet so just break without calling
          // closeScannerIfExhausted
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
        if (!partialResults.isEmpty()) {
          // XXX: continue if there are partial results. But in fact server should not set
          // hasMoreResults to false if there are partial results.
          LOG.warn("Server tells us there is no more results for this region but we still have"
              + " partialResults, this should not happen, retry on the current scanner anyway");
          values = null; // reset values for the next call
          continue;
        }
        values = nextScannerWithRetries(countdown);
        if (values == null) {
          break;
        }
      } else {
        values = null; // reset values for the next call
      }
    }
  }

  protected void addEstimatedSize(long estimatedHeapSizeOfResult) {
    return;
  }

  @VisibleForTesting
  public int getCacheCount() {
    return cache != null ? cache.size() : 0;
  }

  /**
   * This method ensures all of our book keeping regarding partial results is kept up to date. This
   * method should be called once we know that the results we received back from the RPC request do
   * not contain errors. We return a list of results that should be added to the cache. In general,
   * this list will contain all NON-partial results from the input array (unless the client has
   * specified that they are okay with receiving partial results)
   * @param resultsFromServer The array of {@link Result}s returned from the server
   * @param heartbeatMessage Flag indicating whether or not the response received from the server
   *          represented a complete response, or a heartbeat message that was sent to keep the
   *          client-server connection alive
   * @return the list of results that should be added to the cache.
   * @throws IOException
   */
  protected List<Result> getResultsToAddToCache(Result[] resultsFromServer,
      boolean heartbeatMessage) throws IOException {
    int resultSize = resultsFromServer != null ? resultsFromServer.length : 0;
    List<Result> resultsToAddToCache = new ArrayList<Result>(resultSize);

    final boolean isBatchSet = scan != null && scan.getBatch() > 0;
    final boolean allowPartials = scan != null && scan.getAllowPartialResults();

    // If the caller has indicated in their scan that they are okay with seeing partial results,
    // then simply add all results to the list. Note that since scan batching also returns results
    // for a row in pieces we treat batch being set as equivalent to allowing partials. The
    // implication of treating batching as equivalent to partial results is that it is possible
    // the caller will receive a result back where the number of cells in the result is less than
    // the batch size even though it may not be the last group of cells for that row.
    if (allowPartials || isBatchSet) {
      addResultsToList(resultsToAddToCache, resultsFromServer, 0,
        (null == resultsFromServer ? 0 : resultsFromServer.length));
      return resultsToAddToCache;
    }

    // If no results were returned it indicates that either we have the all the partial results
    // necessary to construct the complete result or the server had to send a heartbeat message
    // to the client to keep the client-server connection alive
    if (resultsFromServer == null || resultsFromServer.length == 0) {
      // If this response was an empty heartbeat message, then we have not exhausted the region
      // and thus there may be more partials server side that still need to be added to the partial
      // list before we form the complete Result
      if (!partialResults.isEmpty() && !heartbeatMessage) {
        resultsToAddToCache.add(Result.createCompleteResult(partialResults));
        clearPartialResults();
      }

      return resultsToAddToCache;
    }

    // In every RPC response there should be at most a single partial result. Furthermore, if
    // there is a partial result, it is guaranteed to be in the last position of the array.
    Result last = resultsFromServer[resultsFromServer.length - 1];
    Result partial = last.isPartial() ? last : null;

    if (LOG.isTraceEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("number results from RPC: ").append(resultsFromServer.length).append(",");
      sb.append("partial != null: ").append(partial != null).append(",");
      sb.append("number of partials so far: ").append(partialResults.size());
      LOG.trace(sb.toString());
    }

    // There are three possibilities cases that can occur while handling partial results
    //
    // 1. (partial != null && partialResults.isEmpty())
    // This is the first partial result that we have received. It should be added to
    // the list of partialResults and await the next RPC request at which point another
    // portion of the complete result will be received
    //
    // 2. !partialResults.isEmpty()
    // Since our partialResults list is not empty it means that we have been accumulating partial
    // Results for a particular row. We cannot form the complete/whole Result for that row until
    // all partials for the row have been received. Thus we loop through all of the Results
    // returned from the server and determine whether or not all partial Results for the row have
    // been received. We know that we have received all of the partial Results for the row when:
    // i) We notice a row change in the Results
    // ii) We see a Result for the partial row that is NOT marked as a partial Result
    //
    // 3. (partial == null && partialResults.isEmpty())
    // Business as usual. We are not accumulating partial results and there wasn't a partial result
    // in the RPC response. This means that all of the results we received from the server are
    // complete and can be added directly to the cache
    if (partial != null && partialResults.isEmpty()) {
      addToPartialResults(partial);

      // Exclude the last result, it's a partial
      addResultsToList(resultsToAddToCache, resultsFromServer, 0, resultsFromServer.length - 1);
    } else if (!partialResults.isEmpty()) {
      for (int i = 0; i < resultsFromServer.length; i++) {
        Result result = resultsFromServer[i];

        // This result is from the same row as the partial Results. Add it to the list of partials
        // and check if it was the last partial Result for that row
        if (Bytes.equals(partialResultsRow, result.getRow())) {
          addToPartialResults(result);

          // If the result is not a partial, it is a signal to us that it is the last Result we
          // need to form the complete Result client-side
          if (!result.isPartial()) {
            resultsToAddToCache.add(Result.createCompleteResult(partialResults));
            clearPartialResults();
          }
        } else {
          // The row of this result differs from the row of the partial results we have received so
          // far. If our list of partials isn't empty, this is a signal to form the complete Result
          // since the row has now changed
          if (!partialResults.isEmpty()) {
            resultsToAddToCache.add(Result.createCompleteResult(partialResults));
            clearPartialResults();
          }

          // It's possible that in one response from the server we receive the final partial for
          // one row and receive a partial for a different row. Thus, make sure that all Results
          // are added to the proper list
          if (result.isPartial()) {
            addToPartialResults(result);
          } else {
            resultsToAddToCache.add(result);
          }
        }
      }
    } else { // partial == null && partialResults.isEmpty() -- business as usual
      addResultsToList(resultsToAddToCache, resultsFromServer, 0, resultsFromServer.length);
    }

    return resultsToAddToCache;
  }

  /**
   * A convenience method for adding a Result to our list of partials. This method ensure that only
   * Results that belong to the same row as the other partials can be added to the list.
   * @param result The result that we want to add to our list of partial Results
   * @throws IOException
   */
  private void addToPartialResults(final Result result) throws IOException {
    final byte[] row = result.getRow();
    if (partialResultsRow != null && !Bytes.equals(row, partialResultsRow)) {
      throw new IOException("Partial result row does not match. All partial results must come "
          + "from the same row. partialResultsRow: " + Bytes.toString(partialResultsRow) + "row: "
          + Bytes.toString(row));
    }
    partialResultsRow = row;
    partialResults.add(result);
  }

  /**
   * Convenience method for clearing the list of partials and resetting the partialResultsRow.
   */
  private void clearPartialResults() {
    partialResults.clear();
    partialResultsRow = null;
  }

  /**
   * Helper method for adding results between the indices [start, end) to the outputList
   * @param outputList the list that results will be added to
   * @param inputArray the array that results are taken from
   * @param start beginning index (inclusive)
   * @param end ending index (exclusive)
   */
  private void addResultsToList(List<Result> outputList, Result[] inputArray, int start, int end) {
    if (inputArray == null || start < 0 || end > inputArray.length) return;

    for (int i = start; i < end; i++) {
      outputList.add(inputArray[i]);
    }
  }

  @Override
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    if (callable != null) {
      callable.setClose();
      try {
        call(callable, caller, scannerTimeout);
      } catch (UnknownScannerException e) {
        // We used to catch this error, interpret, and rethrow. However, we
        // have since decided that it's not nice for a scanner's close to
        // throw exceptions. Chances are it was just due to lease time out.
        if (LOG.isDebugEnabled()) {
          LOG.debug("scanner failed to close", e);
        }
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
    if (callable != null) {
      // do not return any rows, do not advance the scanner
      callable.setRenew(true);
      try {
        this.caller.callWithoutRetries(callable, this.scannerTimeout);
      } catch (Exception e) {
        return false;
      } finally {
        callable.setRenew(false);
      }
      return true;
    }
    return false;
  }

  protected void updateLastCellLoadedToCache(Result result) {
    if (result.rawCells().length == 0) {
      return;
    }
    this.lastCellLoadedToCache = result.rawCells()[result.rawCells().length - 1];
  }

  /**
   * Compare two Cells considering reversed scanner. ReversedScanner only reverses rows, not
   * columns.
   */
  private int compare(Cell a, Cell b) {
    CellComparator comparator = currentRegion != null && currentRegion.isMetaRegion()
        ? CellComparator.META_COMPARATOR : CellComparator.COMPARATOR;
    int r = comparator.compareRows(a, b);
    if (r != 0) {
      return this.scan.isReversed() ? -r : r;
    }
    return CellComparator.compareWithoutRow(a, b);
  }

  private Result filterLoadedCell(Result result) {
    // we only filter result when last result is partial
    // so lastCellLoadedToCache and result should have same row key.
    // However, if 1) read some cells; 1.1) delete this row at the same time 2) move region;
    // 3) read more cell. lastCellLoadedToCache and result will be not at same row.
    if (lastCellLoadedToCache == null || result.rawCells().length == 0) {
      return result;
    }
    if (compare(this.lastCellLoadedToCache, result.rawCells()[0]) < 0) {
      // The first cell of this result is larger than the last cell of loadcache.
      // If user do not allow partial result, it must be true.
      return result;
    }
    if (compare(this.lastCellLoadedToCache, result.rawCells()[result.rawCells().length - 1]) >= 0) {
      // The last cell of this result is smaller than the last cell of loadcache, skip all.
      return null;
    }

    // The first one must not in filtered result, we start at the second.
    int index = 1;
    while (index < result.rawCells().length) {
      if (compare(this.lastCellLoadedToCache, result.rawCells()[index]) < 0) {
        break;
      }
      index++;
    }
    Cell[] list = Arrays.copyOfRange(result.rawCells(), index, result.rawCells().length);
    return Result.create(list, result.getExists(), result.isStale(), result.isPartial());
  }
}