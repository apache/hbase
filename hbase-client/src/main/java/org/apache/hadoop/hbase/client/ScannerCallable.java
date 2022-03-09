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

import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.net.DNS;

/**
 * Scanner operations such as create, next, etc.
 * Used by {@link ResultScanner}s made by {@link HTable}. Passed to a retrying caller such as
 * {@link RpcRetryingCaller} so fails are retried.
 */
@InterfaceAudience.Private
public class ScannerCallable extends RegionServerCallable<Result[]> {
  public static final String LOG_SCANNER_LATENCY_CUTOFF
    = "hbase.client.log.scanner.latency.cutoff";
  public static final String LOG_SCANNER_ACTIVITY = "hbase.client.log.scanner.activity";

  // Keeping LOG public as it is being used in TestScannerHeartbeatMessages
  public static final Log LOG = LogFactory.getLog(ScannerCallable.class);
  protected long scannerId = -1L;
  protected boolean instantiated = false;
  protected boolean closed = false;
  protected boolean renew = false;
  protected final Scan scan;
  private int caching = 1;
  protected final ClusterConnection cConnection;
  protected ScanMetrics scanMetrics;
  private boolean logScannerActivity = false;
  private int logCutOffLatency = 1000;
  private static String myAddress;
  protected final int id;

  enum MoreResults {
    YES, NO, UNKNOWN
  }

  private MoreResults moreResultsInRegion;
  private MoreResults moreResultsForScan;

  /**
   * Saves whether or not the most recent response from the server was a heartbeat message.
   * Heartbeat messages are identified by the flag {@link ScanResponse#getHeartbeatMessage()}
   */
  protected boolean heartbeatMessage = false;
  static {
    try {
      myAddress = DNS.getDefaultHost("default", "default");
    } catch (UnknownHostException uhe) {
      LOG.error("cannot determine my address", uhe);
    }
  }

  protected Cursor cursor;

  // indicate if it is a remote server call
  protected boolean isRegionServerRemote = true;
  private long nextCallSeq = 0;
  protected RpcControllerFactory controllerFactory;
  protected HBaseRpcController controller;

  /**
   * @param connection which connection
   * @param tableName table callable is on
   * @param scan the scan to execute
   * @param scanMetrics the ScanMetrics to used, if it is null,
   *        ScannerCallable won't collect metrics
   * @param rpcControllerFactory factory to use when creating
   *        {@link com.google.protobuf.RpcController}
   */
  public ScannerCallable (ClusterConnection connection, TableName tableName, Scan scan,
      ScanMetrics scanMetrics, RpcControllerFactory rpcControllerFactory) {
    this(connection, tableName, scan, scanMetrics, rpcControllerFactory, 0);
  }
  /**
   *
   * @param connection
   * @param tableName
   * @param scan
   * @param scanMetrics
   * @param id the replicaId
   */
  public ScannerCallable (ClusterConnection connection, TableName tableName, Scan scan,
      ScanMetrics scanMetrics, RpcControllerFactory rpcControllerFactory, int id) {
    super(connection, tableName, scan.getStartRow(), scan.getPriority());
    this.id = id;
    this.cConnection = connection;
    this.scan = scan;
    this.scanMetrics = scanMetrics;
    Configuration conf = connection.getConfiguration();
    logScannerActivity = conf.getBoolean(LOG_SCANNER_ACTIVITY, false);
    logCutOffLatency = conf.getInt(LOG_SCANNER_LATENCY_CUTOFF, 1000);
    this.controllerFactory = rpcControllerFactory;
    this.controller = rpcControllerFactory.newController();
  }

  HBaseRpcController getController() {
    return controller;
  }

  protected final HRegionLocation getLocationForReplica(RegionLocations locs)
      throws HBaseIOException {
    HRegionLocation loc = id < locs.size() ? locs.getRegionLocation(id) : null;
    if (loc == null || loc.getServerName() == null) {
      // With this exception, there will be a retry. The location can be null for a replica
      // when the table is created or after a split.
      throw new HBaseIOException("There is no location for replica id #" + id);
    }
    return loc;
  }

  /**
   * Fetch region locations for the row. Since this is for prepare, we always useCache.
   * This is because we can be sure that RpcRetryingCaller will have cleared the cache
   * in error handling if this is a retry.
   *
   * @param row the row to look up region location for
   */
  protected final RegionLocations getRegionLocationsForPrepare(byte[] row)
    throws IOException {
    // always use cache, because cache will have been cleared if necessary
    // in the try/catch before retrying
    return RpcRetryingCallerWithReadReplicas.getRegionLocations(true, id, getConnection(),
      getTableName(), row);
  }

  /**
   * @param reload force reload of server location
   * @throws IOException
   */
  @Override
  public void prepare(boolean reload) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }

    if (reload && getTableName() != null && !getTableName().equals(TableName.META_TABLE_NAME)
      && getConnection().isTableDisabled(getTableName())) {
      throw new TableNotEnabledException(getTableName().getNameAsString() + " is disabled.");
    }

    RegionLocations rl = getRegionLocationsForPrepare(getRow());
    location = getLocationForReplica(rl);
    ServerName dest = location.getServerName();
    setStub(super.getConnection().getClient(dest));
    if (!instantiated || reload) {
      checkIfRegionServerIsRemote();
      instantiated = true;
    }
    cursor = null;
    // check how often we retry.
    // HConnectionManager will call instantiateServer with reload==true
    // if and only if for retries.
    if (reload && this.scanMetrics != null) {
      this.scanMetrics.countOfRPCRetries.incrementAndGet();
      if (isRegionServerRemote) {
        this.scanMetrics.countOfRemoteRPCRetries.incrementAndGet();
      }
    }
  }

  /**
   * compare the local machine hostname with region server's hostname
   * to decide if hbase client connects to a remote region server
   */
  protected void checkIfRegionServerIsRemote() {
    if (getLocation().getHostname().equalsIgnoreCase(myAddress)) {
      isRegionServerRemote = false;
    } else {
      isRegionServerRemote = true;
    }
  }

  private ScanResponse next() throws IOException {
    // Reset the heartbeat flag prior to each RPC in case an exception is thrown by the server
    setHeartbeatMessage(false);
    incRPCcallsMetrics();
    ScanRequest request = RequestConverter.buildScanRequest(scannerId, caching, false, nextCallSeq,
      this.scanMetrics != null, renew, scan.getLimit());
    try {
      ScanResponse response = getStub().scan(controller, request);
      nextCallSeq++;
      return response;
    } catch (Exception e) {
      IOException ioe = ProtobufUtil.handleRemoteException(e);
      if (logScannerActivity) {
        LOG.info("Got exception making request " + TextFormat.shortDebugString(request) + " to " +
            getLocation(),
          e);
      }
      if (logScannerActivity) {
        if (ioe instanceof UnknownScannerException) {
          try {
            HRegionLocation location =
                getConnection().relocateRegion(getTableName(), scan.getStartRow());
            LOG.info("Scanner=" + scannerId + " expired, current region location is "
                + location.toString());
          } catch (Throwable t) {
            LOG.info("Failed to relocate region", t);
          }
        } else if (ioe instanceof ScannerResetException) {
          LOG.info("Scanner=" + scannerId + " has received an exception, and the server "
              + "asked us to reset the scanner state.",
            ioe);
        }
      }
      // The below convertion of exceptions into DoNotRetryExceptions is a little strange.
      // Why not just have these exceptions implment DNRIOE you ask? Well, usually we want
      // ServerCallable#withRetries to just retry when it gets these exceptions. In here in
      // a scan when doing a next in particular, we want to break out and get the scanner to
      // reset itself up again. Throwing a DNRIOE is how we signal this to happen (its ugly,
      // yeah and hard to follow and in need of a refactor).
      if (ioe instanceof NotServingRegionException) {
        // Throw a DNRE so that we break out of cycle of calling NSRE
        // when what we need is to open scanner against new location.
        // Attach NSRE to signal client that it needs to re-setup scanner.
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfNSRE.incrementAndGet();
        }
        throw new DoNotRetryIOException("Resetting the scanner -- see exception cause", ioe);
      } else if (ioe instanceof RegionServerStoppedException) {
        // Throw a DNRE so that we break out of cycle of the retries and instead go and
        // open scanner against new location.
        throw new DoNotRetryIOException("Resetting the scanner -- see exception cause", ioe);
      } else {
        // The outer layers will retry
        throw ioe;
      }
    }
  }

  private void setAlreadyClosed() {
    this.scannerId = -1L;
    this.closed = true;
  }

  @Override
  public Result[] call(int callTimeout) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    if (closed) {
      close();
      return null;
    }
    controller.reset();
    controller.setPriority(getTableName());
    controller.setCallTimeout(callTimeout);
    ScanResponse response;
    if (this.scannerId == -1L) {
      response = openScanner();
    } else {
      response = next();
    }
    long timestamp = System.currentTimeMillis();
    boolean isHeartBeat = response.hasHeartbeatMessage() && response.getHeartbeatMessage();
    setHeartbeatMessage(isHeartBeat);
    if (isHeartBeat && scan.isNeedCursorResult() && response.hasCursor()) {
      cursor = ProtobufUtil.toCursor(response.getCursor());
    }
    Result[] rrs = ResponseConverter.getResults(controller.cellScanner(), response);
    if (logScannerActivity) {
      long now = System.currentTimeMillis();
      if (now - timestamp > logCutOffLatency) {
        int rows = rrs == null ? 0 : rrs.length;
        LOG.info("Took " + (now - timestamp) + "ms to fetch " + rows + " rows from scanner="
            + scannerId);
      }
    }
    updateServerSideMetrics(response);
    // moreResults is only used for the case where a filter exhausts all elements
    if (response.hasMoreResults()) {
      if (response.getMoreResults()) {
        setMoreResultsForScan(MoreResults.YES);
      } else {
        setMoreResultsForScan(MoreResults.NO);
        setAlreadyClosed();
      }
    } else {
      setMoreResultsForScan(MoreResults.UNKNOWN);
    }
    if (response.hasMoreResultsInRegion()) {
      if (response.getMoreResultsInRegion()) {
        setMoreResultsInRegion(MoreResults.YES);
      } else {
        setMoreResultsInRegion(MoreResults.NO);
        setAlreadyClosed();
      }
    } else {
      setMoreResultsInRegion(MoreResults.UNKNOWN);
    }
    updateResultsMetrics(rrs);
    return rrs;
  }

  /**
   * @return true when the most recent RPC response indicated that the response was a heartbeat
   *         message. Heartbeat messages are sent back from the server when the processing of the
   *         scan request exceeds a certain time threshold. Heartbeats allow the server to avoid
   *         timeouts during long running scan operations.
   */
  boolean isHeartbeatMessage() {
    return heartbeatMessage;
  }

  public Cursor getCursor() {
    return cursor;
  }

  private void setHeartbeatMessage(boolean heartbeatMessage) {
    this.heartbeatMessage = heartbeatMessage;
  }

  private void incRPCcallsMetrics() {
    if (this.scanMetrics == null) {
      return;
    }
    this.scanMetrics.countOfRPCcalls.incrementAndGet();
    if (isRegionServerRemote) {
      this.scanMetrics.countOfRemoteRPCcalls.incrementAndGet();
    }
  }

  protected void updateResultsMetrics(Result[] rrs) {
    if (this.scanMetrics == null || rrs == null || rrs.length == 0) {
      return;
    }
    long resultSize = 0;
    for (Result rr : rrs) {
      for (Cell cell : rr.rawCells()) {
        resultSize += CellUtil.estimatedSerializedSizeOf(cell);
      }
    }
    this.scanMetrics.countOfBytesInResults.addAndGet(resultSize);
    if (isRegionServerRemote) {
      this.scanMetrics.countOfBytesInRemoteResults.addAndGet(resultSize);
    }
  }

  /**
   * Use the scan metrics returned by the server to add to the identically named counters in the
   * client side metrics. If a counter does not exist with the same name as the server side metric,
   * the attempt to increase the counter will fail.
   * @param response
   */
  private void updateServerSideMetrics(ScanResponse response) {
    if (this.scanMetrics == null || response == null || !response.hasScanMetrics()) return;

    Map<String, Long> serverMetrics = ResponseConverter.getScanMetrics(response);
    for (Entry<String, Long> entry : serverMetrics.entrySet()) {
      this.scanMetrics.addToCounter(entry.getKey(), entry.getValue());
    }
  }

  private void close() {
    if (this.scannerId == -1L) {
      return;
    }
    try {
      incRPCcallsMetrics();
      ScanRequest request =
          RequestConverter.buildScanRequest(this.scannerId, 0, true, this.scanMetrics != null);
      try {
        getStub().scan(controller, request);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
    } catch (IOException e) {
      TableName table = getTableName();
      String tableDetails = (table == null) ? "" : (" on table: " + table.getNameAsString());
      LOG.warn("Ignore, probably already closed. Current scan: " + getScan().toString()
          + tableDetails, e);
    }
    this.scannerId = -1L;
  }

  private ScanResponse openScanner() throws IOException {
    incRPCcallsMetrics();
    ScanRequest request = RequestConverter.buildScanRequest(
      getLocation().getRegionInfo().getRegionName(), this.scan, this.caching, false);
    try {
      ScanResponse response = getStub().scan(controller, request);
      long id = response.getScannerId();
      if (logScannerActivity) {
        LOG.info("Open scanner=" + id + " for scan=" + scan.toString()
          + " on region " + getLocation().toString());
      }
      if (response.hasMvccReadPoint()) {
        this.scan.setMvccReadPoint(response.getMvccReadPoint());
      }
      this.scannerId = id;
      return response;
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  protected Scan getScan() {
    return scan;
  }

  /**
   * Call this when the next invocation of call should close the scanner
   */
  public void setClose() {
    this.closed = true;
  }

  /**
   * Indicate whether we make a call only to renew the lease, but without affected the scanner in
   * any other way.
   * @param val true if only the lease should be renewed
   */
  public void setRenew(boolean val) {
    this.renew = val;
  }

  /**
   * @return the HRegionInfo for the current region
   */
  @Override
  public HRegionInfo getHRegionInfo() {
    if (!instantiated) {
      return null;
    }
    return getLocation().getRegionInfo();
  }

  /**
   * Get the number of rows that will be fetched on next
   * @return the number of rows for caching
   */
  public int getCaching() {
    return caching;
  }

  @Override
  public ClusterConnection getConnection() {
    return cConnection;
  }

  /**
   * Set the number of rows that will be fetched on next
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
  }

  public ScannerCallable getScannerCallableForReplica(int id) {
    ScannerCallable s = new ScannerCallable(this.getConnection(), this.tableName,
        this.getScan(), this.scanMetrics, controllerFactory, id);
    s.setCaching(this.caching);
    return s;
  }

  /**
   * Should the client attempt to fetch more results from this region
   */
  MoreResults moreResultsInRegion() {
    return moreResultsInRegion;
  }

  void setMoreResultsInRegion(MoreResults moreResults) {
    this.moreResultsInRegion = moreResults;
  }

  /**
   * Should the client attempt to fetch more results for the whole scan.
   */
  MoreResults moreResultsForScan() {
    return moreResultsForScan;
  }

  void setMoreResultsForScan(MoreResults moreResults) {
    this.moreResultsForScan = moreResults;
  }
}
