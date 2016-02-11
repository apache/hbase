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

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;

import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

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

  public static final Log LOG = LogFactory.getLog(ScannerCallable.class);
  private long scannerId = -1L;
  protected boolean instantiated = false;
  protected boolean closed = false;
  protected boolean renew = false;
  private Scan scan;
  private int caching = 1;
  protected ScanMetrics scanMetrics;
  private boolean logScannerActivity = false;
  private int logCutOffLatency = 1000;
  private static String myAddress;
  static {
    try {
      myAddress = DNS.getDefaultHost("default", "default");
    } catch (UnknownHostException uhe) {
      LOG.error("cannot determine my address", uhe);
    }
  }

  // indicate if it is a remote server call
  protected boolean isRegionServerRemote = true;
  private long nextCallSeq = 0;
  protected PayloadCarryingRpcController controller;
  
  /**
   * @param connection which connection
   * @param tableName table callable is on
   * @param scan the scan to execute
   * @param scanMetrics the ScanMetrics to used, if it is null, ScannerCallable won't collect
   *          metrics
   * @param controller to use when writing the rpc
   */
  public ScannerCallable (HConnection connection, TableName tableName, Scan scan,
      ScanMetrics scanMetrics, PayloadCarryingRpcController controller) {
    super(connection, tableName, scan.getStartRow());
    this.scan = scan;
    this.scanMetrics = scanMetrics;
    Configuration conf = connection.getConfiguration();
    logScannerActivity = conf.getBoolean(LOG_SCANNER_ACTIVITY, false);
    logCutOffLatency = conf.getInt(LOG_SCANNER_LATENCY_CUTOFF, 1000);
    this.controller = controller;
  }

  /**
   * @deprecated Use {@link #ScannerCallable(HConnection, TableName, Scan, 
   *  ScanMetrics, PayloadCarryingRpcController)}
   */
  @Deprecated
  public ScannerCallable (HConnection connection, final byte [] tableName, Scan scan,
      ScanMetrics scanMetrics) {
    this(connection, TableName.valueOf(tableName), scan, scanMetrics, RpcControllerFactory
        .instantiate(connection.getConfiguration()).newController());
  }

  /**
   * @param reload force reload of server location
   * @throws IOException
   */
  @Override
  public void prepare(boolean reload) throws IOException {
    if (!instantiated || reload) {
      super.prepare(reload);
      checkIfRegionServerIsRemote();
      instantiated = true;
    }

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

  /**
   * @see java.util.concurrent.Callable#call()
   */
  @SuppressWarnings("deprecation")
  public Result [] call() throws IOException {
    if (controller == null) {
      controller = RpcControllerFactory.instantiate(connection.getConfiguration())
          .newController();
    }
    if (closed) {
      if (scannerId != -1) {
        close();
      }
    } else {
      if (scannerId == -1L) {
        this.scannerId = openScanner();
      } else {
        Result [] rrs = null;
        ScanRequest request = null;
        try {
          incRPCcallsMetrics();
          request =
              RequestConverter.buildScanRequest(scannerId, caching, false, nextCallSeq, renew);
          ScanResponse response = null;
          try {
            controller.setPriority(getTableName());
            response = getStub().scan(controller, request);
            // Client and RS maintain a nextCallSeq number during the scan. Every next() call
            // from client to server will increment this number in both sides. Client passes this
            // number along with the request and at RS side both the incoming nextCallSeq and its
            // nextCallSeq will be matched. In case of a timeout this increment at the client side
            // should not happen. If at the server side fetching of next batch of data was over,
            // there will be mismatch in the nextCallSeq number. Server will throw
            // OutOfOrderScannerNextException and then client will reopen the scanner with startrow
            // as the last successfully retrieved row.
            // See HBASE-5974
            nextCallSeq++;
            long timestamp = System.currentTimeMillis();
            // Results are returned via controller
            CellScanner cellScanner = controller.cellScanner();
            rrs = ResponseConverter.getResults(cellScanner, response);
            if (logScannerActivity) {
              long now = System.currentTimeMillis();
              if (now - timestamp > logCutOffLatency) {
                int rows = rrs == null ? 0 : rrs.length;
                LOG.info("Took " + (now-timestamp) + "ms to fetch "
                  + rows + " rows from scanner=" + scannerId);
              }
            }
            // moreResults is only used for the case where a filter exhausts all elements
            if (response.hasMoreResults() && !response.getMoreResults()) {
              scannerId = -1L;
              closed = true;
              // Implied that no results were returned back, either.
              return null;
            }
            // moreResultsInRegion explicitly defines when a RS may choose to terminate a batch due
            // to size or quantity of results in the response.
            if (response.hasMoreResultsInRegion()) {
              // Set what the RS said
              setHasMoreResultsContext(true);
              setServerHasMoreResults(response.getMoreResultsInRegion());
            } else {
              // Server didn't respond whether it has more results or not.
              setHasMoreResultsContext(false);
            }
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
          updateResultsMetrics(rrs);
        } catch (IOException e) {
          if (logScannerActivity) {
            LOG.info("Got exception making request " + TextFormat.shortDebugString(request)
              + " to " + getLocation(), e);
          }
          IOException ioe = e;
          if (e instanceof RemoteException) {
            ioe = RemoteExceptionHandler.decodeRemoteException((RemoteException)e);
          }
          if (logScannerActivity && (ioe instanceof UnknownScannerException)) {
            try {
              HRegionLocation location =
                getConnection().relocateRegion(getTableName(), scan.getStartRow());
              LOG.info("Scanner=" + scannerId
                + " expired, current region location is " + location.toString());
            } catch (Throwable t) {
              LOG.info("Failed to relocate region", t);
            }
          }
          // The below convertion of exceptions into DoNotRetryExceptions is a little strange.
          // Why not just have these exceptions implment DNRIOE you ask?  Well, usually we want
          // ServerCallable#withRetries to just retry when it gets these exceptions.  In here in
          // a scan when doing a next in particular, we want to break out and get the scanner to
          // reset itself up again.  Throwing a DNRIOE is how we signal this to happen (its ugly,
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
        return rrs;
      }
    }
    return null;
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
      for (Cell kv : rr.rawCells()) {
        // TODO add getLength to Cell/use CellUtil#estimatedSizeOf
        resultSize += KeyValueUtil.ensureKeyValue(kv).getLength();
      }
    }
    this.scanMetrics.countOfBytesInResults.addAndGet(resultSize);
    if (isRegionServerRemote) {
      this.scanMetrics.countOfBytesInRemoteResults.addAndGet(resultSize);
    }
  }

  private void close() {
    if (this.scannerId == -1L) {
      return;
    }
    try {
      incRPCcallsMetrics();
      ScanRequest request =
        RequestConverter.buildScanRequest(this.scannerId, 0, true);
      try {
        getStub().scan(controller, request);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
    } catch (IOException e) {
      LOG.warn("Ignore, probably already closed", e);
    }
    this.scannerId = -1L;
  }

  protected long openScanner() throws IOException {
    incRPCcallsMetrics();
    ScanRequest request =
      RequestConverter.buildScanRequest(
        getLocation().getRegionInfo().getRegionName(),
        this.scan, 0, false);
    try {
      ScanResponse response = getStub().scan(controller, request);
      long id = response.getScannerId();
      if (logScannerActivity) {
        LOG.info("Open scanner=" + id + " for scan=" + scan.toString()
          + " on region " + getLocation().toString());
      }
      return id;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
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

  public void setRenew(boolean val) {
    this.renew = val;
  }

  /**
   * @return the HRegionInfo for the current region
   */
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

  /**
   * Set the number of rows that will be fetched on next
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
  }
}
