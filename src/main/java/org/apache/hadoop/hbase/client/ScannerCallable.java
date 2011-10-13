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
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;

/**
 * Retries scanner operations such as create, next, etc.
 * Used by {@link ResultScanner}s made by {@link HTable}.
 */
public class ScannerCallable extends ServerCallable<Result[]> {
  private static final Log LOG = LogFactory.getLog(ScannerCallable.class);
  private long scannerId = -1L;
  private boolean instantiated = false;
  private boolean closed = false;
  private Scan scan;
  private int caching = 1;
  private ScanMetrics scanMetrics;

  // indicate if it is a remote server call
  private boolean isRegionServerRemote = true;

  /**
   * @param connection which connection
   * @param tableName table callable is on
   * @param scan the scan to execute
   * @param scanMetrics the ScanMetrics to used, if it is null, ScannerCallable
   * won't collect metrics
   */
  public ScannerCallable (HConnection connection, byte [] tableName, Scan scan,
    ScanMetrics scanMetrics) {
    super(connection, tableName, scan.getStartRow());
    this.scan = scan;
    this.scanMetrics = scanMetrics;
  }

  /**
   * @param reload force reload of server location
   * @throws IOException
   */
  @Override
  public void connect(boolean reload) throws IOException {
    if (!instantiated || reload) {
      super.connect(reload);
      checkIfRegionServerIsRemote();
      instantiated = true;
    }

    // check how often we retry.
    // HConnectionManager will call instantiateServer with reload==true
    // if and only if for retries.
    if (reload && this.scanMetrics != null) {
      this.scanMetrics.countOfRPCRetries.inc();
      if (isRegionServerRemote) {
        this.scanMetrics.countOfRemoteRPCRetries.inc();
      }
    }
  }

  /**
   * compare the local machine hostname with region server's hostname
   * to decide if hbase client connects to a remote region server
   * @throws UnknownHostException.
   */
  private void checkIfRegionServerIsRemote() throws UnknownHostException {
    String myAddress = DNS.getDefaultHost("default", "default");
    if (this.location.getHostname().equalsIgnoreCase(myAddress)) {
      isRegionServerRemote = false;
    } else {
      isRegionServerRemote = true;
    }
  }

  /**
   * @see java.util.concurrent.Callable#call()
   */
  public Result [] call() throws IOException {
    if (scannerId != -1L && closed) {
      close();
    } else if (scannerId == -1L && !closed) {
      this.scannerId = openScanner();
    } else {
      Result [] rrs = null;
      try {
        incRPCcallsMetrics();
        rrs = server.next(scannerId, caching);
        updateResultsMetrics(rrs);
      } catch (IOException e) {
        IOException ioe = null;
        if (e instanceof RemoteException) {
          ioe = RemoteExceptionHandler.decodeRemoteException((RemoteException)e);
        }
        if (ioe == null) throw new IOException(e);
        if (ioe instanceof NotServingRegionException) {
          // Throw a DNRE so that we break out of cycle of calling NSRE
          // when what we need is to open scanner against new location.
          // Attach NSRE to signal client that it needs to resetup scanner.
          if (this.scanMetrics != null) {
            this.scanMetrics.countOfNSRE.inc();
          }
          throw new DoNotRetryIOException("Reset scanner", ioe);
        } else if (ioe instanceof RegionServerStoppedException) {
          // Throw a DNRE so that we break out of cycle of calling RSSE
          // when what we need is to open scanner against new location.
          // Attach RSSE to signal client that it needs to resetup scanner.
          throw new DoNotRetryIOException("Reset scanner", ioe);
        } else {
          // The outer layers will retry
          throw ioe;
        }
      }
      return rrs;
    }
    return null;
  }

  private void incRPCcallsMetrics() {
    if (this.scanMetrics == null) {
      return;
    }
    this.scanMetrics.countOfRPCcalls.inc();
    if (isRegionServerRemote) {
      this.scanMetrics.countOfRemoteRPCcalls.inc();
    }
  }

  private void updateResultsMetrics(Result[] rrs) {
    if (this.scanMetrics == null || rrs == null) {
      return;
    }
    for (Result rr : rrs) {
      this.scanMetrics.countOfBytesInResults.inc(rr.getBytes().getLength());
      if (isRegionServerRemote) {
        this.scanMetrics.countOfBytesInRemoteResults.inc(
          rr.getBytes().getLength());
      }
    }
  }

  private void close() {
    if (this.scannerId == -1L) {
      return;
    }
    try {
      incRPCcallsMetrics();
      this.server.close(this.scannerId);
    } catch (IOException e) {
      LOG.warn("Ignore, probably already closed", e);
    }
    this.scannerId = -1L;
  }

  protected long openScanner() throws IOException {
    incRPCcallsMetrics();
    return this.server.openScanner(this.location.getRegionInfo().getRegionName(),
      this.scan);
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
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (!instantiated) {
      return null;
    }
    return location.getRegionInfo();
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
