/**
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.NotServingRegionException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Abstract class that implements {@link Callable}.  Implementation stipulates
 * return type and method we actually invoke on remote Server.  Usually
 * used inside a try/catch that fields usual connection failures all wrapped
 * up in a retry loop.
 * <p>Call {@link #prepare(boolean)} to connect to server hosting region
 * that contains the passed row in the passed table before invoking
 * {@link #call()}.
 * @see HConnection#getRegionServerWithoutRetries(ServerCallable)
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ServerCallable<T> implements Callable<T> {
  static final Log LOG = LogFactory.getLog(ServerCallable.class);

  protected final HConnection connection;
  protected final byte [] tableName;
  protected final byte [] row;
  protected HRegionLocation location;
  protected ClientService.BlockingInterface stub;
  protected int callTimeout;
  protected long globalStartTime;
  protected long startTime, endTime;
  protected final static int MIN_RPC_TIMEOUT = 2000;
  protected final static int MIN_WAIT_DEAD_SERVER = 10000;

  /**
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public ServerCallable(HConnection connection, byte [] tableName, byte [] row) {
    this(connection, tableName, row, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  public ServerCallable(HConnection connection, byte [] tableName, byte [] row, int callTimeout) {
    this.connection = connection;
    this.tableName = tableName;
    this.row = row;
    this.callTimeout = callTimeout;
  }

  /**
   * Prepare for connection to the server hosting region with row from tablename.  Does lookup
   * to find region location and hosting server.
   * @param reload Set this to true if connection should re-find the region
   * @throws IOException e
   */
  public void prepare(final boolean reload) throws IOException {
    this.location = connection.getRegionLocation(tableName, row, reload);
    this.stub = connection.getClient(location.getServerName());
  }

  /** @return the server name
   * @deprecated Just use {@link #toString()} instead.
   */
  public String getServerName() {
    if (location == null) return null;
    return location.getHostnamePort();
  }

  /** @return the region name
   * @deprecated Just use {@link #toString()} instead.
   */
  public byte[] getRegionName() {
    if (location == null) return null;
    return location.getRegionInfo().getRegionName();
  }

  /** @return the row
   * @deprecated Just use {@link #toString()} instead.
   */
  public byte [] getRow() {
    return row;
  }

  public void beforeCall() {
    this.startTime = EnvironmentEdgeManager.currentTimeMillis();
    int remaining = (int)(callTimeout - (this.startTime - this.globalStartTime));
    if (remaining < MIN_RPC_TIMEOUT) {
      // If there is no time left, we're trying anyway. It's too late.
      // 0 means no timeout, and it's not the intent here. So we secure both cases by
      // resetting to the minimum.
      remaining = MIN_RPC_TIMEOUT;
    }
    RpcClient.setRpcTimeout(remaining);
  }

  public void afterCall() {
    RpcClient.resetRpcTimeout();
    this.endTime = EnvironmentEdgeManager.currentTimeMillis();
  }

  /**
   * @return {@link HConnection} instance used by this Callable.
   */
  HConnection getConnection() {
    return this.connection;
  }

  /**
   * Run this instance with retries, timed waits,
   * and refinds of missing regions.
   *
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public T withRetries()
  throws IOException, RuntimeException {
    Configuration c = getConnection().getConfiguration();
    final long pause = c.getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    final int numRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions =
      new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
    this.globalStartTime = EnvironmentEdgeManager.currentTimeMillis();
    for (int tries = 0;; tries++) {
      long expectedSleep = 0;
      try {
        beforeCall();
        prepare(tries != 0); // if called with false, check table status on ZK
        return call();
      } catch (Throwable t) {
        LOG.warn("Call exception, tries=" + tries + ", numRetries=" + numRetries + ": " + t);

        t = translateException(t);
        // translateException throws an exception when we should not retry, i.e. when it's the
        //  request that is bad.

        if (t instanceof SocketTimeoutException ||
            t instanceof ConnectException ||
            t instanceof RetriesExhaustedException ||
            (location != null && getConnection().isDeadServer(location.getServerName()))) {
          // if thrown these exceptions, we clear all the cache entries that
          // map to that slow/dead server; otherwise, let cache miss and ask
          // .META. again to find the new location
          getConnection().clearCaches(location.getServerName());
        } else if (t instanceof NotServingRegionException && numRetries == 1) {
          // Purge cache entries for this specific region from META cache
          // since we don't call connect(true) when number of retries is 1.
          getConnection().deleteCachedRegionLocation(location);
        }

        RetriesExhaustedException.ThrowableWithExtraContext qt =
          new RetriesExhaustedException.ThrowableWithExtraContext(t,
              EnvironmentEdgeManager.currentTimeMillis(), toString());
        exceptions.add(qt);
        if (tries >= numRetries - 1) {
          throw new RetriesExhaustedException(tries, exceptions);
        }

        // If the server is dead, we need to wait a little before retrying, to give
        //  a chance to the regions to be
        expectedSleep = ConnectionUtils.getPauseTime(pause, tries);
        if (expectedSleep < MIN_WAIT_DEAD_SERVER 
            && (location == null || getConnection().isDeadServer(location.getServerName()))) {
          expectedSleep = ConnectionUtils.addJitter(MIN_WAIT_DEAD_SERVER, 0.10f);
        }

        // If, after the planned sleep, there won't be enough time left, we stop now.
        if (((this.endTime - this.globalStartTime) + MIN_RPC_TIMEOUT + expectedSleep) >
            this.callTimeout) {
          throw (SocketTimeoutException) new SocketTimeoutException(
              "Call to access row '" + Bytes.toString(row) + "' on table '"
                  + Bytes.toString(tableName)
                  + "' failed on timeout. " + " callTimeout=" + this.callTimeout +
                  ", time=" + (this.endTime - this.startTime)).initCause(t);
        }
      } finally {
        afterCall();
      }
      try {
        Thread.sleep(expectedSleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted after " + tries + " tries  on " + numRetries, e);
      }
    }
  }

  /**
   * Run this instance against the server once.
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public T withoutRetries()
  throws IOException, RuntimeException {
    // The code of this method should be shared with withRetries.
    this.globalStartTime = EnvironmentEdgeManager.currentTimeMillis();
    try {
      beforeCall();
      prepare(false);
      return call();
    } catch (Throwable t) {
      Throwable t2 = translateException(t);
      // It would be nice to clear the location cache here.
      if (t2 instanceof IOException) {
        throw (IOException)t2;
      } else {
        throw new RuntimeException(t2);
      }
    } finally {
      afterCall();
    }
  }

  /**
   * Get the good or the remote exception if any, throws the DoNotRetryIOException.
   * @param t the throwable to analyze
   * @return the translated exception, if it's not a DoNotRetryIOException
   * @throws DoNotRetryIOException - if we find it, we throw it instead of translating.
   */
  protected static Throwable translateException(Throwable t) throws DoNotRetryIOException {
    if (t instanceof UndeclaredThrowableException) {
      if(t.getCause() != null) {
        t = t.getCause();
      }
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException)t).unwrapRemoteException();
    }
    if (t instanceof ServiceException) {
      ServiceException se = (ServiceException)t;
      Throwable cause = se.getCause();
      if (cause != null && cause instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException)cause;
      }
    } else if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException)t;
    }
    return t;
  }
}
