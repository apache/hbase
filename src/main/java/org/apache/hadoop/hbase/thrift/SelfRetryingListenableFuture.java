/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.thrift;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.ClientSideDoNotRetryException;
import org.apache.hadoop.hbase.client.HTableAsync;
import org.apache.hadoop.hbase.client.PreemptiveFastFailException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.ipc.HConnectionParams;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.RegionOverloadedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Make asynchronous call(s) to the server with built-in retry mechanism
 *
 * @param <V> Expeted result type
 */
public class SelfRetryingListenableFuture<V> extends AbstractFuture<V> {
  private final Log LOG = LogFactory.getLog(SelfRetryingListenableFuture.class);

  private final HTableAsync table;
  private final ServerCallable<ListenableFuture<V>> callable;
  private final HConnectionParams params;
  private final ListeningScheduledExecutorService executorService;

  private boolean instantiateRegionLocation = false;
  private boolean didTry = false;

  private final Runnable attemptWorker = new Runnable() {
    @Override
    public void run() {
      makeAttempt();
    }
  };

  private final SettableFuture<V> downstream;

  private int tries = 0;
  private List<Throwable> exceptions = new ArrayList<>();
  private RegionOverloadedException roe = null;

  long callStartTime = 0;
  int serverRequestedRetries = 0;
  long serverRequestedWaitTime = 0;

  public SelfRetryingListenableFuture(
      HTableAsync table, ServerCallable<ListenableFuture<V>> callable,
      HConnectionParams params, ListeningScheduledExecutorService executorService) {
    this.table = table;
    this.callable = callable;
    this.params = params;
    this.executorService = executorService;
    this.downstream = SettableFuture.create();
  }

  /**
   * Start self-retrying attempts to get result
   *
   * @return ListenableFuture for client
   */
  public ListenableFuture<V> startFuture() {
    // Instantiate region location once
    instantiateRegionLocation = true;
    callStartTime = System.currentTimeMillis();

    // Make the first attempt using thread pool to make HTableAsync APIs full asynchronous,
    // because instantiateRegionLocation() might cause some pause.
    executorService.execute(attemptWorker);

    return downstream;
  }

  /**
   * Make a single attempt to get result from server
   */
  private void makeAttempt() {
    if (instantiateRegionLocation) {
      // Do not tries if region cannot be located. There are enough retries
      // within instantiateRegionLocation.
      try {
        callable.instantiateRegionLocation(false);
      } catch (Exception e) {
        setFailure(e);
        return;
      }
      // By default don't instantiate region location for next attempt
      instantiateRegionLocation = false;
    }

    tries++;
    LOG.debug("Try number: " + tries);

    didTry = false;
    try {
      ListenableFuture<V> upstream =
          table.getConnectionAndResetOperationContext().getRegionServerWithoutRetries(callable, false);
      // The upstream listenable future is created.
      // We assume at this point the client has tried to communicate with the server.
      didTry = true;
      Futures.addCallback(upstream, new FutureCallback<V>() {
        @Override
        public void onSuccess(V v) {
          setSuccess(v);
        }

        @Override
        public void onFailure(Throwable t) {
          // The try block is a safe guard for ensuring the retrying process to
          // enter a final state and notify its downstream.
          // If anything is caught here, there must be some bug.
          try {
            handleException(t);
          } catch (Exception e) {
            LOG.error("Exception should never be caught here!", e);
            setFailure(e);
          }
        }
      }, executorService);
    } catch (Exception e) {
      LOG.error("Cannot create upstream listenable future at the first place", e);
      handleException(e);
    }
  }

  /**
   * Pass result to client. It's the final state of this listenable future.
   *
   * @param v Result from server
   */
  private void setSuccess(V v) {
    downstream.set(v);
  }

  /**
   * Pass exception to downstream ListenableFuture. It's the final state of this listenable future.
   *
   * @param t The exception for client
   */
  private void setFailure(Throwable t) {
    downstream.setException(t);
  }

  /**
   * Unwrap exception if it's from server side and handle all scenarios.
   *
   * @param t The exception from either client side or server side
   */
  protected void handleException(Throwable t) {
    if (t instanceof ThriftHBaseException) {
      Exception e = ((ThriftHBaseException) t).getServerJavaException();
      LOG.debug("Server defined exception", e);
      retryOrStop(e);
    } else if (t instanceof PreemptiveFastFailException) {
      // Client did not try to contact the server.
      // Skip exception handling in TableServers and just fast fail to next retry.
      retryOrStop(t);
    } else {
      LOG.debug("Other exception type, detecting if it's need to refresh connection", t);
      if (!didTry) {
        // When the call to server is actually made,
        //   try to refresh server connection if it's necessary.
        try {
          callable.refreshServerConnection((Exception)t);
        } catch (Exception e) {
          // Decide next step according to the original exception. Do not use the this exception
          //   which is wrapped by connection refreshing.
        }
      }

      // handleThrowable() will determine whether the client could not communicate with the server.
      MutableBoolean couldNotCommunicateWithServer = new MutableBoolean(false);
      try {
        callable.handleThrowable(t, couldNotCommunicateWithServer);
      } catch (Exception e) {
        // Update failure info for fast failure
        callable.updateFailureInfoForServer(didTry, couldNotCommunicateWithServer.booleanValue());
        retryOrStop(e);
        return;
      }
      RuntimeException ex = new RuntimeException("Unexpected code path");
      LOG.error("handleThrowable() should always throw an exception", ex);
      setFailure(ex);
    }
  }

  /**
   * Decide whether to proceed based on exception type
   * @param t The exception from either client side or server side
   */
  private void retryOrStop(Throwable t) {
    if (t instanceof DoNotRetryIOException) {
      if (t.getCause() instanceof NotServingRegionException) {
        HRegionLocation prevLoc = callable.getLocation();
        if (prevLoc != null) {
          table.getConnection().deleteCachedLocation(
              callable.getTableName(), prevLoc.getRegionInfo().getStartKey(), prevLoc.getServerAddress());
        }
      }
      // So there is no retry
      setFailure(t);
    } else if (t instanceof RegionOverloadedException) {
      roe = (RegionOverloadedException)t;
      serverRequestedWaitTime = roe.getBackoffTimeMillis();

      // If server requests wait. We will wait for that time, and start
      // again. Do not count this time/tries against the client retries.
      if (serverRequestedWaitTime > 0) {
        serverRequestedRetries++;

        if (serverRequestedRetries > params.getMaxServerRequestedRetries()) {
          RegionOverloadedException e = RegionOverloadedException.create(
              roe, exceptions, serverRequestedWaitTime);
          setFailure(e);
          return;
        }

        long pauseTime = serverRequestedWaitTime + callStartTime - System.currentTimeMillis();
        serverRequestedWaitTime = 0;
        tries = 0;
        callStartTime = System.currentTimeMillis();
        scheduleNextAttempt(pauseTime);
        return;
      }

      // If server does not request wait, just do a normal retry without any sleep.
      // I guess this should not happen?
      makeAttempt();
    } else if (t instanceof ClientSideDoNotRetryException ||
               t instanceof PreemptiveFastFailException) {
      // No retry for specific exception types
      setFailure(t);
    } else {
      exceptions.add(t);

      if (tries >= params.getNumRetries()) {
        RetriesExhaustedException ree = new RetriesExhaustedException(
            callable.getServerName(), callable.getRegionName(), callable.getRow(), tries, exceptions);
        setFailure(ree);
        return;
      }

      HRegionLocation prevLoc = callable.getLocation();
      if (prevLoc.getRegionInfo() != null) {
        table.getConnection().deleteCachedLocation(
            callable.getTableName(), prevLoc.getRegionInfo().getStartKey(), prevLoc.getServerAddress());
      }

      try {
        callable.instantiateRegionLocation(false);
      } catch (Exception e) {
        exceptions.add(e);
        RetriesExhaustedException ree = new RetriesExhaustedException(
            callable.getServerName(), callable.getRegionName(), callable.getRow(), tries, exceptions);
        // Do not tolerant failure of instantiating region location.
        setFailure(ree);
        return;
      }

      if (prevLoc.getServerAddress().equals(callable.getLocation().getServerAddress())) {
        // Bail out of the retry loop if we have to wait too long
        long pauseTime = params.getPauseTime(tries);
        if ((System.currentTimeMillis() - callStartTime + pauseTime) >
            params.getRpcRetryTimeout()) {
          RetriesExhaustedException ree = new RetriesExhaustedException(callable.getServerName(),
              callable.getRegionName(), callable.getRow(), tries,
              exceptions);
          setFailure(ree);
          return;
        }
        LOG.debug("getRegionServerWithoutRetries failed, sleeping for " +
            pauseTime + "ms. tries = " + tries, t);

        // Reload region location from cache after pause.
        instantiateRegionLocation = true;
        scheduleNextAttempt(pauseTime);
        return;
      } else {
        LOG.debug("getRegionServerWithoutRetries failed, " +
            "region moved from " + prevLoc + " to " + callable.getLocation() +
            "retrying immediately tries=" + tries, t);
      }

      makeAttempt();
    }
  }

  /**
   * Schedule next attempt with delay.
   *
   * @param pauseTime milliseconds to wait before next attempt
   */
  private void scheduleNextAttempt(long pauseTime) {
    executorService.schedule(attemptWorker, pauseTime, TimeUnit.MILLISECONDS);
  }
}
