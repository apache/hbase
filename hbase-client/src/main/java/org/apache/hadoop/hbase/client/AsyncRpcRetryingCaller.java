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

import static org.apache.hadoop.hbase.client.ConnectionUtils.SLEEP_DELTA_NS;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getPauseTime;
import static org.apache.hadoop.hbase.client.ConnectionUtils.resetController;
import static org.apache.hadoop.hbase.client.ConnectionUtils.translateException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.Timer;

@InterfaceAudience.Private
public abstract class AsyncRpcRetryingCaller<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRpcRetryingCaller.class);

  private final Timer retryTimer;

  private final int priority;

  private final long startNs;

  private final long pauseNs;

  private final long pauseForCQTBENs;

  private int tries = 1;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions;

  private final long rpcTimeoutNs;

  protected final long operationTimeoutNs;

  protected final AsyncConnectionImpl conn;

  protected final CompletableFuture<T> future;

  protected final HBaseRpcController controller;

  public AsyncRpcRetryingCaller(Timer retryTimer, AsyncConnectionImpl conn, int priority,
      long pauseNs, long pauseForCQTBENs, int maxAttempts, long operationTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt) {
    this.retryTimer = retryTimer;
    this.conn = conn;
    this.priority = priority;
    this.pauseNs = pauseNs;
    this.pauseForCQTBENs = pauseForCQTBENs;
    this.maxAttempts = maxAttempts;
    this.operationTimeoutNs = operationTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.future = new CompletableFuture<>();
    this.controller = conn.rpcControllerFactory.newController();
    this.controller.setPriority(priority);
    this.exceptions = new ArrayList<>();
    this.startNs = System.nanoTime();
  }

  private long elapsedMs() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
  }

  protected final long remainingTimeNs() {
    return operationTimeoutNs - (System.nanoTime() - startNs);
  }

  protected final void completeExceptionally() {
    future.completeExceptionally(new RetriesExhaustedException(tries - 1, exceptions));
  }

  protected final void resetCallTimeout() {
    long callTimeoutNs;
    if (operationTimeoutNs > 0) {
      callTimeoutNs = remainingTimeNs();
      if (callTimeoutNs <= 0) {
        completeExceptionally();
        return;
      }
      callTimeoutNs = Math.min(callTimeoutNs, rpcTimeoutNs);
    } else {
      callTimeoutNs = rpcTimeoutNs;
    }
    resetController(controller, callTimeoutNs, priority);
  }

  private void tryScheduleRetry(Throwable error) {
    long pauseNsToUse = error instanceof CallQueueTooBigException ? pauseForCQTBENs : pauseNs;
    long delayNs;
    if (operationTimeoutNs > 0) {
      long maxDelayNs = remainingTimeNs() - SLEEP_DELTA_NS;
      if (maxDelayNs <= 0) {
        completeExceptionally();
        return;
      }
      delayNs = Math.min(maxDelayNs, getPauseTime(pauseNsToUse, tries - 1));
    } else {
      delayNs = getPauseTime(pauseNsToUse, tries - 1);
    }
    tries++;
    retryTimer.newTimeout(t -> doCall(), delayNs, TimeUnit.NANOSECONDS);
  }

  protected Optional<TableName> getTableName() {
    return Optional.empty();
  }

  protected final void onError(Throwable t, Supplier<String> errMsg,
      Consumer<Throwable> updateCachedLocation) {
    if (future.isDone()) {
      // Give up if the future is already done, this is possible if user has already canceled the
      // future. And for timeline consistent read, we will also cancel some requests if we have
      // already get one of the responses.
      LOG.debug("The future is already done, canceled={}, give up retrying", future.isCancelled());
      return;
    }
    Throwable error = translateException(t);
    // We use this retrying caller to open a scanner, as it is idempotent, but we may throw
    // ScannerResetException, which is a DoNotRetryIOException when opening a scanner as now we will
    // also fetch data when opening a scanner. The intention here is that if we hit a
    // ScannerResetException when scanning then we should try to open a new scanner, instead of
    // retrying on the old one, so it is declared as a DoNotRetryIOException. But here we are
    // exactly trying to open a new scanner, so we should retry on ScannerResetException.
    if (error instanceof DoNotRetryIOException && !(error instanceof ScannerResetException)) {
      future.completeExceptionally(error);
      return;
    }
    if (tries > startLogErrorsCnt) {
      LOG.warn(errMsg.get() + ", tries = " + tries + ", maxAttempts = " + maxAttempts +
        ", timeout = " + TimeUnit.NANOSECONDS.toMillis(operationTimeoutNs) +
        " ms, time elapsed = " + elapsedMs() + " ms", error);
    }
    updateCachedLocation.accept(error);
    RetriesExhaustedException.ThrowableWithExtraContext qt =
      new RetriesExhaustedException.ThrowableWithExtraContext(error,
        EnvironmentEdgeManager.currentTime(), "");
    exceptions.add(qt);
    if (tries >= maxAttempts) {
      completeExceptionally();
      return;
    }
    // check whether the table has been disabled, notice that the check will introduce a request to
    // meta, so here we only check for disabled for some specific exception types.
    if (error instanceof NotServingRegionException || error instanceof RegionOfflineException) {
      Optional<TableName> tableName = getTableName();
      if (tableName.isPresent()) {
        FutureUtils.addListener(conn.getAdmin().isTableDisabled(tableName.get()), (disabled, e) -> {
          if (e != null) {
            if (e instanceof TableNotFoundException) {
              future.completeExceptionally(e);
            } else {
              // failed to test whether the table is disabled, not a big deal, continue retrying
              tryScheduleRetry(error);
            }
            return;
          }
          if (disabled) {
            future.completeExceptionally(new TableNotEnabledException(tableName.get()));
          } else {
            tryScheduleRetry(error);
          }
        });
      } else {
        tryScheduleRetry(error);
      }
    } else {
      tryScheduleRetry(error);
    }
  }

  protected abstract void doCall();

  CompletableFuture<T> call() {
    doCall();
    return future;
  }
}
