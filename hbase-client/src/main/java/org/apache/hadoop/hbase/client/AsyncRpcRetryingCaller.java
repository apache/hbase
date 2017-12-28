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
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

@InterfaceAudience.Private
public abstract class AsyncRpcRetryingCaller<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRpcRetryingCaller.class);

  private final HashedWheelTimer retryTimer;

  private final long startNs;

  private final long pauseNs;

  private int tries = 1;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions;

  private final long rpcTimeoutNs;

  protected final long operationTimeoutNs;

  protected final AsyncConnectionImpl conn;

  protected final CompletableFuture<T> future;

  protected final HBaseRpcController controller;

  public AsyncRpcRetryingCaller(HashedWheelTimer retryTimer, AsyncConnectionImpl conn,
      long pauseNs, int maxAttempts, long operationTimeoutNs,
      long rpcTimeoutNs, int startLogErrorsCnt) {
    this.retryTimer = retryTimer;
    this.conn = conn;
    this.pauseNs = pauseNs;
    this.maxAttempts = maxAttempts;
    this.operationTimeoutNs = operationTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.future = new CompletableFuture<>();
    this.controller = conn.rpcControllerFactory.newController();
    this.exceptions = new ArrayList<>();
    this.startNs = System.nanoTime();
  }

  private long elapsedMs() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
  }

  protected long remainingTimeNs() {
    return operationTimeoutNs - (System.nanoTime() - startNs);
  }

  protected void completeExceptionally() {
    future.completeExceptionally(new RetriesExhaustedException(tries - 1, exceptions));
  }

  protected void resetCallTimeout() {
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
    resetController(controller, callTimeoutNs);
  }

  protected void onError(Throwable error, Supplier<String> errMsg,
      Consumer<Throwable> updateCachedLocation) {
    error = translateException(error);
    if (error instanceof DoNotRetryIOException) {
      future.completeExceptionally(error);
      return;
    }
    if (tries > startLogErrorsCnt) {
      LOG.warn(errMsg.get() + ", tries = " + tries + ", maxAttempts = " + maxAttempts
          + ", timeout = " + TimeUnit.NANOSECONDS.toMillis(operationTimeoutNs)
          + " ms, time elapsed = " + elapsedMs() + " ms", error);
    }
    RetriesExhaustedException.ThrowableWithExtraContext qt = new RetriesExhaustedException.ThrowableWithExtraContext(
        error, EnvironmentEdgeManager.currentTime(), "");
    exceptions.add(qt);
    if (tries >= maxAttempts) {
      completeExceptionally();
      return;
    }
    long delayNs;
    if (operationTimeoutNs > 0) {
      long maxDelayNs = remainingTimeNs() - SLEEP_DELTA_NS;
      if (maxDelayNs <= 0) {
        completeExceptionally();
        return;
      }
      delayNs = Math.min(maxDelayNs, getPauseTime(pauseNs, tries - 1));
    } else {
      delayNs = getPauseTime(pauseNs, tries - 1);
    }
    updateCachedLocation.accept(error);
    tries++;
    retryTimer.newTimeout(t -> doCall(), delayNs, TimeUnit.NANOSECONDS);
  }

  protected abstract void doCall();

  CompletableFuture<T> call() {
    doCall();
    return future;
  }
}
