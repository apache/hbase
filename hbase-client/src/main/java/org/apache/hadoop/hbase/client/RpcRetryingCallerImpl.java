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

import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

/**
 * Runs an rpc'ing {@link RetryingCallable}. Sets into rpc client
 * threadlocal outstanding timeouts as so we don't persist too much.
 * Dynamic rather than static so can set the generic appropriately.
 *
 * This object has a state. It should not be used by in parallel by different threads.
 * Reusing it is possible however, even between multiple threads. However, the user will
 *  have to manage the synchronization on its side: there is no synchronization inside the class.
 */
@InterfaceAudience.Private
public class RpcRetryingCallerImpl<T> implements RpcRetryingCaller<T> {
  // LOG is being used in TestMultiRowRangeFilter, hence leaving it public
  public static final Logger LOG = LoggerFactory.getLogger(RpcRetryingCallerImpl.class);

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  private final long pause;
  private final long pauseForCQTBE;
  private final int maxAttempts;// how many times to try
  private final int rpcTimeout;// timeout for each rpc request
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final RetryingCallerInterceptor interceptor;
  private final RetryingCallerInterceptorContext context;
  private final RetryingTimeTracker tracker;

  public RpcRetryingCallerImpl(long pause, long pauseForCQTBE, int retries, int startLogErrorsCnt) {
    this(pause, pauseForCQTBE, retries, RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR,
        startLogErrorsCnt, 0);
  }

  public RpcRetryingCallerImpl(long pause, long pauseForCQTBE, int retries,
      RetryingCallerInterceptor interceptor, int startLogErrorsCnt, int rpcTimeout) {
    this.pause = pause;
    this.pauseForCQTBE = pauseForCQTBE;
    this.maxAttempts = retries2Attempts(retries);
    this.interceptor = interceptor;
    context = interceptor.createEmptyContext();
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.tracker = new RetryingTimeTracker();
    this.rpcTimeout = rpcTimeout;
  }

  @Override
  public void cancel(){
    cancelled.set(true);
    synchronized (cancelled){
      cancelled.notifyAll();
    }
  }

  @Override
  public T callWithRetries(RetryingCallable<T> callable, int callTimeout)
  throws IOException, RuntimeException {
    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions = new ArrayList<>();
    tracker.start();
    context.clear();
    for (int tries = 0;; tries++) {
      long expectedSleep;
      try {
        // bad cache entries are cleared in the call to RetryingCallable#throwable() in catch block
        callable.prepare(tries != 0);
        interceptor.intercept(context.prepare(callable, tries));
        return callable.call(getTimeout(callTimeout));
      } catch (PreemptiveFastFailException e) {
        throw e;
      } catch (Throwable t) {
        ExceptionUtil.rethrowIfInterrupt(t);
        Throwable cause = t.getCause();
        if (cause instanceof DoNotRetryIOException) {
          // Fail fast
          throw (DoNotRetryIOException) cause;
        }
        // translateException throws exception when should not retry: i.e. when request is bad.
        interceptor.handleFailure(context, t);
        t = translateException(t);

        if (tries > startLogErrorsCnt) {
          if (LOG.isInfoEnabled()) {
            StringBuilder builder = new StringBuilder("Call exception, tries=").append(tries)
                .append(", retries=").append(maxAttempts).append(", started=")
                .append((EnvironmentEdgeManager.currentTime() - tracker.getStartTime()))
                .append(" ms ago, ").append("cancelled=").append(cancelled.get())
                .append(", msg=").append(t.getMessage())
                .append(", details=").append(callable.getExceptionMessageAdditionalDetail())
                .append(", see https://s.apache.org/timeout");
            if (LOG.isDebugEnabled()) {
              builder.append(", exception=").append(StringUtils.stringifyException(t));
              LOG.debug(builder.toString());
            } else {
              LOG.info(builder.toString());
            }
          }
        }

        callable.throwable(t, maxAttempts != 1);
        RetriesExhaustedException.ThrowableWithExtraContext qt =
            new RetriesExhaustedException.ThrowableWithExtraContext(t,
                EnvironmentEdgeManager.currentTime(), toString());
        exceptions.add(qt);
        if (tries >= maxAttempts - 1) {
          throw new RetriesExhaustedException(tries, exceptions);
        }
        // If the server is dead, we need to wait a little before retrying, to give
        // a chance to the regions to be moved
        // get right pause time, start by RETRY_BACKOFF[0] * pauseBase, where pauseBase might be
        // special when encountering CallQueueTooBigException, see #HBASE-17114
        long pauseBase = (t instanceof CallQueueTooBigException) ? pauseForCQTBE : pause;
        expectedSleep = callable.sleep(pauseBase, tries);

        // If, after the planned sleep, there won't be enough time left, we stop now.
        long duration = singleCallDuration(expectedSleep);
        if (duration > callTimeout) {
          String msg = "callTimeout=" + callTimeout + ", callDuration=" + duration +
              ": " +  t.getMessage() + " " + callable.getExceptionMessageAdditionalDetail();
          throw (SocketTimeoutException)(new SocketTimeoutException(msg).initCause(t));
        }
      } finally {
        interceptor.updateFailureInfo(context);
      }
      try {
        if (expectedSleep > 0) {
          synchronized (cancelled) {
            if (cancelled.get()) return null;
            cancelled.wait(expectedSleep);
          }
        }
        if (cancelled.get()) return null;
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted after " + tries
            + " tries while maxAttempts=" + maxAttempts);
      }
    }
  }

  /**
   * @return Calculate how long a single call took
   */
  private long singleCallDuration(final long expectedSleep) {
    return (EnvironmentEdgeManager.currentTime() - tracker.getStartTime()) + expectedSleep;
  }

  @Override
  public T callWithoutRetries(RetryingCallable<T> callable, int callTimeout)
  throws IOException, RuntimeException {
    // The code of this method should be shared with withRetries.
    try {
      callable.prepare(false);
      return callable.call(callTimeout);
    } catch (Throwable t) {
      Throwable t2 = translateException(t);
      ExceptionUtil.rethrowIfInterrupt(t2);
      // It would be nice to clear the location cache here.
      if (t2 instanceof IOException) {
        throw (IOException)t2;
      } else {
        throw new RuntimeException(t2);
      }
    }
  }

  /**
   * Get the good or the remote exception if any, throws the DoNotRetryIOException.
   * @param t the throwable to analyze
   * @return the translated exception, if it's not a DoNotRetryIOException
   * @throws DoNotRetryIOException - if we find it, we throw it instead of translating.
   */
  static Throwable translateException(Throwable t) throws DoNotRetryIOException {
    if (t instanceof UndeclaredThrowableException) {
      if (t.getCause() != null) {
        t = t.getCause();
      }
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException)t).unwrapRemoteException();
    }
    if (t instanceof LinkageError) {
      throw new DoNotRetryIOException(t);
    }
    if (t instanceof ServiceException) {
      ServiceException se = (ServiceException)t;
      Throwable cause = se.getCause();
      if (cause instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException)cause;
      }
      // Don't let ServiceException out; its rpc specific.
      // It also could be a RemoteException, so go around again.
      t = translateException(cause);
    } else if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException)t;
    }
    return t;
  }

  private int getTimeout(int callTimeout){
    int timeout = tracker.getRemainingTime(callTimeout);
    if (timeout <= 0 || rpcTimeout > 0 && rpcTimeout < timeout){
      timeout = rpcTimeout;
    }
    return timeout;
  }

  @Override
  public String toString() {
    return "RpcRetryingCaller{" + "globalStartTime=" +
      Instant.ofEpochMilli(tracker.getStartTime()) +
        ", pause=" + pause + ", maxAttempts=" + maxAttempts + '}';
  }
}
