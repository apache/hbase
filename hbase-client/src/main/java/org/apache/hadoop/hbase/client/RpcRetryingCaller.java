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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ServiceException;

/**
 * Runs an rpc'ing {@link RetryingCallable}. Sets into rpc client
 * threadlocal outstanding timeouts as so we don't persist too much.
 * Dynamic rather than static so can set the generic appropriately.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings
    (value = "IS2_INCONSISTENT_SYNC", justification = "na")
public class RpcRetryingCaller<T> {
  static final Log LOG = LogFactory.getLog(RpcRetryingCaller.class);
  /**
   * Timeout for the call including retries
   */
  private int callTimeout;
  /**
   * When we started making calls.
   */
  private long globalStartTime;
  /**
   * Start and end times for a single call.
   */
  private final static int MIN_RPC_TIMEOUT = 2000;

  private final long pause;
  private final int retries;

  public RpcRetryingCaller(long pause, int retries) {
    this.pause = pause;
    this.retries = retries;
  }

  private void beforeCall() {
    int remaining = (int)(callTimeout -
      (EnvironmentEdgeManager.currentTimeMillis() - this.globalStartTime));
    if (remaining < MIN_RPC_TIMEOUT) {
      // If there is no time left, we're trying anyway. It's too late.
      // 0 means no timeout, and it's not the intent here. So we secure both cases by
      // resetting to the minimum.
      remaining = MIN_RPC_TIMEOUT;
    }
    RpcClient.setRpcTimeout(remaining);
  }

  private void afterCall() {
    RpcClient.resetRpcTimeout();
  }

  public synchronized T callWithRetries(RetryingCallable<T> callable) throws IOException,
      RuntimeException {
    return callWithRetries(callable, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  /**
   * Retries if invocation fails.
   * @param callTimeout Timeout for this call
   * @param callable The {@link RetryingCallable} to run.
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings
      (value = "SWL_SLEEP_WITH_LOCK_HELD", justification = "na")
  public synchronized T callWithRetries(RetryingCallable<T> callable, int callTimeout)
  throws IOException, RuntimeException {
    this.callTimeout = callTimeout;
    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions =
      new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
    this.globalStartTime = EnvironmentEdgeManager.currentTimeMillis();
    for (int tries = 0;; tries++) {
      long expectedSleep = 0;
      try {
        beforeCall();
        callable.prepare(tries != 0); // if called with false, check table status on ZK
        return callable.call();
      } catch (Throwable t) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Call exception, tries=" + tries + ", retries=" + retries + ", retryTime=" +
              (EnvironmentEdgeManager.currentTimeMillis() - this.globalStartTime) + "ms", t);
        }
        // translateException throws exception when should not retry: i.e. when request is bad.
        t = translateException(t);
        callable.throwable(t, retries != 1);
        RetriesExhaustedException.ThrowableWithExtraContext qt =
            new RetriesExhaustedException.ThrowableWithExtraContext(t,
                EnvironmentEdgeManager.currentTimeMillis(), toString());
        exceptions.add(qt);
        ExceptionUtil.rethrowIfInterrupt(t);
        if (tries >= retries - 1) {
          throw new RetriesExhaustedException(tries, exceptions);
        }
        // If the server is dead, we need to wait a little before retrying, to give
        //  a chance to the regions to be
        // tries hasn't been bumped up yet so we use "tries + 1" to get right pause time
        expectedSleep = callable.sleep(pause, tries + 1);

        // If, after the planned sleep, there won't be enough time left, we stop now.
        long duration = singleCallDuration(expectedSleep);
        if (duration > this.callTimeout) {
          String msg = "callTimeout=" + this.callTimeout + ", callDuration=" + duration +
              ": " + callable.getExceptionMessageAdditionalDetail();
          throw (SocketTimeoutException)(new SocketTimeoutException(msg).initCause(t));
        }
      } finally {
        afterCall();
      }
      try {
        Thread.sleep(expectedSleep);
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted after " + tries + " tries  on " + retries);
      }
    }
  }

  /**
   * @param expectedSleep
   * @return Calculate how long a single call took
   */
  private long singleCallDuration(final long expectedSleep) {
    return (EnvironmentEdgeManager.currentTimeMillis() - this.globalStartTime)
      + MIN_RPC_TIMEOUT + expectedSleep;
  }

  /**
   * Call the server once only.
   * {@link RetryingCallable} has a strange shape so we can do retrys.  Use this invocation if you
   * want to do a single call only (A call to {@link RetryingCallable#call()} will not likely
   * succeed).
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public T callWithoutRetries(RetryingCallable<T> callable, int callTimeout)
  throws IOException, RuntimeException {
    // The code of this method should be shared with withRetries.
    this.globalStartTime = EnvironmentEdgeManager.currentTimeMillis();
    this.callTimeout = callTimeout;
    try {
      beforeCall();
      callable.prepare(false);
      return callable.call();
    } catch (Throwable t) {
      Throwable t2 = translateException(t);
      ExceptionUtil.rethrowIfInterrupt(t2);
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
      if (cause != null && cause instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException)cause;
      }
      // Don't let ServiceException out; its rpc specific.
      t = cause;
      // t could be a RemoteException so go aaround again.
      translateException(t);
    } else if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException)t;
    }
    return t;
  }
}
