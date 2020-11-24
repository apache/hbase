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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The concrete {@link RetryingCallerInterceptor} class that implements the preemptive fast fail
 * feature.
 * <p>
 * The motivation is as follows : In case where a large number of clients try and talk to a
 * particular region server in hbase, if the region server goes down due to network problems, we
 * might end up in a scenario where the clients would go into a state where they all start to retry.
 * This behavior will set off many of the threads in pretty much the same path and they all would be
 * sleeping giving rise to a state where the client either needs to create more threads to send new
 * requests to other hbase machines or block because the client cannot create anymore threads.
 * <p>
 * In most cases the clients might prefer to have a bound on the number of threads that are created
 * in order to send requests to hbase. This would mostly result in the client thread starvation.
 * <p>
 * To circumvent this problem, the approach that is being taken here under is to let 1 of the many
 * threads who are trying to contact the regionserver with connection problems and let the other
 * threads get a {@link PreemptiveFastFailException} so that they can move on and take other
 * requests.
 * <p>
 * This would give the client more flexibility on the kind of action he would want to take in cases
 * where the regionserver is down. He can either discard the requests and send a nack upstream
 * faster or have an application level retry or buffer the requests up so as to send them down to
 * hbase later.
 */
@InterfaceAudience.Private
class PreemptiveFastFailInterceptor extends RetryingCallerInterceptor {

  private static final Logger LOG = LoggerFactory
      .getLogger(PreemptiveFastFailInterceptor.class);

  // amount of time to wait before we consider a server to be in fast fail
  // mode
  protected final long fastFailThresholdMilliSec;

  // Keeps track of failures when we cannot talk to a server. Helps in
  // fast failing clients if the server is down for a long time.
  protected final ConcurrentMap<ServerName, FailureInfo> repeatedFailuresMap = new ConcurrentHashMap<>();

  // We populate repeatedFailuresMap every time there is a failure. So, to
  // keep it from growing unbounded, we garbage collect the failure information
  // every cleanupInterval.
  protected final long failureMapCleanupIntervalMilliSec;

  protected volatile long lastFailureMapCleanupTimeMilliSec;

  // clear failure Info. Used to clean out all entries.
  // A safety valve, in case the client does not exit the
  // fast fail mode for any reason.
  private long fastFailClearingTimeMilliSec;

  private final ThreadLocal<MutableBoolean> threadRetryingInFastFailMode = new ThreadLocal<>();

  public PreemptiveFastFailInterceptor(Configuration conf) {
    this.fastFailThresholdMilliSec = conf.getLong(
        HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS,
        HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS_DEFAULT);
    this.failureMapCleanupIntervalMilliSec = conf.getLong(
            HConstants.HBASE_CLIENT_FAILURE_MAP_CLEANUP_INTERVAL_MS,
            HConstants.HBASE_CLIENT_FAILURE_MAP_CLEANUP_INTERVAL_MS_DEFAULT);
    this.fastFailClearingTimeMilliSec = conf.getLong(
            HConstants.HBASE_CLIENT_FAST_FAIL_CLEANUP_MS_DURATION_MS,
            HConstants.HBASE_CLIENT_FAST_FAIL_CLEANUP_DURATION_MS_DEFAULT);
    lastFailureMapCleanupTimeMilliSec = EnvironmentEdgeManager.currentTime();
  }

  public void intercept(FastFailInterceptorContext context)
      throws PreemptiveFastFailException {
    context.setFailureInfo(repeatedFailuresMap.get(context.getServer()));
    if (inFastFailMode(context.getServer()) && !currentThreadInFastFailMode()) {
      // In Fast-fail mode, all but one thread will fast fail. Check
      // if we are that one chosen thread.
      context.setRetryDespiteFastFailMode(shouldRetryInspiteOfFastFail(context
          .getFailureInfo()));
      if (!context.isRetryDespiteFastFailMode()) { // we don't have to retry
        LOG.debug("Throwing PFFE : " + context.getFailureInfo() + " tries : "
            + context.getTries());
        throw new PreemptiveFastFailException(
            context.getFailureInfo().numConsecutiveFailures.get(),
            context.getFailureInfo().timeOfFirstFailureMilliSec,
            context.getFailureInfo().timeOfLatestAttemptMilliSec, context.getServer(),
            context.getGuaranteedClientSideOnly().isTrue());
      }
    }
    context.setDidTry(true);
  }

  public void handleFailure(FastFailInterceptorContext context,
      Throwable t) throws IOException {
    handleThrowable(t, context.getServer(),
        context.getCouldNotCommunicateWithServer(),
        context.getGuaranteedClientSideOnly());
  }

  public void updateFailureInfo(FastFailInterceptorContext context) {
    updateFailureInfoForServer(context.getServer(), context.getFailureInfo(),
        context.didTry(), context.getCouldNotCommunicateWithServer()
            .booleanValue(), context.isRetryDespiteFastFailMode());
  }

  /**
   * Handles failures encountered when communicating with a server.
   *
   * Updates the FailureInfo in repeatedFailuresMap to reflect the failure.
   * Throws RepeatedConnectException if the client is in Fast fail mode.
   *
   * @param serverName
   * @param t
   *          - the throwable to be handled.
   * @throws PreemptiveFastFailException
   */
  protected void handleFailureToServer(ServerName serverName, Throwable t) {
    if (serverName == null || t == null) {
      return;
    }
    long currentTime = EnvironmentEdgeManager.currentTime();
    FailureInfo fInfo =
        computeIfAbsent(repeatedFailuresMap, serverName, () -> new FailureInfo(currentTime));
    fInfo.timeOfLatestAttemptMilliSec = currentTime;
    fInfo.numConsecutiveFailures.incrementAndGet();
  }

  public void handleThrowable(Throwable t1, ServerName serverName,
      MutableBoolean couldNotCommunicateWithServer,
      MutableBoolean guaranteedClientSideOnly) throws IOException {
    Throwable t2 = ClientExceptionsUtil.translatePFFE(t1);
    boolean isLocalException = !(t2 instanceof RemoteException);

    if ((isLocalException && ClientExceptionsUtil.isConnectionException(t2))) {
      couldNotCommunicateWithServer.setValue(true);
      guaranteedClientSideOnly.setValue(!(t2 instanceof CallTimeoutException));
      handleFailureToServer(serverName, t2);
    }
  }

  /**
   * Occasionally cleans up unused information in repeatedFailuresMap.
   *
   * repeatedFailuresMap stores the failure information for all remote hosts
   * that had failures. In order to avoid these from growing indefinitely,
   * occassionallyCleanupFailureInformation() will clear these up once every
   * cleanupInterval ms.
   */
  protected void occasionallyCleanupFailureInformation() {
    long now = System.currentTimeMillis();
    if (!(now > lastFailureMapCleanupTimeMilliSec
        + failureMapCleanupIntervalMilliSec))
      return;

    // remove entries that haven't been attempted in a while
    // No synchronization needed. It is okay if multiple threads try to
    // remove the entry again and again from a concurrent hash map.
    StringBuilder sb = new StringBuilder();
    for (Entry<ServerName, FailureInfo> entry : repeatedFailuresMap.entrySet()) {
      if (now > entry.getValue().timeOfLatestAttemptMilliSec
          + failureMapCleanupIntervalMilliSec) { // no recent failures
        repeatedFailuresMap.remove(entry.getKey());
      } else if (now > entry.getValue().timeOfFirstFailureMilliSec
          + this.fastFailClearingTimeMilliSec) { // been failing for a long
                                                 // time
        LOG.error(entry.getKey()
            + " been failing for a long time. clearing out."
            + entry.getValue().toString());
        repeatedFailuresMap.remove(entry.getKey());
      } else {
        sb.append(entry.getKey().toString()).append(" failing ")
            .append(entry.getValue().toString()).append("\n");
      }
    }
    if (sb.length() > 0) {
      LOG.warn("Preemptive failure enabled for : " + sb.toString());
    }
    lastFailureMapCleanupTimeMilliSec = now;
  }

  /**
   * Checks to see if we are in the Fast fail mode for requests to the server.
   *
   * If a client is unable to contact a server for more than
   * fastFailThresholdMilliSec the client will get into fast fail mode.
   *
   * @param server
   * @return true if the client is in fast fail mode for the server.
   */
  private boolean inFastFailMode(ServerName server) {
    FailureInfo fInfo = repeatedFailuresMap.get(server);
    // if fInfo is null --> The server is considered good.
    // If the server is bad, wait long enough to believe that the server is
    // down.
    return (fInfo != null &&
        EnvironmentEdgeManager.currentTime() >
          (fInfo.timeOfFirstFailureMilliSec + this.fastFailThresholdMilliSec));
  }

  /**
   * Checks to see if the current thread is already in FastFail mode for *some*
   * server.
   *
   * @return true, if the thread is already in FF mode.
   */
  private boolean currentThreadInFastFailMode() {
    return (this.threadRetryingInFastFailMode.get() != null && (this.threadRetryingInFastFailMode
        .get().booleanValue() == true));
  }

  /**
   * Check to see if the client should try to connnect to the server, inspite of
   * knowing that it is in the fast fail mode.
   *
   * The idea here is that we want just one client thread to be actively trying
   * to reconnect, while all the other threads trying to reach the server will
   * short circuit.
   *
   * @param fInfo
   * @return true if the client should try to connect to the server.
   */
  protected boolean shouldRetryInspiteOfFastFail(FailureInfo fInfo) {
    // We believe that the server is down, But, we want to have just one
    // client
    // actively trying to connect. If we are the chosen one, we will retry
    // and not throw an exception.
    if (fInfo != null
        && fInfo.exclusivelyRetringInspiteOfFastFail.compareAndSet(false, true)) {
      MutableBoolean threadAlreadyInFF = this.threadRetryingInFastFailMode
          .get();
      if (threadAlreadyInFF == null) {
        threadAlreadyInFF = new MutableBoolean();
        this.threadRetryingInFastFailMode.set(threadAlreadyInFF);
      }
      threadAlreadyInFF.setValue(true);
      return true;
    } else {
      return false;
    }
  }

  /**
   *
   * This function updates the Failure info for a particular server after the
   * attempt to 
   *
   * @param server
   * @param fInfo
   * @param couldNotCommunicate
   * @param retryDespiteFastFailMode
   */
  private void updateFailureInfoForServer(ServerName server,
      FailureInfo fInfo, boolean didTry, boolean couldNotCommunicate,
      boolean retryDespiteFastFailMode) {
    if (server == null || fInfo == null || didTry == false)
      return;

    // If we were able to connect to the server, reset the failure
    // information.
    if (couldNotCommunicate == false) {
      LOG.info("Clearing out PFFE for server " + server);
      repeatedFailuresMap.remove(server);
    } else {
      // update time of last attempt
      long currentTime = System.currentTimeMillis();
      fInfo.timeOfLatestAttemptMilliSec = currentTime;

      // Release the lock if we were retrying inspite of FastFail
      if (retryDespiteFastFailMode) {
        fInfo.exclusivelyRetringInspiteOfFastFail.set(false);
        threadRetryingInFastFailMode.get().setValue(false);
      }
    }

    occasionallyCleanupFailureInformation();
  }

  @Override
  public void intercept(RetryingCallerInterceptorContext context)
      throws PreemptiveFastFailException {
    if (context instanceof FastFailInterceptorContext) {
      intercept((FastFailInterceptorContext) context);
    }
  }

  @Override
  public void handleFailure(RetryingCallerInterceptorContext context,
      Throwable t) throws IOException {
    if (context instanceof FastFailInterceptorContext) {
      handleFailure((FastFailInterceptorContext) context, t);
    }
  }

  @Override
  public void updateFailureInfo(RetryingCallerInterceptorContext context) {
    if (context instanceof FastFailInterceptorContext) {
      updateFailureInfo((FastFailInterceptorContext) context);
    }
  }

  @Override
  public RetryingCallerInterceptorContext createEmptyContext() {
    return new FastFailInterceptorContext();
  }

  protected boolean isServerInFailureMap(ServerName serverName) {
    return this.repeatedFailuresMap.containsKey(serverName);
  }

  @Override
  public String toString() {
    return "PreemptiveFastFailInterceptor";
  }
}
