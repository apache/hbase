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

import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.SyncFailedException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestFastFailWithoutTestUtil {
  private static final Log LOG = LogFactory.getLog(TestFastFailWithoutTestUtil.class);

  @Test
  public void testInterceptorFactoryMethods() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    RetryingCallerInterceptorFactory interceptorFactory = new RetryingCallerInterceptorFactory(
        conf);

    RetryingCallerInterceptor interceptorBeforeCast = interceptorFactory
        .build();
    assertTrue("We should be getting a PreemptiveFastFailInterceptor",
        interceptorBeforeCast instanceof PreemptiveFastFailInterceptor);
    PreemptiveFastFailInterceptor interceptor = (PreemptiveFastFailInterceptor) interceptorBeforeCast;

    RetryingCallerInterceptorContext contextBeforeCast = interceptor
        .createEmptyContext();
    assertTrue(
        "We should be getting a FastFailInterceptorContext since we are interacting with the"
            + " PreemptiveFastFailInterceptor",
        contextBeforeCast instanceof FastFailInterceptorContext);

    FastFailInterceptorContext context = (FastFailInterceptorContext) contextBeforeCast;
    assertTrue(context != null);

    conf = HBaseConfiguration.create();
    interceptorFactory = new RetryingCallerInterceptorFactory(conf);

    interceptorBeforeCast = interceptorFactory.build();
    assertTrue(
        "We should be getting a NoOpRetryableCallerInterceptor since we disabled PFFE",
        interceptorBeforeCast instanceof NoOpRetryableCallerInterceptor);

    contextBeforeCast = interceptorBeforeCast.createEmptyContext();
    assertTrue(
        "We should be getting a NoOpRetryingInterceptorContext from NoOpRetryableCallerInterceptor",
        contextBeforeCast instanceof NoOpRetryingInterceptorContext);

    assertTrue(context != null);
  }

  @Test
  public void testInterceptorContextClear() {
    PreemptiveFastFailInterceptor interceptor = createPreemptiveInterceptor();
    FastFailInterceptorContext context = (FastFailInterceptorContext) interceptor
        .createEmptyContext();
    context.clear();
    assertFalse(context.getCouldNotCommunicateWithServer().booleanValue());
    assertEquals(context.didTry(), false);
    assertEquals(context.getFailureInfo(), null);
    assertEquals(context.getServer(), null);
    assertEquals(context.getTries(), 0);
  }

  @Test
  public void testInterceptorContextPrepare() throws IOException {
    PreemptiveFastFailInterceptor interceptor = TestFastFailWithoutTestUtil
        .createPreemptiveInterceptor();
    FastFailInterceptorContext context = (FastFailInterceptorContext) interceptor
        .createEmptyContext();
    RetryingCallable<?> callable = new RegionServerCallable<Boolean>(null,
        null, null) {
      @Override
      public Boolean call(int callTimeout) throws Exception {
        return true;
      }

      @Override
      protected HRegionLocation getLocation() {
        return new HRegionLocation(null, ServerName.valueOf("localhost", 1234,
            987654321));
      }
    };
    context.prepare(callable);
    ServerName server = getSomeServerName();
    assertEquals(context.getServer(), server);
    context.clear();
    context.prepare(callable, 2);
    assertEquals(context.getServer(), server);
  }

  @Test
  public void testInterceptorIntercept50Times() throws IOException,
      InterruptedException {
    for (int i = 0; i < 50; i++) {
      testInterceptorIntercept();
    }
  }

  public void testInterceptorIntercept() throws IOException,
      InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    long CLEANUP_TIMEOUT = 50;
    long FAST_FAIL_THRESHOLD = 10;
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_CLEANUP_MS_DURATION_MS,
        CLEANUP_TIMEOUT);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS,
        FAST_FAIL_THRESHOLD);

    PreemptiveFastFailInterceptor interceptor = TestFastFailWithoutTestUtil
        .createPreemptiveInterceptor(conf);
    FastFailInterceptorContext context = (FastFailInterceptorContext) interceptor
        .createEmptyContext();

    RetryingCallable<?> callable = getDummyRetryingCallable(getSomeServerName());

    // Lets simulate some work flow here.
    int tries = 0;
    context.prepare(callable, tries);
    interceptor.intercept(context);
    interceptor.handleFailure(context, new ConnectException(
        "Failed to connect to server"));
    interceptor.updateFailureInfo(context);
    assertTrue("Interceptor should have updated didTry to true",
        context.didTry());
    assertTrue(
        "The call shouldn't have been successful if there was a ConnectException",
        context.getCouldNotCommunicateWithServer().booleanValue());
    assertNull(
        "Once a failure is identified, the first time the FailureInfo is generated for the server,"
            + " but it is not assigned to the context yet. It would be assigned on the next"
            + " intercept.", context.getFailureInfo());
    assertEquals(context.getTries(), tries);
    assertFalse(
        "We are still in the first attempt and so we dont set this variable to true yet.",
        context.isRetryDespiteFastFailMode());

    Thread.sleep(FAST_FAIL_THRESHOLD + 1); // We sleep so as to make sure that
                                           // we
    // actually consider this server as a
    // dead server in the next attempt.
    tries++;

    context.prepare(callable, tries);
    interceptor.intercept(context);
    interceptor.handleFailure(context, new ConnectException(
        "Failed to connect to server"));
    interceptor.updateFailureInfo(context);
    assertTrue("didTru should remain true", context.didTry());
    assertTrue(
        "The call shouldn't have been successful if there was a ConnectException",
        context.getCouldNotCommunicateWithServer().booleanValue());
    assertNotNull(
        "The context this time is updated with a failureInfo, since we already gave it a try.",
        context.getFailureInfo());
    assertEquals(context.getTries(), tries);
    assertTrue(
        "Since we are alone here we would be given the permission to retryDespiteFailures.",
        context.isRetryDespiteFastFailMode());
    context.clear();

    Thread.sleep(CLEANUP_TIMEOUT); // Lets try and cleanup the data in the fast
                                   // fail failure maps.

    tries++;

    context.clear();
    context.prepare(callable, tries);
    interceptor.occasionallyCleanupFailureInformation();
    assertNull("The cleanup should have cleared the server",
        interceptor.repeatedFailuresMap.get(context.getServer()));
    interceptor.intercept(context);
    interceptor.handleFailure(context, new ConnectException(
        "Failed to connect to server"));
    interceptor.updateFailureInfo(context);
    assertTrue("didTru should remain true", context.didTry());
    assertTrue(
        "The call shouldn't have been successful if there was a ConnectException",
        context.getCouldNotCommunicateWithServer().booleanValue());
    assertNull("The failureInfo is cleared off from the maps.",
        context.getFailureInfo());
    assertEquals(context.getTries(), tries);
    assertFalse(
        "Since we are alone here we would be given the permission to retryDespiteFailures.",
        context.isRetryDespiteFastFailMode());
    context.clear();

  }

  private <T> RetryingCallable<T> getDummyRetryingCallable(
      ServerName someServerName) {
    return new RegionServerCallable<T>(null, null, null) {
      @Override
      public T call(int callTimeout) throws Exception {
        return null;
      }

      @Override
      protected HRegionLocation getLocation() {
        return new HRegionLocation(null, serverName);
      }
    };
  }

  @Test
  public void testExceptionsIdentifiedByInterceptor() throws IOException {
    Throwable[] networkexceptions = new Throwable[] {
        new ConnectException("Mary is unwell"),
        new SocketTimeoutException("Mike is too late"),
        new ClosedChannelException(),
        new SyncFailedException("Dave is not on the same page"),
        new TimeoutException("Mike is late again"),
        new EOFException("This is the end... "),
        new ConnectionClosingException("Its closing") };
    final String INDUCED = "Induced";
    Throwable[] nonNetworkExceptions = new Throwable[] {
        new IOException("Bob died"),
        new RemoteException("Bob's cousin died", null),
        new NoSuchMethodError(INDUCED), new NullPointerException(INDUCED),
        new DoNotRetryIOException(INDUCED), new Error(INDUCED) };

    Configuration conf = HBaseConfiguration.create();
    long CLEANUP_TIMEOUT = 0;
    long FAST_FAIL_THRESHOLD = 1000000;
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_CLEANUP_MS_DURATION_MS,
        CLEANUP_TIMEOUT);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS,
        FAST_FAIL_THRESHOLD);
    for (Throwable e : networkexceptions) {
      PreemptiveFastFailInterceptor interceptor = TestFastFailWithoutTestUtil
          .createPreemptiveInterceptor(conf);
      FastFailInterceptorContext context = (FastFailInterceptorContext) interceptor
          .createEmptyContext();

      RetryingCallable<?> callable = getDummyRetryingCallable(getSomeServerName());
      context.prepare(callable, 0);
      interceptor.intercept(context);
      interceptor.handleFailure(context, e);
      interceptor.updateFailureInfo(context);
      assertTrue(
          "The call shouldn't have been successful if there was a ConnectException",
          context.getCouldNotCommunicateWithServer().booleanValue());
    }
    for (Throwable e : nonNetworkExceptions) {
      try {
        PreemptiveFastFailInterceptor interceptor = TestFastFailWithoutTestUtil
            .createPreemptiveInterceptor(conf);
        FastFailInterceptorContext context = (FastFailInterceptorContext) interceptor
            .createEmptyContext();

        RetryingCallable<?> callable = getDummyRetryingCallable(getSomeServerName());
        context.prepare(callable, 0);
        interceptor.intercept(context);
        interceptor.handleFailure(context, e);
        interceptor.updateFailureInfo(context);
        assertFalse(
            "The call shouldn't have been successful if there was a ConnectException",
            context.getCouldNotCommunicateWithServer().booleanValue());
      } catch (NoSuchMethodError t) {
        assertTrue("Exception not induced", t.getMessage().contains(INDUCED));
      } catch (NullPointerException t) {
        assertTrue("Exception not induced", t.getMessage().contains(INDUCED));
      } catch (DoNotRetryIOException t) {
        assertTrue("Exception not induced", t.getMessage().contains(INDUCED));
      } catch (Error t) {
        assertTrue("Exception not induced", t.getMessage().contains(INDUCED));
      }
    }
  }

  protected static PreemptiveFastFailInterceptor createPreemptiveInterceptor(
      Configuration conf) {
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    RetryingCallerInterceptorFactory interceptorFactory = new RetryingCallerInterceptorFactory(
        conf);
    RetryingCallerInterceptor interceptorBeforeCast = interceptorFactory
        .build();
    return (PreemptiveFastFailInterceptor) interceptorBeforeCast;
  }

  static PreemptiveFastFailInterceptor createPreemptiveInterceptor() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    return createPreemptiveInterceptor(conf);
  }

  @Test(timeout = 120000)
  public void testPreemptiveFastFailException50Times()
      throws InterruptedException, ExecutionException {
    for (int i = 0; i < 50; i++) {
      testPreemptiveFastFailException();
    }
  }

  /***
   * This test tries to create a thread interleaving of the 2 threads trying to do a
   * Retrying operation using a {@link PreemptiveFastFailInterceptor}. The goal here is to make sure
   * that the second thread will be attempting the operation while the first thread is in the
   * process of making an attempt after it has marked the server in fast fail.
   *
   * The thread execution is as follows :
   * The PreemptiveFastFailInterceptor is extended in this test to achieve a good interleaving
   * behavior without using any thread sleeps.
   *
   *              Privileged Thread 1                         NonPrivileged Thread 2
   *
   *  Retry 0 :   intercept
   *
   *  Retry 0 :   handleFailure
   *                      latches[0].countdown
   *                      latches2[0].await
   *                                                                          latches[0].await
   *                                                    intercept                 : Retry 0
   *
   *                                                    handleFailure             : Retry 0
   *
   *                                                    updateFailureinfo         : Retry 0
   *                                                                          latches2[0].countdown
   *
   *  Retry 0 :   updateFailureInfo
   *
   *  Retry 1 : intercept
   *
   *  Retry 1 :   handleFailure
   *                      latches[1].countdown
   *                      latches2[1].await
   *
   *                                                                          latches[1].await
   *                                                    intercept                 : Retry 1
   *                                                        (throws PFFE)
   *                                                    handleFailure             : Retry 1
   *
   *                                                    updateFailureinfo         : Retry 1
   *                                                                          latches2[1].countdown
   *  Retry 1 :   updateFailureInfo
   *
   *
   *  See getInterceptor() for more details on the interceptor implementation to make sure this
   *  thread interleaving is achieved.
   *
   *  We need 2 sets of latches of size MAX_RETRIES. We use an AtomicInteger done to make sure that
   *  we short circuit the Thread 1 after we hit the PFFE on Thread 2
   *
   *
   * @throws InterruptedException
   * @throws ExecutionException
   */
  private void testPreemptiveFastFailException() throws InterruptedException,
      ExecutionException {
    LOG.debug("Setting up the counters to start the test");
    priviRetryCounter.set(0);
    nonPriviRetryCounter.set(0);
    done.set(0);

    for (int i = 0; i <= RETRIES; i++) {
      latches[i] = new CountDownLatch(1);
      latches2[i] = new CountDownLatch(1);
    }

    PreemptiveFastFailInterceptor interceptor = getInterceptor();

    final RpcRetryingCaller<Void> priviCaller = getRpcRetryingCaller(
        PAUSE_TIME, RETRIES, interceptor);
    final RpcRetryingCaller<Void> nonPriviCaller = getRpcRetryingCaller(
        PAUSE_TIME, RETRIES, interceptor);

    LOG.debug("Submitting the thread 1");
    Future<Boolean> priviFuture = executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          isPriviThreadLocal.get().set(true);
          priviCaller
              .callWithRetries(
                  getRetryingCallable(serverName, exception),
                  CLEANUP_TIMEOUT);
        } catch (RetriesExhaustedException e) {
          return true;
        } catch (PreemptiveFastFailException e) {
          return false;
        }
        return false;
      }
    });
    LOG.debug("Submitting the thread 2");
    Future<Boolean> nonPriviFuture = executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          isPriviThreadLocal.get().set(false);
          nonPriviCaller.callWithRetries(
              getRetryingCallable(serverName, exception),
              CLEANUP_TIMEOUT);
        } catch (PreemptiveFastFailException e) {
          return true;
        }
        return false;
      }
    });
    LOG.debug("Waiting for Thread 2 to finish");
    assertTrue(nonPriviFuture.get());
    LOG.debug("Waiting for Thread 1 to finish");
    assertTrue(priviFuture.get());

    // Now that the server in fast fail mode. Lets try to make contact with the
    // server with a third thread. And make sure that when there is no
    // exception,
    // the fast fail gets cleared up.
    assertTrue(interceptor.isServerInFailureMap(serverName));
    final RpcRetryingCaller<Void> priviCallerNew = getRpcRetryingCaller(
        PAUSE_TIME, RETRIES, interceptor);
    executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        priviCallerNew.callWithRetries(
            getRetryingCallable(serverName, null), CLEANUP_TIMEOUT);
        return false;
      }
    }).get();
    assertFalse("The server was supposed to be removed from the map",
        interceptor.isServerInFailureMap(serverName));
  }

  ExecutorService executor = Executors.newCachedThreadPool();

  /**
   * Some timeouts to make the test execution resonable.
   */
  final int PAUSE_TIME = 10;
  final int RETRIES = 3;
  final int CLEANUP_TIMEOUT = 10000;
  final long FAST_FAIL_THRESHOLD = PAUSE_TIME / 1;

  /**
   * The latches necessary to make the thread interleaving possible.
   */
  final CountDownLatch[] latches = new CountDownLatch[RETRIES + 1];
  final CountDownLatch[] latches2 = new CountDownLatch[RETRIES + 1];
  final AtomicInteger done = new AtomicInteger(0);

  /**
   * Global retry counters that give us an idea about which iteration of the retry we are in
   */
  final AtomicInteger priviRetryCounter = new AtomicInteger();
  final AtomicInteger nonPriviRetryCounter = new AtomicInteger();
  final ServerName serverName = getSomeServerName();

  /**
   * The variable which is used as an identifier within the 2 threads.
   */
  public final ThreadLocal<AtomicBoolean> isPriviThreadLocal = new ThreadLocal<AtomicBoolean>() {
    @Override
    public AtomicBoolean initialValue() {
      return new AtomicBoolean(true);
    }
  };
  final Exception exception = new ConnectionClosingException("The current connection is closed");

  public PreemptiveFastFailInterceptor getInterceptor() {
    final Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED, true);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_CLEANUP_MS_DURATION_MS,
        CLEANUP_TIMEOUT);
    conf.setLong(HConstants.HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS,
        FAST_FAIL_THRESHOLD);

    return new PreemptiveFastFailInterceptor(
        conf) {
      @Override
      public void updateFailureInfo(RetryingCallerInterceptorContext context) {
        boolean pffe = false;
        if (!isPriviThreadLocal.get().get()) {
          pffe = !((FastFailInterceptorContext)context).isRetryDespiteFastFailMode();
        }
        if (isPriviThreadLocal.get().get()) {
          try {
            // Thread 2 should be done by 2 iterations. We should short circuit Thread 1 because
            // Thread 2 would be dead and can't do a countdown.
            if (done.get() <= 1) {
              latches2[priviRetryCounter.get()].await();
            }
          } catch (InterruptedException e) {
            fail();
          }
        }
        super.updateFailureInfo(context);
        if (!isPriviThreadLocal.get().get()) {
          if (pffe) done.incrementAndGet();
          latches2[nonPriviRetryCounter.get()].countDown();
        }
      }

      @Override
      public void intercept(RetryingCallerInterceptorContext context)
          throws PreemptiveFastFailException {
        if (!isPriviThreadLocal.get().get()) {
          try {
            latches[nonPriviRetryCounter.getAndIncrement()].await();
          } catch (InterruptedException e) {
            fail();
          }
        }
        super.intercept(context);
      }

      @Override
      public void handleFailure(RetryingCallerInterceptorContext context,
          Throwable t) throws IOException {
        super.handleFailure(context, t);
        if (isPriviThreadLocal.get().get()) {
          latches[priviRetryCounter.getAndIncrement()].countDown();
        }
      }
    };
  }

  public RpcRetryingCaller<Void> getRpcRetryingCaller(int pauseTime,
      int retries, RetryingCallerInterceptor interceptor) {
    return new RpcRetryingCaller<Void>(pauseTime, retries, interceptor, 9) {
      @Override
      public Void callWithRetries(RetryingCallable<Void> callable,
          int callTimeout) throws IOException, RuntimeException {
        Void ret = super.callWithRetries(callable, callTimeout);
        return ret;
      }
    };
  }

  protected static ServerName getSomeServerName() {
    return ServerName.valueOf("localhost", 1234, 987654321);
  }

  private RegionServerCallable<Void> getRetryingCallable(
      final ServerName serverName, final Exception e) {
    return new RegionServerCallable<Void>(null, null, null) {
      @Override
      public void prepare(boolean reload) throws IOException {
        this.location = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO,
            serverName);
      }

      @Override
      public Void call(int callTimeout) throws Exception {
        if (e != null)
          throw e;
        return null;
      }

      @Override
      protected HRegionLocation getLocation() {
        return new HRegionLocation(null, serverName);
      }

      @Override
      public void throwable(Throwable t, boolean retrying) {
        // Do nothing
      }

      @Override
      public long sleep(long pause, int tries) {
        return ConnectionUtils.getPauseTime(pause, tries + 1);
      }
    };
  }
}
