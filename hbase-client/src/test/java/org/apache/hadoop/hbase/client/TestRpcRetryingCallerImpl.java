/*
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

import static org.hamcrest.MatcherAssert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestRpcRetryingCallerImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcRetryingCallerImpl.class);

  @Test
  public void itUsesSpecialPauseForCQTBE() throws Exception {
    itUsesSpecialPauseForServerOverloaded(CallQueueTooBigException.class);
  }

  @Test
  public void itUsesSpecialPauseForCDE() throws Exception {
    itUsesSpecialPauseForServerOverloaded(CallDroppedException.class);
  }

  @Test
  public void itUsesSpecialPauseForThrottleException() throws Exception {
    itUsesSpecialPauseForServerOverloaded(RpcThrottlingException.class);
  }

  private void itUsesSpecialPauseForServerOverloaded(Class<? extends Exception> exceptionClass)
    throws Exception {

    // the actual values don't matter here as long as they're distinct.
    // the ThrowingCallable will assert that the passed in pause from RpcRetryingCallerImpl
    // matches the specialPauseMillis
    long pauseMillis = 1;
    long specialPauseMillis = 2;

    ScanMetrics scanMetrics = new ScanMetrics();
    RpcRetryingCallerImpl<Void> caller =
      new RpcRetryingCallerImpl<>(pauseMillis, specialPauseMillis, 2,
        RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, 0, 0, null, scanMetrics);

    RetryingCallable<Void> callable;
    if (HBaseServerException.class.isAssignableFrom(exceptionClass)) {
      callable = new ThrowingCallable(() -> construct(exceptionClass), specialPauseMillis);
    } else if (exceptionClass.equals(RpcThrottlingException.class)) {
      callable = new ThrowingCallable(() -> new RpcThrottlingException(
        RpcThrottlingException.Type.NumReadRequestsExceeded, 1000, "test"), -1);
    } else {
      throw new RuntimeException("Unexpected exceptionClass type " + exceptionClass.getName());
    }

    long start = System.currentTimeMillis();
    try {
      caller.callWithRetries(callable, 5000);
      fail("Expected " + exceptionClass.getSimpleName());
    } catch (RetriesExhaustedException e) {
      assertEquals(e.getCause().getClass(), exceptionClass);
      long duration = System.currentTimeMillis() - start;
      // we set a waitTime of 1s for the throttle exception above
      // other exceptions only have a backoff time of 2ms. we can validate that we backoff
      // appropriately by using a conservative in the middle threshold of 500ms.
      if (e.getCause() instanceof RpcThrottlingException) {
        Matcher<Long> durationMatcher = Matchers.greaterThan(500L);
        assertThat(duration, durationMatcher);
        // expect to see it accumulated in the scanMetrics as well.
        assertThat(scanMetrics.throttleTime.get(), durationMatcher);
      } else {
        Matcher<Long> durationMatcher =
          Matchers.allOf(Matchers.lessThan(500L), Matchers.greaterThan(0L));
        assertThat(duration, durationMatcher);
        // expect to see it accumulated in the scanMetrics as well.
        assertThat(scanMetrics.throttleTime.get(), durationMatcher);
      }
    }
  }

  private <T extends Exception> T construct(Class<T> clazz) {
    try {
      return clazz.getConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class ThrowingCallable implements RetryingCallable<Void> {
    private final Supplier<? extends Exception> exceptionSupplier;
    private final long specialPauseMillis;

    public ThrowingCallable(Supplier<? extends Exception> exceptionSupplier,
      long specialPauseMillis) {
      this.exceptionSupplier = exceptionSupplier;
      this.specialPauseMillis = specialPauseMillis;
    }

    @Override
    public void prepare(boolean reload) throws IOException {

    }

    @Override
    public void throwable(Throwable t, boolean retrying) {

    }

    @Override
    public String getExceptionMessageAdditionalDetail() {
      return null;
    }

    @Override
    public long sleep(long pause, int tries) {
      if (specialPauseMillis < 0) {
        fail("Should not have called with a sleep, but got " + pause + " and tries=" + tries);
      } else {
        assertEquals(pause, specialPauseMillis);
      }
      return 0;
    }

    @Override
    public Void call(int callTimeout) throws Exception {
      throw exceptionSupplier.get();
    }
  }
}
