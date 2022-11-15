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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
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

  private void itUsesSpecialPauseForServerOverloaded(
    Class<? extends HBaseServerException> exceptionClass) throws Exception {

    // the actual values don't matter here as long as they're distinct.
    // the ThrowingCallable will assert that the passed in pause from RpcRetryingCallerImpl
    // matches the specialPauseMillis
    long pauseMillis = 1;
    long specialPauseMillis = 2;

    RpcRetryingCallerImpl<Void> caller = new RpcRetryingCallerImpl<>(pauseMillis,
      specialPauseMillis, 2, RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, 0, 0, null);

    RetryingCallable<Void> callable =
      new ThrowingCallable(CallQueueTooBigException.class, specialPauseMillis);
    try {
      caller.callWithRetries(callable, 5000);
      fail("Expected " + exceptionClass.getSimpleName());
    } catch (RetriesExhaustedException e) {
      assertTrue(e.getCause() instanceof HBaseServerException);
    }
  }

  private static class ThrowingCallable implements RetryingCallable<Void> {
    private final Class<? extends HBaseServerException> exceptionClass;
    private final long specialPauseMillis;

    public ThrowingCallable(Class<? extends HBaseServerException> exceptionClass,
      long specialPauseMillis) {
      this.exceptionClass = exceptionClass;
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
      assertEquals(pause, specialPauseMillis);
      return 0;
    }

    @Override
    public Void call(int callTimeout) throws Exception {
      throw exceptionClass.getConstructor().newInstance();
    }
  }
}
