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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestOperationInterceptor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOperationInterceptor.class);

  private static class MockOperationInterceptor extends OperationInterceptor {
    final List<String> events = new ArrayList<>();
    volatile Throwable lastException;
    volatile Object lastResult;

    public MockOperationInterceptor() {
      super();
    }

    @Override
    public void beforeAttempt(RetryingCallable<?> callable) {
      events.add("beforeAttempt");
    }

    @Override
    public void afterAttemptSuccess(RetryingCallable<?> callable, Object result) {
      events.add("afterAttemptSuccess");
      this.lastResult = result;
    }

    @Override
    public void afterAttemptFailure(RetryingCallable<?> callable, Throwable cause) {
      events.add("afterAttemptFailure");
      this.lastException = cause;
    }

    @Override
    public void afterOperation() {
      events.add("afterOperation");
    }
  }

  private static class TestOperationInterceptorFactory implements OperationInterceptorFactory {
    final AtomicInteger createCount = new AtomicInteger(0);

    @Override
    public OperationInterceptor createInterceptor() {
      createCount.incrementAndGet();
      return new MockOperationInterceptor();
    }
  }

  // Helper method to create RpcRetryingCallerImpl with test factory
  private RpcRetryingCallerImpl<String> createCaller(TestOperationInterceptorFactory factory) {
    return new RpcRetryingCallerImpl<>(100, 500, 3,
      RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, 0, 0, null, factory);
  }

  // Helper method to create RpcRetryingCallerImpl with fast retry settings
  private RpcRetryingCallerImpl<String>
    createFastRetryCaller(TestOperationInterceptorFactory factory) {
    return new RpcRetryingCallerImpl<>(10, 50, 2,
      RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, 0, 0, null, factory);
  }

  // Base RetryingCallable implementation with common no-op methods
  private static abstract class BaseRetryingCallable implements RetryingCallable<String> {
    @Override
    public void prepare(boolean reload) {
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
      return tries == 0 ? 1 : 0; // Short sleep for tests
    }
  }

  // Simple success callable
  private static class SuccessCallable extends BaseRetryingCallable {
    private final String result;

    SuccessCallable(String result) {
      this.result = result;
    }

    @Override
    public String call(int callTimeout) {
      return result;
    }
  }

  // Simple failure callable
  private static class FailureCallable extends BaseRetryingCallable {
    private final Exception exception;

    FailureCallable(Exception exception) {
      this.exception = exception;
    }

    @Override
    public String call(int callTimeout) throws Exception {
      throw exception;
    }
  }

  // Callable that fails on first attempt, succeeds on second
  private static class RetrySuccessCallable extends BaseRetryingCallable {
    private final AtomicInteger callCount = new AtomicInteger(0);
    private final String successResult;

    RetrySuccessCallable(String successResult) {
      this.successResult = successResult;
    }

    @Override
    public String call(int callTimeout) throws Exception {
      int attempt = callCount.incrementAndGet();
      if (attempt == 1) {
        throw new IOException("first failure");
      }
      return successResult + " " + attempt;
    }

    int getCallCount() {
      return callCount.get();
    }
  }

  @Test
  public void testSuccessfulSingleOperation() throws Exception {
    TestOperationInterceptorFactory factory = new TestOperationInterceptorFactory();
    RpcRetryingCallerImpl<String> caller = createCaller(factory);

    String result = caller.callWithoutRetries(new SuccessCallable("success"), 1000);
    assertEquals("success", result);
    assertEquals(1, factory.createCount.get());
  }

  @Test
  public void testFailedSingleOperation() throws Exception {
    TestOperationInterceptorFactory factory = new TestOperationInterceptorFactory();
    RpcRetryingCallerImpl<String> caller = createCaller(factory);

    try {
      caller.callWithoutRetries(new FailureCallable(new IOException("test failure")), 1000);
      fail("Expected IOException");
    } catch (IOException e) {
      assertEquals("test failure", e.getMessage());
    }
    assertEquals(1, factory.createCount.get());
  }

  @Test
  public void testRetryingOperation() throws Exception {
    TestOperationInterceptorFactory factory = new TestOperationInterceptorFactory();
    RpcRetryingCallerImpl<String> caller = createFastRetryCaller(factory);

    RetrySuccessCallable callable = new RetrySuccessCallable("success on attempt");
    String result = caller.callWithRetries(callable, 5000);
    assertEquals("success on attempt 2", result);
    assertEquals(1, factory.createCount.get());
    assertEquals(2, callable.getCallCount());
  }

  @Test
  public void testNoOpFactory() {
    OperationInterceptorFactory factory = OperationInterceptorFactory.NO_OP;
    OperationInterceptor interceptor = factory.createInterceptor();
    assertNotNull(interceptor);

    // Verify it creates new instances each time (no longer singleton)
    OperationInterceptor interceptor2 = factory.createInterceptor();
    assertNotNull(interceptor2);
    // They should be different instances
    assertNotSame(interceptor, interceptor2);
  }

  @Test
  public void testConfigurationBasedFactory() {
    Configuration conf = new Configuration();
    conf.set(OperationInterceptorFactory.HBASE_CLIENT_OPERATION_INTERCEPTOR_IMPL,
      TestOperationInterceptorFactory.class.getName());

    // Test factory creation
    String clazz = conf.get(OperationInterceptorFactory.HBASE_CLIENT_OPERATION_INTERCEPTOR_IMPL);
    assertNotNull(clazz);
    assertEquals(TestOperationInterceptorFactory.class.getName(), clazz);
  }
}
