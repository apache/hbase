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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Class test rule implementation for JUnit5.
 * <p>
 * It ensures that all JUnit5 tests should have at least one of {@link SmallTests},
 * {@link MediumTests}, {@link LargeTests}, {@link IntegrationTests} tags, and set timeout based on
 * the tag.
 * <p>
 * It also controls the timeout for the whole test class running, while the timeout annotation in
 * JUnit5 can only enforce the timeout for each test method.
 * <p>
 * Finally, it also forbid System.exit call in tests. TODO: need to find a new way as
 * SecurityManager has been removed since Java 21.
 */
@InterfaceAudience.Private
public class HBaseJupitorExtension
  implements InvocationInterceptor, BeforeAllCallback, AfterAllCallback {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseJupitorExtension.class);

  private static final SecurityManager securityManager = new TestSecurityManager();

  private static final ExtensionContext.Namespace NAMESPACE =
    ExtensionContext.Namespace.create(HBaseJupitorExtension.class);

  private static final Map<String, Duration> TAG_TO_TIMEOUT =
    ImmutableMap.of(SmallTests.TAG, Duration.ofMinutes(3), MediumTests.TAG, Duration.ofMinutes(6),
      LargeTests.TAG, Duration.ofMinutes(13), IntegrationTests.TAG, Duration.ZERO);

  private static final String EXECUTOR = "executor";

  private static final String DEADLINE = "deadline";

  public HBaseJupitorExtension() {
    super();
  }

  private Duration pickTimeout(ExtensionContext ctx) {
    Set<String> timeoutTags = TAG_TO_TIMEOUT.keySet();
    Set<String> timeoutTag = Sets.intersection(timeoutTags, ctx.getTags());
    if (timeoutTag.isEmpty()) {
      fail("Test class " + ctx.getDisplayName() + " does not have any of the following scale tags "
        + timeoutTags);
    }
    if (timeoutTag.size() > 1) {
      fail("Test class " + ctx.getDisplayName() + " has multiple scale tags " + timeoutTag);
    }
    return TAG_TO_TIMEOUT.get(Iterables.getOnlyElement(timeoutTag));
  }

  @Override
  public void beforeAll(ExtensionContext ctx) throws Exception {
    // TODO: remove this usage
    System.setSecurityManager(securityManager);
    Duration timeout = pickTimeout(ctx);
    if (timeout.isZero() || timeout.isNegative()) {
      LOG.info("No timeout for {}", ctx.getDisplayName());
      // zero means no timeout
      return;
    }
    Instant deadline = Instant.now().plus(timeout);
    LOG.info("Timeout for {} is {}, it should be finished before {}", ctx.getDisplayName(), timeout,
      deadline);
    ExecutorService executor =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("HBase-Test-" + ctx.getDisplayName() + "-Main-Thread").build());
    Store store = ctx.getStore(NAMESPACE);
    store.put(EXECUTOR, executor);
    store.put(DEADLINE, deadline);
  }

  @Override
  public void afterAll(ExtensionContext ctx) throws Exception {
    Store store = ctx.getStore(NAMESPACE);
    ExecutorService executor = store.remove(EXECUTOR, ExecutorService.class);
    if (executor != null) {
      executor.shutdownNow();
    }
    store.remove(DEADLINE);
    // reset secutiry manager
    System.setSecurityManager(null);
  }

  private <T> T runWithTimeout(Invocation<T> invocation, ExtensionContext ctx) throws Throwable {
    Store store = ctx.getStore(NAMESPACE);
    ExecutorService executor = store.get(EXECUTOR, ExecutorService.class);
    if (executor == null) {
      return invocation.proceed();
    }
    Instant deadline = store.get(DEADLINE, Instant.class);
    Instant now = Instant.now();
    if (!now.isBefore(deadline)) {
      fail("Test " + ctx.getDisplayName() + " timed out, deadline is " + deadline);
      return null;
    }

    Duration remaining = Duration.between(now, deadline);
    LOG.info("remaining timeout for {} is {}", ctx.getDisplayName(), remaining);
    Future<T> future = executor.submit(() -> {
      try {
        return invocation.proceed();
      } catch (Throwable t) {
        // follow the same pattern with junit5
        throw ExceptionUtils.throwAsUncheckedException(t);
      }
    });
    try {
      return future.get(remaining.toNanos(), TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("Test " + ctx.getDisplayName() + " interrupted");
      return null;
    } catch (ExecutionException e) {
      throw ExceptionUtils.throwAsUncheckedException(e.getCause());
    } catch (TimeoutException e) {

      throw new JUnitException(
        "Test " + ctx.getDisplayName() + " timed out, deadline is " + deadline, e);
    }
  }

  @Override
  public void interceptBeforeAllMethod(Invocation<Void> invocation,
    ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
    throws Throwable {
    runWithTimeout(invocation, extensionContext);
  }

  @Override
  public void interceptBeforeEachMethod(Invocation<Void> invocation,
    ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
    throws Throwable {
    runWithTimeout(invocation, extensionContext);
  }

  @Override
  public void interceptTestMethod(Invocation<Void> invocation,
    ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
    throws Throwable {
    runWithTimeout(invocation, extensionContext);
  }

  @Override
  public void interceptAfterEachMethod(Invocation<Void> invocation,
    ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
    throws Throwable {
    runWithTimeout(invocation, extensionContext);
  }

  @Override
  public void interceptAfterAllMethod(Invocation<Void> invocation,
    ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
    throws Throwable {
    runWithTimeout(invocation, extensionContext);
  }

  @Override
  public <T> T interceptTestClassConstructor(Invocation<T> invocation,
    ReflectiveInvocationContext<Constructor<T>> invocationContext,
    ExtensionContext extensionContext) throws Throwable {
    return runWithTimeout(invocation, extensionContext);
  }
}
