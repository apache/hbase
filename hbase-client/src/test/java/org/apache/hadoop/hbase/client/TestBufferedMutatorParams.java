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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ ClientTests.class, SmallTests.class })
public class TestBufferedMutatorParams {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBufferedMutatorParams.class);

  @Rule
  public TestName name = new TestName();

  /**
   * Just to create in instance, this doesn't actually function.
   */
  private class MockExecutorService implements ExecutorService {

    @Override
    public void execute(Runnable command) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
      return null;
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return false;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return null;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return null;
    }

    @Override
    public Future<?> submit(Runnable task) {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
        long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return null;
    }
  }

  /**
   * Just to create an instance, this doesn't actually function.
   */
  private static class MockExceptionListener implements BufferedMutator.ExceptionListener {
    @Override
    public void onException(RetriesExhaustedWithDetailsException exception,
        BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
    }
  }

  @Test
  public void testClone() {
    ExecutorService pool = new MockExecutorService();
    final String tableName = name.getMethodName();
    BufferedMutatorParams bmp = new BufferedMutatorParams(TableName.valueOf(tableName));

    BufferedMutator.ExceptionListener listener = new MockExceptionListener();
    bmp
            .writeBufferSize(17)
            .setWriteBufferPeriodicFlushTimeoutMs(123)
            .setWriteBufferPeriodicFlushTimerTickMs(456)
            .maxKeyValueSize(13)
            .pool(pool)
            .listener(listener);
    bmp.implementationClassName("someClassName");
    BufferedMutatorParams clone = bmp.clone();

    // Confirm some literals
    assertEquals(tableName, clone.getTableName().toString());
    assertEquals(17, clone.getWriteBufferSize());
    assertEquals(123, clone.getWriteBufferPeriodicFlushTimeoutMs());
    assertEquals(456, clone.getWriteBufferPeriodicFlushTimerTickMs());
    assertEquals(13, clone.getMaxKeyValueSize());
    assertEquals("someClassName", clone.getImplementationClassName());

    cloneTest(bmp, clone);

    BufferedMutatorParams cloneWars = clone.clone();
    cloneTest(clone, cloneWars);
    cloneTest(bmp, cloneWars);

    // Mocking with clone leave original unaffected.
    clone.implementationClassName(null);
    assertEquals("someClassName", bmp.getImplementationClassName());
  }

  /**
   * Confirm all fields are equal.
   * @param some some instance
   * @param clone a clone of that instance, but not the same instance.
   */
  private void cloneTest(BufferedMutatorParams some,
      BufferedMutatorParams clone) {
    assertFalse(some == clone);
    assertEquals(some.getTableName().toString(),
        clone.getTableName().toString());
    assertEquals(some.getWriteBufferSize(), clone.getWriteBufferSize());
    assertEquals(some.getWriteBufferPeriodicFlushTimeoutMs(),
        clone.getWriteBufferPeriodicFlushTimeoutMs());
    assertEquals(some.getWriteBufferPeriodicFlushTimerTickMs(),
        clone.getWriteBufferPeriodicFlushTimerTickMs());
    assertEquals(some.getMaxKeyValueSize(), clone.getMaxKeyValueSize());
    assertTrue(some.getListener() == clone.getListener());
    assertTrue(some.getPool() == clone.getPool());
    assertEquals(some.getImplementationClassName(), clone.getImplementationClassName());
  }

}
