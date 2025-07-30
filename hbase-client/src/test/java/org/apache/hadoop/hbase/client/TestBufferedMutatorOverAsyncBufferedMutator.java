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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Category({ ClientTests.class, SmallTests.class })
public class TestBufferedMutatorOverAsyncBufferedMutator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBufferedMutatorOverAsyncBufferedMutator.class);

  private AsyncBufferedMutator asyncMutator;

  private BufferedMutatorOverAsyncBufferedMutator mutator;

  private ExecutorService executor;

  private MockedStatic<Pair> mockedPair;

  @Before
  public void setUp() {
    asyncMutator = mock(AsyncBufferedMutator.class);
    when(asyncMutator.getWriteBufferSize()).thenReturn(1024L * 1024);
    mutator = new BufferedMutatorOverAsyncBufferedMutator(asyncMutator, (e, m) -> {
      throw e;
    });
    executor =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
    mockedPair = mockStatic(Pair.class);
  }

  @After
  public void tearDown() {
    mockedPair.closeOnDemand();
    executor.shutdown();
  }

  @Test
  public void testRace() throws IOException {
    CompletableFuture<Void> future = new CompletableFuture<>();
    when(asyncMutator.mutate(anyList())).thenReturn(Arrays.asList(future));
    mutator.mutate(new Put(Bytes.toBytes("aaa")));
    verify(asyncMutator).mutate(anyList());
    CountDownLatch beforeFlush = new CountDownLatch(1);
    CountDownLatch afterFlush = new CountDownLatch(1);
    Future<?> flushFuture = executor.submit(() -> {
      beforeFlush.await();
      mutator.flush();
      afterFlush.countDown();
      return null;
    });
    mockedPair.when(() -> Pair.newPair(any(), any())).then(i -> {
      beforeFlush.countDown();
      afterFlush.await(5, TimeUnit.SECONDS);
      return i.callRealMethod();
    });
    future.completeExceptionally(new IOException("inject error"));
    ExecutionException error = assertThrows(ExecutionException.class, () -> flushFuture.get());
    assertThat(error.getCause(), instanceOf(RetriesExhaustedWithDetailsException.class));
    assertEquals("inject error",
      ((RetriesExhaustedWithDetailsException) error.getCause()).getCause(0).getMessage());
  }
}
