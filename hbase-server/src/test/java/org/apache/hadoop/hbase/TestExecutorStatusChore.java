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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.executor.TestExecutorService.TestEventHandler;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceImpl;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestExecutorStatusChore {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExecutorStatusChore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestExecutorStatusChore.class);

  @Test
  public void testMetricsCollect() throws Exception {
    int maxThreads = 5;
    int maxTries = 10;
    int sleepInterval = 1000;

    Server mockedServer = mock(Server.class);
    when(mockedServer.getConfiguration()).thenReturn(HBaseConfiguration.create());

    // Start an executor service pool with max 5 threads
    ExecutorService executorService = new ExecutorService("unit_test");
    executorService.startExecutorService(
      ExecutorType.RS_PARALLEL_SEEK, maxThreads);

    MetricsRegionServerSource serverSource = CompatibilitySingletonFactory
        .getInstance(MetricsRegionServerSourceFactory.class).createServer(null);
    assertTrue(serverSource instanceof MetricsRegionServerSourceImpl);

    ExecutorStatusChore statusChore = new ExecutorStatusChore(60000,
        mockedServer, executorService, serverSource);

    AtomicBoolean lock = new AtomicBoolean(true);
    AtomicInteger counter = new AtomicInteger(0);
    for (int i = 0; i < maxThreads + 1; i++) {
      executorService
        .submit(new TestEventHandler(mockedServer, EventType.RS_PARALLEL_SEEK, lock, counter));
    }

    // The TestEventHandler will increment counter when it starts.
    int tries = 0;
    while (counter.get() < maxThreads && tries < maxTries) {
      LOG.info("Waiting for all event handlers to start...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    // Assert that pool is at max threads.
    assertEquals(maxThreads, counter.get());

    statusChore.chore();
    Pair<Long, Long> executorStatus = statusChore.getExecutorStatus("RS_PARALLEL_SEEK");
    assertEquals(maxThreads, executorStatus.getFirst().intValue()); // running
    assertEquals(1, executorStatus.getSecond().intValue()); // pending

    // Now interrupt the running Executor
    synchronized (lock) {
      lock.set(false);
      lock.notifyAll();
    }
    executorService.shutdown();
  }
}
