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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncClientPushback extends ClientPushbackTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncClientPushback.class);

  private AsyncConnectionImpl conn;

  private AsyncBufferedMutator mutator;

  @Before
  public void setUp() throws Exception {
    conn =
      (AsyncConnectionImpl) ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    mutator = conn.getBufferedMutator(tableName);
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(mutator, true);
    Closeables.close(conn, true);
  }

  @Override
  protected ClientBackoffPolicy getBackoffPolicy() throws IOException {
    return conn.getBackoffPolicy();
  }

  @Override
  protected ServerStatisticTracker getStatisticsTracker() throws IOException {
    return conn.getStatisticsTracker().get();
  }

  @Override
  protected MetricsConnection getConnectionMetrics() throws IOException {
    return conn.getConnectionMetrics().get();
  }

  @Override
  protected void mutate(Put put) throws IOException {
    CompletableFuture<?> future = mutator.mutate(put);
    mutator.flush();
    future.join();
  }

  @Override
  protected void mutate(Put put, AtomicLong endTime, CountDownLatch latch) throws IOException {
    FutureUtils.addListener(mutator.mutate(put), (r, e) -> {
      endTime.set(EnvironmentEdgeManager.currentTime());
      latch.countDown();
    });
    mutator.flush();
  }

  @Override
  protected void mutateRow(RowMutations mutations) throws IOException {
    conn.getTable(tableName).mutateRow(mutations).join();
  }
}
