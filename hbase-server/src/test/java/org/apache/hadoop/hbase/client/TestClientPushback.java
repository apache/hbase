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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ LargeTests.class, ClientTests.class })
public class TestClientPushback extends ClientPushbackTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientPushback.class);

  private ConnectionImplementation conn;

  private BufferedMutatorImpl mutator;

  @Before
  public void setUp() throws IOException {
    conn = (ConnectionImplementation) ConnectionFactory.createConnection(UTIL.getConfiguration());
    mutator = (BufferedMutatorImpl) conn.getBufferedMutator(tableName);
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
    return conn.getStatisticsTracker();
  }

  @Override
  protected MetricsConnection getConnectionMetrics() throws IOException {
    return conn.getConnectionMetrics();
  }

  @Override
  protected void mutate(Put put) throws IOException {
    mutator.mutate(put);
    mutator.flush();
  }

  @Override
  protected void mutate(Put put, AtomicLong endTime, CountDownLatch latch) throws IOException {
    // Reach into the connection and submit work directly to AsyncProcess so we can
    // monitor how long the submission was delayed via a callback
    List<Row> ops = new ArrayList<>(1);
    ops.add(put);
    Batch.Callback<Result> callback = (byte[] r, byte[] row, Result result) -> {
      endTime.set(EnvironmentEdgeManager.currentTime());
      latch.countDown();
    };
    AsyncProcessTask<Result> task =
      AsyncProcessTask.newBuilder(callback).setPool(mutator.getPool()).setTableName(tableName)
        .setRowAccess(ops).setSubmittedRows(AsyncProcessTask.SubmittedRows.AT_LEAST_ONE)
        .setOperationTimeout(conn.getConnectionConfiguration().getOperationTimeout())
        .setRpcTimeout(60 * 1000).build();
    mutator.getAsyncProcess().submit(task);
  }

  @Override
  protected void mutateRow(RowMutations mutations) throws IOException {
    try (Table table = conn.getTable(tableName)) {
      table.mutateRow(mutations);
    }
  }
}
