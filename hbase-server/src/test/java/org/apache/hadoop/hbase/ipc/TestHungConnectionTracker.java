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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class, ClientTests.class })
public class TestHungConnectionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(TestHungConnectionTracker.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHungConnectionTracker.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeEach() throws Exception {
    HungConnectionMocking.reset();
    HungConnectionTracker.INTERRUPTED.reset();
  }

  /**
   * This tests the race condition where a new reader thread is spun up while the original is still
   * in readResponse. In this case, by the time the original thread finishes readResponse (and thus
   * goes into waitForWork()), "thread" wont be null. Previously this would result in extra threads
   * hanging around forever. Now, we check also that "thread" is the current thread and if not, end.
   * This tests that we properly do that.
   */
  @Test
  public void testBlockingWithCompetingReadThread() throws Exception {
    testBlocking(true);
    assertEquals(1, HungConnectionMocking.getThreadReplacedCount());
    assertEquals(0, HungConnectionMocking.getThreadEndedCount());
  }

  /**
   * This tests the problem where we previously would not handle InterruptedException properly in
   * the waitForWork method. We'd reset interrupt state and just restart the loop, which creates a
   * tight look and opens the opportunity for another race condition. Now we handle the exception
   * and exit the reader thread.
   */
  @Test
  public void testBlockingWithInterruptedReaderThread() throws Exception {
    testBlocking(false);
    assertEquals(0, HungConnectionMocking.getThreadReplacedCount());
    assertEquals(1, HungConnectionMocking.getThreadEndedCount());
  }

  private void testBlocking(boolean shouldCompeteForThreadCheck) throws Exception {
    TableName tableName = TableName.valueOf("foo-" + shouldCompeteForThreadCheck);
    TEST_UTIL.createTable(tableName, "0");
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, BlockingRpcClient.class.getName());
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5_000);
    conf.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, 5_000);

    Connection conn = ConnectionFactory.createConnection(conf);
    Table table = conn.getTable(tableName);

    long initialInterrupts = HungConnectionTracker.INTERRUPTED.sum();

    // warm meta
    table.getRegionLocator().getAllRegionLocations();

    CompletableFuture<Void> pausedReadFuture;
    if (shouldCompeteForThreadCheck) {
      HungConnectionMocking.pauseReads();
      pausedReadFuture = new CompletableFuture<>();
      Thread pausedReadThread = new Thread(() -> {
        try {
          LOG.info("Executing get which should get paused in read");
          table.get(new Get(Bytes.toBytes("foo")));
          LOG.info("Done paused read get");
          pausedReadFuture.complete(null);
        } catch (Throwable e) {
          pausedReadFuture.completeExceptionally(e);
          LOG.info("Paused read get failed", e);
        }
      });

      pausedReadThread.start();

      HungConnectionMocking.awaitPausedReadState();
    } else {
      pausedReadFuture = new CompletableFuture<>();
      pausedReadFuture.complete(null);
    }

    HungConnectionMocking.enableMockedWrite();

    CompletableFuture<Void> hungWriteFuture = new CompletableFuture<>();
    Thread hungWriteThread = new Thread(() -> {
      try {
        LOG.info("Executing hanging get");
        table.get(new Get(Bytes.toBytes("foo")));
        LOG.info("Done hanging get");
        hungWriteFuture.complete(null);
      } catch (Throwable e) {
        hungWriteFuture.completeExceptionally(e);
        LOG.info("Hanging get failed", e);
      }
    });
    hungWriteThread.start();

    HungConnectionMocking.awaitSleepingState();
    HungConnectionMocking.disableMockedWrite();

    CompletableFuture<Void> finalTestGetFuture = new CompletableFuture<>();
    Thread finalTestGet = new Thread(() -> {
      LOG.info("Executing test get");
      try {
        table.get(new Get(Bytes.toBytes("foo")));
        finalTestGetFuture.complete(null);
      } catch (IOException e) {
        finalTestGetFuture.completeExceptionally(e);
      }
      LOG.info("Test get done");
    });
    finalTestGet.start();

    while (HungConnectionTracker.INTERRUPTED.sum() <= initialInterrupts) {
      Thread.sleep(100);
    }
    LOG.info("Hung connection interrupted");

    if (shouldCompeteForThreadCheck) {
      HungConnectionMocking.unpauseReads();
    }

    // actually expect no error here because the hung connection will have been retried
    hungWriteFuture.join();
    pausedReadFuture.join();
    finalTestGetFuture.join();

    assertEquals(1, HungConnectionTracker.INTERRUPTED.sum());
  }
}
