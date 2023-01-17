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
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBlocking() throws IOException, InterruptedException {
    TEST_UTIL.createTable(TableName.valueOf("foo"), "0");
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, BlockingRpcClient.class.getName());
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 5_000);
    conf.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, 5_000);

    Connection conn = ConnectionFactory.createConnection(conf);
    Table table = conn.getTable(TableName.valueOf("foo"));

    // warm meta
    table.getRegionLocator().getAllRegionLocations();

    MockOutputStreamWithTimeout.enable();

    CompletableFuture<Void> future = new CompletableFuture<>();
    Thread blockedThread = new Thread(() -> {
      try {
        LOG.info("Executing hanging get");
        table.get(new Get(Bytes.toBytes("foo")));
        LOG.info("Done hanging get");
        future.complete(null);
      } catch (Throwable e) {
        future.completeExceptionally(e);
        LOG.info("Hanging get failed", e);
      }
    });
    blockedThread.start();

    MockOutputStreamWithTimeout.awaitSleepingState();
    MockOutputStreamWithTimeout.disable();

    LOG.info("Executing test get");
    table.get(new Get(Bytes.toBytes("foo")));
    LOG.info("Test get done");

    // actually expect no error here because the hung connection will have been retried
    future.join();

    assertEquals(1, HungConnectionTracker.INTERRUPTED.sum());
  }
}
