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

import static org.apache.hadoop.hbase.client.AsyncConnectionConfiguration.HBASE_CLIENT_PAUSE_FOR_SERVER_OVERLOADED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.PluggableBlockingQueue;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.SimpleRpcScheduler;
import org.apache.hadoop.hbase.ipc.TestPluggableQueueImpl;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncClientPauseForServerOverloaded {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncClientPauseForServerOverloaded.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("ServerOverloaded");

  private static byte[] FAMILY = Bytes.toBytes("Family");

  private static byte[] QUALIFIER = Bytes.toBytes("Qualifier");

  private static long PAUSE_FOR_SERVER_OVERLOADED_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static long PAUSE_FOR_SERVER_OVERLOADED_MILLIS = TimeUnit.NANOSECONDS.toMillis(
    PAUSE_FOR_SERVER_OVERLOADED_NANOS);

  private static AsyncConnection CONN;

  private static volatile FailMode MODE = null;

  enum FailMode {
    CALL_QUEUE_TOO_BIG,
    CALL_DROPPED;

    private ConcurrentMap<MethodDescriptor, AtomicInteger> invoked = new ConcurrentHashMap<>();

    // this is for test scan, where we will send a open scanner first and then a next, and we
    // expect that we hit CQTBE two times.
    private boolean shouldFail(CallRunner callRunner) {
      MethodDescriptor method = callRunner.getRpcCall().getMethod();
      return invoked.computeIfAbsent(method,
        k -> new AtomicInteger(0)).getAndIncrement() % 2 == 0;
    }
  }

  public static final class OverloadedRpcScheduler extends SimpleRpcScheduler {

    public OverloadedRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
        int replicationHandlerCount, int metaTransitionHandler, PriorityFunction priority,
        Abortable server, int highPriorityLevel) {
      super(conf, handlerCount, priorityHandlerCount, replicationHandlerCount,
        metaTransitionHandler, priority, server, highPriorityLevel);
    }

    @Override
    public boolean dispatch(CallRunner callTask) {
      if (MODE == FailMode.CALL_QUEUE_TOO_BIG && MODE.shouldFail(callTask)) {
        return false;
      }
      return super.dispatch(callTask);
    }
  }

  public static final class OverloadedQueue extends TestPluggableQueueImpl {

    public OverloadedQueue(int maxQueueLength, PriorityFunction priority, Configuration conf) {
      super(maxQueueLength, priority, conf);
    }

    @Override
    public boolean offer(CallRunner callRunner) {
      if (MODE == FailMode.CALL_DROPPED && MODE.shouldFail(callRunner)) {
        callRunner.drop();
        return true;
      }
      return super.offer(callRunner);
    }
  }

  public static final class OverloadedRpcSchedulerFactory extends SimpleRpcSchedulerFactory {

    @Override
    public RpcScheduler create(Configuration conf, PriorityFunction priority, Abortable server) {
      int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
        HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
      return new OverloadedRpcScheduler(conf, handlerCount,
        conf.getInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT,
          HConstants.DEFAULT_REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT),
        conf.getInt(HConstants.REGION_SERVER_REPLICATION_HANDLER_COUNT,
          HConstants.DEFAULT_REGION_SERVER_REPLICATION_HANDLER_COUNT),
        conf.getInt(HConstants.MASTER_META_TRANSITION_HANDLER_COUNT,
          HConstants.MASTER__META_TRANSITION_HANDLER_COUNT_DEFAULT),
        priority, server, HConstants.QOS_THRESHOLD);
    }

  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_PAUSE, 10);
    UTIL.getConfiguration().set("hbase.ipc.server.callqueue.type", "pluggable");
    UTIL.getConfiguration().setClass("hbase.ipc.server.callqueue.pluggable.queue.class.name",
      OverloadedQueue.class, PluggableBlockingQueue.class);
    UTIL.getConfiguration().setClass(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      OverloadedRpcSchedulerFactory.class, RpcSchedulerFactory.class);
    UTIL.startMiniCluster(1);

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setLong(HBASE_CLIENT_PAUSE_FOR_SERVER_OVERLOADED, PAUSE_FOR_SERVER_OVERLOADED_MILLIS);
    CONN = ConnectionFactory.createAsyncConnection(conf).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest() throws IOException {
    try (Table table = UTIL.createTable(TABLE_NAME, FAMILY)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i)));
      }
    }
    MODE = FailMode.CALL_QUEUE_TOO_BIG;
  }

  @After
  public void tearDownAfterTest() throws IOException {
    for (FailMode mode : FailMode.values()) {
      mode.invoked.clear();
    }
    MODE = null;
    UTIL.getAdmin().disableTable(TABLE_NAME);
    UTIL.getAdmin().deleteTable(TABLE_NAME);
  }

  private void assertTime(Callable<Void> callable, long time) throws Exception {
    for (FailMode mode : FailMode.values()) {
      MODE = mode;

      long startNs = System.nanoTime();
      callable.call();
      long costNs = System.nanoTime() - startNs;
      assertTrue(costNs > time);
    }
  }

  @Test
  public void testGet() throws Exception {
    assertTime(() -> {
      Result result = CONN.getTable(TABLE_NAME).get(new Get(Bytes.toBytes(0))).get();
      assertArrayEquals(Bytes.toBytes(0), result.getValue(FAMILY, QUALIFIER));
      return null;
    }, PAUSE_FOR_SERVER_OVERLOADED_NANOS);
  }

  @Test
  public void testBatch() throws Exception {
    assertTime(() -> {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      try (AsyncBufferedMutator mutator = CONN.getBufferedMutator(TABLE_NAME)) {
        for (int i = 100; i < 110; i++) {
          futures.add(mutator
            .mutate(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i))));
        }
      }
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }, PAUSE_FOR_SERVER_OVERLOADED_NANOS);
  }

  @Test
  public void testScan() throws Exception {
    // we will hit CallQueueTooBigException two times so the sleep time should be twice
    assertTime(() -> {
      try (
        ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(new Scan().setCaching(80))) {
        for (int i = 0; i < 100; i++) {
          Result result = scanner.next();
          assertArrayEquals(Bytes.toBytes(i), result.getValue(FAMILY, QUALIFIER));
        }
        assertNull(scanner.next());
      }
      return null;
    }, PAUSE_FOR_SERVER_OVERLOADED_NANOS * 2);
  }
}
