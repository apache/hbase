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

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.TABLES_ON_MASTER;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Will split the table, and move region randomly when testing.
 */
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncGetMultiThread {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static int COUNT = 1000;

  private static AsyncConnection CONN;

  private static byte[][] SPLIT_KEYS;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(TABLES_ON_MASTER, "none");
    TEST_UTIL.getConfiguration().setLong(HBASE_CLIENT_META_OPERATION_TIMEOUT, 60000L);
    TEST_UTIL.getConfiguration().setLong(HBASE_RPC_READ_TIMEOUT_KEY, 1000L);
    TEST_UTIL.getConfiguration().setInt(HBASE_CLIENT_RETRIES_NUMBER, 1000);
    TEST_UTIL.getConfiguration().setInt(ByteBufferPool.MAX_POOL_SIZE_KEY, 100);
    TEST_UTIL.startMiniCluster(5);
    SPLIT_KEYS = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      SPLIT_KEYS[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration());
    AsyncTable table = CONN.getTable(TABLE_NAME);
    List<CompletableFuture<?>> futures = new ArrayList<>();
    IntStream.range(0, COUNT)
        .forEach(i -> futures.add(table.put(new Put(Bytes.toBytes(String.format("%03d", i)))
            .addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i)))));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void run(AtomicBoolean stop) throws InterruptedException, ExecutionException {
    while (!stop.get()) {
      int i = ThreadLocalRandom.current().nextInt(COUNT);
      assertEquals(i,
        Bytes.toInt(CONN.getTable(TABLE_NAME).get(new Get(Bytes.toBytes(String.format("%03d", i))))
            .get().getValue(FAMILY, QUALIFIER)));
    }
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    int numThreads = 20;
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService executor =
        Executors.newFixedThreadPool(numThreads, Threads.newDaemonThreadFactory("TestAsyncGet-"));
    List<Future<?>> futures = new ArrayList<>();
    IntStream.range(0, numThreads).forEach(i -> futures.add(executor.submit(() -> {
      run(stop);
      return null;
    })));
    Collections.shuffle(Arrays.asList(SPLIT_KEYS), new Random(123));
    Admin admin = TEST_UTIL.getAdmin();
    for (byte[] splitPoint : SPLIT_KEYS) {
      admin.split(TABLE_NAME, splitPoint);
      for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME)) {
        region.compact(true);
      }
      Thread.sleep(5000);
      admin.balancer(true);
      Thread.sleep(5000);
      ServerName metaServer = TEST_UTIL.getHBaseCluster().getServerHoldingMeta();
      ServerName newMetaServer = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
          .map(t -> t.getRegionServer().getServerName()).filter(s -> !s.equals(metaServer))
          .findAny().get();
      admin.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
        Bytes.toBytes(newMetaServer.getServerName()));
      Thread.sleep(5000);
    }
    stop.set(true);
    executor.shutdown();
    for (Future<?> future : futures) {
      future.get();
    }
  }
}
