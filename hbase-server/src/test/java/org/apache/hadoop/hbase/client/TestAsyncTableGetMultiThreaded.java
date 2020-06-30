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
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MAX_BUFFER_COUNT_KEY;
import static org.apache.hadoop.hbase.master.LoadBalancer.TABLES_ON_MASTER;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Will split the table, and move region randomly when testing.
 */
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableGetMultiThreaded {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableGetMultiThreaded.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncTableGetMultiThreaded.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static int COUNT = 1000;

  private static AsyncConnection CONN;

  private static AsyncTable<?> TABLE;

  private static byte[][] SPLIT_KEYS;

  @BeforeClass
  public static void setUp() throws Exception {
    setUp(MemoryCompactionPolicy.NONE);
  }

  protected static void setUp(MemoryCompactionPolicy memoryCompaction) throws Exception {
    TEST_UTIL.getConfiguration().set(TABLES_ON_MASTER, "none");
    TEST_UTIL.getConfiguration().setLong(HBASE_CLIENT_META_OPERATION_TIMEOUT, 60000L);
    TEST_UTIL.getConfiguration().setInt(MAX_BUFFER_COUNT_KEY, 100);
    TEST_UTIL.getConfiguration().set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
      String.valueOf(memoryCompaction));

    TEST_UTIL.startMiniCluster(3);
    SPLIT_KEYS = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      SPLIT_KEYS[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    TABLE = CONN.getTableBuilder(TABLE_NAME).setReadRpcTimeout(1, TimeUnit.SECONDS)
        .setMaxRetries(1000).build();
    TABLE.putAll(
      IntStream.range(0, COUNT).mapToObj(i -> new Put(Bytes.toBytes(String.format("%03d", i)))
          .addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i))).collect(Collectors.toList()))
        .get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  private void run(AtomicBoolean stop) throws InterruptedException, ExecutionException {
    while (!stop.get()) {
      for (int i = 0; i < COUNT; i++) {
        assertEquals(i, Bytes.toInt(TABLE.get(new Get(Bytes.toBytes(String.format("%03d", i))))
            .get().getValue(FAMILY, QUALIFIER)));
      }
      // sleep a bit so we do not add to much load to the test machine as we have 20 threads here
      Thread.sleep(10);
    }
  }

  @Test
  public void test() throws Exception {
    LOG.info("====== Test started ======");
    int numThreads = 7;
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService executor =
      Executors.newFixedThreadPool(numThreads, Threads.newDaemonThreadFactory("TestAsyncGet-"));
    List<Future<?>> futures = new ArrayList<>();
    IntStream.range(0, numThreads).forEach(i -> futures.add(executor.submit(() -> {
      run(stop);
      return null;
    })));
    LOG.info("====== Scheduled {} read threads ======", numThreads);
    Collections.shuffle(Arrays.asList(SPLIT_KEYS), new Random(123));
    Admin admin = TEST_UTIL.getAdmin();
    for (byte[] splitPoint : SPLIT_KEYS) {
      int oldRegionCount = admin.getRegions(TABLE_NAME).size();
      LOG.info("====== Splitting at {} ======, region count before splitting is {}",
        Bytes.toStringBinary(splitPoint), oldRegionCount);
      admin.split(TABLE_NAME, splitPoint);
      TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return TEST_UTIL.getMiniHBaseCluster().getRegions(TABLE_NAME).size() > oldRegionCount;
        }

        @Override
        public String explainFailure() throws Exception {
          return "Split has not finished yet";
        }
      });
      List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegions(TABLE_NAME);
      LOG.info("====== Split at {} ======, region count after splitting is {}",
        Bytes.toStringBinary(splitPoint), regions.size());
      for (HRegion region : regions) {
        LOG.info("====== Compact {} ======", region.getRegionInfo());
        region.compact(true);
      }
      for (HRegion region : regions) {
        // Waiting for compaction to complete and references are cleaned up
        LOG.info("====== Waiting for compaction on {} ======", region.getRegionInfo());
        RetryCounter retrier = new RetryCounter(30, 1, TimeUnit.SECONDS);
        for (;;) {
          try {
            if (admin.getCompactionStateForRegion(
              region.getRegionInfo().getRegionName()) == CompactionState.NONE) {
              break;
            }
          } catch (IOException e) {
            LOG.warn("Failed to query");
          }
          if (!retrier.shouldRetry()) {
            throw new IOException("Can not finish compaction in time after attempt " +
              retrier.getAttemptTimes() + " times");
          }
          retrier.sleepUntilNextRetry();
        }
        LOG.info("====== Compaction on {} finished, close and archive compacted files ======",
          region.getRegionInfo());
        region.getStores().get(0).closeAndArchiveCompactedFiles();
        LOG.info("====== Close and archive compacted files on {} done ======",
          region.getRegionInfo());
      }
      Thread.sleep(5000);
      LOG.info("====== Balancing cluster ======");
      admin.balance(true);
      LOG.info("====== Balance cluster done ======");
      Thread.sleep(5000);
      ServerName metaServer = TEST_UTIL.getHBaseCluster().getServerHoldingMeta();
      ServerName newMetaServer = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
          .map(t -> t.getRegionServer().getServerName()).filter(s -> !s.equals(metaServer))
          .findAny().get();
      LOG.info("====== Moving meta from {} to {} ======", metaServer, newMetaServer);
      RegionInfo meta = admin.getRegions(TableName.META_TABLE_NAME).get(0);
      admin.move(meta.getEncodedNameAsBytes(), newMetaServer);
      LOG.info("====== Move meta done ======");
      Thread.sleep(5000);
    }
    LOG.info("====== Read test finished, shutdown thread pool ======");
    stop.set(true);
    executor.shutdown();
    for (int i = 0; i < numThreads; i++) {
      LOG.info("====== Waiting for {} threads to finish, remaining {} ======", numThreads,
        numThreads - i);
      futures.get(i).get();
    }
    LOG.info("====== Test test finished ======");
  }
}
