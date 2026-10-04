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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stress test to reproduce the cell ordering violation caused by the HBASE-28055
 * change to trySkipToNextColumn in StoreScanner.
 * <p>
 * HBASE-28055 replaced the HBASE-19863 backward-jump safety check
 * (compareKeyForNextColumn on the post-loop cell) with a narrower
 * timestamp == OLDEST_TIMESTAMP check. The narrower check misses backward jumps
 * involving real cells (not fake bloom filter cells), which can cause
 * StoreScanner.checkScanOrder to throw:
 * <pre>
 *   Key row/d:q40/500/Put followed by a smaller key row/d:q30/1001/DeleteColumn
 * </pre>
 * <p>
 * This test creates multiple StoreFiles with ROWCOL bloom filters and small
 * block sizes, then runs concurrent scans while a background thread continuously
 * writes, flushes, and triggers minor compactions. The combination of bloom filter
 * fake cell optimization, multiple StoreFiles, and concurrent flush/scan interleaving
 * triggers the backward jump that the buggy check fails to catch.
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestCellOrderingViolation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCellOrderingViolation.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestCellOrderingViolation.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  private static final byte[] FAMILY = Bytes.toBytes("d");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.client.pause", 250);
    conf.setInt("hbase.client.retries.number", 2);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStressConcurrentCompactionWithLargeRows() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());

    // Disable auto-compaction so we control StoreFile count
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);
    cfBuilder.setBloomFilterType(BloomType.ROWCOL);
    cfBuilder.setMaxVersions(5);
    cfBuilder.setBlocksize(256);
    tableBuilder.setColumnFamily(cfBuilder.build());
    // Large memstore flush size to avoid premature flushes
    tableBuilder.setMemStoreFlushSize(128 * 1024 * 1024L);

    Admin admin = TEST_UTIL.getAdmin();
    // Keep many StoreFiles before triggering compaction
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 50);
    TEST_UTIL.getConfiguration().setLong("hbase.hregion.majorcompaction", 0);

    admin.createTable(tableBuilder.build());
    Table table = TEST_UTIL.getConnection().getTable(tableName);

    final int NUM_ROWS = 200;
    final int NUM_QUALIFIERS = 60;
    final int NUM_SCAN_THREADS = 8;
    final int NUM_WRITER_THREADS = 2;
    final int NUM_ITERATIONS = 10;
    final int STRESS_DURATION_SECONDS = 90;

    final AtomicBoolean stopFlag = new AtomicBoolean(false);
    final AtomicReference<Throwable> orderingViolation = new AtomicReference<>(null);
    final AtomicInteger scanCount = new AtomicInteger(0);
    final AtomicInteger flushCount = new AtomicInteger(0);
    final AtomicInteger compactionCount = new AtomicInteger(0);

    try {
      byte[][] qualifiers = new byte[NUM_QUALIFIERS][];
      for (int q = 0; q < NUM_QUALIFIERS; q++) {
        qualifiers[q] = Bytes.toBytes(String.format("q%02d", q));
      }

      // Sparse scan columns with wide gaps
      byte[][] scanColumns = new byte[][] {
        qualifiers[0], qualifiers[8], qualifiers[15], qualifiers[21],
        qualifiers[30], qualifiers[40], qualifiers[49], qualifiers[55]
      };

      LOG.info("=== STRESS TEST: {} rows x {} qualifiers, scanning {} columns, "
          + "{} scan threads, {} writer threads, {}s duration ===",
        NUM_ROWS, NUM_QUALIFIERS, scanColumns.length,
        NUM_SCAN_THREADS, NUM_WRITER_THREADS, STRESS_DURATION_SECONDS);

      // Phase 1: Create initial data across many StoreFiles
      for (int iter = 0; iter < NUM_ITERATIONS; iter++) {
        long ts = (iter + 1) * 100L;
        int startQ = (iter * 7) % NUM_QUALIFIERS;
        int numQ = 12 + (iter % 8);

        for (int r = 0; r < NUM_ROWS; r++) {
          byte[] row = Bytes.toBytes(String.format("row%05d", r));
          Put put = new Put(row);
          for (int q = 0; q < numQ; q++) {
            int qIdx = (startQ + q) % NUM_QUALIFIERS;
            put.addColumn(FAMILY, qualifiers[qIdx], ts,
              Bytes.toBytes("v" + iter + "_" + r + "_" + qIdx));
          }
          table.put(put);
        }
        TEST_UTIL.flush(tableName);

        // Add delete markers for scan columns every other iteration
        if (iter % 2 == 1) {
          for (int r = 0; r < NUM_ROWS; r++) {
            byte[] row = Bytes.toBytes(String.format("row%05d", r));
            for (int sc = 0; sc < scanColumns.length; sc += 2) {
              Delete del = new Delete(row);
              del.addColumns(FAMILY, scanColumns[sc], ts + 1);
              table.delete(del);
            }
          }
        }
      }

      // Flush the delete markers into their own StoreFile
      TEST_UTIL.flush(tableName);
      LOG.info("=== Initial data loaded: {} StoreFiles ===", NUM_ITERATIONS);

      // Phase 2: Concurrent scans + writes + flush (no major compaction)
      ExecutorService executor =
        Executors.newFixedThreadPool(NUM_SCAN_THREADS + NUM_WRITER_THREADS);
      CountDownLatch startLatch = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();

      // Background writer/flusher threads
      for (int w = 0; w < NUM_WRITER_THREADS; w++) {
        final int writerId = w;
        futures.add(executor.submit(() -> {
          try {
            startLatch.await();
            Random rng = new Random(42 + writerId * 1000);
            int round = 0;

            while (!stopFlag.get() && orderingViolation.get() == null) {
              round++;
              long ts = 2000L + writerId * 100000L + round * 10;

              for (int r = rng.nextInt(5); r < NUM_ROWS; r += 3 + rng.nextInt(5)) {
                byte[] row = Bytes.toBytes(String.format("row%05d", r));
                Put put = new Put(row);
                int nq = 1 + rng.nextInt(3);
                for (int qi = 0; qi < nq; qi++) {
                  put.addColumn(FAMILY, qualifiers[rng.nextInt(NUM_QUALIFIERS)], ts,
                    Bytes.toBytes("s" + writerId + "_" + round + "_" + r));
                }
                table.put(put);

                // Delete a scan column ~40% of the time
                if (rng.nextInt(5) < 2) {
                  Delete del = new Delete(row);
                  del.addColumns(FAMILY, scanColumns[rng.nextInt(scanColumns.length)], ts + 1);
                  table.delete(del);
                }
              }

              // Flush every few rounds
              if (round % 3 == 0) {
                try {
                  admin.flush(tableName);
                  flushCount.incrementAndGet();
                } catch (Exception e) {
                  // ok
                }
              }

              // Minor compaction occasionally
              if (round % 20 == 0) {
                try {
                  admin.compact(tableName);
                  compactionCount.incrementAndGet();
                } catch (Exception e) {
                  // ok
                }
              }

              Thread.sleep(20 + rng.nextInt(60));
            }
            LOG.info("  Writer-{} completed {} rounds", writerId, round);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            LOG.error("Writer-{} error", writerId, e);
          }
        }));
      }

      // Scanner threads
      for (int t = 0; t < NUM_SCAN_THREADS; t++) {
        final int threadId = t;
        futures.add(executor.submit(() -> {
          try {
            startLatch.await();
            Random rng = new Random(threadId * 7919L);

            while (!stopFlag.get() && orderingViolation.get() == null) {
              Scan scan = new Scan();

              int numScanCols = 3 + rng.nextInt(scanColumns.length - 2);
              boolean[] picked = new boolean[scanColumns.length];
              for (int c = 0; c < numScanCols; c++) {
                int idx;
                do { idx = rng.nextInt(scanColumns.length); } while (picked[idx]);
                picked[idx] = true;
                scan.addColumn(FAMILY, scanColumns[idx]);
              }
              scan.readVersions(1 + rng.nextInt(3));
              scan.setCaching(5 + rng.nextInt(30));

              int rowsSeen = 0;
              try {
                for (Result result : table.getScanner(scan)) {
                  rowsSeen++;
                  if (rng.nextInt(100) == 0) {
                    Thread.sleep(1);
                  }
                }
                int cnt = scanCount.incrementAndGet();
                if (cnt % 200 == 0) {
                  LOG.info("  Progress: {} scans, {} flushes, {} compactions",
                    cnt, flushCount.get(), compactionCount.get());
                }
              } catch (Exception e) {
                // Walk the full cause chain collecting all messages
                String allMessages = "";
                Throwable cur = e;
                while (cur != null) {
                  if (cur.getMessage() != null) {
                    allMessages += " " + cur.getMessage();
                  }
                  cur = cur.getCause();
                }
                if (allMessages.contains("Cell ordering violation")
                  || allMessages.contains("ordering violation")
                  || allMessages.contains("Expected increasing")
                  || allMessages.contains("followed by a smaller key")
                  || allMessages.contains("isDelete failed")) {
                  LOG.error("*** ORDERING VIOLATION in thread {} at row {} ***",
                    threadId, rowsSeen, e);
                  orderingViolation.compareAndSet(null, e);
                } else {
                  LOG.debug("Thread {} scan error at row {} (transient): {}",
                    threadId, rowsSeen, allMessages.trim());
                }
              }
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }));
      }

      LOG.info("=== Starting stress phase: {} scan + {} writer threads for {}s ===",
        NUM_SCAN_THREADS, NUM_WRITER_THREADS, STRESS_DURATION_SECONDS);
      startLatch.countDown();

      long deadline = System.currentTimeMillis() + STRESS_DURATION_SECONDS * 1000L;
      while (System.currentTimeMillis() < deadline && orderingViolation.get() == null) {
        Thread.sleep(1000);
      }

      stopFlag.set(true);
      LOG.info("=== Stopping stress test (violation={}) ===",
        orderingViolation.get() != null ? "YES" : "no");

      executor.shutdown();
      executor.awaitTermination(30, TimeUnit.SECONDS);

      LOG.info("=== STRESS TEST COMPLETE: {} scans, {} flushes, {} compactions ===",
        scanCount.get(), flushCount.get(), compactionCount.get());

      Throwable violation = orderingViolation.get();
      if (violation != null) {
        LOG.error("=== ORDERING VIOLATION DETECTED ===", violation);
        fail("Cell ordering violation detected during stress test: " + violation.getMessage());
      }

      assertTrue("Should have completed at least some scans", scanCount.get() > 0);

    } finally {
      table.close();
      TEST_UTIL.deleteTable(tableName);
    }
  }
}
