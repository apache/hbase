/**
 * Copyright 2014 The Apache Software Foundation
 *
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testcase for HTableClientScanner.
 * TODO daviddeng add some edge case explicitly.
 */
public class TestHTableClientScanner {
  final Log LOG = LogFactory.getLog(getClass());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("FAMILY");
  private static final int SLAVES = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(
        HBaseTestingUtility.FS_TYPE_KEY,
        HBaseTestingUtility.FS_TYPE_LFS);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScanner() throws IOException {
    final StringBytes TABLE_NAME = new StringBytes("testScanner");
    HTable table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY }, 3,
        Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), 25);

    int rowCount = TEST_UTIL.loadTable(table, FAMILY);

    int counted = HBaseTestingUtility.countRows(table, new Scan());
    assertEquals("rowCount", rowCount, counted);
  }

  /**
   * Testing parallel scanning with more threads than background threads.
   */
  @Test
  public void testMoreThreads() throws Exception {
    final int ROW_COUNT = 10000;
    final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() + 1;
    final StringBytes TABLE_NAME = new StringBytes("testMoreThreads");

    HTable table = TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    table.setAutoFlush(false);
    for (int i = 0; i < ROW_COUNT; i++) {
      byte[] row = Bytes.toBytes("row-" + i);
      Put put = new Put(row).add(FAMILY, row, row);
      table.put(put);
    }
    table.flushCommits();

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    Future<?>[] futures = new Future<?>[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
      futures[i] = executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            HTable table = new HTableAsync(TEST_UTIL.getConfiguration(),
                TABLE_NAME);
            try (ResultScanner scanner = table.getScanner(new Scan())) {
              for (Result result : scanner) {
                Assert.assertTrue("result.size should > 0", result.size() > 0);
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    for (Future<?> future : futures) {
      future.get();
    }
  }
}
