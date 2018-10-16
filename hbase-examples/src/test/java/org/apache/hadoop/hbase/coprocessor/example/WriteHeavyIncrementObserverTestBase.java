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
package org.apache.hadoop.hbase.coprocessor.example;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class WriteHeavyIncrementObserverTestBase {

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static TableName NAME = TableName.valueOf("TestCP");

  protected static byte[] FAMILY = Bytes.toBytes("cf");

  protected static byte[] ROW = Bytes.toBytes("row");

  protected static byte[] CQ1 = Bytes.toBytes("cq1");

  protected static byte[] CQ2 = Bytes.toBytes("cq2");

  protected static Table TABLE;

  protected static long UPPER = 1000;

  protected static int THREADS = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 64 * 1024L);
    UTIL.getConfiguration().setLong("hbase.hregion.memstore.flush.size.limit", 1024L);
    UTIL.getConfiguration().setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY,
        0.014);
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (TABLE != null) {
      TABLE.close();
    }
    UTIL.shutdownMiniCluster();
  }

  private static void increment(int sleepSteps) throws IOException {
    for (long i = 1; i <= UPPER; i++) {
      TABLE.increment(new Increment(ROW).addColumn(FAMILY, CQ1, i).addColumn(FAMILY, CQ2, 2 * i));
      if (sleepSteps > 0 && i % sleepSteps == 0) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  protected final void assertSum() throws IOException {
    Result result = TABLE.get(new Get(ROW).addColumn(FAMILY, CQ1).addColumn(FAMILY, CQ2));
    assertEquals(THREADS * (1 + UPPER) * UPPER / 2, Bytes.toLong(result.getValue(FAMILY, CQ1)));
    assertEquals(THREADS * (1 + UPPER) * UPPER, Bytes.toLong(result.getValue(FAMILY, CQ2)));
  }

  protected final void doIncrement(int sleepSteps) throws InterruptedException {
    Thread[] threads = IntStream.range(0, THREADS).mapToObj(i -> new Thread(() -> {
      try {
        increment(sleepSteps);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }, "increment-" + i)).toArray(Thread[]::new);
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }
}
