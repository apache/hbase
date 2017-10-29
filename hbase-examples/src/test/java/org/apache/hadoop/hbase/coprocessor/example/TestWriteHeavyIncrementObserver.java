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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestWriteHeavyIncrementObserver {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("TestCP");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] CQ1 = Bytes.toBytes("cq1");

  private static byte[] CQ2 = Bytes.toBytes("cq2");

  private static Table TABLE;

  private static long UPPER = 1000;

  private static int THREADS = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 64 * 1024L);
    UTIL.getConfiguration().setLong("hbase.hregion.memstore.flush.size.limit", 1024L);
    UTIL.startMiniCluster(3);
    UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(NAME)
            .addCoprocessor(WriteHeavyIncrementObserver.class.getName())
            .addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    TABLE = UTIL.getConnection().getTable(NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (TABLE != null) {
      TABLE.close();
    }
    UTIL.shutdownMiniCluster();
  }

  private static void increment() throws IOException {
    for (long i = 1; i <= UPPER; i++) {
      TABLE.increment(new Increment(ROW).addColumn(FAMILY, CQ1, i).addColumn(FAMILY, CQ2, 2 * i));
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(5, 10));
      } catch (InterruptedException e) {
      }
    }
  }

  private void assertSum() throws IOException {
    Result result = TABLE.get(new Get(ROW).addColumn(FAMILY, CQ1).addColumn(FAMILY, CQ2));
    assertEquals(THREADS * (1 + UPPER) * UPPER / 2, Bytes.toLong(result.getValue(FAMILY, CQ1)));
    assertEquals(THREADS * (1 + UPPER) * UPPER, Bytes.toLong(result.getValue(FAMILY, CQ2)));
  }

  @Test
  public void test() throws Exception {
    Thread[] threads = IntStream.range(0, THREADS).mapToObj(i -> new Thread(() -> {
      try {
        increment();
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
    assertSum();
    // we do not hack scan operation so using scan we could get the original values added into the
    // table.
    try (ResultScanner scanner = TABLE.getScanner(new Scan().withStartRow(ROW)
        .withStopRow(ROW, true).addFamily(FAMILY).readAllVersions().setAllowPartialResults(true))) {
      Result r = scanner.next();
      assertTrue(r.rawCells().length > 2);
    }
    UTIL.getAdmin().flush(NAME);
    UTIL.getAdmin().majorCompact(NAME);
    HStore store = UTIL.getHBaseCluster().findRegionsForTable(NAME).get(0).getStore(FAMILY);
    Waiter.waitFor(UTIL.getConfiguration(), 30000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return store.getStorefilesCount() == 1;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Major compaction hangs, there are still " + store.getStorefilesCount() +
            " store files";
      }
    });
    assertSum();
    // Should only have two cells after flush and major compaction
    try (ResultScanner scanner = TABLE.getScanner(new Scan().withStartRow(ROW)
        .withStopRow(ROW, true).addFamily(FAMILY).readAllVersions().setAllowPartialResults(true))) {
      Result r = scanner.next();
      assertEquals(2, r.rawCells().length);
    }
  }
}
