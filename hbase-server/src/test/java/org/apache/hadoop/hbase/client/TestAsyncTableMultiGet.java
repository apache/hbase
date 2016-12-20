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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableMultiGet {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int COUNT = 100;

  private static AsyncConnection ASYNC_CONN;

  @Parameter
  public Supplier<AsyncTableBase> getTable;

  private static RawAsyncTable getRawTable() {
    return ASYNC_CONN.getRawTable(TABLE_NAME);
  }

  private static AsyncTable getTable() {
    return ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
  }

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestAsyncTableMultiGet::getRawTable },
      new Supplier<?>[] { TestAsyncTableMultiGet::getTable });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 11; i < 99; i += 11) {
      splitKeys[i / 11 - 1] = Bytes.toBytes(String.format("%02d", i));
    }
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, splitKeys);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    TEST_UTIL.getAdmin().setBalancerRunning(false, true);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration());
    RawAsyncTable table = ASYNC_CONN.getRawTable(TABLE_NAME);
    List<CompletableFuture<?>> futures = new ArrayList<>();
    IntStream.range(0, COUNT).forEach(i -> futures.add(table.put(
      new Put(Bytes.toBytes(String.format("%02d", i))).addColumn(FAMILY, CQ, Bytes.toBytes(i)))));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void move() throws IOException, InterruptedException {
    HRegionServer src = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    HRegionServer dst = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
        .map(t -> t.getRegionServer()).filter(r -> r != src).findAny().get();
    Region region = src.getOnlineRegions(TABLE_NAME).stream().findAny().get();
    TEST_UTIL.getAdmin().move(region.getRegionInfo().getEncodedNameAsBytes(),
      Bytes.toBytes(dst.getServerName().getServerName()));
    Thread.sleep(1000);
  }

  private void test(BiFunction<AsyncTableBase, List<Get>, List<Result>> getFunc)
      throws IOException, InterruptedException {
    AsyncTableBase table = getTable.get();
    List<Get> gets =
        IntStream.range(0, COUNT).mapToObj(i -> new Get(Bytes.toBytes(String.format("%02d", i))))
            .collect(Collectors.toList());
    List<Result> results = getFunc.apply(table, gets);
    assertEquals(COUNT, results.size());
    for (int i = 0; i < COUNT; i++) {
      Result result = results.get(i);
      assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ)));
    }
    // test basic failure recovery
    move();
    results = getFunc.apply(table, gets);
    assertEquals(COUNT, results.size());
    for (int i = 0; i < COUNT; i++) {
      Result result = results.get(i);
      assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ)));
    }
  }

  @Test
  public void testGet() throws InterruptedException, IOException {
    test((table, gets) -> {
      return table.get(gets).stream().map(f -> {
        try {
          return f.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
    });

  }

  @Test
  public void testGetAll() throws InterruptedException, IOException {
    test((table, gets) -> {
      try {
        return table.getAll(gets).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
