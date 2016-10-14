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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTable {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection ASYNC_CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = new AsyncConnectionImpl(TEST_UTIL.getConfiguration(), User.getCurrent());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)).get();
    assertTrue(table.exists(new Get(ROW).addColumn(FAMILY, QUALIFIER)).get());
    Result result = table.get(new Get(ROW).addColumn(FAMILY, QUALIFIER)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    table.delete(new Delete(ROW)).get();
    result = table.get(new Get(ROW).addColumn(FAMILY, QUALIFIER)).get();
    assertTrue(result.isEmpty());
    assertFalse(table.exists(new Get(ROW).addColumn(FAMILY, QUALIFIER)).get());
  }

  private byte[] concat(byte[] base, int index) {
    return Bytes.toBytes(Bytes.toString(base) + "-" + index);
  }

  @Test
  public void testMultiple() throws Exception {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int count = 100;
    CountDownLatch putLatch = new CountDownLatch(count);
    IntStream.range(0, count).forEach(
      i -> table.put(new Put(concat(ROW, i)).addColumn(FAMILY, QUALIFIER, concat(VALUE, i)))
          .thenAccept(x -> putLatch.countDown()));
    putLatch.await();
    BlockingQueue<Boolean> existsResp = new ArrayBlockingQueue<>(count);
    IntStream.range(0, count)
        .forEach(i -> table.exists(new Get(concat(ROW, i)).addColumn(FAMILY, QUALIFIER))
            .thenAccept(x -> existsResp.add(x)));
    for (int i = 0; i < count; i++) {
      assertTrue(existsResp.take());
    }
    BlockingQueue<Pair<Integer, Result>> getResp = new ArrayBlockingQueue<>(count);
    IntStream.range(0, count)
        .forEach(i -> table.get(new Get(concat(ROW, i)).addColumn(FAMILY, QUALIFIER))
            .thenAccept(x -> getResp.add(Pair.newPair(i, x))));
    for (int i = 0; i < count; i++) {
      Pair<Integer, Result> pair = getResp.take();
      assertArrayEquals(concat(VALUE, pair.getFirst()),
        pair.getSecond().getValue(FAMILY, QUALIFIER));
    }
    CountDownLatch deleteLatch = new CountDownLatch(count);
    IntStream.range(0, count).forEach(
      i -> table.delete(new Delete(concat(ROW, i))).thenAccept(x -> deleteLatch.countDown()));
    deleteLatch.await();
    IntStream.range(0, count)
        .forEach(i -> table.exists(new Get(concat(ROW, i)).addColumn(FAMILY, QUALIFIER))
            .thenAccept(x -> existsResp.add(x)));
    for (int i = 0; i < count; i++) {
      assertFalse(existsResp.take());
    }
    IntStream.range(0, count)
        .forEach(i -> table.get(new Get(concat(ROW, i)).addColumn(FAMILY, QUALIFIER))
            .thenAccept(x -> getResp.add(Pair.newPair(i, x))));
    for (int i = 0; i < count; i++) {
      Pair<Integer, Result> pair = getResp.take();
      assertTrue(pair.getSecond().isEmpty());
    }
  }
}
