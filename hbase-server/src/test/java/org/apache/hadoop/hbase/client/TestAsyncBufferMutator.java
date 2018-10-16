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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncBufferMutator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncBufferMutator.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static TableName MULTI_REGION_TABLE_NAME = TableName.valueOf("async-multi-region");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int COUNT = 100;

  private static byte[] VALUE = new byte[1024];

  private static AsyncConnection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, CF);
    TEST_UTIL.createMultiRegionTable(MULTI_REGION_TABLE_NAME, CF);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    ThreadLocalRandom.current().nextBytes(VALUE);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testWithMultiRegionTable() throws InterruptedException {
    test(MULTI_REGION_TABLE_NAME);
  }

  @Test
  public void testWithSingleRegionTable() throws InterruptedException {
    test(TABLE_NAME);
  }

  private void test(TableName tableName) throws InterruptedException {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    try (AsyncBufferedMutator mutator =
        CONN.getBufferedMutatorBuilder(tableName).setWriteBufferSize(16 * 1024).build()) {
      List<CompletableFuture<Void>> fs = mutator.mutate(IntStream.range(0, COUNT / 2)
          .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
          .collect(Collectors.toList()));
      // exceeded the write buffer size, a flush will be called directly
      fs.forEach(f -> f.join());
      IntStream.range(COUNT / 2, COUNT).forEach(i -> {
        futures.add(mutator.mutate(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE)));
      });
      // the first future should have been sent out.
      futures.get(0).join();
      Thread.sleep(2000);
      // the last one should still be in write buffer
      assertFalse(futures.get(futures.size() - 1).isDone());
    }
    // mutator.close will call mutator.flush automatically so all tasks should have been done.
    futures.forEach(f -> f.join());
    AsyncTable<?> table = CONN.getTable(tableName);
    IntStream.range(0, COUNT).mapToObj(i -> new Get(Bytes.toBytes(i))).map(g -> table.get(g).join())
        .forEach(r -> {
          assertArrayEquals(VALUE, r.getValue(CF, CQ));
        });
  }

  @Test
  public void testClosedMutate() throws InterruptedException {
    AsyncBufferedMutator mutator = CONN.getBufferedMutator(TABLE_NAME);
    mutator.close();
    Put put = new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE);
    try {
      mutator.mutate(put).get();
      fail("Close check failed");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IOException.class));
      assertTrue(e.getCause().getMessage().startsWith("Already closed"));
    }
    for (CompletableFuture<Void> f : mutator.mutate(Arrays.asList(put))) {
      try {
        f.get();
        fail("Close check failed");
      } catch (ExecutionException e) {
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertTrue(e.getCause().getMessage().startsWith("Already closed"));
      }
    }
  }
}
