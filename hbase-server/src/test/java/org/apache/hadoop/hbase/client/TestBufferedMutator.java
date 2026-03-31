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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
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

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Category({ MediumTests.class, ClientTests.class })
public class TestBufferedMutator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBufferedMutator.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static byte[] VALUE = new byte[1024];

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    ThreadLocalRandom.current().nextBytes(VALUE);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest() throws IOException {
    TEST_UTIL.createTable(TABLE_NAME, CF);
  }

  @After
  public void tearDownAfterTest() throws IOException {
    TEST_UTIL.deleteTable(TABLE_NAME);
  }

  @Test
  public void test() throws Exception {
    int count = 1024;
    try (BufferedMutator mutator = TEST_UTIL.getConnection()
      .getBufferedMutator(new BufferedMutatorParams(TABLE_NAME).writeBufferSize(64 * 1024))) {
      mutator.mutate(IntStream.range(0, count / 2)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .collect(Collectors.toList()));
      mutator.flush();
      mutator.mutate(IntStream.range(count / 2, count)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .collect(Collectors.toList()));
      mutator.close();
      verifyData(count);
    }
  }

  @Test
  public void testMultiThread() throws Exception {
    ExecutorService executor =
      Executors.newFixedThreadPool(16, new ThreadFactoryBuilder().setDaemon(true).build());
    // use a greater count and less write buffer size to trigger auto flush when mutate
    int count = 16384;
    try (BufferedMutator mutator = TEST_UTIL.getConnection()
      .getBufferedMutator(new BufferedMutatorParams(TABLE_NAME).writeBufferSize(4 * 1024))) {
      IntStream.range(0, count / 2)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .forEach(put -> executor.execute(() -> {
          try {
            mutator.mutate(put);
          } catch (IOException e) {
            fail("failed to mutate: " + e.getMessage());
          }
        }));
      mutator.flush();
      IntStream.range(count / 2, count)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .forEach(put -> executor.execute(() -> {
          try {
            mutator.mutate(put);
          } catch (IOException e) {
            fail("failed to mutate: " + e.getMessage());
          }
        }));
      executor.shutdown();
      assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
      mutator.close();
    } finally {
      executor.shutdownNow();
    }
    verifyData(count);
  }

  private void verifyData(int count) throws IOException {
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < count; i++) {
        Result r = table.get(new Get(Bytes.toBytes(i)));
        assertArrayEquals(VALUE, ((Result) r).getValue(CF, CQ));
      }
    }
  }
}
