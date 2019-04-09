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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableNoncedRetry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableNoncedRetry.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection ASYNC_CONN;

  @Rule
  public TestName testName = new TestName();

  private byte[] row;

  private static AtomicInteger CALLED = new AtomicInteger();

  private static long SLEEP_TIME = 2000;

  public static final class SleepOnceCP implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append,
        Result result) throws IOException {
      if (CALLED.getAndIncrement() == 0) {
        Threads.sleepWithoutInterrupt(SLEEP_TIME);
      }
      return RegionObserver.super.postAppend(c, append, result);
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c,
        Increment increment, Result result) throws IOException {
      if (CALLED.getAndIncrement() == 0) {
        Threads.sleepWithoutInterrupt(SLEEP_TIME);
      }
      return RegionObserver.super.postIncrement(c, increment, result);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setCoprocessor(SleepOnceCP.class.getName()).build());
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    IOUtils.closeQuietly(ASYNC_CONN);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    row = Bytes.toBytes(testName.getMethodName().replaceAll("[^0-9A-Za-z]", "_"));
    CALLED.set(0);
  }

  @Test
  public void testAppend() throws InterruptedException, ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(SLEEP_TIME / 2, TimeUnit.MILLISECONDS).build();
    Result result = table.append(new Append(row).addColumn(FAMILY, QUALIFIER, VALUE)).get();
    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
  }

  @Test
  public void testIncrement() throws InterruptedException, ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(SLEEP_TIME / 2, TimeUnit.MILLISECONDS).build();
    assertEquals(1L, table.incrementColumnValue(row, FAMILY, QUALIFIER, 1L).get().longValue());
    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
  }
}
