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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableRegionReplicasGet {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRegionReplicasGet.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection ASYNC_CONN;

  @Rule
  public TestName testName = new TestName();

  @Parameter
  public Supplier<AsyncTable<?>> getTable;

  private static AsyncTable<?> getRawTable() {
    return ASYNC_CONN.getTable(TABLE_NAME);
  }

  private static AsyncTable<?> getTable() {
    return ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
  }

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestAsyncTableRegionReplicasGet::getRawTable },
      new Supplier<?>[] { TestAsyncTableRegionReplicasGet::getTable });
  }

  private static volatile boolean FAIL_PRIMARY_GET = false;

  private static AtomicInteger PRIMARY_GET_COUNT = new AtomicInteger(0);

  private static AtomicInteger SECONDARY_GET_COUNT = new AtomicInteger(0);

  public static final class FailPrimaryGetCP implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
        List<Cell> result) throws IOException {
      RegionInfo region = c.getEnvironment().getRegionInfo();
      if (!region.getTable().equals(TABLE_NAME)) {
        return;
      }
      if (region.getReplicaId() != RegionReplicaUtil.DEFAULT_REPLICA_ID) {
        SECONDARY_GET_COUNT.incrementAndGet();
      } else {
        PRIMARY_GET_COUNT.incrementAndGet();
        if (FAIL_PRIMARY_GET) {
          throw new IOException("Inject error");
        }
      }
    }
  }

  private static boolean allReplicasHaveRow() throws IOException {
    for (RegionServerThread t : TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      for (HRegion region : t.getRegionServer().getRegions(TABLE_NAME)) {
        if (region.get(new Get(ROW), false).isEmpty()) {
          return false;
        }
      }
    }
    return true;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // 10 mins
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_RPC_READ_TIMEOUT_KEY,
      TimeUnit.MINUTES.toMillis(10));
    // 1 second
    TEST_UTIL.getConfiguration().setLong(ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND,
      TimeUnit.SECONDS.toMicros(1));
    // set a small pause so we will retry very quickly
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_PAUSE, 10);
    // infinite retry
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, Integer.MAX_VALUE);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).setRegionReplication(3)
        .setCoprocessor(FailPrimaryGetCP.class.getName()).build());
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    AsyncTable<?> table = ASYNC_CONN.getTable(TABLE_NAME);
    table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)).get();
    // this is the fastest way to let all replicas have the row
    TEST_UTIL.getAdmin().disableTable(TABLE_NAME);
    TEST_UTIL.getAdmin().enableTable(TABLE_NAME);
    TEST_UTIL.waitFor(30000, () -> allReplicasHaveRow());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    IOUtils.closeQuietly(ASYNC_CONN);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNoReplicaRead() throws Exception {
    FAIL_PRIMARY_GET = false;
    SECONDARY_GET_COUNT.set(0);
    AsyncTable<?> table = getTable.get();
    Get get = new Get(ROW).setConsistency(Consistency.TIMELINE);
    for (int i = 0; i < 1000; i++) {
      assertArrayEquals(VALUE, table.get(get).get().getValue(FAMILY, QUALIFIER));
    }
    // the primary region is fine and the primary timeout is 1 second which is long enough, so we
    // should not send any requests to secondary replicas even if the consistency is timeline.
    Thread.sleep(5000);
    assertEquals(0, SECONDARY_GET_COUNT.get());
  }

  @Test
  public void testReplicaRead() throws Exception {
    // fail the primary get request
    FAIL_PRIMARY_GET = true;
    Get get = new Get(ROW).setConsistency(Consistency.TIMELINE);
    // make sure that we could still get the value from secondary replicas
    AsyncTable<?> table = getTable.get();
    assertArrayEquals(VALUE, table.get(get).get().getValue(FAMILY, QUALIFIER));
    // make sure that the primary request has been canceled
    Thread.sleep(5000);
    int count = PRIMARY_GET_COUNT.get();
    Thread.sleep(10000);
    assertEquals(count, PRIMARY_GET_COUNT.get());
  }
}
