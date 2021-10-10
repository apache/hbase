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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

public abstract class AbstractTestAsyncTableRegionReplicasRead {

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static TableName TABLE_NAME = TableName.valueOf("async");

  protected static byte[] FAMILY = Bytes.toBytes("cf");

  protected static byte[] QUALIFIER = Bytes.toBytes("cq");

  protected static byte[] ROW = Bytes.toBytes("row");

  protected static byte[] VALUE = Bytes.toBytes("value");

  protected static int REPLICA_COUNT = 3;

  protected static AsyncConnection ASYNC_CONN;

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
    return Arrays.asList(
      new Supplier<?>[] { AbstractTestAsyncTableRegionReplicasRead::getRawTable },
      new Supplier<?>[] { AbstractTestAsyncTableRegionReplicasRead::getTable });
  }

  protected static volatile boolean FAIL_PRIMARY_GET = false;

  protected static ConcurrentMap<Integer, AtomicInteger> REPLICA_ID_TO_COUNT =
    new ConcurrentHashMap<>();

  public static final class FailPrimaryGetCP implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    private void recordAndTryFail(ObserverContext<RegionCoprocessorEnvironment> c)
        throws IOException {
      RegionInfo region = c.getEnvironment().getRegionInfo();
      if (!region.getTable().equals(TABLE_NAME)) {
        return;
      }
      REPLICA_ID_TO_COUNT.computeIfAbsent(region.getReplicaId(), k -> new AtomicInteger())
        .incrementAndGet();
      if (region.getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID && FAIL_PRIMARY_GET) {
        throw new IOException("Inject error");
      }
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
        List<Cell> result) throws IOException {
      recordAndTryFail(c);
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
        throws IOException {
      recordAndTryFail(c);
    }
  }

  private static boolean allReplicasHaveRow(byte[] row) throws IOException {
    for (RegionServerThread t : TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      for (HRegion region : t.getRegionServer().getRegions(TABLE_NAME)) {
        if (region.get(new Get(row), false).isEmpty()) {
          return false;
        }
      }
    }
    return true;
  }

  protected static void startClusterAndCreateTable() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).setRegionReplication(REPLICA_COUNT)
      .setCoprocessor(FailPrimaryGetCP.class.getName()).build());
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  protected static void waitUntilAllReplicasHaveRow(byte[] row) throws IOException {
    // this is the fastest way to let all replicas have the row
    TEST_UTIL.getAdmin().disableTable(TABLE_NAME);
    TEST_UTIL.getAdmin().enableTable(TABLE_NAME);
    TEST_UTIL.waitFor(30000, () -> allReplicasHaveRow(row));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  protected static int getSecondaryGetCount() {
    return REPLICA_ID_TO_COUNT.entrySet().stream()
      .filter(e -> e.getKey().intValue() != RegionReplicaUtil.DEFAULT_REPLICA_ID)
      .mapToInt(e -> e.getValue().get()).sum();
  }

  protected static int getPrimaryGetCount() {
    AtomicInteger primaryGetCount = REPLICA_ID_TO_COUNT.get(RegionReplicaUtil.DEFAULT_REPLICA_ID);
    return primaryGetCount != null ? primaryGetCount.get() : 0;
  }

  // replicaId = -1 means do not set replica
  protected abstract void readAndCheck(AsyncTable<?> table, int replicaId) throws Exception;

  @Test
  public void testNoReplicaRead() throws Exception {
    FAIL_PRIMARY_GET = false;
    REPLICA_ID_TO_COUNT.clear();
    AsyncTable<?> table = getTable.get();
    readAndCheck(table, -1);
    // the primary region is fine and the primary timeout is 1 second which is long enough, so we
    // should not send any requests to secondary replicas even if the consistency is timeline.
    Thread.sleep(5000);
    assertEquals(0, getSecondaryGetCount());
  }

  @Test
  public void testReplicaRead() throws Exception {
    // fail the primary get request
    FAIL_PRIMARY_GET = true;
    REPLICA_ID_TO_COUNT.clear();
    // make sure that we could still get the value from secondary replicas
    AsyncTable<?> table = getTable.get();
    readAndCheck(table, -1);
    // make sure that the primary request has been canceled
    Thread.sleep(5000);
    int count = getPrimaryGetCount();
    Thread.sleep(10000);
    assertEquals(count, getPrimaryGetCount());
  }

  @Test
  public void testReadSpecificReplica() throws Exception {
    FAIL_PRIMARY_GET = false;
    REPLICA_ID_TO_COUNT.clear();
    AsyncTable<?> table = getTable.get();
    for (int replicaId = 0; replicaId < REPLICA_COUNT; replicaId++) {
      readAndCheck(table, replicaId);
      assertEquals(1, REPLICA_ID_TO_COUNT.get(replicaId).get());
    }
  }
}
