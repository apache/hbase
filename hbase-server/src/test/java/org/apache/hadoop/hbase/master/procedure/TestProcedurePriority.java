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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Test to ensure that the priority for procedures and stuck checker can partially solve the problem
 * describe in HBASE-19976, that is, RecoverMetaProcedure can finally be executed within a certain
 * period of time.
 * <p>
 * As of HBASE-28199, we no longer block a worker when updating meta now, so this test can not test
 * adding procedure worker now, but it could still be used to make sure that we could make progress
 * when meta is gone and we have a lot of pending TRSPs.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestProcedurePriority {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedurePriority.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static String TABLE_NAME_PREFIX = "TestProcedurePriority-";

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int CORE_POOL_SIZE;

  private static int TABLE_COUNT;

  private static volatile boolean FAIL = false;

  public static final class MyCP implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<? extends RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {
      if (FAIL && c.getEnvironment().getRegionInfo().isMetaRegion()) {
        throw new IOException("Inject error");
      }
    }

    @Override
    public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
      WALEdit edit, Durability durability) throws IOException {
      if (FAIL && c.getEnvironment().getRegionInfo().isMetaRegion()) {
        throw new IOException("Inject error");
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setLong(ProcedureExecutor.WORKER_KEEP_ALIVE_TIME_CONF_KEY, 5000);
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 4);
    UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, MyCP.class.getName());
    UTIL.startMiniCluster(3);
    CORE_POOL_SIZE =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getCorePoolSize();
    TABLE_COUNT = 50 * CORE_POOL_SIZE;
    List<Future<?>> futures = new ArrayList<>();
    AsyncAdmin admin = UTIL.getAsyncConnection().getAdmin();
    Semaphore concurrency = new Semaphore(10);
    for (int i = 0; i < TABLE_COUNT; i++) {
      concurrency.acquire();
      futures.add(admin
        .createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME_PREFIX + i))
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build())
        .whenComplete((r, e) -> concurrency.release()));
    }
    for (Future<?> future : futures) {
      future.get(3, TimeUnit.MINUTES);
    }
    UTIL.getAdmin().balance(BalanceRequest.newBuilder().setIgnoreRegionsInTransition(true).build());
    UTIL.waitUntilNoRegionsInTransition();
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    RegionServerThread rsWithMetaThread = UTIL.getMiniHBaseCluster().getRegionServerThreads()
      .stream().filter(t -> !t.getRegionServer().getRegions(TableName.META_TABLE_NAME).isEmpty())
      .findAny().get();
    HRegionServer rsNoMeta = UTIL.getOtherRegionServer(rsWithMetaThread.getRegionServer());
    FAIL = true;
    UTIL.getMiniHBaseCluster().killRegionServer(rsNoMeta.getServerName());
    ProcedureExecutor<?> executor =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // wait until we have way more TRSPs than the core pool size, and then make sure we can recover
    // normally
    UTIL.waitFor(60000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return executor.getProcedures().stream().filter(p -> !p.isFinished())
          .filter(p -> p.getState() != ProcedureState.INITIALIZING)
          .filter(p -> p instanceof TransitRegionStateProcedure).count() > 5 * CORE_POOL_SIZE;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Not enough TRSPs scheduled";
      }
    });
    // sleep more time to make sure the TRSPs have been executed
    Thread.sleep(10000);
    UTIL.getMiniHBaseCluster().killRegionServer(rsWithMetaThread.getRegionServer().getServerName());
    rsWithMetaThread.join();
    FAIL = false;
    // verify that the cluster is back
    UTIL.waitUntilNoRegionsInTransition(480000);
    for (int i = 0; i < TABLE_COUNT; i++) {
      try (Table table = UTIL.getConnection().getTable(TableName.valueOf(TABLE_NAME_PREFIX + i))) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    UTIL.waitFor(60000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return executor.getWorkerThreadCount() == CORE_POOL_SIZE;
      }

      @Override
      public String explainFailure() throws Exception {
        return "The new workers do not timeout";
      }
    });
  }
}
