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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;

/**
 * Confirm that we will do backoff when retrying on closing a region, to avoid consuming all the
 * CPUs.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestCloseRegionWhileRSCrash {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCloseRegionWhileRSCrash.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCloseRegionWhileRSCrash.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("Backoff");

  private static byte[] CF = Bytes.toBytes("cf");

  private static CountDownLatch ARRIVE = new CountDownLatch(1);

  private static CountDownLatch RESUME = new CountDownLatch(1);

  public static final class DummyServerProcedure extends Procedure<MasterProcedureEnv>
      implements ServerProcedureInterface {

    private ServerName serverName;

    public DummyServerProcedure() {
    }

    public DummyServerProcedure(ServerName serverName) {
      this.serverName = serverName;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public boolean hasMetaTableRegion() {
      return false;
    }

    @Override
    public ServerOperationType getServerOperationType() {
      return ServerOperationType.CRASH_HANDLER;
    }

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      ARRIVE.countDown();
      RESUME.await();
      return null;
    }

    @Override
    protected LockState acquireLock(final MasterProcedureEnv env) {
      if (env.getProcedureScheduler().waitServerExclusiveLock(this, getServerName())) {
        return LockState.LOCK_EVENT_WAIT;
      }
      return LockState.LOCK_ACQUIRED;
    }

    @Override
    protected void releaseLock(final MasterProcedureEnv env) {
      env.getProcedureScheduler().wakeServerExclusiveLock(this, getServerName());
    }

    @Override
    protected boolean holdLock(MasterProcedureEnv env) {
      return true;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {

    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.startMiniCluster(3);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.getAdmin().balancerSwitch(false, true);
    HRegionServer srcRs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    if (!srcRs.getRegions(TableName.META_TABLE_NAME).isEmpty()) {
      RegionInfo metaRegion = srcRs.getRegions(TableName.META_TABLE_NAME).get(0).getRegionInfo();
      HRegionServer dstRs = UTIL.getOtherRegionServer(srcRs);
      UTIL.getAdmin().move(metaRegion.getEncodedNameAsBytes(),
        Bytes.toBytes(dstRs.getServerName().getServerName()));
      UTIL.waitFor(30000, () -> !dstRs.getRegions(TableName.META_TABLE_NAME).isEmpty());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRetryBackoff() throws IOException, InterruptedException {
    HRegionServer srcRs = UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    RegionInfo region = srcRs.getRegions(TABLE_NAME).get(0).getRegionInfo();
    HRegionServer dstRs = UTIL.getOtherRegionServer(srcRs);
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    long dummyProcId = procExec.submitProcedure(new DummyServerProcedure(srcRs.getServerName()));
    ARRIVE.await();
    UTIL.getMiniHBaseCluster().killRegionServer(srcRs.getServerName());
    UTIL.waitFor(30000,
      () -> procExec.getProcedures().stream().anyMatch(p -> p instanceof ServerCrashProcedure));
    Thread t = new Thread(() -> {
      try {
        UTIL.getAdmin().move(region.getEncodedNameAsBytes(),
          Bytes.toBytes(dstRs.getServerName().getServerName()));
      } catch (IOException e) {
      }
    });
    t.start();
    JsonParser parser = new JsonParser();
    long oldTimeout = 0;
    int timeoutIncrements = 0;
    // wait until we enter the WAITING_TIMEOUT state
    UTIL.waitFor(30000, () -> getTimeout(parser, UTIL.getAdmin().getProcedures()) > 0);
    while (true) {
      long timeout = getTimeout(parser, UTIL.getAdmin().getProcedures());
      if (timeout > oldTimeout) {
        LOG.info("Timeout incremented, was {}, now is {}, increments={}", timeout, oldTimeout,
          timeoutIncrements);
        oldTimeout = timeout;
        timeoutIncrements++;
        if (timeoutIncrements > 3) {
          // If we incremented at least twice, break; the backoff is working.
          break;
        }
      }
      Thread.sleep(1000);
    }
    // let's close the connection to make sure that the SCP can not update meta successfully
    UTIL.getMiniHBaseCluster().getMaster().getConnection().close();
    RESUME.countDown();
    UTIL.waitFor(30000, () -> procExec.isFinished(dummyProcId));
    Thread.sleep(2000);
    // here we restart
    UTIL.getMiniHBaseCluster().stopMaster(0).join();
    UTIL.getMiniHBaseCluster().startMaster();
    t.join();
    // Make sure that the region is online, it may not on the original target server, as we will set
    // forceNewPlan to true if there is a server crash
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(new Put(Bytes.toBytes(1)).addColumn(CF, Bytes.toBytes("cq"), Bytes.toBytes(1)));
    }
  }

  /**
   * @param proceduresAsJSON This is String returned by admin.getProcedures call... an array of
   *          Procedures as JSON.
   * @return The Procedure timeout value parsed from the TRSP.
   */
  private long getTimeout(JsonParser parser, String proceduresAsJSON) {
    JsonArray array = parser.parse(proceduresAsJSON).getAsJsonArray();
    Iterator<JsonElement> iterator = array.iterator();
    while (iterator.hasNext()) {
      JsonElement element = iterator.next();
      JsonObject obj = element.getAsJsonObject();
      String className = obj.get("className").getAsString();
      String actualClassName = TransitRegionStateProcedure.class.getName();
      if (className.equals(actualClassName) && obj.has("timeout")) {
        return obj.get("timeout").getAsLong();
      }
    }
    return -1L;
  }
}
