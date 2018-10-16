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
package org.apache.hadoop.hbase.master.locking;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.locking.LockServiceClient;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.core.IsInstanceOf;
import org.hamcrest.core.StringStartsWith;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockResponse;

@Category({MasterTests.class, MediumTests.class})
public class TestLockProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLockProcedure.class);

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  @Rule
  public TestName testName = new TestName();
  // crank this up if this test turns out to be flaky.
  private static final int HEARTBEAT_TIMEOUT = 2000;
  private static final int LOCAL_LOCKS_TIMEOUT = 4000;

  private static final Logger LOG = LoggerFactory.getLogger(TestLockProcedure.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static MasterRpcServices masterRpcService;
  private static ProcedureExecutor<MasterProcedureEnv> procExec;

  private static String namespace = "namespace";
  private static TableName tableName1 = TableName.valueOf(namespace, "table1");
  private static List<RegionInfo> tableRegions1;
  private static TableName tableName2 = TableName.valueOf(namespace, "table2");
  private static List<RegionInfo> tableRegions2;

  private String testMethodName;

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setBoolean("hbase.procedure.check.owner.set", false);  // since rpc user will be null
    conf.setInt(LockProcedure.REMOTE_LOCKS_TIMEOUT_MS_CONF, HEARTBEAT_TIMEOUT);
    conf.setInt(LockProcedure.LOCAL_MASTER_LOCKS_TIMEOUT_MS_CONF, LOCAL_LOCKS_TIMEOUT);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
    UTIL.getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
    UTIL.createTable(tableName1,
        new byte[][]{ Bytes.toBytes("fam")}, new byte[][] {Bytes.toBytes("1")});
    UTIL.createTable(tableName2,
        new byte[][]{Bytes.toBytes("fam")}, new byte[][] {Bytes.toBytes("1")});
    masterRpcService = UTIL.getHBaseCluster().getMaster().getMasterRpcServices();
    procExec = UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    tableRegions1 = UTIL.getAdmin().getRegions(tableName1);
    tableRegions2 = UTIL.getAdmin().getRegions(tableName2);
    assert tableRegions1.size() > 0;
    assert tableRegions2.size() > 0;
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setup() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    testMethodName = testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    // Kill all running procedures.
    for (Procedure<?> proc : procExec.getProcedures()) {
      procExec.abort(proc.getProcId());
      ProcedureTestingUtility.waitProcedure(procExec, proc);
    }
    assertEquals(0, procExec.getEnvironment().getProcedureScheduler().size());
  }

  private LockRequest getNamespaceLock(String namespace, String description) {
    return LockServiceClient.buildLockRequest(LockServiceProtos.LockType.EXCLUSIVE,
        namespace, null, null, description, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private LockRequest getTableExclusiveLock(TableName tableName, String description) {
    return LockServiceClient.buildLockRequest(LockServiceProtos.LockType.EXCLUSIVE,
        null, tableName, null, description, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private LockRequest getRegionLock(List<RegionInfo> regionInfos, String description) {
    return LockServiceClient.buildLockRequest(LockServiceProtos.LockType.EXCLUSIVE,
        null, null, regionInfos, description, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private void validateLockRequestException(LockRequest lockRequest, String message)
      throws Exception {
    exception.expect(ServiceException.class);
    exception.expectCause(IsInstanceOf.instanceOf(DoNotRetryIOException.class));
    exception.expectMessage(
        StringStartsWith.startsWith("org.apache.hadoop.hbase.DoNotRetryIOException: "
            + "java.lang.IllegalArgumentException: " + message));
    masterRpcService.requestLock(null, lockRequest);
  }

  @Test
  public void testLockRequestValidationEmptyDescription() throws Exception {
    validateLockRequestException(getNamespaceLock("", ""), "Empty description");
  }

  @Test
  public void testLockRequestValidationEmptyNamespaceName() throws Exception {
    validateLockRequestException(getNamespaceLock("", "desc"), "Empty namespace");
  }

  @Test
  public void testLockRequestValidationRegionsFromDifferentTable() throws Exception {
    List<RegionInfo> regions = new ArrayList<>();
    regions.addAll(tableRegions1);
    regions.addAll(tableRegions2);
    validateLockRequestException(getRegionLock(regions, "desc"),
        "All regions should be from same table");
  }

  /**
   * Returns immediately if the lock is acquired.
   * @throws TimeoutException if lock couldn't be acquired.
   */
  private boolean awaitForLocked(long procId, long timeoutInMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutInMs;
    while (System.currentTimeMillis() < deadline) {
      LockHeartbeatResponse response = masterRpcService.lockHeartbeat(null,
          LockHeartbeatRequest.newBuilder().setProcId(procId).build());
      if (response.getLockStatus() == LockHeartbeatResponse.LockStatus.LOCKED) {
        assertEquals(HEARTBEAT_TIMEOUT, response.getTimeoutMs());
        LOG.debug(String.format("Proc id %s acquired lock.", procId));
        return true;
      }
      Thread.sleep(100);
    }
    return false;
  }

  private long queueLock(LockRequest lockRequest) throws ServiceException {
    LockResponse response = masterRpcService.requestLock(null, lockRequest);
    return response.getProcId();
  }

  private void sendHeartbeatAndCheckLocked(long procId, boolean isLocked) throws ServiceException {
    LockHeartbeatResponse response = masterRpcService.lockHeartbeat(null,
        LockHeartbeatRequest.newBuilder().setProcId(procId).build());
    if (isLocked) {
      assertEquals(LockHeartbeatResponse.LockStatus.LOCKED, response.getLockStatus());
    } else {
      assertEquals(LockHeartbeatResponse.LockStatus.UNLOCKED, response.getLockStatus());
    }
    LOG.debug(String.format("Proc id %s : %s.", procId, response.getLockStatus()));
  }

  private void releaseLock(long procId) throws ServiceException {
    masterRpcService.lockHeartbeat(null,
        LockHeartbeatRequest.newBuilder().setProcId(procId).setKeepAlive(false).build());
  }

  @Test
  public void testUpdateHeartbeatAndUnlockForTable() throws Exception {
    LockRequest lock = getTableExclusiveLock(tableName1, testMethodName);
    final long procId = queueLock(lock);
    assertTrue(awaitForLocked(procId, 2000));
    Thread.sleep(HEARTBEAT_TIMEOUT /2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(HEARTBEAT_TIMEOUT /2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(HEARTBEAT_TIMEOUT /2);
    sendHeartbeatAndCheckLocked(procId, true);
    releaseLock(procId);
    sendHeartbeatAndCheckLocked(procId, false);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  @Test
  public void testAbort() throws Exception {
    LockRequest lock = getTableExclusiveLock(tableName1, testMethodName);
    final long procId = queueLock(lock);
    assertTrue(awaitForLocked(procId, 2000));
    assertTrue(procExec.abort(procId));
    sendHeartbeatAndCheckLocked(procId, false);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  @Test
  public void testUpdateHeartbeatAndUnlockForNamespace() throws Exception {
    LockRequest lock = getNamespaceLock(namespace, testMethodName);
    final long procId = queueLock(lock);
    assertTrue(awaitForLocked(procId, 2000));
    Thread.sleep(HEARTBEAT_TIMEOUT /2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(HEARTBEAT_TIMEOUT /2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(HEARTBEAT_TIMEOUT /2);
    sendHeartbeatAndCheckLocked(procId, true);
    releaseLock(procId);
    sendHeartbeatAndCheckLocked(procId, false);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  @Test
  public void testTimeout() throws Exception {
    LockRequest lock = getNamespaceLock(namespace, testMethodName);
    final long procId = queueLock(lock);
    assertTrue(awaitForLocked(procId, 2000));
    Thread.sleep(HEARTBEAT_TIMEOUT / 2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(HEARTBEAT_TIMEOUT / 2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(4 * HEARTBEAT_TIMEOUT);
    sendHeartbeatAndCheckLocked(procId, false);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  @Test
  public void testMultipleLocks() throws Exception {
    LockRequest nsLock = getNamespaceLock(namespace, testMethodName);
    LockRequest tableLock1 = getTableExclusiveLock(tableName1, testMethodName);
    LockRequest tableLock2 =  getTableExclusiveLock(tableName2, testMethodName);
    LockRequest regionsLock1 = getRegionLock(tableRegions1, testMethodName);
    LockRequest regionsLock2 = getRegionLock(tableRegions2, testMethodName);
    // Acquire namespace lock, then queue other locks.
    long nsProcId = queueLock(nsLock);
    assertTrue(awaitForLocked(nsProcId, 2000));
    long start = System.currentTimeMillis();
    sendHeartbeatAndCheckLocked(nsProcId, true);
    long table1ProcId = queueLock(tableLock1);
    long table2ProcId = queueLock(tableLock2);
    long regions1ProcId = queueLock(regionsLock1);
    long regions2ProcId = queueLock(regionsLock2);

    // Assert tables & region locks are waiting because of namespace lock.
    long now = System.currentTimeMillis();
    // leave extra 10 msec in case more than half the HEARTBEAT_TIMEOUT has passed
    Thread.sleep(Math.min(HEARTBEAT_TIMEOUT / 2, Math.max(HEARTBEAT_TIMEOUT-(now-start)-10, 0)));
    sendHeartbeatAndCheckLocked(nsProcId, true);
    sendHeartbeatAndCheckLocked(table1ProcId, false);
    sendHeartbeatAndCheckLocked(table2ProcId, false);
    sendHeartbeatAndCheckLocked(regions1ProcId, false);
    sendHeartbeatAndCheckLocked(regions2ProcId, false);

    // Release namespace lock and assert tables locks are acquired but not region lock
    releaseLock(nsProcId);
    assertTrue(awaitForLocked(table1ProcId, 2000));
    assertTrue(awaitForLocked(table2ProcId, 2000));
    sendHeartbeatAndCheckLocked(regions1ProcId, false);
    sendHeartbeatAndCheckLocked(regions2ProcId, false);

    // Release table1 lock and assert region lock is acquired.
    releaseLock(table1ProcId);
    sendHeartbeatAndCheckLocked(table1ProcId, false);
    assertTrue(awaitForLocked(regions1ProcId, 2000));
    sendHeartbeatAndCheckLocked(table2ProcId, true);
    sendHeartbeatAndCheckLocked(regions2ProcId, false);

    // Release table2 lock and assert region lock is acquired.
    releaseLock(table2ProcId);
    sendHeartbeatAndCheckLocked(table2ProcId, false);
    assertTrue(awaitForLocked(regions2ProcId, 2000));
    sendHeartbeatAndCheckLocked(regions1ProcId, true);
    sendHeartbeatAndCheckLocked(regions2ProcId, true);

    // Release region locks.
    releaseLock(regions1ProcId);
    releaseLock(regions2ProcId);
    sendHeartbeatAndCheckLocked(regions1ProcId, false);
    sendHeartbeatAndCheckLocked(regions2ProcId, false);
    ProcedureTestingUtility.waitAllProcedures(procExec);
    ProcedureTestingUtility.assertProcNotFailed(procExec, nsProcId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, table1ProcId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, table2ProcId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, regions1ProcId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, regions2ProcId);
  }

  // Test latch is decreased in count when lock is acquired.
  @Test
  public void testLatch() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    // MasterRpcServices don't set latch with LockProcedure, so create one and submit it directly.
    LockProcedure lockProc = new LockProcedure(UTIL.getConfiguration(),
        TableName.valueOf("table"),
        org.apache.hadoop.hbase.procedure2.LockType.EXCLUSIVE, "desc", latch);
    procExec.submitProcedure(lockProc);
    assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
    releaseLock(lockProc.getProcId());
    ProcedureTestingUtility.waitProcedure(procExec, lockProc.getProcId());
    ProcedureTestingUtility.assertProcNotFailed(procExec, lockProc.getProcId());
  }

  // LockProcedures with latch are considered local locks.
  @Test
  public void testLocalLockTimeout() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    // MasterRpcServices don't set latch with LockProcedure, so create one and submit it directly.
    LockProcedure lockProc = new LockProcedure(UTIL.getConfiguration(),
        TableName.valueOf("table"), LockType.EXCLUSIVE, "desc", latch);
    procExec.submitProcedure(lockProc);
    assertTrue(awaitForLocked(lockProc.getProcId(), 2000));
    Thread.sleep(LOCAL_LOCKS_TIMEOUT / 2);
    assertTrue(lockProc.isLocked());
    Thread.sleep(2 * LOCAL_LOCKS_TIMEOUT);
    assertFalse(lockProc.isLocked());
    releaseLock(lockProc.getProcId());
    ProcedureTestingUtility.waitProcedure(procExec, lockProc.getProcId());
    ProcedureTestingUtility.assertProcNotFailed(procExec, lockProc.getProcId());
  }

  private void testRemoteLockRecovery(LockRequest lock) throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    final long procId = queueLock(lock);
    assertTrue(awaitForLocked(procId, 2000));

    // wait for proc Executor to die, then restart it and wait for Lock Procedure to get started.
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertEquals(false, procExec.isRunning());
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.restart(procExec);
    while (!procExec.isStarted(procId)) {
      Thread.sleep(250);
    }
    assertEquals(true, procExec.isRunning());

    // After recovery, remote locks should reacquire locks and function normally.
    assertTrue(awaitForLocked(procId, 2000));
    Thread.sleep(HEARTBEAT_TIMEOUT/2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(HEARTBEAT_TIMEOUT/2);
    sendHeartbeatAndCheckLocked(procId, true);
    Thread.sleep(2 * HEARTBEAT_TIMEOUT + HEARTBEAT_TIMEOUT/2);
    sendHeartbeatAndCheckLocked(procId, false);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  @Test
  public void testRemoteTableLockRecovery() throws Exception {
    LockRequest lock = getTableExclusiveLock(tableName1, testMethodName);
    testRemoteLockRecovery(lock);
  }

  @Test
  public void testRemoteNamespaceLockRecovery() throws Exception {
    LockRequest lock = getNamespaceLock(namespace, testMethodName);
    testRemoteLockRecovery(lock);
  }

  @Test
  public void testRemoteRegionLockRecovery() throws Exception {
    LockRequest lock = getRegionLock(tableRegions1, testMethodName);
    testRemoteLockRecovery(lock);
  }

  @Test
  public void testLocalMasterLockRecovery() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    CountDownLatch latch = new CountDownLatch(1);
    LockProcedure lockProc = new LockProcedure(UTIL.getConfiguration(),
        TableName.valueOf("table"), LockType.EXCLUSIVE, "desc", latch);
    procExec.submitProcedure(lockProc);
    assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));

    // wait for proc Executor to die, then restart it and wait for Lock Procedure to get started.
    ProcedureTestingUtility.waitProcedure(procExec, lockProc.getProcId());
    assertEquals(false, procExec.isRunning());
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    // remove zk lock node otherwise recovered lock will keep waiting on it.
    ProcedureTestingUtility.restart(procExec);
    while (!procExec.isStarted(lockProc.getProcId())) {
      Thread.sleep(250);
    }
    assertEquals(true, procExec.isRunning());
    ProcedureTestingUtility.waitProcedure(procExec, lockProc.getProcId());
    Procedure<?> result = procExec.getResultOrProcedure(lockProc.getProcId());
    assertTrue(result != null && !result.isFailed());
    ProcedureTestingUtility.assertProcNotFailed(procExec, lockProc.getProcId());
  }
}
