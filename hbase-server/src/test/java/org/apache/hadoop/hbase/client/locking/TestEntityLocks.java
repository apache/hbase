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
package org.apache.hadoop.hbase.client.locking;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.PerClientRandomNonceGenerator;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockType;

@Category({ClientTests.class, SmallTests.class})
public class TestEntityLocks {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEntityLocks.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestEntityLocks.class);

  private final Configuration conf = HBaseConfiguration.create();

  private final LockService.BlockingInterface master =
    Mockito.mock(LockService.BlockingInterface.class);

  private LockServiceClient admin;
  private ArgumentCaptor<LockRequest> lockReqArgCaptor;
  private ArgumentCaptor<LockHeartbeatRequest> lockHeartbeatReqArgCaptor;

  private static final LockHeartbeatResponse UNLOCKED_RESPONSE =
      LockHeartbeatResponse.newBuilder().setLockStatus(
          LockHeartbeatResponse.LockStatus.UNLOCKED).build();
  // timeout such that worker thread waits for 500ms for each heartbeat.
  private static final LockHeartbeatResponse LOCKED_RESPONSE =
      LockHeartbeatResponse.newBuilder().setLockStatus(
          LockHeartbeatResponse.LockStatus.LOCKED).setTimeoutMs(10000).build();
  private long procId;

  // Setup mock admin.
  LockServiceClient getAdmin() throws Exception {
    conf.setInt("hbase.client.retries.number", 3);
    conf.setInt("hbase.client.pause", 1);  // 1ms. Immediately retry rpc on failure.
    return new LockServiceClient(conf, master, PerClientRandomNonceGenerator.get());
  }

  @Before
  public void setUp() throws Exception {
    admin = getAdmin();
    lockReqArgCaptor = ArgumentCaptor.forClass(LockRequest.class);
    lockHeartbeatReqArgCaptor = ArgumentCaptor.forClass(LockHeartbeatRequest.class);
    procId = ThreadLocalRandom.current().nextLong();
  }

  private boolean waitLockTimeOut(EntityLock lock, long maxWaitTimeMillis) {
    long startMillis = EnvironmentEdgeManager.currentTime();
    while (lock.isLocked()) {
      LOG.info("Sleeping...");
      Threads.sleepWithoutInterrupt(100);
      if (!lock.isLocked()) {
        return true;
      }
      if (EnvironmentEdgeManager.currentTime() - startMillis > maxWaitTimeMillis) {
        LOG.info("Timedout...");
        return false;
      }
    }
    return true; // to make compiler happy.
  }

  /**
   * Test basic lock function - requestLock, await, unlock.
   * @throws Exception
   */
  @Test
  public void testEntityLock() throws Exception {
    final long procId = 100;
    final long workerSleepTime = 200;  // in ms
    EntityLock lock = admin.namespaceLock("namespace", "description", null);
    lock.setTestingSleepTime(workerSleepTime);

    when(master.requestLock(any(), any())).thenReturn(
        LockResponse.newBuilder().setProcId(procId).build());
    when(master.lockHeartbeat(any(), any())).thenReturn(
        UNLOCKED_RESPONSE, UNLOCKED_RESPONSE, UNLOCKED_RESPONSE, LOCKED_RESPONSE);

    lock.requestLock();
    // we return unlock response 3 times, so actual wait time should be around 2 * workerSleepTime
    lock.await(4 * workerSleepTime, TimeUnit.MILLISECONDS);
    assertTrue(lock.isLocked());
    lock.unlock();
    assertTrue(!lock.getWorker().isAlive());
    assertFalse(lock.isLocked());

    // check LockRequest in requestLock()
    verify(master, times(1)).requestLock(any(), lockReqArgCaptor.capture());
    LockRequest request = lockReqArgCaptor.getValue();
    assertEquals("namespace", request.getNamespace());
    assertEquals("description", request.getDescription());
    assertEquals(LockType.EXCLUSIVE, request.getLockType());
    assertEquals(0, request.getRegionInfoCount());

    // check LockHeartbeatRequest in lockHeartbeat()
    verify(master, atLeastOnce()).lockHeartbeat(any(), lockHeartbeatReqArgCaptor.capture());
    for (LockHeartbeatRequest req : lockHeartbeatReqArgCaptor.getAllValues()) {
      assertEquals(procId, req.getProcId());
    }
  }

  /**
   * Test that abort is called when lock times out.
   */
  @Test
  public void testEntityLockTimeout() throws Exception {
    final long workerSleepTime = 200;  // in ms
    Abortable abortable = Mockito.mock(Abortable.class);
    EntityLock lock = admin.namespaceLock("namespace", "description", abortable);
    lock.setTestingSleepTime(workerSleepTime);

    when(master.requestLock(any(), any()))
        .thenReturn(LockResponse.newBuilder().setProcId(procId).build());
    // Acquires the lock, but then it times out (since we don't call unlock() on it).
    when(master.lockHeartbeat(any(), any()))
      .thenReturn(LOCKED_RESPONSE, UNLOCKED_RESPONSE);

    lock.requestLock();
    lock.await();
    assertTrue(lock.isLocked());
    // Should get unlocked in next heartbeat i.e. after workerSleepTime. Wait 10x time to be sure.
    assertTrue(waitLockTimeOut(lock, 10 * workerSleepTime));

    // Works' run() returns, there is a small gap that the thread is still alive(os
    // has not declare it is dead yet), so remove the following assertion.
    // assertFalse(lock.getWorker().isAlive());
    verify(abortable, times(1)).abort(any(), eq(null));
  }

  /**
   * Test that abort is called when lockHeartbeat fails with IOException.
   */
  @Test
  public void testHeartbeatException() throws Exception {
    final long workerSleepTime = 100;  // in ms
    Abortable abortable = Mockito.mock(Abortable.class);
    EntityLock lock = admin.namespaceLock("namespace", "description", abortable);
    lock.setTestingSleepTime(workerSleepTime);

    when(master.requestLock(any(), any()))
        .thenReturn(LockResponse.newBuilder().setProcId(procId).build());
    when(master.lockHeartbeat(any(), any()))
        .thenReturn(LOCKED_RESPONSE)
        .thenThrow(new ServiceException("Failed heartbeat!"));

    lock.requestLock();
    lock.await();
    assertTrue(waitLockTimeOut(lock, 100 * workerSleepTime));
    while (lock.getWorker().isAlive()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    verify(abortable, times(1)).abort(any(), isA(HBaseIOException.class));
    assertFalse(lock.getWorker().isAlive());
  }
}
