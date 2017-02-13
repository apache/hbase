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
package org.apache.hadoop.hbase.shaded.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ProcedureState;
import org.apache.hadoop.hbase.procedure2.LockInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestProtobufUtil {
  public TestProtobufUtil() {
  }

  private static ProcedureInfo createProcedureInfo(long procId)
  {
    return new ProcedureInfo(procId, "java.lang.Object", null,
        ProcedureState.RUNNABLE, -1, null, null, 0, 0, null);
  }

  private static void assertProcedureInfoEquals(ProcedureInfo expected,
      ProcedureInfo result)
  {
    if (expected == result) {
      return;
    } else if (expected == null || result == null) {
      fail();
    }

    assertEquals(expected.getProcId(), result.getProcId());
  }

  private static void assertLockInfoEquals(LockInfo expected, LockInfo result)
  {
    assertEquals(expected.getResourceType(), result.getResourceType());
    assertEquals(expected.getResourceName(), result.getResourceName());
    assertEquals(expected.getLockType(), result.getLockType());
    assertProcedureInfoEquals(expected.getExclusiveLockOwnerProcedure(),
        result.getExclusiveLockOwnerProcedure());
    assertEquals(expected.getSharedLockCount(), result.getSharedLockCount());
  }

  private static void assertWaitingProcedureEquals(
      LockInfo.WaitingProcedure expected, LockInfo.WaitingProcedure result)
  {
    assertEquals(expected.getLockType(), result.getLockType());
    assertProcedureInfoEquals(expected.getProcedure(),
        result.getProcedure());
  }

  @Test
  public void testServerLockInfo() {
    LockInfo lock = new LockInfo();
    lock.setResourceType(LockInfo.ResourceType.SERVER);
    lock.setResourceName("server");
    lock.setLockType(LockInfo.LockType.SHARED);
    lock.setSharedLockCount(2);

    LockServiceProtos.LockInfo proto = ProtobufUtil.toProtoLockInfo(lock);
    LockInfo lock2 = ProtobufUtil.toLockInfo(proto);

    assertLockInfoEquals(lock, lock2);
  }

  @Test
  public void testNamespaceLockInfo() {
    LockInfo lock = new LockInfo();
    lock.setResourceType(LockInfo.ResourceType.NAMESPACE);
    lock.setResourceName("ns");
    lock.setLockType(LockInfo.LockType.EXCLUSIVE);
    lock.setExclusiveLockOwnerProcedure(createProcedureInfo(2));

    LockServiceProtos.LockInfo proto = ProtobufUtil.toProtoLockInfo(lock);
    LockInfo lock2 = ProtobufUtil.toLockInfo(proto);

    assertLockInfoEquals(lock, lock2);
  }

  @Test
  public void testTableLockInfo() {
    LockInfo lock = new LockInfo();
    lock.setResourceType(LockInfo.ResourceType.TABLE);
    lock.setResourceName("table");
    lock.setLockType(LockInfo.LockType.SHARED);
    lock.setSharedLockCount(2);

    LockServiceProtos.LockInfo proto = ProtobufUtil.toProtoLockInfo(lock);
    LockInfo lock2 = ProtobufUtil.toLockInfo(proto);

    assertLockInfoEquals(lock, lock2);
  }

  @Test
  public void testRegionLockInfo() {
    LockInfo lock = new LockInfo();
    lock.setResourceType(LockInfo.ResourceType.REGION);
    lock.setResourceName("region");
    lock.setLockType(LockInfo.LockType.EXCLUSIVE);
    lock.setExclusiveLockOwnerProcedure(createProcedureInfo(2));

    LockServiceProtos.LockInfo proto = ProtobufUtil.toProtoLockInfo(lock);
    LockInfo lock2 = ProtobufUtil.toLockInfo(proto);

    assertLockInfoEquals(lock, lock2);
  }

  @Test
  public void testExclusiveWaitingLockInfo() {
    LockInfo.WaitingProcedure waitingProcedure = new LockInfo.WaitingProcedure();
    waitingProcedure.setLockType(LockInfo.LockType.EXCLUSIVE);
    waitingProcedure.setProcedure(createProcedureInfo(1));

    LockServiceProtos.WaitingProcedure proto = ProtobufUtil.toProtoWaitingProcedure(waitingProcedure);
    LockInfo.WaitingProcedure waitingProcedure2 = ProtobufUtil.toWaitingProcedure(proto);

    assertWaitingProcedureEquals(waitingProcedure, waitingProcedure2);
  }

  @Test
  public void testSharedWaitingLockInfo() {
    LockInfo.WaitingProcedure waitingProcedure = new LockInfo.WaitingProcedure();
    waitingProcedure.setLockType(LockInfo.LockType.SHARED);
    waitingProcedure.setProcedure(createProcedureInfo(2));

    LockServiceProtos.WaitingProcedure proto = ProtobufUtil.toProtoWaitingProcedure(waitingProcedure);
    LockInfo.WaitingProcedure waitingProcedure2 = ProtobufUtil.toWaitingProcedure(proto);

    assertWaitingProcedureEquals(waitingProcedure, waitingProcedure2);
  }
}
