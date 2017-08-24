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
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.procedure2.LockInfo;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

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

  /**
   * Test Increment Mutate conversions.
   *
   * @throws IOException
   */
  @Test
  public void testIncrement() throws IOException {
    long timeStamp = 111111;
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationProto.MutationType.INCREMENT);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(11L)));
    qualifierBuilder.setTimestamp(timeStamp);
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(22L)));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    MutationProto proto = mutateBuilder.build();
    // default fields
    assertEquals(MutationProto.Durability.USE_DEFAULT, proto.getDurability());

    // set the default value for equal comparison
    mutateBuilder = MutationProto.newBuilder(proto);
    mutateBuilder.setDurability(MutationProto.Durability.USE_DEFAULT);

    Increment increment = ProtobufUtil.toIncrement(proto, null);
    mutateBuilder.setTimestamp(increment.getTimeStamp());
    assertEquals(mutateBuilder.build(), ProtobufUtil.toMutation(MutationType.INCREMENT, increment));
  }

  /**
   * Test Append Mutate conversions.
   *
   * @throws IOException
   */
  @Test
  public void testAppend() throws IOException {
    long timeStamp = 111111;
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationType.APPEND);
    mutateBuilder.setTimestamp(timeStamp);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v1"));
    qualifierBuilder.setTimestamp(timeStamp);
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v2"));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    MutationProto proto = mutateBuilder.build();
    // default fields
    assertEquals(MutationProto.Durability.USE_DEFAULT, proto.getDurability());

    // set the default value for equal comparison
    mutateBuilder = MutationProto.newBuilder(proto);
    mutateBuilder.setDurability(MutationProto.Durability.USE_DEFAULT);

    Append append = ProtobufUtil.toAppend(proto, null);

    // append always use the latest timestamp,
    // reset the timestamp to the original mutate
    mutateBuilder.setTimestamp(append.getTimeStamp());
    assertEquals(mutateBuilder.build(), ProtobufUtil.toMutation(MutationType.APPEND, append));
  }
}
