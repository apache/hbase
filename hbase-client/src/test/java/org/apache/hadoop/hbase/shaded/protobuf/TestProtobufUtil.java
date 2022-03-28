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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.Any;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.BytesValue;

import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.DeleteType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@Category(SmallTests.class)
public class TestProtobufUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProtobufUtil.class);
  private static final String TAG_STR = "tag-1";
  private static final byte TAG_TYPE = (byte)10;
  public TestProtobufUtil() {
  }

  @Test
  public void testException() throws IOException {
    NameBytesPair.Builder builder = NameBytesPair.newBuilder();
    final String omg = "OMG!!!";
    builder.setName("java.io.IOException");
    builder.setValue(ByteString.copyFrom(Bytes.toBytes(omg)));
    Throwable t = ProtobufUtil.toException(builder.build());
    assertEquals(omg, t.getMessage());
    builder.clear();
    builder.setName("org.apache.hadoop.ipc.RemoteException");
    builder.setValue(ByteString.copyFrom(Bytes.toBytes(omg)));
    t = ProtobufUtil.toException(builder.build());
    assertEquals(omg, t.getMessage());
  }

  /**
   * Test basic Get conversions.
   *
   * @throws IOException if the conversion to a {@link Get} fails
   */
  @Test
  public void testGet() throws IOException {
    ClientProtos.Get.Builder getBuilder = ClientProtos.Get.newBuilder();
    getBuilder.setRow(ByteString.copyFromUtf8("row"));
    Column.Builder columnBuilder = Column.newBuilder();
    columnBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    columnBuilder.addQualifier(ByteString.copyFromUtf8("c1"));
    columnBuilder.addQualifier(ByteString.copyFromUtf8("c2"));
    getBuilder.addColumn(columnBuilder.build());

    columnBuilder.clear();
    columnBuilder.setFamily(ByteString.copyFromUtf8("f2"));
    getBuilder.addColumn(columnBuilder.build());
    getBuilder.setLoadColumnFamiliesOnDemand(true);
    ClientProtos.Get proto = getBuilder.build();
    // default fields
    assertEquals(1, proto.getMaxVersions());
    assertEquals(true, proto.getCacheBlocks());

    // set the default value for equal comparison
    getBuilder = ClientProtos.Get.newBuilder(proto);
    getBuilder.setMaxVersions(1);
    getBuilder.setCacheBlocks(true);
    getBuilder.setTimeRange(ProtobufUtil.toTimeRange(TimeRange.allTime()));
    Get get = ProtobufUtil.toGet(proto);
    assertEquals(getBuilder.build(), ProtobufUtil.toGet(get));
  }

  /**
   * Test Delete Mutate conversions.
   *
   * @throws IOException if the conversion to a {@link Delete} or a
   *                     {@link org.apache.hadoop.hbase.client.Mutation} fails
   */
  @Test
  public void testDelete() throws IOException {
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationType.DELETE);
    mutateBuilder.setTimestamp(111111);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setDeleteType(DeleteType.DELETE_ONE_VERSION);
    qualifierBuilder.setTimestamp(111222);
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setDeleteType(DeleteType.DELETE_MULTIPLE_VERSIONS);
    qualifierBuilder.setTimestamp(111333);
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    MutationProto proto = mutateBuilder.build();
    // default fields
    assertEquals(MutationProto.Durability.USE_DEFAULT, proto.getDurability());

    // set the default value for equal comparison
    mutateBuilder = MutationProto.newBuilder(proto);
    mutateBuilder.setDurability(MutationProto.Durability.USE_DEFAULT);

    Delete delete = ProtobufUtil.toDelete(proto);

    // delete always have empty value,
    // add empty value to the original mutate
    for (ColumnValue.Builder column:
        mutateBuilder.getColumnValueBuilderList()) {
      for (QualifierValue.Builder qualifier:
          column.getQualifierValueBuilderList()) {
        qualifier.setValue(ByteString.EMPTY);
      }
    }
    assertEquals(mutateBuilder.build(),
      ProtobufUtil.toMutation(MutationType.DELETE, delete));
  }

  /**
   * Test Put Mutate conversions.
   *
   * @throws IOException if the conversion to a {@link Put} or a
   *                     {@link org.apache.hadoop.hbase.client.Mutation} fails
   */
  @Test
  public void testPut() throws IOException {
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationType.PUT);
    mutateBuilder.setTimestamp(111111);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v1"));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v2"));
    qualifierBuilder.setTimestamp(222222);
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    MutationProto proto = mutateBuilder.build();
    // default fields
    assertEquals(MutationProto.Durability.USE_DEFAULT, proto.getDurability());

    // set the default value for equal comparison
    mutateBuilder = MutationProto.newBuilder(proto);
    mutateBuilder.setDurability(MutationProto.Durability.USE_DEFAULT);

    Put put = ProtobufUtil.toPut(proto);

    // put value always use the default timestamp if no
    // value level timestamp specified,
    // add the timestamp to the original mutate
    long timestamp = put.getTimestamp();
    for (ColumnValue.Builder column:
        mutateBuilder.getColumnValueBuilderList()) {
      for (QualifierValue.Builder qualifier:
          column.getQualifierValueBuilderList()) {
        if (!qualifier.hasTimestamp()) {
          qualifier.setTimestamp(timestamp);
        }
      }
    }
    assertEquals(mutateBuilder.build(),
      ProtobufUtil.toMutation(MutationType.PUT, put));
  }

  /**
   * Test basic Scan conversions.
   *
   * @throws IOException if the conversion to a {@link org.apache.hadoop.hbase.client.Scan} fails
   */
  @Test
  public void testScan() throws IOException {
    ClientProtos.Scan.Builder scanBuilder = ClientProtos.Scan.newBuilder();
    scanBuilder.setStartRow(ByteString.copyFromUtf8("row1"));
    scanBuilder.setStopRow(ByteString.copyFromUtf8("row2"));
    Column.Builder columnBuilder = Column.newBuilder();
    columnBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    columnBuilder.addQualifier(ByteString.copyFromUtf8("c1"));
    columnBuilder.addQualifier(ByteString.copyFromUtf8("c2"));
    scanBuilder.addColumn(columnBuilder.build());

    columnBuilder.clear();
    columnBuilder.setFamily(ByteString.copyFromUtf8("f2"));
    scanBuilder.addColumn(columnBuilder.build());

    ClientProtos.Scan proto = scanBuilder.build();

    // Verify default values
    assertEquals(1, proto.getMaxVersions());
    assertEquals(true, proto.getCacheBlocks());

    // Verify fields survive ClientProtos.Scan -> Scan -> ClientProtos.Scan
    // conversion
    scanBuilder = ClientProtos.Scan.newBuilder(proto);
    scanBuilder.setMaxVersions(2);
    scanBuilder.setCacheBlocks(false);
    scanBuilder.setCaching(1024);
    scanBuilder.setTimeRange(ProtobufUtil.toTimeRange(TimeRange.allTime()));
    scanBuilder.setIncludeStopRow(false);
    ClientProtos.Scan expectedProto = scanBuilder.build();

    ClientProtos.Scan actualProto = ProtobufUtil.toScan(
        ProtobufUtil.toScan(expectedProto));
    assertEquals(expectedProto, actualProto);
  }

  @Test
  public void testToCell() {
    KeyValue kv1 =
        new KeyValue(Bytes.toBytes("aaa"), Bytes.toBytes("f1"), Bytes.toBytes("q1"), new byte[30]);
    KeyValue kv2 =
        new KeyValue(Bytes.toBytes("bbb"), Bytes.toBytes("f1"), Bytes.toBytes("q1"), new byte[30]);
    KeyValue kv3 =
        new KeyValue(Bytes.toBytes("ccc"), Bytes.toBytes("f1"), Bytes.toBytes("q1"), new byte[30]);
    byte[] arr = new byte[kv1.getLength() + kv2.getLength() + kv3.getLength()];
    System.arraycopy(kv1.getBuffer(), kv1.getOffset(), arr, 0, kv1.getLength());
    System.arraycopy(kv2.getBuffer(), kv2.getOffset(), arr, kv1.getLength(), kv2.getLength());
    System.arraycopy(kv3.getBuffer(), kv3.getOffset(), arr, kv1.getLength() + kv2.getLength(),
      kv3.getLength());
    ByteBuffer dbb = ByteBuffer.allocateDirect(arr.length);
    dbb.put(arr);
    ByteBufferKeyValue offheapKV = new ByteBufferKeyValue(dbb, kv1.getLength(), kv2.getLength());
    CellProtos.Cell cell = ProtobufUtil.toCell(offheapKV, false);
    Cell newOffheapKV =
        ProtobufUtil.toCell(ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY), cell,
          false);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(offheapKV, newOffheapKV) == 0);
  }

  /**
   * Test Increment Mutate conversions.
   *
   * @throws IOException if converting to an {@link Increment} or
   *                     {@link org.apache.hadoop.hbase.client.Mutation} fails
   */
  @Test
  public void testIncrement() throws IOException {

    MutationProto proto = getIncrementMutation(111111L);
    // default fields
    assertEquals(MutationProto.Durability.USE_DEFAULT, proto.getDurability());

    // set the default value for equal comparison
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder(proto);
    mutateBuilder.setDurability(MutationProto.Durability.USE_DEFAULT);

    Increment increment = ProtobufUtil.toIncrement(proto, null);
    mutateBuilder.setTimestamp(increment.getTimestamp());
    mutateBuilder.setTimeRange(ProtobufUtil.toTimeRange(increment.getTimeRange()));
    assertEquals(mutateBuilder.build(), ProtobufUtil.toMutation(MutationType.INCREMENT, increment));
  }

  private MutationProto getIncrementMutation(Long timestamp) {
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationProto.MutationType.INCREMENT);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(11L)));

    if (timestamp != null) {
      qualifierBuilder.setTimestamp(timestamp);
    }

    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(22L)));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    return mutateBuilder.build();
  }

  /**
   * Older clients may not send along a timestamp in the MutationProto. Check that we
   * default correctly.
   */
  @Test
  public void testIncrementNoTimestamp() throws IOException {
    MutationProto mutation = getIncrementMutation(null);
    Increment increment = ProtobufUtil.toIncrement(mutation, null);
    assertEquals(HConstants.LATEST_TIMESTAMP, increment.getTimestamp());
    increment.getFamilyCellMap().values()
      .forEach(cells ->
        cells.forEach(cell ->
          assertEquals(HConstants.LATEST_TIMESTAMP, cell.getTimestamp())));
  }

  /**
   * Test Append Mutate conversions.
   *
   * @throws IOException if converting to an {@link Append} fails
   */
  @Test
  public void testAppend() throws IOException {
    MutationProto proto = getAppendMutation(111111L);
    // default fields
    assertEquals(MutationProto.Durability.USE_DEFAULT, proto.getDurability());

    // set the default value for equal comparison
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder(proto);
    mutateBuilder.setDurability(MutationProto.Durability.USE_DEFAULT);

    Append append = ProtobufUtil.toAppend(proto, null);

    // append always use the latest timestamp,
    // reset the timestamp to the original mutate
    mutateBuilder.setTimestamp(append.getTimestamp());
    mutateBuilder.setTimeRange(ProtobufUtil.toTimeRange(append.getTimeRange()));
    assertEquals(mutateBuilder.build(), ProtobufUtil.toMutation(MutationType.APPEND, append));
  }

  /**
   * Older clients may not send along a timestamp in the MutationProto. Check that we
   * default correctly.
   */
  @Test
  public void testAppendNoTimestamp() throws IOException {
    MutationProto mutation = getAppendMutation(null);
    Append append = ProtobufUtil.toAppend(mutation, null);
    assertEquals(HConstants.LATEST_TIMESTAMP, append.getTimestamp());
    append.getFamilyCellMap().values().forEach(cells -> cells.forEach(cell -> assertEquals(HConstants.LATEST_TIMESTAMP, cell.getTimestamp())));
  }

  private MutationProto getAppendMutation(Long timestamp) {
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationType.APPEND);
    if (timestamp != null) {
      mutateBuilder.setTimestamp(timestamp);
    }
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v1"));
    if (timestamp != null) {
      qualifierBuilder.setTimestamp(timestamp);
    }
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v2"));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    return mutateBuilder.build();
  }

  private static ProcedureProtos.Procedure.Builder createProcedureBuilder(long procId) {
    ProcedureProtos.Procedure.Builder builder = ProcedureProtos.Procedure.newBuilder();
    builder.setProcId(procId);
    builder.setClassName("java.lang.Object");
    builder.setSubmittedTime(0);
    builder.setState(ProcedureProtos.ProcedureState.RUNNABLE);
    builder.setLastUpdate(0);

    return builder;
  }

  private static ProcedureProtos.Procedure createProcedure(long procId) {
    return createProcedureBuilder(procId).build();
  }

  private static LockServiceProtos.LockedResource createLockedResource(
      LockServiceProtos.LockedResourceType resourceType, String resourceName,
      LockServiceProtos.LockType lockType,
      ProcedureProtos.Procedure exclusiveLockOwnerProcedure, int sharedLockCount) {
    LockServiceProtos.LockedResource.Builder build = LockServiceProtos.LockedResource.newBuilder();
    build.setResourceType(resourceType);
    build.setResourceName(resourceName);
    build.setLockType(lockType);
    if (exclusiveLockOwnerProcedure != null) {
      build.setExclusiveLockOwnerProcedure(exclusiveLockOwnerProcedure);
    }
    build.setSharedLockCount(sharedLockCount);

    return build.build();
  }

  @Test
  public void testProcedureInfo() {
    ProcedureProtos.Procedure.Builder builder = createProcedureBuilder(1);
    ByteString stateBytes = ByteString.copyFrom(new byte[] { 65 });
    BytesValue state = BytesValue.newBuilder().setValue(stateBytes).build();
    builder.addStateMessage(Any.pack(state));
    ProcedureProtos.Procedure procedure = builder.build();

    String procJson = ProtobufUtil.toProcedureJson(Lists.newArrayList(procedure));
    assertEquals("[{"
        + "\"className\":\"java.lang.Object\","
        + "\"procId\":\"1\","
        + "\"submittedTime\":\"0\","
        + "\"state\":\"RUNNABLE\","
        + "\"lastUpdate\":\"0\","
        + "\"stateMessage\":[{\"value\":\"QQ==\"}]"
        + "}]", procJson);
  }

  @Test
  public void testServerLockInfo() {
    LockServiceProtos.LockedResource resource = createLockedResource(
        LockServiceProtos.LockedResourceType.SERVER, "server",
        LockServiceProtos.LockType.SHARED, null, 2);

    String lockJson = ProtobufUtil.toLockJson(Lists.newArrayList(resource));
    assertEquals("[{"
        + "\"resourceType\":\"SERVER\","
        + "\"resourceName\":\"server\","
        + "\"lockType\":\"SHARED\","
        + "\"sharedLockCount\":2"
        + "}]", lockJson);
  }

  @Test
  public void testNamespaceLockInfo() {
    LockServiceProtos.LockedResource resource = createLockedResource(
        LockServiceProtos.LockedResourceType.NAMESPACE, "ns",
        LockServiceProtos.LockType.EXCLUSIVE, createProcedure(2), 0);

    String lockJson = ProtobufUtil.toLockJson(Lists.newArrayList(resource));
    assertEquals("[{"
        + "\"resourceType\":\"NAMESPACE\","
        + "\"resourceName\":\"ns\","
        + "\"lockType\":\"EXCLUSIVE\","
        + "\"exclusiveLockOwnerProcedure\":{"
          + "\"className\":\"java.lang.Object\","
          + "\"procId\":\"2\","
          + "\"submittedTime\":\"0\","
          + "\"state\":\"RUNNABLE\","
          + "\"lastUpdate\":\"0\""
        + "},"
        + "\"sharedLockCount\":0"
        + "}]", lockJson);
  }

  @Test
  public void testTableLockInfo() {
    LockServiceProtos.LockedResource resource = createLockedResource(
        LockServiceProtos.LockedResourceType.TABLE, "table",
        LockServiceProtos.LockType.SHARED, null, 2);

    String lockJson = ProtobufUtil.toLockJson(Lists.newArrayList(resource));
    assertEquals("[{"
        + "\"resourceType\":\"TABLE\","
        + "\"resourceName\":\"table\","
        + "\"lockType\":\"SHARED\","
        + "\"sharedLockCount\":2"
        + "}]", lockJson);
  }

  @Test
  public void testRegionLockInfo() {
    LockServiceProtos.LockedResource resource = createLockedResource(
        LockServiceProtos.LockedResourceType.REGION, "region",
        LockServiceProtos.LockType.EXCLUSIVE, createProcedure(3), 0);

    String lockJson = ProtobufUtil.toLockJson(Lists.newArrayList(resource));
    assertEquals("[{"
        + "\"resourceType\":\"REGION\","
        + "\"resourceName\":\"region\","
        + "\"lockType\":\"EXCLUSIVE\","
        + "\"exclusiveLockOwnerProcedure\":{"
          + "\"className\":\"java.lang.Object\","
          + "\"procId\":\"3\","
          + "\"submittedTime\":\"0\","
          + "\"state\":\"RUNNABLE\","
          + "\"lastUpdate\":\"0\""
        + "},"
        + "\"sharedLockCount\":0"
        + "}]", lockJson);
  }

  /**
   * Test {@link ProtobufUtil#toCell(Cell, boolean)} and
   * {@link ProtobufUtil#toCell(ExtendedCellBuilder, CellProtos.Cell, boolean)} conversion
   * methods when it contains tags and encode/decode tags is set to true.
   */
  @Test
  public void testCellConversionWithTags() {

    Cell cell = getCellWithTags();
    CellProtos.Cell protoCell = ProtobufUtil.toCell(cell, true);
    assertNotNull(protoCell);

    Cell decodedCell = getCellFromProtoResult(protoCell, true);
    List<Tag> decodedTags = PrivateCellUtil.getTags(decodedCell);
    assertEquals(1,  decodedTags.size());
    Tag decodedTag = decodedTags.get(0);
    assertEquals(TAG_TYPE, decodedTag.getType());
    assertEquals(TAG_STR, Tag.getValueAsString(decodedTag));
  }

  private Cell getCellWithTags() {
    Tag tag = new ArrayBackedTag(TAG_TYPE, TAG_STR);
    ExtendedCellBuilder cellBuilder = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY);
    cellBuilder.setRow(Bytes.toBytes("row1"));
    cellBuilder.setFamily(Bytes.toBytes("f1"));
    cellBuilder.setQualifier(Bytes.toBytes("q1"));
    cellBuilder.setValue(Bytes.toBytes("value1"));
    cellBuilder.setType(Cell.Type.Delete);
    cellBuilder.setTags(Collections.singletonList(tag));
    return cellBuilder.build();
  }

  private Cell getCellFromProtoResult(CellProtos.Cell protoCell, boolean decodeTags) {
    ExtendedCellBuilder decodedBuilder =
      ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY);
    return ProtobufUtil.toCell(decodedBuilder, protoCell, decodeTags);
  }

  /**
   * Test {@link ProtobufUtil#toCell(Cell, boolean)} and
   * {@link ProtobufUtil#toCell(ExtendedCellBuilder, CellProtos.Cell, boolean)} conversion
   * methods when it contains tags and encode/decode tags is set to false.
   */
  @Test
  public void testCellConversionWithoutTags() {
    Cell cell = getCellWithTags();
    CellProtos.Cell protoCell = ProtobufUtil.toCell(cell, false);
    assertNotNull(protoCell);

    Cell decodedCell = getCellFromProtoResult(protoCell, false);
    List<Tag> decodedTags = PrivateCellUtil.getTags(decodedCell);
    assertEquals(0,  decodedTags.size());
  }

  /**
   * Test {@link ProtobufUtil#toCell(Cell, boolean)} and
   * {@link ProtobufUtil#toCell(ExtendedCellBuilder, CellProtos.Cell, boolean)} conversion
   * methods when it contains tags and encoding of tags is set to false
   * and decoding of tags is set to true.
   */
  @Test
  public void testTagEncodeFalseDecodeTrue() {
    Cell cell = getCellWithTags();
    CellProtos.Cell protoCell = ProtobufUtil.toCell(cell, false);
    assertNotNull(protoCell);

    Cell decodedCell = getCellFromProtoResult(protoCell, true);
    List<Tag> decodedTags = PrivateCellUtil.getTags(decodedCell);
    assertEquals(0,  decodedTags.size());
  }

  /**
   * Test {@link ProtobufUtil#toCell(Cell, boolean)} and
   * {@link ProtobufUtil#toCell(ExtendedCellBuilder, CellProtos.Cell, boolean)} conversion
   * methods when it contains tags and encoding of tags is set to true
   * and decoding of tags is set to false.
   */
  @Test
  public void testTagEncodeTrueDecodeFalse() {
    Cell cell = getCellWithTags();
    CellProtos.Cell protoCell = ProtobufUtil.toCell(cell, true);
    assertNotNull(protoCell);

    Cell decodedCell = getCellFromProtoResult(protoCell, false);
    List<Tag> decodedTags = PrivateCellUtil.getTags(decodedCell);
    assertEquals(0,  decodedTags.size());
  }
}
