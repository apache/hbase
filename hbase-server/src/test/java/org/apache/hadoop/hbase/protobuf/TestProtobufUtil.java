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
package org.apache.hadoop.hbase.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.DeleteType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos.MetaRegionServer;

/**
 * Class to test ProtobufUtil.
 */
@Category({ MiscTests.class, SmallTests.class})
public class TestProtobufUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProtobufUtil.class);

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
   * @throws IOException
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
   * Test Append Mutate conversions.
   *
   * @throws IOException
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

  /**
   * Test Delete Mutate conversions.
   *
   * @throws IOException
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
   * Test Increment Mutate conversions.
   *
   * @throws IOException
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

  private MutationProto getIncrementMutation(Long timestamp) {
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationType.INCREMENT);
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
   * Test Put Mutate conversions.
   *
   * @throws IOException
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
   * @throws IOException
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
  public void testToCell() throws Exception {
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
    CellProtos.Cell cell = ProtobufUtil.toCell(offheapKV);
    Cell newOffheapKV =
        ProtobufUtil.toCell(ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY), cell);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(offheapKV, newOffheapKV) == 0);
  }

  @Test
  public void testMetaRegionState() throws Exception {
    ServerName serverName = ServerName.valueOf("localhost", 1234, 5678);
    // New region state style.
    for (RegionState.State state: RegionState.State.values()) {
      RegionState regionState =
          new RegionState(RegionInfoBuilder.FIRST_META_REGIONINFO, state, serverName);
      MetaRegionServer metars = MetaRegionServer.newBuilder()
          .setServer(org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toServerName(serverName))
          .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
          .setState(state.convert()).build();
      // Serialize
      byte[] data = ProtobufUtil.prependPBMagic(metars.toByteArray());
      ProtobufUtil.prependPBMagic(data);
      // Deserialize
      RegionState regionStateNew =
          org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.parseMetaRegionStateFrom(data, 1);
      assertEquals(regionState.getServerName(), regionStateNew.getServerName());
      assertEquals(regionState.getState(), regionStateNew.getState());
    }
    // old style.
    RegionState rs =
        org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.parseMetaRegionStateFrom(
            serverName.getVersionedBytes(), 1);
    assertEquals(serverName, rs.getServerName());
    assertEquals(rs.getState(), RegionState.State.OPEN);
  }
}
