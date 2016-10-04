/**
 *
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

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ByteString;

/**
 * Class to test ProtobufUtil.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestProtobufUtil {
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

    ClientProtos.Get proto = getBuilder.build();
    // default fields
    assertEquals(1, proto.getMaxVersions());
    assertEquals(true, proto.getCacheBlocks());

    // set the default value for equal comparison
    getBuilder = ClientProtos.Get.newBuilder(proto);
    getBuilder.setMaxVersions(1);
    getBuilder.setCacheBlocks(true);

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
    qualifierBuilder.setTimestamp(timeStamp);
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
    MutationProto.Builder mutateBuilder = MutationProto.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutationType.INCREMENT);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(11L)));
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
    assertEquals(mutateBuilder.build(),
      ProtobufUtil.toMutation(increment, MutationProto.newBuilder(), HConstants.NO_NONCE));
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
    long timestamp = put.getTimeStamp();
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
    ClientProtos.Scan expectedProto = scanBuilder.build();

    ClientProtos.Scan actualProto = ProtobufUtil.toScan(
        ProtobufUtil.toScan(expectedProto));
    assertEquals(expectedProto, actualProto);
  }
}
