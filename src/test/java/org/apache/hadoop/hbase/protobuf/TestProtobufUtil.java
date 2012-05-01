/**
 * Copyright 2011 The Apache Software Foundation
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

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.DeleteType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.MutateType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ByteString;

/**
 * Class to test ProtobufUtil.
 */
@Category(SmallTests.class)
public class TestProtobufUtil {

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
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutateType.APPEND);
    mutateBuilder.setTimestamp(111111);
    ColumnValue.Builder valueBuilder = ColumnValue.newBuilder();
    valueBuilder.setFamily(ByteString.copyFromUtf8("f1"));
    QualifierValue.Builder qualifierBuilder = QualifierValue.newBuilder();
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c1"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v1"));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    qualifierBuilder.setQualifier(ByteString.copyFromUtf8("c2"));
    qualifierBuilder.setValue(ByteString.copyFromUtf8("v2"));
    valueBuilder.addQualifierValue(qualifierBuilder.build());
    mutateBuilder.addColumnValue(valueBuilder.build());

    Mutate proto = mutateBuilder.build();
    // default fields
    assertEquals(true, proto.getWriteToWAL());

    // set the default value for equal comparison
    mutateBuilder = Mutate.newBuilder(proto);
    mutateBuilder.setWriteToWAL(true);

    Append append = ProtobufUtil.toAppend(proto);

    // append always use the latest timestamp,
    // add the timestamp to the original mutate
    long timestamp = append.getTimeStamp();
    mutateBuilder.setTimestamp(timestamp);
    for (ColumnValue.Builder column:
        mutateBuilder.getColumnValueBuilderList()) {
      for (QualifierValue.Builder qualifier:
          column.getQualifierValueBuilderList()) {
        qualifier.setTimestamp(timestamp);
      }
    }
    assertEquals(mutateBuilder.build(),
      ProtobufUtil.toMutate(MutateType.APPEND, append));
  }

  /**
   * Test Delete Mutate conversions.
   *
   * @throws IOException
   */
  @Test
  public void testDelete() throws IOException {
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutateType.DELETE);
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

    Mutate proto = mutateBuilder.build();
    // default fields
    assertEquals(true, proto.getWriteToWAL());

    // set the default value for equal comparison
    mutateBuilder = Mutate.newBuilder(proto);
    mutateBuilder.setWriteToWAL(true);

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
      ProtobufUtil.toMutate(MutateType.DELETE, delete));
  }

  /**
   * Test Increment Mutate conversions.
   *
   * @throws IOException
   */
  @Test
  public void testIncrement() throws IOException {
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutateType.INCREMENT);
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

    Mutate proto = mutateBuilder.build();
    // default fields
    assertEquals(true, proto.getWriteToWAL());

    // set the default value for equal comparison
    mutateBuilder = Mutate.newBuilder(proto);
    mutateBuilder.setWriteToWAL(true);

    Increment increment = ProtobufUtil.toIncrement(proto);
    assertEquals(mutateBuilder.build(), ProtobufUtil.toMutate(increment));
  }

  /**
   * Test Put Mutate conversions.
   *
   * @throws IOException
   */
  @Test
  public void testPut() throws IOException {
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFromUtf8("row"));
    mutateBuilder.setMutateType(MutateType.PUT);
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

    Mutate proto = mutateBuilder.build();
    // default fields
    assertEquals(true, proto.getWriteToWAL());

    // set the default value for equal comparison
    mutateBuilder = Mutate.newBuilder(proto);
    mutateBuilder.setWriteToWAL(true);

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
      ProtobufUtil.toMutate(MutateType.PUT, put));
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
    // default fields
    assertEquals(1, proto.getMaxVersions());
    assertEquals(true, proto.getCacheBlocks());

    scanBuilder = ClientProtos.Scan.newBuilder(proto);
    scanBuilder.setMaxVersions(1);
    scanBuilder.setCacheBlocks(true);

    Scan scan = ProtobufUtil.toScan(proto);
    assertEquals(scanBuilder.build(), ProtobufUtil.toScan(scan));
  }

  /**
   * Test basic Exec conversions.
   *
   * @throws IOException
   */
  @Test
  public void testExec() throws IOException {
    ClientProtos.Exec.Builder execBuilder = ClientProtos.Exec.newBuilder();
    execBuilder.setRow(ByteString.copyFromUtf8("row"));
    execBuilder.setProtocolName(ColumnAggregationEndpoint.class.getName());
    execBuilder.setMethodName("sum");
    execBuilder.addParameter(ProtobufUtil.toParameter(Bytes.toBytes("f")));
    execBuilder.addParameter(ProtobufUtil.toParameter(Bytes.toBytes("c")));

    ClientProtos.Exec proto = execBuilder.build();
    Exec exec = ProtobufUtil.toExec(proto);
    execBuilder = ClientProtos.Exec.newBuilder(ProtobufUtil.toExec(exec));
    execBuilder.clearProperty(); // remove properties added by default
    assertEquals(proto, execBuilder.build());
  }
}
