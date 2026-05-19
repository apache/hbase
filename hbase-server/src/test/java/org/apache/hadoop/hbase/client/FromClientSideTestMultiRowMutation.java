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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.TestTemplate;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;

public class FromClientSideTestMultiRowMutation extends FromClientSideTestBase {

  protected FromClientSideTestMultiRowMutation(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @TestTemplate
  public void testMultiRowMutation() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] ROW3 = Bytes.toBytes("testRow3");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.batch(Arrays.asList(new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE),
        new Put(ROW2).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(1L)),
        new Put(ROW3).addColumn(FAMILY, QUALIFIER, VALUE)), new Object[3]);

      // Execute MultiRowMutation
      Put put = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);

      Delete delete = new Delete(ROW1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      Increment increment = new Increment(ROW2).addColumn(FAMILY, QUALIFIER, 1L);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.INCREMENT, increment);

      Append append = new Append(ROW3).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m4 = ProtobufUtil.toMutation(MutationType.APPEND, append);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addMutationRequest(m4);

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(2L, Bytes.toLong(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW3));
      assertEquals(Bytes.toString(VALUE) + Bytes.toString(VALUE),
        Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @TestTemplate
  public void testMultiRowMutationWithSingleConditionWhenConditionMatches() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] VALUE1 = Bytes.toBytes("testValue1");
    final byte[] VALUE2 = Bytes.toBytes("testValue2");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(
        ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE2, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertEquals(Bytes.toString(VALUE1), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW2));
      assertTrue(r.isEmpty());
    }
  }

  @TestTemplate
  public void testMultiRowMutationWithSingleConditionWhenConditionNotMatch() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] VALUE1 = Bytes.toBytes("testValue1");
    final byte[] VALUE2 = Bytes.toBytes("testValue2");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(
        ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE1, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertFalse(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(Bytes.toString(VALUE2), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @TestTemplate
  public void testMultiRowMutationWithMultipleConditionsWhenConditionsMatch() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] VALUE1 = Bytes.toBytes("testValue1");
    final byte[] VALUE2 = Bytes.toBytes("testValue2");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(
        ProtobufUtil.toCondition(ROW, FAMILY, QUALIFIER, CompareOperator.EQUAL, null, null));
      mrmBuilder.addCondition(
        ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE2, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertEquals(Bytes.toString(VALUE1), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW2));
      assertTrue(r.isEmpty());
    }
  }

  @TestTemplate
  public void testMultiRowMutationWithMultipleConditionsWhenConditionsNotMatch() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] VALUE1 = Bytes.toBytes("testValue1");
    final byte[] VALUE2 = Bytes.toBytes("testValue2");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(
        ProtobufUtil.toCondition(ROW1, FAMILY, QUALIFIER, CompareOperator.EQUAL, null, null));
      mrmBuilder.addCondition(
        ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE1, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertFalse(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(Bytes.toString(VALUE2), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @TestTemplate
  public void testMultiRowMutationWithFilterConditionWhenConditionMatches() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] QUALIFIER2 = Bytes.toBytes("testQualifier2");
    final byte[] VALUE1 = Bytes.toBytes("testValue1");
    final byte[] VALUE2 = Bytes.toBytes("testValue2");
    final byte[] VALUE3 = Bytes.toBytes("testValue3");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.put(
        new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2).addColumn(FAMILY, QUALIFIER2, VALUE3));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2,
        new FilterList(
          new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE2),
          new SingleColumnValueFilter(FAMILY, QUALIFIER2, CompareOperator.EQUAL, VALUE3)),
        null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertEquals(Bytes.toString(VALUE1), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW2));
      assertTrue(r.isEmpty());
    }
  }

  @TestTemplate
  public void testMultiRowMutationWithFilterConditionWhenConditionNotMatch() throws Exception {
    final byte[] ROW1 = Bytes.toBytes("testRow1");
    final byte[] ROW2 = Bytes.toBytes("testRow2");
    final byte[] QUALIFIER2 = Bytes.toBytes("testQualifier2");
    final byte[] VALUE1 = Bytes.toBytes("testValue1");
    final byte[] VALUE2 = Bytes.toBytes("testValue2");
    final byte[] VALUE3 = Bytes.toBytes("testValue3");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      // Add initial data
      t.put(
        new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2).addColumn(FAMILY, QUALIFIER2, VALUE3));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2,
        new FilterList(
          new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE2),
          new SingleColumnValueFilter(FAMILY, QUALIFIER2, CompareOperator.EQUAL, VALUE2)),
        null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertFalse(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(Bytes.toString(VALUE2), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }
}
