/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentMatcher;

/**
 * Test case for {@link TableSpaceQuotaViolationNotifier}.
 */
@Category(SmallTests.class)
public class TestTableSpaceQuotaViolationNotifier {

  private TableSpaceQuotaViolationNotifier notifier;
  private Connection conn;

  @Before
  public void setup() throws Exception {
    notifier = new TableSpaceQuotaViolationNotifier();
    conn = mock(Connection.class);
    notifier.initialize(conn);
  }

  @Test
  public void testToViolation() throws Exception {
    final TableName tn = TableName.valueOf("inviolation");
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_INSERTS;
    final Table quotaTable = mock(Table.class);
    when(conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);

    final Put expectedPut = new Put(Bytes.toBytes("t." + tn.getNameAsString()));
    final SpaceQuota protoQuota = SpaceQuota.newBuilder()
        .setViolationPolicy(ProtobufUtil.toProtoViolationPolicy(policy))
        .build();
    expectedPut.addColumn(Bytes.toBytes("u"), Bytes.toBytes("v"), protoQuota.toByteArray());

    notifier.transitionTableToViolation(tn, policy);

    verify(quotaTable).put(argThat(new SingleCellPutMatcher(expectedPut)));
  }

  @Test
  public void testToObservance() throws Exception {
    final TableName tn = TableName.valueOf("notinviolation");
    final Table quotaTable = mock(Table.class);
    when(conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);

    final Delete expectedDelete = new Delete(Bytes.toBytes("t." + tn.getNameAsString()));
    expectedDelete.addColumn(Bytes.toBytes("u"), Bytes.toBytes("v"));

    notifier.transitionTableToObservance(tn);

    verify(quotaTable).delete(argThat(new SingleCellDeleteMatcher(expectedDelete)));
  }

  /**
   * Parameterized for Puts.
   */
  private static class SingleCellPutMatcher extends SingleCellMutationMatcher<Put> {
    private SingleCellPutMatcher(Put expected) {
      super(expected);
    }
  }

  /**
   * Parameterized for Deletes.
   */
  private static class SingleCellDeleteMatcher extends SingleCellMutationMatcher<Delete> {
    private SingleCellDeleteMatcher(Delete expected) {
      super(expected);
    }
  }

  /**
   * Quick hack to verify a Mutation with one column.
   */
  private static class SingleCellMutationMatcher<T> extends ArgumentMatcher<T> {
    private final Mutation expected;

    private SingleCellMutationMatcher(Mutation expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Object argument) {
      if (!expected.getClass().isAssignableFrom(argument.getClass())) {
        return false;
      }
      Mutation actual = (Mutation) argument;
      if (!Arrays.equals(expected.getRow(), actual.getRow())) {
        return false;
      }
      if (expected.size() != actual.size()) {
        return false;
      }
      NavigableMap<byte[],List<Cell>> expectedCells = expected.getFamilyCellMap();
      NavigableMap<byte[],List<Cell>> actualCells = actual.getFamilyCellMap();
      Entry<byte[],List<Cell>> expectedEntry = expectedCells.entrySet().iterator().next();
      Entry<byte[],List<Cell>> actualEntry = actualCells.entrySet().iterator().next();
      if (!Arrays.equals(expectedEntry.getKey(), actualEntry.getKey())) {
        return false;
      }
      return Objects.equals(expectedEntry.getValue(), actualEntry.getValue());
    }
  }
}
