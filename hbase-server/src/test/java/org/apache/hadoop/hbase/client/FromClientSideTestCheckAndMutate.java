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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.TestTemplate;

@SuppressWarnings("deprecation")
public class FromClientSideTestCheckAndMutate extends FromClientSideTestBase {

  protected FromClientSideTestCheckAndMutate(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @TestTemplate
  public void testCheckAndPut() throws IOException {
    final byte[] anotherrow = Bytes.toBytes("anotherrow");
    final byte[] value2 = Bytes.toBytes("abcd");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put1 = new Put(ROW);
      put1.addColumn(FAMILY, QUALIFIER, VALUE);

      // row doesn't exist, so using non-null value should be considered "not match".
      boolean ok =
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(VALUE).thenPut(put1);
      assertFalse(ok);

      // row doesn't exist, so using "ifNotExists" should be considered "match".
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put1);
      assertTrue(ok);

      // row now exists, so using "ifNotExists" should be considered "not match".
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put1);
      assertFalse(ok);

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);

      // row now exists, use the matching value to check
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(VALUE).thenPut(put2);
      assertTrue(ok);

      Put put3 = new Put(anotherrow);
      put3.addColumn(FAMILY, QUALIFIER, VALUE);

      // try to do CheckAndPut on different rows
      try {
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(value2).thenPut(put3);
        fail("trying to check and modify different rows should have failed.");
      } catch (Exception ignored) {
      }
    }
  }

  @TestTemplate
  public void testCheckAndMutateWithTimeRange() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      final long ts = EnvironmentEdgeManager.currentTime() / 2;
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, ts, VALUE);

      boolean ok =
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.at(ts + 10000)).ifEquals(VALUE).thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.from(ts + 10000)).ifEquals(VALUE).thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.between(ts + 10000, ts + 20000)).ifEquals(VALUE).thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.until(ts))
        .ifEquals(VALUE).thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts))
        .ifEquals(VALUE).thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.from(ts))
        .ifEquals(VALUE).thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.between(ts, ts + 20000)).ifEquals(VALUE).thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.until(ts + 10000)).ifEquals(VALUE).thenPut(put);
      assertTrue(ok);

      RowMutations rm = new RowMutations(ROW).add((Mutation) put);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.at(ts + 10000)).ifEquals(VALUE).thenMutate(rm);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts))
        .ifEquals(VALUE).thenMutate(rm);
      assertTrue(ok);

      Delete delete = new Delete(ROW).addColumn(FAMILY, QUALIFIER);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .timeRange(TimeRange.at(ts + 10000)).ifEquals(VALUE).thenDelete(delete);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts))
        .ifEquals(VALUE).thenDelete(delete);
      assertTrue(ok);
    }
  }

  @TestTemplate
  public void testCheckAndPutWithCompareOp() throws IOException {
    final byte[] value1 = Bytes.toBytes("aaaa");
    final byte[] value2 = Bytes.toBytes("bbbb");
    final byte[] value3 = Bytes.toBytes("cccc");
    final byte[] value4 = Bytes.toBytes("dddd");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);

      Put put3 = new Put(ROW);
      put3.addColumn(FAMILY, QUALIFIER, value3);

      // row doesn't exist, so using "ifNotExists" should be considered "match".
      boolean ok =
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put2);
      assertTrue(ok);

      // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.EQUAL, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS, value1).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, value1).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.NOT_EQUAL, value1).thenPut(put3);
      assertTrue(ok);

      // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.EQUAL, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER, value4).thenPut(put3);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, value4).thenPut(put3);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.NOT_EQUAL, value4).thenPut(put2);
      assertTrue(ok);

      // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.NOT_EQUAL, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, value2).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, value2).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.EQUAL, value2).thenPut(put3);
      assertTrue(ok);
    }
  }

  @TestTemplate
  public void testCheckAndDelete() throws IOException {
    final byte[] value1 = Bytes.toBytes("aaaa");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, value1);
      table.put(put);

      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, QUALIFIER);

      boolean ok =
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(value1).thenDelete(delete);
      assertTrue(ok);
    }
  }

  @TestTemplate
  public void testCheckAndDeleteWithCompareOp() throws IOException {
    final byte[] value1 = Bytes.toBytes("aaaa");
    final byte[] value2 = Bytes.toBytes("bbbb");
    final byte[] value3 = Bytes.toBytes("cccc");
    final byte[] value4 = Bytes.toBytes("dddd");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);
      table.put(put2);

      Put put3 = new Put(ROW);
      put3.addColumn(FAMILY, QUALIFIER, value3);

      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, QUALIFIER);

      // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.EQUAL, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS, value1).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, value1).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.NOT_EQUAL, value1).thenDelete(delete);
      assertTrue(ok);

      // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.EQUAL, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER, value4).thenDelete(delete);
      assertTrue(ok);
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, value4).thenDelete(delete);
      assertTrue(ok);
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.NOT_EQUAL, value4).thenDelete(delete);
      assertTrue(ok);

      // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
      // turns out "match"
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.NOT_EQUAL, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.GREATER_OR_EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.LESS_OR_EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
        .ifMatches(CompareOperator.EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
    }
  }
}
