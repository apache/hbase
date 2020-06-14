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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestCheckAndMutate {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCheckAndMutate.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROWKEY = Bytes.toBytes("12345");
  private static final byte[] ROWKEY2 = Bytes.toBytes("67890");
  private static final byte[] ROWKEY3 = Bytes.toBytes("abcde");
  private static final byte[] ROWKEY4 = Bytes.toBytes("fghij");
  private static final byte[] FAMILY = Bytes.toBytes("cf");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Table createTable()
  throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
    return table;
  }

  private void putOneRow(Table table) throws IOException {
    Put put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    table.put(put);
  }

  private void getOneRowAndAssertAllExist(final Table table) throws IOException {
    Get get = new Get(ROWKEY);
    Result result = table.get(get);
    assertTrue("Column A value should be a",
      Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))).equals("a"));
    assertTrue("Column B value should be b",
      Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))).equals("b"));
    assertTrue("Column C value should be c",
      Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))).equals("c"));
  }

  private void getOneRowAndAssertAllButCExist(final Table table) throws IOException {
    Get get = new Get(ROWKEY);
    Result result = table.get(get);
    assertTrue("Column A value should be a",
      Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))).equals("a"));
    assertTrue("Column B value should be b",
      Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))).equals("b"));
    assertTrue("Column C should not exist",
    result.getValue(FAMILY, Bytes.toBytes("C")) == null);
  }

  private RowMutations makeRowMutationsWithColumnCDeleted() throws IOException {
    RowMutations rm = new RowMutations(ROWKEY, 2);
    Put put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    rm.add(put);
    Delete del = new Delete(ROWKEY);
    del.addColumn(FAMILY, Bytes.toBytes("C"));
    rm.add(del);
    return rm;
  }

  private RowMutations getBogusRowMutations() throws IOException {
    Put p = new Put(ROWKEY);
    byte[] value = new byte[0];
    p.addColumn(new byte[]{'b', 'o', 'g', 'u', 's'}, new byte[]{'A'}, value);
    RowMutations rm = new RowMutations(ROWKEY);
    rm.add(p);
    return rm;
  }

  // Tests for old checkAndMutate API

  @Test
  @Deprecated
  public void testCheckAndMutateForOldApi() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // put the same row again with C column deleted
      RowMutations rm = makeRowMutationsWithColumnCDeleted();
      boolean res = table.checkAndMutate(ROWKEY, FAMILY).qualifier(Bytes.toBytes("A"))
          .ifEquals(Bytes.toBytes("a")).thenMutate(rm);
      assertTrue(res);

      // get row back and assert the values
      getOneRowAndAssertAllButCExist(table);

      //Test that we get a region level exception
      try {
        rm = getBogusRowMutations();
        table.checkAndMutate(ROWKEY, FAMILY).qualifier(Bytes.toBytes("A"))
            .ifEquals(Bytes.toBytes("a")).thenMutate(rm);
        fail("Expected NoSuchColumnFamilyException");
      } catch (RetriesExhaustedWithDetailsException e) {
        try {
          throw e.getCause(0);
        } catch (NoSuchColumnFamilyException e1) {
          // expected
        }
      }
    }
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithSingleFilterForOldApi() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // Put with success
      boolean ok = table.checkAndMutate(ROWKEY, new SingleColumnValueFilter(FAMILY,
          Bytes.toBytes("A"), CompareOperator.EQUAL, Bytes.toBytes("a")))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      // Put with failure
      ok = table.checkAndMutate(ROWKEY, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
          CompareOperator.EQUAL, Bytes.toBytes("b")))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"))));

      // Delete with success
      ok = table.checkAndMutate(ROWKEY, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
          CompareOperator.EQUAL, Bytes.toBytes("a")))
        .thenDelete(new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("D")));
      assertTrue(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"))));

      // Mutate with success
      ok = table.checkAndMutate(ROWKEY, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"),
          CompareOperator.EQUAL, Bytes.toBytes("b")))
        .thenMutate(new RowMutations(ROWKEY)
          .add((Mutation) new Put(ROWKEY)
            .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
          .add((Mutation) new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("A"))));
      assertTrue(ok);

      result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"))));
    }
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithMultipleFiltersForOldApi() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // Put with success
      boolean ok = table.checkAndMutate(ROWKEY, new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))
        ))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      // Put with failure
      ok = table.checkAndMutate(ROWKEY, new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("c"))
        ))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"))));

      // Delete with success
      ok = table.checkAndMutate(ROWKEY, new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))
        ))
        .thenDelete(new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("D")));
      assertTrue(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"))));

      // Mutate with success
      ok = table.checkAndMutate(ROWKEY, new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))
        ))
        .thenMutate(new RowMutations(ROWKEY)
          .add((Mutation) new Put(ROWKEY)
            .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
          .add((Mutation) new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("A"))));
      assertTrue(ok);

      result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"))));
    }
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithTimestampFilterForOldApi() throws Throwable {
    try (Table table = createTable()) {
      // Put with specifying the timestamp
      table.put(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")));

      // Put with success
      boolean ok = table.checkAndMutate(ROWKEY, new FilterList(
          new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
          new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
          new TimestampsFilter(Collections.singletonList(100L))
        ))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Put with failure
      ok = table.checkAndMutate(ROWKEY, new FilterList(
          new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
          new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
          new TimestampsFilter(Collections.singletonList(101L))
        ))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"))));
    }
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithFilterAndTimeRangeForOldApi() throws Throwable {
    try (Table table = createTable()) {
      // Put with specifying the timestamp
      table.put(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")));

      // Put with success
      boolean ok = table.checkAndMutate(ROWKEY, new SingleColumnValueFilter(FAMILY,
          Bytes.toBytes("A"), CompareOperator.EQUAL, Bytes.toBytes("a")))
        .timeRange(TimeRange.between(0, 101))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Put with failure
      ok = table.checkAndMutate(ROWKEY, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
          CompareOperator.EQUAL, Bytes.toBytes("a")))
        .timeRange(TimeRange.between(0, 100))
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"))));
    }
  }

  @Test(expected = NullPointerException.class)
  @Deprecated
  public void testCheckAndMutateWithoutConditionForOldApi() throws Throwable {
    try (Table table = createTable()) {
      table.checkAndMutate(ROWKEY, FAMILY)
        .thenPut(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));
    }
  }

  // Tests for new CheckAndMutate API

  @Test
  public void testCheckAndMutate() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // put the same row again with C column deleted
      RowMutations rm = makeRowMutationsWithColumnCDeleted();
      boolean res = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .build(rm));
      assertTrue(res);

      // get row back and assert the values
      getOneRowAndAssertAllButCExist(table);

      // Test that we get a region level exception
      try {
        rm = getBogusRowMutations();
        table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
          .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
          .build(rm));
        fail("Expected NoSuchColumnFamilyException");
      } catch (RetriesExhaustedWithDetailsException e) {
        try {
          throw e.getCause(0);
        } catch (NoSuchColumnFamilyException e1) {
          // expected
        }
      }
    }
  }

  @Test
  public void testCheckAndMutateWithSingleFilter() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // Put with success
      boolean ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new SingleColumnValueFilter(FAMILY,
          Bytes.toBytes("A"), CompareOperator.EQUAL, Bytes.toBytes("a")))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      // Put with failure
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("b")))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e"))));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"))));

      // Delete with success
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
          CompareOperator.EQUAL, Bytes.toBytes("a")))
        .build(new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("D"))));
      assertTrue(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"))));

      // Mutate with success
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"),
          CompareOperator.EQUAL, Bytes.toBytes("b")))
        .build(new RowMutations(ROWKEY)
          .add((Mutation) new Put(ROWKEY)
            .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
          .add((Mutation) new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("A")))));
      assertTrue(ok);

      result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"))));
    }
  }

  @Test
  public void testCheckAndMutateWithMultipleFilters() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // Put with success
      boolean ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      // Put with failure
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("c"))))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e"))));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("E"))));

      // Delete with success
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
            new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
              Bytes.toBytes("a")),
            new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
              Bytes.toBytes("b"))))
        .build(new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("D"))));
      assertTrue(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"))));

      // Mutate with success
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new RowMutations(ROWKEY)
          .add((Mutation) new Put(ROWKEY)
            .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
          .add((Mutation) new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("A")))));
      assertTrue(ok);

      result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"))));
    }
  }

  @Test
  public void testCheckAndMutateWithTimestampFilter() throws Throwable {
    try (Table table = createTable()) {
      // Put with specifying the timestamp
      table.put(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")));

      // Put with success
      boolean ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
          new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
          new TimestampsFilter(Collections.singletonList(100L))))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Put with failure
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
          new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
          new TimestampsFilter(Collections.singletonList(101L))))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"))));
    }
  }

  @Test
  public void testCheckAndMutateWithFilterAndTimeRange() throws Throwable {
    try (Table table = createTable()) {
      // Put with specifying the timestamp
      table.put(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")));

      // Put with success
      boolean ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new SingleColumnValueFilter(FAMILY,
          Bytes.toBytes("A"), CompareOperator.EQUAL, Bytes.toBytes("a")))
        .timeRange(TimeRange.between(0, 101))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))));
      assertTrue(ok);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Put with failure
      ok = table.checkAndMutate(CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
          CompareOperator.EQUAL, Bytes.toBytes("a")))
        .timeRange(TimeRange.between(0, 100))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))));
      assertFalse(ok);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"))));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckAndMutateBuilderWithoutCondition() {
    CheckAndMutate.newBuilder(ROWKEY)
      .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));
  }

  // Tests for batch version of checkAndMutate

  @Test
  public void testCheckAndMutateBatch() throws Throwable {
    try (Table table = createTable()) {
      table.put(Arrays.asList(
        new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
        new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
        new Put(ROWKEY3).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
        new Put(ROWKEY4).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))));

      // Test for Put
      CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("e")));

      CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifEquals(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("a"))
        .build(new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("f")));

      boolean[] results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A")));
      assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Test for Delete
      checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("e"))
        .build(new Delete(ROWKEY));

      checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifEquals(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("a"))
        .build(new Delete(ROWKEY2));

      results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Test for RowMutations
      checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY3)
        .ifEquals(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))
        .build(new RowMutations(ROWKEY3)
          .add((Mutation) new Put(ROWKEY3)
            .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))
          .add((Mutation) new Delete(ROWKEY3).addColumns(FAMILY, Bytes.toBytes("C"))));

      checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY4)
        .ifEquals(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("f"))
        .build(new RowMutations(ROWKEY4)
          .add((Mutation) new Put(ROWKEY4)
            .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))
          .add((Mutation) new Delete(ROWKEY4).addColumns(FAMILY, Bytes.toBytes("D"))));

      results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      result = table.get(new Get(ROWKEY3));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
      assertNull(result.getValue(FAMILY, Bytes.toBytes("D")));

      result = table.get(new Get(ROWKEY4));
      assertNull(result.getValue(FAMILY, Bytes.toBytes("F")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
    }
  }

  @Test
  public void testCheckAndMutateBatch2() throws Throwable {
    try (Table table = createTable()) {
      table.put(Arrays.asList(
        new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
        new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
        new Put(ROWKEY3).addColumn(FAMILY, Bytes.toBytes("C"), 100, Bytes.toBytes("c")),
        new Put(ROWKEY4).addColumn(FAMILY, Bytes.toBytes("D"), 100, Bytes.toBytes("d"))));

      // Test for ifNotExists()
      CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifNotExists(FAMILY, Bytes.toBytes("B"))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("e")));

      CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifNotExists(FAMILY, Bytes.toBytes("B"))
        .build(new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("f")));

      boolean[] results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A")));
      assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Test for ifMatches()
      checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(FAMILY, Bytes.toBytes("A"), CompareOperator.NOT_EQUAL, Bytes.toBytes("a"))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")));

      checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifMatches(FAMILY, Bytes.toBytes("B"), CompareOperator.GREATER, Bytes.toBytes("b"))
        .build(new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("f")));

      results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A")));
      assertEquals("a", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("B")));
      assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

      // Test for timeRange()
      checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY3)
        .ifEquals(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))
        .timeRange(TimeRange.between(0, 101))
        .build(new Put(ROWKEY3).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("e")));

      checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY4)
        .ifEquals(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))
        .timeRange(TimeRange.between(0, 100))
        .build(new Put(ROWKEY4).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("f")));

      results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      result = table.get(new Get(ROWKEY3).addColumn(FAMILY, Bytes.toBytes("C")));
      assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

      result = table.get(new Get(ROWKEY4).addColumn(FAMILY, Bytes.toBytes("D")));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
    }
  }

  @Test
  public void testCheckAndMutateBatchWithFilter() throws Throwable {
    try (Table table = createTable()) {
      table.put(Arrays.asList(
        new Put(ROWKEY)
          .addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
          .addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))
          .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
        new Put(ROWKEY2)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))
          .addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e"))
          .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f"))));

      // Test for Put
      CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("g")));

      CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("h")));

      boolean[] results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C")));
      assertEquals("g", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("F")));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));

      // Test for Delete
      checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("C")));

      checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new Delete(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("F")));

      results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      assertFalse(table.exists(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("F")));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));

      // Test for RowMutations
      checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new RowMutations(ROWKEY)
          .add((Mutation) new Put(ROWKEY)
            .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")))
          .add((Mutation) new Delete(ROWKEY).addColumns(FAMILY, Bytes.toBytes("A"))));

      checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .build(new RowMutations(ROWKEY2)
          .add((Mutation) new Put(ROWKEY2)
            .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("g")))
          .add((Mutation) new Delete(ROWKEY2).addColumns(FAMILY, Bytes.toBytes("D"))));

      results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      result = table.get(new Get(ROWKEY));
      assertNull(result.getValue(FAMILY, Bytes.toBytes("A")));
      assertEquals("c", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

      result = table.get(new Get(ROWKEY2));
      assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
    }
  }

  @Test
  public void testCheckAndMutateBatchWithFilterAndTimeRange() throws Throwable {
    try (Table table = createTable()) {
      table.put(Arrays.asList(
        new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a"))
          .addColumn(FAMILY, Bytes.toBytes("B"), 100, Bytes.toBytes("b"))
          .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
        new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("D"), 100, Bytes.toBytes("d"))
          .addColumn(FAMILY, Bytes.toBytes("E"), 100, Bytes.toBytes("e"))
          .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f"))));

      CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(ROWKEY)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
            Bytes.toBytes("a")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
            Bytes.toBytes("b"))))
        .timeRange(TimeRange.between(0, 101))
        .build(new Put(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("g")));

      CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(ROWKEY2)
        .ifMatches(new FilterList(
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
            Bytes.toBytes("d")),
          new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
            Bytes.toBytes("e"))))
        .timeRange(TimeRange.between(0, 100))
        .build(new Put(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("h")));

      boolean[] results = table.checkAndMutate(Arrays.asList(checkAndMutate1, checkAndMutate2));

      assertTrue(results[0]);
      assertFalse(results[1]);

      Result result = table.get(new Get(ROWKEY).addColumn(FAMILY, Bytes.toBytes("C")));
      assertEquals("g", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

      result = table.get(new Get(ROWKEY2).addColumn(FAMILY, Bytes.toBytes("F")));
      assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
    }
  }
}
