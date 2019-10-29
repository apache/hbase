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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
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
public class TestCheckAndRowMutate {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCheckAndRowMutate.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROWKEY = Bytes.toBytes("12345");
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

  private Table createTable() throws IOException, InterruptedException {
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
    assertEquals("Column A value should be a", "a",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));
    assertEquals("Column B value should be b", "b",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
    assertEquals("Column C value should be c", "c",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));
  }

  private void deleteOneRow(Table table) throws IOException {
    Delete delete = new Delete(ROWKEY);
    table.delete(delete);
  }

  private List<CheckAndRowMutate> makeCheckAndRowMutatesWithPut() throws IOException {
    List<CheckAndRowMutate> checkAndRowMutates = new ArrayList<>();
    Put put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("aa"));
    CheckAndRowMutate checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("A"))
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes("a"))
        .addMutation(put);
    checkAndRowMutates.add(checkAndRowMutate);

    put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("bb"));
    checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("B"))
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes("b"))
        .addMutation(put);
    checkAndRowMutates.add(checkAndRowMutate);

    put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("cc"));
    checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("C"))
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes("c"))
        .addMutation(put);
    checkAndRowMutates.add(checkAndRowMutate);
    return checkAndRowMutates;
  }

  private void getOneRowAndAssertCNotChanged(final Table table) throws IOException {
    Get get = new Get(ROWKEY);
    Result result = table.get(get);
    assertEquals("Column A value should be aa", "aa",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));
    assertEquals("Column B value should be bb", "bb",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
    assertEquals("Column B value should be c", "c",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));
  }

  private List<CheckAndRowMutate> makeCheckAndRowMutatesWithDelete() throws IOException {
    List<CheckAndRowMutate> checkAndRowMutates = new ArrayList<>();
    Delete delete = new Delete(ROWKEY);
    delete.addColumns(FAMILY, Bytes.toBytes("A"));
    CheckAndRowMutate checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("A"))
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes("a"))
        .addMutation(delete);
    checkAndRowMutates.add(checkAndRowMutate);

    delete = new Delete(ROWKEY);
    delete.addColumns(FAMILY, Bytes.toBytes("B"));
    checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("B"))
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes("b"))
        .addMutation(delete);
    checkAndRowMutates.add(checkAndRowMutate);

    delete = new Delete(ROWKEY);
    delete.addColumns(FAMILY, Bytes.toBytes("C"));
    checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("C"))
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes("c"))
        .addMutation(delete);
    checkAndRowMutates.add(checkAndRowMutate);
    return checkAndRowMutates;
  }

  private void getOneRowAndAssertCNotDelete(final Table table) throws IOException {
    Get get = new Get(ROWKEY);
    Result result = table.get(get);
    assertNull("Column A should not exist", result.getValue(FAMILY, Bytes.toBytes("A")));
    assertNull("Column B should not exist", result.getValue(FAMILY, Bytes.toBytes("B")));
    assertEquals("Column C value should be c", "c",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));
  }

  private List<CheckAndRowMutate> makeCheckAndRowMutatesWithPutAndDelete() throws IOException {
    List<CheckAndRowMutate> checkAndRowMutates = new ArrayList<>();
    Put put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("aa"));
    CheckAndRowMutate checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("A"))
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes("a"))
        .addMutation(put);
    checkAndRowMutates.add(checkAndRowMutate);

    put = new Put(ROWKEY);
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("bb"));
    checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("B"))
        .ifMatches(CompareOperator.GREATER, Bytes.toBytes("b"))
        .addMutation(put);
    checkAndRowMutates.add(checkAndRowMutate);

    Delete delete = new Delete(ROWKEY);
    delete.addColumns(FAMILY, Bytes.toBytes("C"));
    checkAndRowMutate = new CheckAndRowMutate(ROWKEY, FAMILY)
        .qualifier(Bytes.toBytes("C"))
        .ifMatches(CompareOperator.EQUAL, Bytes.toBytes("c"))
        .addMutation(delete);
    checkAndRowMutates.add(checkAndRowMutate);
    return checkAndRowMutates;
  }

  private void getOneRowAndAssertAChangedAndCDelete(final Table table) throws IOException {
    Get get = new Get(ROWKEY);
    Result result = table.get(get);
    assertEquals("Column A value should be aa", "aa",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));
    assertEquals("Column B value should be bb", "b",
        Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
    assertNull("Column C should not exist", result.getValue(FAMILY, Bytes.toBytes("C")));
  }

  @Test
  public void testCheckAndRowMutateWithPut() throws Throwable {
    try (Table table = createTable()) {
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // put the same row again with C column not changed
      List<CheckAndRowMutate> carm = makeCheckAndRowMutatesWithPut();
      boolean[] result = table.checkAndRowMutate(carm);
      assertTrue(result[0]);
      assertTrue(result[1]);
      assertFalse(result[2]);
      getOneRowAndAssertCNotChanged(table);

      // delete one row
      deleteOneRow(table);
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // put the same row again with C column not deleted
      carm = makeCheckAndRowMutatesWithDelete();
      result = table.checkAndRowMutate(carm);
      assertTrue(result[0]);
      assertTrue(result[1]);
      assertFalse(result[2]);
      getOneRowAndAssertCNotDelete(table);

      // delete one row
      deleteOneRow(table);
      // put one row
      putOneRow(table);
      // get row back and assert the values
      getOneRowAndAssertAllExist(table);

      // put the same row again with A column changed and C column deleted
      carm = makeCheckAndRowMutatesWithPutAndDelete();
      result = table.checkAndRowMutate(carm);
      assertTrue(result[0]);
      assertFalse(result[1]);
      assertTrue(result[2]);
      getOneRowAndAssertAChangedAndCDelete(table);
    }
  }
}
