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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
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

  @Test
  public void testCheckAndMutate() throws Throwable {
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

      // Test that we get a region level exception
      try {
        rm = getBogusRowMutations();
        table.checkAndMutate(ROWKEY, FAMILY).qualifier(Bytes.toBytes("A"))
          .ifEquals(Bytes.toBytes("a")).thenMutate(rm);
        fail("Expected NoSuchColumnFamilyException");
      } catch (NoSuchColumnFamilyException e) {
        // expected
      } catch (RetriesExhaustedException e) {
        assertThat(e.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }
    }
  }

  @Test
  public void testCheckAndMutateWithBuilder() throws Throwable {
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

      // Test that we get a region level exception
      try {
        rm = getBogusRowMutations();
        table.checkAndMutate(ROWKEY, FAMILY).qualifier(Bytes.toBytes("A"))
          .ifEquals(Bytes.toBytes("a")).thenMutate(rm);
        fail("Expected NoSuchColumnFamilyException");
      } catch (NoSuchColumnFamilyException e) {
        // expected
      } catch (RetriesExhaustedException e) {
        assertThat(e.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }
    }
  }

}
