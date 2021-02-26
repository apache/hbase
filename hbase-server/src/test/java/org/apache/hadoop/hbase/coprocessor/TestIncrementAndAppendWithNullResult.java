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

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestIncrementAndAppendWithNullResult {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementAndAppendWithNullResult.class);

  private static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static final TableName TEST_TABLE = TableName.valueOf("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");
  private static Table table;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MyObserver.class.getName());
    // reduce the retry count so as to speed up the test
    util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    util.startMiniCluster();
    table = util.createTable(TEST_TABLE, TEST_FAMILY);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }


  public static class MyObserver implements RegionCoprocessor, RegionObserver {
    private static final Result TMP_RESULT = Result.create(Arrays.asList(
        CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(Bytes.toBytes("row"))
          .setFamily(Bytes.toBytes("family"))
          .setQualifier(Bytes.toBytes("qualifier"))
          .setType(Cell.Type.Put)
          .setValue(Bytes.toBytes("value"))
          .build()));
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
        Increment increment) throws IOException {
      return TMP_RESULT;
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c,
      Increment increment, Result result) throws IOException {
      return null;
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append,
        Result result) {
      return null;
    }

    @Override
    public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c,
        Append append) {
      return TMP_RESULT;
    }
  }

  @Test
  public void testIncrement() throws Exception {
    testAppend(new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 10L));
    testAppend(new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 10L)
              .setReturnResults(false));
  }

  private void testAppend(Increment inc) throws Exception {
    checkResult(table.increment(inc));
    List<Row> actions = Arrays.asList(inc, inc);
    Object[] results = new Object[actions.size()];
    table.batch(actions, results);
    checkResult(results);
  }

  @Test
  public void testAppend() throws Exception {
    testAppend(new Append(ROW_A).addColumn(TEST_FAMILY, qualifierCol1,
        Bytes.toBytes("value")));
    testAppend(new Append(ROW_A).addColumn(TEST_FAMILY, qualifierCol1,
        Bytes.toBytes("value")).setReturnResults(false));

  }

  private void testAppend(Append append) throws Exception {
    checkResult(table.append(append));
    List<Row> actions = Arrays.asList(append, append);
    Object[] results = new Object[actions.size()];
    table.batch(actions, results);
    checkResult(results);
  }

  private static void checkResult(Result r) {
    checkResult(new Object[]{r});
  }

  private static void checkResult(Object[] results) {
    for (int i = 0; i != results.length; ++i) {
      assertNotNull("The result[" + i + "] should not be null", results[i]);
      assertTrue("The result[" + i + "] should be Result type", results[i] instanceof Result);
      assertTrue("The result[" + i + "] shuold be empty", ((Result) results[i]).isEmpty());
    }
  }
}
