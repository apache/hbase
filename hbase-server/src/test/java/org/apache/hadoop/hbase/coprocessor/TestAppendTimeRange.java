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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestAppendTimeRange {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAppendTimeRange.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static final ManualEnvironmentEdge mee = new ManualEnvironmentEdge();

  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW = Bytes.toBytes("aaa");

  private static final byte[] QUAL = Bytes.toBytes("col1");

  private static final byte[] VALUE = Bytes.toBytes("1");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MyObserver.class.getName());
    // Make general delay zero rather than default. Timing is off in this
    // test that depends on an evironment edge that is manually moved forward.
    util.getConfiguration().setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 0);
    util.startMiniCluster();
    EnvironmentEdgeManager.injectEdge(mee);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  public static class MyObserver implements RegionCoprocessor, RegionObserver {
    private static TimeRange tr10 = null;
    private static TimeRange tr2 = null;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Append append) throws IOException {
      NavigableMap<byte [], List<Cell>> map = append.getFamilyCellMap();
      for (Map.Entry<byte [], List<Cell>> entry : map.entrySet()) {
        for (Cell cell : entry.getValue()) {
          String appendStr = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
              cell.getValueLength());
          if (appendStr.equals("b")) {
            tr10 = append.getTimeRange();
          } else if (appendStr.equals("c") && !append.getTimeRange().isAllTime()) {
            tr2 = append.getTimeRange();
          }
        }
      }
      return null;
    }
  }

  @Test
  public void testHTableInterfaceMethods() throws Exception {
    try (Table table = util.createTable(TableName.valueOf(name.getMethodName()), TEST_FAMILY)) {
      table.put(new Put(ROW).addColumn(TEST_FAMILY, QUAL, VALUE));
      long time = EnvironmentEdgeManager.currentTime();
      mee.setValue(time);
      table.put(new Put(ROW).addColumn(TEST_FAMILY, QUAL, Bytes.toBytes("a")));
      checkRowValue(table, ROW, Bytes.toBytes("a"));

      time = EnvironmentEdgeManager.currentTime();
      mee.setValue(time);
      TimeRange range10 = new TimeRange(1, time + 10);
      Result r = table.append(new Append(ROW).addColumn(TEST_FAMILY, QUAL, Bytes.toBytes("b"))
          .setTimeRange(range10.getMin(), range10.getMax()));
      checkRowValue(table, ROW, Bytes.toBytes("ab"));
      assertEquals(MyObserver.tr10.getMin(), range10.getMin());
      assertEquals(MyObserver.tr10.getMax(), range10.getMax());
      time = EnvironmentEdgeManager.currentTime();
      mee.setValue(time);
      TimeRange range2 = new TimeRange(1, time+20);
      List<Row> actions =
          Arrays.asList(new Row[] {
              new Append(ROW).addColumn(TEST_FAMILY, QUAL, Bytes.toBytes("c"))
                  .setTimeRange(range2.getMin(), range2.getMax()),
              new Append(ROW).addColumn(TEST_FAMILY, QUAL, Bytes.toBytes("c"))
                  .setTimeRange(range2.getMin(), range2.getMax()) });
      Object[] results1 = new Object[actions.size()];
      table.batch(actions, results1);
      assertEquals(MyObserver.tr2.getMin(), range2.getMin());
      assertEquals(MyObserver.tr2.getMax(), range2.getMax());
      for (Object r2 : results1) {
        assertTrue(r2 instanceof Result);
      }
      checkRowValue(table, ROW, Bytes.toBytes("abcc"));
    }
  }

  private void checkRowValue(Table table, byte[] row, byte[] expectedValue) throws IOException {
    Get get = new Get(row).addColumn(TEST_FAMILY, QUAL);
    Result result = table.get(get);
    byte[] actualValue = result.getValue(TEST_FAMILY, QUAL);
    assertArrayEquals(expectedValue, actualValue);
  }
}
