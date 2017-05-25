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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This test runs batch mutation with Increments which have custom TimeRange.
 * Custom Observer records the TimeRange.
 * We then verify that the recorded TimeRange has same bounds as the initial TimeRange.
 * See HBASE-15698
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestIncrementTimeRange {

  private static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static ManualEnvironmentEdge mee = new ManualEnvironmentEdge();

  private static final TableName TEST_TABLE = TableName.valueOf("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");

  private static final byte[] bytes1 = Bytes.toBytes(1);
  private static final byte[] bytes2 = Bytes.toBytes(2);
  private static final byte[] bytes3 = Bytes.toBytes(3);

  private Table hTableInterface;
  private Table table;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MyObserver.class.getName());
    util.startMiniCluster();
    EnvironmentEdgeManager.injectEdge(mee);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    table = util.createTable(TEST_TABLE, TEST_FAMILY);

    Put puta = new Put(ROW_A);
    puta.addColumn(TEST_FAMILY, qualifierCol1, bytes1);
    table.put(puta);

    Put putb = new Put(ROW_B);
    putb.addColumn(TEST_FAMILY, qualifierCol1, bytes2);
    table.put(putb);

    Put putc = new Put(ROW_C);
    putc.addColumn(TEST_FAMILY, qualifierCol1, bytes3);
    table.put(putc);
  }

  @After
  public void after() throws Exception {
    try {
      if (table != null) {
        table.close();
      }
    } finally {
      try {
        util.deleteTable(TEST_TABLE);
      } catch (IOException ioe) {
      }
    }
  }

  public static class MyObserver extends SimpleRegionObserver {
    static TimeRange tr10 = null, tr2 = null;
    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
      NavigableMap<byte [], List<Cell>> map = increment.getFamilyCellMap();
      for (Map.Entry<byte [], List<Cell>> entry : map.entrySet()) {
        for (Cell cell : entry.getValue()) {
          long incr = Bytes.toLong(cell.getValueArray(), cell.getValueOffset(),
              cell.getValueLength());
          if (incr == 10) {
            tr10 = increment.getTimeRange();
          } else if (incr == 2 && !increment.getTimeRange().isAllTime()) {
            tr2 = increment.getTimeRange();
          }
        }
      }
      return super.preIncrement(e, increment);
    }
  }

  @Test
  public void testHTableInterfaceMethods() throws Exception {
    hTableInterface = util.getConnection().getTable(TEST_TABLE);
    checkHTableInterfaceMethods();
  }

  private void checkHTableInterfaceMethods() throws Exception {
    long time = EnvironmentEdgeManager.currentTime();
    mee.setValue(time);
    hTableInterface.put(new Put(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, Bytes.toBytes(1L)));
    checkRowValue(ROW_A, Bytes.toBytes(1L));

    time = EnvironmentEdgeManager.currentTime();
    mee.setValue(time);
    TimeRange range10 = new TimeRange(1, time+10);
    hTableInterface.increment(new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 10L)
        .setTimeRange(range10.getMin(), range10.getMax()));
    checkRowValue(ROW_A, Bytes.toBytes(11L));
    assertEquals(MyObserver.tr10.getMin(), range10.getMin());
    assertEquals(MyObserver.tr10.getMax(), range10.getMax());

    time = EnvironmentEdgeManager.currentTime();
    mee.setValue(time);
    TimeRange range2 = new TimeRange(1, time+20);
    List<Row> actions =
        Arrays.asList(new Row[] { new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L)
            .setTimeRange(range2.getMin(), range2.getMax()),
            new Increment(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, 2L)
            .setTimeRange(range2.getMin(), range2.getMax()) });
    Object[] results3 = new Object[actions.size()];
    Object[] results1 = results3;
    hTableInterface.batch(actions, results1);
    assertEquals(MyObserver.tr2.getMin(), range2.getMin());
    assertEquals(MyObserver.tr2.getMax(), range2.getMax());
    for (Object r2 : results1) {
      assertTrue(r2 instanceof Result);
    }
    checkRowValue(ROW_A, Bytes.toBytes(15L));

    hTableInterface.close();
  }

  private void checkRowValue(byte[] row, byte[] expectedValue) throws IOException {
    Get get = new Get(row).addColumn(TEST_FAMILY, qualifierCol1);
    Result result = hTableInterface.get(get);
    byte[] actualValue = result.getValue(TEST_FAMILY, qualifierCol1);
    assertArrayEquals(expectedValue, actualValue);
  }
}
