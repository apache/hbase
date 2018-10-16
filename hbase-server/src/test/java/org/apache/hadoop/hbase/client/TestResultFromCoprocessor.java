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

import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, ClientTests.class})
public class TestResultFromCoprocessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestResultFromCoprocessor.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROW = Bytes.toBytes("normal_row");
  private static final byte[] FAMILY = Bytes.toBytes("fm");
  private static final byte[] QUAL = Bytes.toBytes("qual");
  private static final byte[] VALUE = Bytes.toBytes(100L);
  private static final byte[] FIXED_VALUE = Bytes.toBytes("fixed_value");
  private static final Cell FIXED_CELL = CellUtil.createCell(ROW, FAMILY,
          QUAL, 0, KeyValue.Type.Put.getCode(), FIXED_VALUE);
  private static final Result FIXED_RESULT = Result.create(Arrays.asList(FIXED_CELL));
  private static final TableName TABLE_NAME = TableName.valueOf("TestResultFromCoprocessor");
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
            .setCoprocessor(MyObserver.class.getName())
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
            .build();
    TEST_UTIL.getAdmin().createTable(desc);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAppend() throws IOException {
    try (Table t = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUAL, VALUE);
      t.put(put);
      assertRowAndValue(t.get(new Get(ROW)), ROW, VALUE);
      Append append = new Append(ROW);
      append.addColumn(FAMILY, QUAL, FIXED_VALUE);
      assertRowAndValue(t.append(append), ROW, FIXED_VALUE);
      assertRowAndValue(t.get(new Get(ROW)), ROW, Bytes.add(VALUE, FIXED_VALUE));
    }
  }

  @Test
  public void testIncrement() throws IOException {
    try (Table t = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUAL, VALUE);
      t.put(put);
      assertRowAndValue(t.get(new Get(ROW)), ROW, VALUE);
      Increment inc = new Increment(ROW);
      inc.addColumn(FAMILY, QUAL, 99);
      assertRowAndValue(t.increment(inc), ROW, FIXED_VALUE);
      assertRowAndValue(t.get(new Get(ROW)), ROW, Bytes.toBytes(199L));
    }
  }

  private static void assertRowAndValue(Result r, byte[] row, byte[] value) {
    for (Cell c : r.rawCells()) {
      assertTrue(Bytes.equals(CellUtil.cloneRow(c), row));
      assertTrue(Bytes.equals(CellUtil.cloneValue(c), value));
    }
  }

  public static class MyObserver implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Result postAppend(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Append append, final Result result) {
      return FIXED_RESULT;
    }

    @Override
    public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result) {
      return FIXED_RESULT;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }
  }

}
