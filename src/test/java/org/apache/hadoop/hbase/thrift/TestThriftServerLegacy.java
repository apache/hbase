/*
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.TestThriftServer.assertBufferEquals;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.closeScanner;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.columnAname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.columnBname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.getBatchMutations;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.getColumnDescriptors;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.getColumnList;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.getHandlerForMetricsTest;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.getMetrics;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.getMutations;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.rowAname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.rowBname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.tableAname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.tableBname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.valueAname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.valueBname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.valueCname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.valueDname;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.verifyMetricRange;
import static org.apache.hadoop.hbase.thrift.TestThriftServer.verifyMetrics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

/**
 * Unit testing for ThriftServerRunner.HBaseHandler, a part of the org.apache.hadoop.hbase.thrift
 * package. Most of this stuff exists in {@link TestThriftServer} but there might be differences in
 * details, so leaving this around for better test coverage.
 */
@Category(MediumTests.class)
public class TestThriftServerLegacy extends HBaseClusterTestCase {

  /**
   * Runs all of the tests under a single JUnit test method.  We
   * consolidate all testing to one method because HBaseClusterTestCase
   * is prone to OutOfMemoryExceptions when there are three or more
   * JUnit test methods.
   *
   * @throws Exception
   */
  public void testAll() throws Exception {
    // Run all tests
    doTestTableCreateDrop();
    doTestThriftMetrics();
    doTestTableMutations();
    doTestTableTimestampsAndColumns();
    doTestTableScanners();
    doTestTableMultiGet();
    doTestTableCheckAndMutate();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.  Also
   * tests that creating a table with an invalid column name yields an
   * IllegalArgument exception.
   *
   * @throws Exception
   */
  public void doTestTableCreateDrop() throws Exception {
    ThriftServerRunner.HBaseHandler handler = new ThriftServerRunner.HBaseHandler(conf);

    // Create/enable/disable/delete tables, ensure methods act correctly
    assertEquals(handler.getTableNames().size(), 0);
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(handler.getTableNames().size(), 1);
    assertEquals(handler.getColumnDescriptors(tableAname).size(), 2);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.createTable(tableBname, new ArrayList<ColumnDescriptor>());
    assertEquals(handler.getTableNames().size(), 2);
    handler.disableTable(tableBname);
    assertFalse(handler.isTableEnabled(tableBname));
    handler.deleteTable(tableBname);
    assertEquals(handler.getTableNames().size(), 1);
    handler.disableTable(tableAname);
    assertFalse(handler.isTableEnabled(tableAname));
    handler.enableTable(tableAname);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Tests if the metrics for thrift handler work correctly
   */
  public void doTestThriftMetrics() throws Exception {
    ThriftMetrics metrics = getMetrics(conf);
    Hbase.Iface handler = getHandlerForMetricsTest(metrics, conf);
    handler.createTable(tableAname, getColumnDescriptors());
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
    handler.createTable(tableBname, getColumnDescriptors());
    handler.disableTable(tableBname);
    handler.deleteTable(tableBname);
    verifyMetrics(metrics, "createTable_num_ops", 2);
    verifyMetrics(metrics, "deleteTable_num_ops", 2);
    verifyMetrics(metrics, "disableTable_num_ops", 2);
    handler.getTableNames(); // This will have an artificial delay.

    // 3 to 6 seconds (to account for potential slowness), measured in nanoseconds.
    verifyMetricRange(metrics, "getTableNames_avg_time", 3L * 1000 * 1000 * 1000,
        6L * 1000 * 1000 * 1000);
  }

  /**
   * Tests adding a series of Mutations and BatchMutations, including a
   * delete mutation.  Also tests data retrieval, and getting back multiple
   * versions.
   *
   * @throws Exception
   */
  public void doTestTableMutations() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler = new ThriftServerRunner.HBaseHandler(conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply a few Mutations to rowA
    //     mutations.add(new Mutation(false, columnAname, valueAname));
    //     mutations.add(new Mutation(false, columnBname, valueBname));
    handler.mutateRow(tableAname, rowAname, getMutations(), null, null);

    // Assert that the changes were made
    assertBufferEquals(valueAname, handler.get(tableAname, rowAname, columnAname,
        null).get(0).value);
    TRowResult rowResult1 = handler.getRow(tableAname, rowAname, null).get(0);
    assertBufferEquals(rowAname, rowResult1.row);
    assertBufferEquals(valueBname, rowResult1.columns.get(columnBname).value);

    // Apply a few BatchMutations for rowA and rowB
    // rowAmutations.add(new Mutation(true, columnAname, null));
    // rowAmutations.add(new Mutation(false, columnBname, valueCname));
    // batchMutations.add(new BatchMutation(rowAname, rowAmutations));
    // Mutations to rowB
    // rowBmutations.add(new Mutation(false, columnAname, valueCname));
    // rowBmutations.add(new Mutation(false, columnBname, valueDname));
    // batchMutations.add(new BatchMutation(rowBname, rowBmutations));
    handler.mutateRows(tableAname, getBatchMutations(), null, null);

    // Assert that changes were made to rowA
    List<TCell> cells = handler.get(tableAname, rowAname, columnAname, null);
    assertFalse(cells.size() > 0);
    assertBufferEquals(valueCname, handler.get(tableAname, rowAname, columnBname,
        null).get(0).value);
    List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS, null);
    assertBufferEquals(valueCname, versions.get(0).value);
    assertBufferEquals(valueBname, versions.get(1).value);

    // Assert that changes were made to rowB
    TRowResult rowResult2 = handler.getRow(tableAname, rowBname, null).get(0);
    assertBufferEquals(rowBname, rowResult2.row);
    assertBufferEquals(valueCname, rowResult2.columns.get(columnAname).value);
    assertBufferEquals(valueDname, rowResult2.columns.get(columnBname).value);

    // Apply some deletes
    handler.deleteAll(tableAname, rowAname, columnBname, null);
    handler.deleteAllRow(tableAname, rowBname, null, null);

    // Assert that the deletes were applied
    int size = handler.get(tableAname, rowAname, columnBname, null).size();
    assertEquals(0, size);
    size = handler.getRow(tableAname, rowBname, null).size();
    assertEquals(0, size);

    // Try null mutation
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, columnAname, null, true, HConstants.LATEST_TIMESTAMP));
    handler.mutateRow(tableAname, rowAname, mutations, null, null);
    TRowResult rowResult3 = handler.getRow(tableAname, rowAname, null).get(0);
    assertEquals(rowAname, rowResult3.row);
    assertEquals(0, rowResult3.columns.get(columnAname).value.remaining());

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Similar to testTableMutations(), except Mutations are applied with
   * specific timestamps and data retrieval uses these timestamps to
   * extract specific versions of data.
   *
   * @throws Exception
   */
  public void doTestTableTimestampsAndColumns() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler = new ThriftServerRunner.HBaseHandler(conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1, null, null);

    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2, null, null);

    // Apply an overlapping timestamped mutation to rowB
    handler.mutateRowTs(tableAname, rowBname, getMutations(), time2, null, null);

    // the getVerTs is [inf, ts) so you need to increment one.
    time1 += 1;
    time2 += 2;

    // Assert that the timestamp-related methods retrieve the correct data
    assertEquals(2, handler.getVerTs(tableAname, rowAname, columnBname, time2,
        MAXVERSIONS, null).size());
    assertEquals(1, handler.getVerTs(tableAname, rowAname, columnBname, time1,
        MAXVERSIONS, null).size());

    TRowResult rowResult1 = handler.getRowTs(tableAname, rowAname, time1, null).get(0);
    TRowResult rowResult2 = handler.getRowTs(tableAname, rowAname, time2, null).get(0);
    // columnA was completely deleted
    //assertBufferEquals(rowResult1.columns.get(columnAname).value, valueAname);
    assertBufferEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertBufferEquals(rowResult2.columns.get(columnBname).value, valueCname);

    // ColumnAname has been deleted, and will never be visible even with a getRowTs()
    assertFalse(rowResult2.columns.containsKey(columnAname));

    List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
    columns.add(columnBname);

    rowResult1 = handler.getRowWithColumns(tableAname, rowAname, columns, null).get(0);
    assertBufferEquals(rowResult1.columns.get(columnBname).value, valueCname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    rowResult1 = handler.getRowWithColumnsTs(tableAname, rowAname, columns, time1, null).get(0);
    assertBufferEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    // Apply some timestamped deletes
    // this actually deletes _everything_.
    // nukes everything in columnB: forever.
    handler.deleteAllTs(tableAname, rowAname, columnBname, time1, null);
    handler.deleteAllRowTs(tableAname, rowBname, time2, null);

    // Assert that the timestamp-related methods retrieve the correct data
    int size = handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS,
        null).size();
    assertEquals(0, size);

    size = handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS,
        null).size();
    assertEquals(1, size);

    // should be available....
    assertBufferEquals(handler.get(tableAname, rowAname, columnBname, null).get(0).value,
        valueCname);

    assertEquals(0, handler.getRow(tableAname, rowBname, null).size());

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Tests the four different scanner-opening methods (with and without
   * a stoprow, with and without a timestamp).
   *
   * @throws Exception
   */
  public void doTestTableScanners() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler = new ThriftServerRunner.HBaseHandler(conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1, null, null);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2, null, null);

    time1 += 1;

    // Test a scanner on all rows and all columns, no timestamp
    int scanner1 = handler.scannerOpen(tableAname, rowAname, getColumnList(true, true));
    TRowResult rowResult1a = handler.scannerGet(scanner1).get(0);
    assertBufferEquals(rowResult1a.row, rowAname);
    // This used to be '1'.  I don't know why when we are asking for two columns
    // and when the mutations above would seem to add two columns to the row.
    // -- St.Ack 05/12/2009
    assertEquals(rowResult1a.columns.size(), 1);
    assertBufferEquals(rowResult1a.columns.get(columnBname).value, valueCname);

    TRowResult rowResult1b = handler.scannerGet(scanner1).get(0);
    assertBufferEquals(rowResult1b.row, rowBname);
    assertEquals(rowResult1b.columns.size(), 2);
    assertBufferEquals(rowResult1b.columns.get(columnAname).value, valueCname);
    assertBufferEquals(rowResult1b.columns.get(columnBname).value, valueDname);
    closeScanner(scanner1, handler);

    // Test a scanner on all rows and all columns, with timestamp
    int scanner2 = handler.scannerOpenTs(tableAname, rowAname, getColumnList(true, true), time1);
    TRowResult rowResult2a = handler.scannerGet(scanner2).get(0);
    assertEquals(rowResult2a.columns.size(), 1);
    // column A deleted, does not exist.
    //assertBufferEquals(rowResult2a.columns.get(columnAname).value, valueAname);
    assertBufferEquals(rowResult2a.columns.get(columnBname).value, valueBname);
    closeScanner(scanner2, handler);

    // Test a scanner on the first row and first column only, no timestamp
    int scanner3 = handler.scannerOpenWithStop(tableAname, rowAname, rowBname,
        getColumnList(true, false));
    closeScanner(scanner3, handler);

    // Test a scanner on the first row and second column only, with timestamp
    int scanner4 = handler.scannerOpenWithStopTs(tableAname, rowAname, rowBname,
        getColumnList(false, true), time1);
    TRowResult rowResult4a = handler.scannerGet(scanner4).get(0);
    assertEquals(rowResult4a.columns.size(), 1);
    assertBufferEquals(rowResult4a.columns.get(columnBname).value, valueBname);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Tests some of the getRows*() calls.
   *
   * @throws Exception
   */
  public void doTestTableMultiGet() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler = new ThriftServerRunner.HBaseHandler(conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1, null, null);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2, null, null);

    time1 += 1;

    // test getting the two rows with a multiget
    List<ByteBuffer> rows = new ArrayList<ByteBuffer>();
    rows.add(rowAname);
    rows.add(rowBname);

    List<TRowResult> results = handler.getRows(tableAname, rows, null);
    assertEquals(results.get(0).row, rowAname);
    assertEquals(results.get(1).row, rowBname);

    assertEquals(results.get(0).columns.get(columnBname).value, valueCname);
    assertEquals(results.get(1).columns.get(columnAname).value, valueCname);
    assertEquals(results.get(1).columns.get(columnBname).value, valueDname);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * Test some of the checkAndMutate calls
   *
   * @throws Exception
   */
  public void doTestTableCheckAndMutate() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler = new ThriftServerRunner.HBaseHandler(conf);
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    assertTrue(handler.checkAndMutateRowTs(tableAname, rowAname, columnAname, null,
        getMutations(), time1, null));

    List<TRowResult> res1 = handler.getRow(tableAname, rowAname, null);
    assertEquals(1, res1.size());
    // Check that all went according to plan
    assertEquals(res1.get(0).columns.get(columnAname).value, valueAname);
    assertEquals(res1.get(0).columns.get(columnBname).value, valueBname);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply later timestamped mutations to rowA
    long time2 = System.currentTimeMillis();

    // Mess stuff up; shouldn't pass, null check value
    handler.checkAndMutateRowTs(tableAname, rowAname, columnAname, null,
        getMutations2(), time2, null);
    handler.checkAndMutateRowTs(tableAname, rowAname, columnBname, null,
        getMutations2(), time2, null);

    // Check that all still the same!
    assertEquals(res1.get(0).columns.get(columnAname).value, valueAname);
    assertEquals(res1.get(0).columns.get(columnBname).value, valueBname);

    // Mess stuff up; shouldn't pass, wrong check value
    handler.checkAndMutateRowTs(tableAname, rowAname, columnAname,
        ByteBuffer.wrap(Bytes.toBytes("randovalue1")), getMutations2(), time2, null);
    handler.checkAndMutateRowTs(tableAname, rowAname, columnBname,
        ByteBuffer.wrap(Bytes.toBytes("randovalue2")), getMutations2(), time2, null);

    // Check that all still the same!
    assertEquals(res1.get(0).columns.get(columnAname).value, valueAname);
    assertEquals(res1.get(0).columns.get(columnBname).value, valueBname);

    // Now actually change things
    handler.checkAndMutateRowTs(tableAname, rowAname, columnAname, valueAname,
        getMutations2(), time2, null);

    res1 = handler.getRow(tableAname, rowAname, null);

    // Check that actually changed!
    assertEquals(res1.get(0).columns.get(columnAname).value, valueCname);
    assertEquals(res1.get(0).columns.get(columnBname).value, valueDname);
    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * @return a List of Mutations for a row, with columnA having valueC and
   * columnB having valueD
   */
  private List<Mutation> getMutations2() {
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, columnAname, valueCname, true, HConstants.LATEST_TIMESTAMP));
    mutations.add(new Mutation(false, columnBname, valueDname, true, HConstants.LATEST_TIMESTAMP));
    return mutations;
  }

}
