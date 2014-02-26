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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.HbaseConstants;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Unit testing for ThriftServerRunner.HBaseHandler, a part of the
 * org.apache.hadoop.hbase.thrift package.
 */
public class TestThriftServer {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestThriftServer.class);
  protected static final int MAXVERSIONS = 3;

  private static ByteBuffer asByteBuffer(String i) {
    return ByteBuffer.wrap(Bytes.toBytes(i));
  }

  // Static names for tables, columns, rows, and values
  static final ByteBuffer tableAname = asByteBuffer("tableA");
  static final ByteBuffer tableBname = asByteBuffer("tableB");
  static final ByteBuffer columnAname = asByteBuffer("columnA:");
  static final ByteBuffer columnBname = asByteBuffer("columnB:");
  static final ByteBuffer rowAname = asByteBuffer("rowA");
  static final ByteBuffer rowBname = asByteBuffer("rowB");
  static final ByteBuffer valueAname = asByteBuffer("valueA");
  static final ByteBuffer valueAModified = asByteBuffer("valueAModified");
  static final ByteBuffer valueBname = asByteBuffer("valueB");
  static final ByteBuffer valueCname = asByteBuffer("valueC");
  static final ByteBuffer valueDname = asByteBuffer("valueD");

  private static final int MAX_VERSIONS = 2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

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
    doTestGetTableRegions();
    doTestFilterRegistration();
    doTestGetRegionInfo();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.  Also
   * tests that creating a table with an invalid column name yields an
   * IllegalArgument exception.
   *
   * @throws Exception
   */
  @Test
  public void doTestTableCreateDrop() throws Exception {
    ThriftServerRunner.HBaseHandler handler =
      new ThriftServerRunner.HBaseHandler(UTIL.getConfiguration());
    doTestTableCreateDrop(handler);
  }

  public static void doTestTableCreateDrop(Hbase.Iface handler) throws Exception {
    createTestTables(handler);
    dropTestTables(handler);
  }

  static final class MySlowHBaseHandler extends ThriftServerRunner.HBaseHandler
      implements Hbase.Iface {

    protected MySlowHBaseHandler(Configuration c)
        throws IOException {
      super(c);
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError {
      Threads.sleepWithoutInterrupt(3000);
      return super.getTableNames();
    }
  }

  /**
   * Tests if the metrics for thrift handler work correctly
   */
  @Test
  public void doTestThriftMetrics() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ThriftMetrics metrics = getMetrics(conf);
    Hbase.Iface handler = getHandlerForMetricsTest(metrics, conf);
    createTestTables(handler);
    dropTestTables(handler);
    verifyMetrics(metrics, "createTable_num_ops", 2);
    verifyMetrics(metrics, "deleteTable_num_ops", 2);
    verifyMetrics(metrics, "disableTable_num_ops", 3);
    handler.getTableNames(); // This will have an artificial delay.

    // 3 to 6 seconds (to account for potential slowness), measured in nanoseconds.
    verifyMetricRange(metrics, "getTableNames_avg_time", 3L * 1000 * 1000 * 1000,
        6L * 1000 * 1000 * 1000);
  }

  static Hbase.Iface getHandlerForMetricsTest(ThriftMetrics metrics, Configuration conf)
      throws Exception {
    Hbase.Iface handler = new MySlowHBaseHandler(conf);
    return HbaseHandlerMetricsProxy.newInstance(handler, metrics, conf);
  }

  static ThriftMetrics getMetrics(Configuration conf) throws Exception {
    setupMetricsContext();
    // Port is not important here. It is just for setting a "tag" within metrics.
    return new ThriftMetrics(HConstants.DEFAULT_THRIFT_PROXY_PORT, conf, Hbase.Iface.class);
  }

  private static void setupMetricsContext() throws IOException {
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute(ThriftMetrics.CONTEXT_NAME + ".class",
        NoEmitMetricsContext.class.getName());
    MetricsUtil.getContext(ThriftMetrics.CONTEXT_NAME)
               .createRecord(ThriftMetrics.CONTEXT_NAME).remove();
  }

  public static void createTestTables(Hbase.Iface handler) throws Exception {
    // Create/enable/disable/delete tables, ensure methods act correctly
    assertEquals(handler.getTableNames().size(), 0);
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(handler.getTableNames().size(), 1);
    assertEquals(handler.getColumnDescriptors(tableAname).size(), 2);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.createTable(tableBname, new ArrayList<ColumnDescriptor>());
    assertEquals(handler.getTableNames().size(), 2);
  }

  public static void dropTestTables(Hbase.Iface handler) throws Exception {
    handler.disableTable(tableBname);
    assertFalse(handler.isTableEnabled(tableBname));
    handler.deleteTable(tableBname);
    assertEquals(handler.getTableNames().size(), 1);
    handler.disableTable(tableAname);
    assertFalse(handler.isTableEnabled(tableAname));

    // Reenable the table.
    handler.enableTable(tableAname);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
    assertEquals(handler.getTableNames().size(), 0);
  }

  static void verifyMetrics(ThriftMetrics metrics, String name, long expectValue)
      throws Exception {
    long metricVal = getMetricValue(metrics, name);
    assertEquals(expectValue, metricVal);
  }

  static void verifyMetricRange(ThriftMetrics metrics, String name,
      long minValue, long maxValue)
      throws Exception {
    long metricVal = getMetricValue(metrics, name);
    if (metricVal < minValue || metricVal > maxValue) {
      throw new AssertionError("Value of metric " + name + " is outside of the expected " +
          "range [" +  minValue + ", " + maxValue + "]: " + metricVal);
    }
  }

  static long getMetricValue(ThriftMetrics metrics, String name) {
    MetricsContext context = MetricsUtil.getContext(
        ThriftMetrics.CONTEXT_NAME);
    metrics.doUpdates(context);
    OutputRecord record = context.getAllRecords().get(
        ThriftMetrics.CONTEXT_NAME).iterator().next();
    return record.getMetric(name).longValue();
  }

  /**
   * Tests adding a series of Mutations and BatchMutations, including a
   * delete mutation.  Also tests data retrieval, and getting back multiple
   * versions.
   *
   * @throws Exception
   */
  @Test
  public void doTestTableMutations() throws Exception {
    ThriftServerRunner.HBaseHandler handler =
      new ThriftServerRunner.HBaseHandler(UTIL.getConfiguration());
    doTestTableMutations(handler);
  }

  public static void doTestTableMutations(Hbase.Iface handler) throws Exception {
    // Setup
    handler.createTable(tableAname, getColumnDescriptors());
    try {
      // Apply a few Mutations to rowA
      handler.mutateRow(tableAname, rowAname, getMutations(), null, null);
  
      // Assert that the changes were made
      assertBufferEquals(valueAname,
        handler.get(tableAname, rowAname, columnAname, null).get(0).value);
      TRowResult rowResult1 = handler.getRow(tableAname, rowAname, null).get(0);
      assertBufferEquals(rowAname, rowResult1.row);
      assertBufferEquals(valueBname,
        rowResult1.columns.get(columnBname).value);
  
      // Apply a few BatchMutations for rowA and rowB
      handler.mutateRows(tableAname, getBatchMutations(), null, null);
  
      // Assert that changes were made to rowA
      List<TCell> cells = handler.get(tableAname, rowAname, columnAname, null);
      assertFalse(cells.size() > 0);
      assertBufferEquals(valueCname, handler.get(tableAname, rowAname, columnBname, 
          null).get(0).value);
      List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS,
          null);
      assertBufferEquals(valueCname, versions.get(0).value);
      assertBufferEquals(valueBname, versions.get(1).value);
  
      // Assert that changes were made to rowB
      TRowResult rowResult2 = handler.getRow(tableAname, rowBname,
          null).get(0);
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
  
      // Try specifying timestamps with mutations
      mutations.clear();
      handler.deleteAllRow(tableAname, rowAname, null, null);
      assertEquals(0, handler.getRow(tableAname, rowAname, null).size());

      // Use a high timestamp to make sure our insertions come later than the above delete-all with
      // an auto-generated timestamp.
      // Testing mutateRow.
      long highTS = HConstants.LATEST_TIMESTAMP / 2;
      assertEquals(0, handler.getRow(tableAname, rowAname, null).size());
      mutations.add(new Mutation(false, columnAname, valueAModified, true, highTS + 257));
      mutations.add(new Mutation(false, columnAname, valueAname, true, highTS + 135));
      handler.mutateRow(tableAname, rowAname, mutations, null, null);
      List<TRowResult> results = handler.getRow(tableAname, rowAname, null);
      assertEquals(1, results.size());
      assertBufferEquals(valueAModified, results.get(0).getColumns().get(columnAname).value);

      handler.deleteAllTs(tableAname, rowAname, columnAname, highTS + 999, null);
      assertEquals(0, handler.getRow(tableAname, rowAname, null).size());

      // Testing mutateRowTs.
      for (boolean firstIsDeletion : HConstants.BOOLEAN_VALUES) {
        for (boolean secondIsDeletion : HConstants.BOOLEAN_VALUES) {
          mutations.clear();
          // This mutation has a default timestamp specified, so it will be executed with the
          // timestamp given to the mutateRowTs method.
          mutations.add(new Mutation(firstIsDeletion, columnAname, valueAModified, true,
              HConstants.LATEST_TIMESTAMP)); 
          mutations.add(new Mutation(secondIsDeletion, columnAname, valueAname, true,
              highTS + 1200));
          handler.mutateRowTs(tableAname, rowAname, mutations, highTS + 1100, null, null);
          results = handler.getRow(tableAname, rowAname, null);
          if (secondIsDeletion) {
            assertEquals(0, results.size());
          } else {
            assertEquals(1, results.size());
            assertBufferEquals(valueAname, results.get(0).getColumns().get(columnAname).value);
          }
          handler.deleteAllRowTs(tableAname, rowAname, highTS + 10000, null);
          assertEquals(0, handler.getRow(tableAname, rowAname, null).size());
          highTS += 20000;
        }
      }

    } finally {
      // Teardown
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  /**
   * Similar to testTableMutations(), except Mutations are applied with
   * specific timestamps and data retrieval uses these timestamps to
   * extract specific versions of data.
   *
   * @throws Exception
   */
  @SuppressWarnings("deprecation")
  @Test
  public void doTestTableTimestampsAndColumns() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler =
      new ThriftServerRunner.HBaseHandler(UTIL.getConfiguration());
    handler.createTable(tableAname, getColumnDescriptors());

    try {
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
      //assertTrue(Bytes.equals(rowResult1.columns.get(columnAname).value, valueAname));
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
  
      size = handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS, null).size();
      assertEquals(1, size);
  
      // should be available....
      assertBufferEquals(handler.get(tableAname, rowAname, columnBname, null).get(0).value,
          valueCname);
  
      assertEquals(0, handler.getRow(tableAname, rowBname, null).size());
    } finally {
      // Teardown
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  /**
   * Tests the four different scanner-opening methods (with and without
   * a stoprow, with and without a timestamp).
   *
   * @throws Exception
   */
  @Test
  public void doTestTableScanners() throws Exception {
    // Setup
    ThriftServerRunner.HBaseHandler handler =
      new ThriftServerRunner.HBaseHandler(UTIL.getConfiguration());
    handler.createTable(tableAname, getColumnDescriptors());
    
    try {
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
      //assertTrue(Bytes.equals(rowResult2a.columns.get(columnAname).value, valueAname));
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
      closeScanner(scanner4, handler);

      // Test MinTimestamp
      TScan tScan = new TScan();
      tScan.setStartRow(rowAname);
      tScan.setColumns(getColumnList(true, true));
      tScan.setMinTimestamp(time1 - 1);
      tScan.setTimestamp(time1);
      int scanner5 = handler.scannerOpenWithScan(tableAname, tScan);
      TRowResult result5 = handler.scannerGet(scanner5).get(0);
      assertBufferEquals(result5.columns.get(columnBname).value, valueBname);
      closeScanner(scanner5, handler);
    } finally {
      // Teardown
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  /**
   * For HBASE-2556
   * Tests for GetTableRegions
   *
   * @throws Exception
   */
  @Test
  public void doTestGetTableRegions() throws Exception {
    ThriftServerRunner.HBaseHandler handler =
      new ThriftServerRunner.HBaseHandler(UTIL.getConfiguration());
    doTestGetTableRegions(handler);
  }

  public static void doTestGetTableRegions(Hbase.Iface handler)
      throws Exception {
    assertEquals(handler.getTableNames().size(), 0);
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(handler.getTableNames().size(), 1);
    List<TRegionInfo> regions = handler.getTableRegions(tableAname);
    int regionCount = regions.size();
    assertEquals("empty table should have only 1 region, " +
            "but found " + regionCount, regionCount, 1);
    LOG.info("Region found:" + regions.get(0));
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
    if (handler instanceof ThriftServerRunner.HBaseHandler) {
      ((ThriftServerRunner.HBaseHandler) handler).getTable(tableAname)
        .clearRegionCache();
    }
    regionCount = handler.getTableRegions(tableAname).size();
    assertEquals("non-existing table should have 0 region, " +
            "but found " + regionCount, regionCount, 0);
  }

  @Test
  public void doTestFilterRegistration() throws Exception {
    Configuration conf = UTIL.getConfiguration();

    conf.set("hbase.thrift.filters", "MyFilter:filterclass");

    ThriftServerRunner.registerFilters(conf);

    Map<String, String> registeredFilters = ParseFilter.getAllFilters();

    assertEquals("filterclass", registeredFilters.get("MyFilter"));
  }

  @Test
  public void doTestGetRegionInfo() throws Exception {
    ThriftServerRunner.HBaseHandler handler =
      new ThriftServerRunner.HBaseHandler(UTIL.getConfiguration());
    doTestGetRegionInfo(handler);
  }

  public static void doTestGetRegionInfo(Hbase.Iface handler) throws Exception {
    // Create tableA and add two columns to rowA
    handler.createTable(tableAname, getColumnDescriptors());
    try {
      handler.mutateRow(tableAname, rowAname, getMutations(), null, null);
      byte[] searchRow = HRegionInfo.createRegionName(
          tableAname.array(), rowAname.array(), HConstants.NINES, false);
      TRegionInfo regionInfo = handler.getRegionInfo(ByteBuffer.wrap(searchRow));
      assertTrue(Bytes.toStringBinary(regionInfo.getName()).startsWith(
            Bytes.toStringBinary(tableAname)));
    } finally {
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  /**
   *
   * @return a List of ColumnDescriptors for use in creating a table.  Has one
   * default ColumnDescriptor and one ColumnDescriptor with fewer versions
   */
  static List<ColumnDescriptor> getColumnDescriptors() {
    ArrayList<ColumnDescriptor> cDescriptors = new ArrayList<ColumnDescriptor>();

    // A default ColumnDescriptor
    ColumnDescriptor cDescA = new ColumnDescriptor();
    cDescA.name = columnAname;
    cDescriptors.add(cDescA);

    // A slightly customized ColumnDescriptor (only 2 versions)
    ColumnDescriptor cDescB =
        new ColumnDescriptor(columnBname, MAX_VERSIONS, Compression.Algorithm.NONE.toString(),
            false, StoreFile.BloomType.NONE.toString(), 0, 0, false, -1);
    cDescriptors.add(cDescB);

    return cDescriptors;
  }

  /**
   *
   * @param includeA whether or not to include columnA
   * @param includeB whether or not to include columnB
   * @return a List of column names for use in retrieving a scanner
   */
  static List<ByteBuffer> getColumnList(boolean includeA, boolean includeB) {
    List<ByteBuffer> columnList = new ArrayList<ByteBuffer>();
    if (includeA) columnList.add(columnAname);
    if (includeB) columnList.add(columnBname);
    return columnList;
  }

  /**
   *
   * @return a List of Mutations for a row, with columnA having valueA
   * and columnB having valueB
   */
  static List<Mutation> getMutations() {
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, columnAname, valueAname, true, HConstants.LATEST_TIMESTAMP));
    mutations.add(new Mutation(false, columnBname, valueBname, true, HConstants.LATEST_TIMESTAMP));
    return mutations;
  }

  /**
   *
   * @return a List of BatchMutations with the following effects:
   * (rowA, columnA): delete
   * (rowA, columnB): place valueC
   * (rowB, columnA): place valueC
   * (rowB, columnB): place valueD
   */
  static List<BatchMutation> getBatchMutations() {
    List<BatchMutation> batchMutations = new ArrayList<BatchMutation>();

    // Mutations to rowA.  You can't mix delete and put anymore.
    batchMutations.add(new BatchMutation(rowAname,
        ImmutableList.of(
            new Mutation(true, columnAname, null, true, HConstants.LATEST_TIMESTAMP)
        )));

    batchMutations.add(new BatchMutation(rowAname,
        ImmutableList.of(new Mutation(false, columnBname, valueCname, true,
            HConstants.LATEST_TIMESTAMP))));

    // Mutations to rowB
    batchMutations.add(new BatchMutation(rowBname, ImmutableList.of(
        new Mutation(false, columnAname, valueCname, true, HConstants.LATEST_TIMESTAMP),
        new Mutation(false, columnBname, valueDname, true, HConstants.LATEST_TIMESTAMP)
    )));

    return batchMutations;
  }

  /**
   * Asserts that the passed scanner is exhausted, and then closes
   * the scanner.
   *
   * @param scannerId the scanner to close
   * @param handler the HBaseHandler interfacing to HBase
   * @throws Exception
   */
  static void closeScanner(
      int scannerId, ThriftServerRunner.HBaseHandler handler) throws Exception {
    handler.scannerGet(scannerId);
    handler.scannerClose(scannerId);
  }

  static void assertBufferEquals(ByteBuffer expected, ByteBuffer actual) {
    if (!expected.equals(actual)) {
      throw new AssertionError("Expected " + Bytes.toStringBinaryRemaining(expected) + ", " +
          "got " + Bytes.toStringBinaryRemaining(actual));
    }
  }

  @Test
  public void testMaxTimestamp() {
    assertEquals(HConstants.LATEST_TIMESTAMP, HbaseConstants.LATEST_TIMESTAMP);
  }
}
