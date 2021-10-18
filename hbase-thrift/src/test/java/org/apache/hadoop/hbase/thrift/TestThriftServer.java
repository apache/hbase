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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.Constants.COALESCE_INC_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.ThriftMetrics.ThriftServerType;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TAppend;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TIncrement;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.thrift.generated.TThriftServerType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit testing for ThriftServerRunner.HBaseServiceHandler, a part of the
 * org.apache.hadoop.hbase.thrift package.
 */
@Category({ClientTests.class, LargeTests.class})
public class TestThriftServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftServer.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestThriftServer.class);
  private static final MetricsAssertHelper metricsHelper = CompatibilityFactory
      .getInstance(MetricsAssertHelper.class);
  protected static final int MAXVERSIONS = 3;

  private static ByteBuffer asByteBuffer(String i) {
    return ByteBuffer.wrap(Bytes.toBytes(i));
  }
  private static ByteBuffer asByteBuffer(long l) {
    return ByteBuffer.wrap(Bytes.toBytes(l));
  }

  // Static names for tables, columns, rows, and values
  private static ByteBuffer tableAname = asByteBuffer("tableA");
  private static ByteBuffer tableBname = asByteBuffer("tableB");
  private static ByteBuffer columnAname = asByteBuffer("columnA:");
  private static ByteBuffer columnAAname = asByteBuffer("columnA:A");
  private static ByteBuffer columnBname = asByteBuffer("columnB:");
  private static ByteBuffer rowAname = asByteBuffer("rowA");
  private static ByteBuffer rowBname = asByteBuffer("rowB");
  private static ByteBuffer valueAname = asByteBuffer("valueA");
  private static ByteBuffer valueBname = asByteBuffer("valueB");
  private static ByteBuffer valueCname = asByteBuffer("valueC");
  private static ByteBuffer valueDname = asByteBuffer("valueD");
  private static ByteBuffer valueEname = asByteBuffer(100l);

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean(COALESCE_INC_KEY, true);
    UTIL.getConfiguration().setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, false);
    UTIL.getConfiguration().setInt("hbase.client.retries.number", 3);
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
  @Test
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
    doTestIncrements();
    doTestAppend();
    doTestCheckAndPut();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.  Also
   * tests that creating a table with an invalid column name yields an
   * IllegalArgument exception.
   *
   * @throws Exception
   */
  public void doTestTableCreateDrop() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    doTestTableCreateDrop(handler);
  }

  public static void doTestTableCreateDrop(Hbase.Iface handler) throws Exception {
    createTestTables(handler);
    dropTestTables(handler);
  }

  public static final class MySlowHBaseHandler extends ThriftHBaseServiceHandler
      implements Hbase.Iface {

    protected MySlowHBaseHandler(Configuration c)
        throws IOException {
      super(c, UserProvider.instantiate(c));
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError {
      Threads.sleepWithoutInterrupt(3000);
      return super.getTableNames();
    }
  }

  /**
   * TODO: These counts are supposed to be zero but sometimes they are not, they are equal to the
   * passed in maybe.  Investigate why.  My guess is they are set by the test that runs just
   * previous to this one.  Sometimes they are cleared.  Sometimes not.
   * @param name
   * @param maybe
   * @param metrics
   * @return
   */
  private int getCurrentCount(final String name, final int maybe, final ThriftMetrics metrics) {
    int currentCount = 0;
    try {
      metricsHelper.assertCounter(name, maybe, metrics.getSource());
      LOG.info("Shouldn't this be null? name=" + name + ", equals=" + maybe);
      currentCount = maybe;
    } catch (AssertionError e) {
      // Ignore
    }
    return currentCount;
  }

  /**
   * Tests if the metrics for thrift handler work correctly
   */
  public void doTestThriftMetrics() throws Exception {
    LOG.info("START doTestThriftMetrics");
    Configuration conf = UTIL.getConfiguration();
    ThriftMetrics metrics = getMetrics(conf);
    Hbase.Iface handler = getHandlerForMetricsTest(metrics, conf);
    int currentCountCreateTable = getCurrentCount("createTable_num_ops", 2, metrics);
    int currentCountDeleteTable = getCurrentCount("deleteTable_num_ops", 2, metrics);
    int currentCountDisableTable = getCurrentCount("disableTable_num_ops", 2, metrics);
    createTestTables(handler);
    dropTestTables(handler);;
    metricsHelper.assertCounter("createTable_num_ops", currentCountCreateTable + 2,
      metrics.getSource());
    metricsHelper.assertCounter("deleteTable_num_ops", currentCountDeleteTable + 2,
      metrics.getSource());
    metricsHelper.assertCounter("disableTable_num_ops", currentCountDisableTable + 2,
      metrics.getSource());
    handler.getTableNames(); // This will have an artificial delay.

    // 3 to 6 seconds (to account for potential slowness), measured in nanoseconds
   try {
     metricsHelper.assertGaugeGt("getTableNames_avg_time", 3L * 1000 * 1000 * 1000, metrics.getSource());
     metricsHelper.assertGaugeLt("getTableNames_avg_time",6L * 1000 * 1000 * 1000, metrics.getSource());
   } catch (AssertionError e) {
     LOG.info("Fix me!  Why does this happen?  A concurrent cluster running?", e);
   }
  }

  private static Hbase.Iface getHandlerForMetricsTest(ThriftMetrics metrics, Configuration conf)
      throws Exception {
    Hbase.Iface handler = new MySlowHBaseHandler(conf);
    return HbaseHandlerMetricsProxy.newInstance((ThriftHBaseServiceHandler)handler, metrics, conf);
  }

  private static ThriftMetrics getMetrics(Configuration conf) throws Exception {
    return new ThriftMetrics( conf, ThriftMetrics.ThriftServerType.ONE);
  }


  public static void createTestTables(Hbase.Iface handler) throws Exception {
    // Create/enable/disable/delete tables, ensure methods act correctly
    List<java.nio.ByteBuffer> bbs = handler.getTableNames();
    assertEquals(bbs.stream().map(b -> Bytes.toString(b.array())).
      collect(Collectors.joining(",")), 0, bbs.size());
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(1, handler.getTableNames().size());
    assertEquals(2, handler.getColumnDescriptors(tableAname).size());
    assertTrue(handler.isTableEnabled(tableAname));
    handler.createTable(tableBname, getColumnDescriptors());
    assertEquals(2, handler.getTableNames().size());
  }

  public static void checkTableList(Hbase.Iface handler) throws Exception {
    assertTrue(handler.getTableNames().contains(tableAname));
  }

  public static void dropTestTables(Hbase.Iface handler) throws Exception {
    handler.disableTable(tableBname);
    assertFalse(handler.isTableEnabled(tableBname));
    handler.deleteTable(tableBname);
    assertEquals(1, handler.getTableNames().size());
    handler.disableTable(tableAname);
    assertFalse(handler.isTableEnabled(tableAname));
    /* TODO Reenable.
    assertFalse(handler.isTableEnabled(tableAname));
    handler.enableTable(tableAname);
    assertTrue(handler.isTableEnabled(tableAname));
    handler.disableTable(tableAname);*/
    handler.deleteTable(tableAname);
    assertEquals(0, handler.getTableNames().size());
  }

  public void doTestIncrements() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    createTestTables(handler);
    doTestIncrements(handler);
    dropTestTables(handler);
  }

  public static void doTestIncrements(ThriftHBaseServiceHandler handler) throws Exception {
    List<Mutation> mutations = new ArrayList<>(1);
    mutations.add(new Mutation(false, columnAAname, valueEname, true));
    mutations.add(new Mutation(false, columnAname, valueEname, true));
    handler.mutateRow(tableAname, rowAname, mutations, null);
    handler.mutateRow(tableAname, rowBname, mutations, null);

    List<TIncrement> increments = new ArrayList<>(3);
    increments.add(new TIncrement(tableAname, rowBname, columnAAname, 7));
    increments.add(new TIncrement(tableAname, rowBname, columnAAname, 7));
    increments.add(new TIncrement(tableAname, rowBname, columnAAname, 7));

    int numIncrements = 60000;
    for (int i = 0; i < numIncrements; i++) {
      handler.increment(new TIncrement(tableAname, rowAname, columnAname, 2));
      handler.incrementRows(increments);
    }

    Thread.sleep(1000);
    long lv = handler.get(tableAname, rowAname, columnAname, null).get(0).value.getLong();
    // Wait on all increments being flushed
    while (handler.coalescer.getQueueSize() != 0) Threads.sleep(10);
    assertEquals((100 + (2 * numIncrements)), lv );


    lv = handler.get(tableAname, rowBname, columnAAname, null).get(0).value.getLong();
    assertEquals((100 + (3 * 7 * numIncrements)), lv);

    assertTrue(handler.coalescer.getSuccessfulCoalescings() > 0);

  }

  /**
   * Tests adding a series of Mutations and BatchMutations, including a
   * delete mutation.  Also tests data retrieval, and getting back multiple
   * versions.
   *
   * @throws Exception
   */
  public void doTestTableMutations() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    doTestTableMutations(handler);
  }

  public static void doTestTableMutations(Hbase.Iface handler) throws Exception {
    // Setup
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply a few Mutations to rowA
    //     mutations.add(new Mutation(false, columnAname, valueAname));
    //     mutations.add(new Mutation(false, columnBname, valueBname));
    handler.mutateRow(tableAname, rowAname, getMutations(), null);

    // Assert that the changes were made
    assertEquals(valueAname,
      handler.get(tableAname, rowAname, columnAname, null).get(0).value);
    TRowResult rowResult1 = handler.getRow(tableAname, rowAname, null).get(0);
    assertEquals(rowAname, rowResult1.row);
    assertEquals(valueBname,
      rowResult1.columns.get(columnBname).value);

    // Apply a few BatchMutations for rowA and rowB
    // rowAmutations.add(new Mutation(true, columnAname, null));
    // rowAmutations.add(new Mutation(false, columnBname, valueCname));
    // batchMutations.add(new BatchMutation(rowAname, rowAmutations));
    // Mutations to rowB
    // rowBmutations.add(new Mutation(false, columnAname, valueCname));
    // rowBmutations.add(new Mutation(false, columnBname, valueDname));
    // batchMutations.add(new BatchMutation(rowBname, rowBmutations));
    handler.mutateRows(tableAname, getBatchMutations(), null);

    // Assert that changes were made to rowA
    List<TCell> cells = handler.get(tableAname, rowAname, columnAname, null);
    assertFalse(cells.size() > 0);
    assertEquals(valueCname, handler.get(tableAname, rowAname, columnBname, null).get(0).value);
    List<TCell> versions = handler.getVer(tableAname, rowAname, columnBname, MAXVERSIONS, null);
    assertEquals(valueCname, versions.get(0).value);
    assertEquals(valueBname, versions.get(1).value);

    // Assert that changes were made to rowB
    TRowResult rowResult2 = handler.getRow(tableAname, rowBname, null).get(0);
    assertEquals(rowBname, rowResult2.row);
    assertEquals(valueCname, rowResult2.columns.get(columnAname).value);
    assertEquals(valueDname, rowResult2.columns.get(columnBname).value);

    // Apply some deletes
    handler.deleteAll(tableAname, rowAname, columnBname, null);
    handler.deleteAllRow(tableAname, rowBname, null);

    // Assert that the deletes were applied
    int size = handler.get(tableAname, rowAname, columnBname, null).size();
    assertEquals(0, size);
    size = handler.getRow(tableAname, rowBname, null).size();
    assertEquals(0, size);

    // Try null mutation
    List<Mutation> mutations = new ArrayList<>(1);
    mutations.add(new Mutation(false, columnAname, null, true));
    handler.mutateRow(tableAname, rowAname, mutations, null);
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
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1, null);

    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2, null);

    // Apply an overlapping timestamped mutation to rowB
    handler.mutateRowTs(tableAname, rowBname, getMutations(), time2, null);

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
    assertEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertEquals(rowResult2.columns.get(columnBname).value, valueCname);

    // ColumnAname has been deleted, and will never be visible even with a getRowTs()
    assertFalse(rowResult2.columns.containsKey(columnAname));

    List<ByteBuffer> columns = new ArrayList<>(1);
    columns.add(columnBname);

    rowResult1 = handler.getRowWithColumns(tableAname, rowAname, columns, null).get(0);
    assertEquals(rowResult1.columns.get(columnBname).value, valueCname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    rowResult1 = handler.getRowWithColumnsTs(tableAname, rowAname, columns, time1, null).get(0);
    assertEquals(rowResult1.columns.get(columnBname).value, valueBname);
    assertFalse(rowResult1.columns.containsKey(columnAname));

    // Apply some timestamped deletes
    // this actually deletes _everything_.
    // nukes everything in columnB: forever.
    handler.deleteAllTs(tableAname, rowAname, columnBname, time1, null);
    handler.deleteAllRowTs(tableAname, rowBname, time2, null);

    // Assert that the timestamp-related methods retrieve the correct data
    int size = handler.getVerTs(tableAname, rowAname, columnBname, time1, MAXVERSIONS, null).size();
    assertEquals(0, size);

    size = handler.getVerTs(tableAname, rowAname, columnBname, time2, MAXVERSIONS, null).size();
    assertEquals(1, size);

    // should be available....
    assertEquals(handler.get(tableAname, rowAname, columnBname, null).get(0).value, valueCname);

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
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    handler.createTable(tableAname, getColumnDescriptors());

    // Apply timestamped Mutations to rowA
    long time1 = System.currentTimeMillis();
    handler.mutateRowTs(tableAname, rowAname, getMutations(), time1, null);

    // Sleep to assure that 'time1' and 'time2' will be different even with a
    // coarse grained system timer.
    Thread.sleep(1000);

    // Apply timestamped BatchMutations for rowA and rowB
    long time2 = System.currentTimeMillis();
    handler.mutateRowsTs(tableAname, getBatchMutations(), time2, null);

    time1 += 1;

    // Test a scanner on all rows and all columns, no timestamp
    int scanner1 = handler.scannerOpen(tableAname, rowAname, getColumnList(true, true), null);
    TRowResult rowResult1a = handler.scannerGet(scanner1).get(0);
    assertEquals(rowResult1a.row, rowAname);
    // This used to be '1'.  I don't know why when we are asking for two columns
    // and when the mutations above would seem to add two columns to the row.
    // -- St.Ack 05/12/2009
    assertEquals(1, rowResult1a.columns.size());
    assertEquals(rowResult1a.columns.get(columnBname).value, valueCname);

    TRowResult rowResult1b = handler.scannerGet(scanner1).get(0);
    assertEquals(rowResult1b.row, rowBname);
    assertEquals(2, rowResult1b.columns.size());
    assertEquals(rowResult1b.columns.get(columnAname).value, valueCname);
    assertEquals(rowResult1b.columns.get(columnBname).value, valueDname);
    closeScanner(scanner1, handler);

    // Test a scanner on all rows and all columns, with timestamp
    int scanner2 = handler.scannerOpenTs(tableAname, rowAname, getColumnList(true, true), time1, null);
    TRowResult rowResult2a = handler.scannerGet(scanner2).get(0);
    assertEquals(1, rowResult2a.columns.size());
    // column A deleted, does not exist.
    //assertTrue(Bytes.equals(rowResult2a.columns.get(columnAname).value, valueAname));
    assertEquals(rowResult2a.columns.get(columnBname).value, valueBname);
    closeScanner(scanner2, handler);

    // Test a scanner on the first row and first column only, no timestamp
    int scanner3 = handler.scannerOpenWithStop(tableAname, rowAname, rowBname,
        getColumnList(true, false), null);
    closeScanner(scanner3, handler);

    // Test a scanner on the first row and second column only, with timestamp
    int scanner4 = handler.scannerOpenWithStopTs(tableAname, rowAname, rowBname,
        getColumnList(false, true), time1, null);
    TRowResult rowResult4a = handler.scannerGet(scanner4).get(0);
    assertEquals(1, rowResult4a.columns.size());
    assertEquals(rowResult4a.columns.get(columnBname).value, valueBname);

    // Test scanner using a TScan object once with sortColumns False and once with sortColumns true
    TScan scanNoSortColumns = new TScan();
    scanNoSortColumns.setStartRow(rowAname);
    scanNoSortColumns.setStopRow(rowBname);

    int scanner5 = handler.scannerOpenWithScan(tableAname , scanNoSortColumns, null);
    TRowResult rowResult5 = handler.scannerGet(scanner5).get(0);
    assertEquals(1, rowResult5.columns.size());
    assertEquals(rowResult5.columns.get(columnBname).value, valueCname);

    TScan scanSortColumns = new TScan();
    scanSortColumns.setStartRow(rowAname);
    scanSortColumns.setStopRow(rowBname);
    scanSortColumns = scanSortColumns.setSortColumns(true);

    int scanner6 = handler.scannerOpenWithScan(tableAname ,scanSortColumns, null);
    TRowResult rowResult6 = handler.scannerGet(scanner6).get(0);
    assertEquals(1, rowResult6.sortedColumns.size());
    assertEquals(rowResult6.sortedColumns.get(0).getCell().value, valueCname);

    List<Mutation> rowBmutations = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      rowBmutations.add(new Mutation(false, asByteBuffer("columnA:" + i), valueCname, true));
    }
    ByteBuffer rowC = asByteBuffer("rowC");
    handler.mutateRow(tableAname, rowC, rowBmutations, null);

    TScan scanSortMultiColumns = new TScan();
    scanSortMultiColumns.setStartRow(rowC);
    scanSortMultiColumns = scanSortMultiColumns.setSortColumns(true);
    int scanner7 = handler.scannerOpenWithScan(tableAname, scanSortMultiColumns, null);
    TRowResult rowResult7 = handler.scannerGet(scanner7).get(0);

    ByteBuffer smallerColumn = asByteBuffer("columnA:");
    for (int i = 0; i < 20; i++) {
      ByteBuffer currentColumn = rowResult7.sortedColumns.get(i).columnName;
      assertTrue(Bytes.compareTo(smallerColumn.array(), currentColumn.array()) < 0);
      smallerColumn = currentColumn;
    }

    TScan reversedScan = new TScan();
    reversedScan.setReversed(true);
    reversedScan.setStartRow(rowBname);
    reversedScan.setStopRow(rowAname);

    int scanner8 = handler.scannerOpenWithScan(tableAname , reversedScan, null);
    List<TRowResult> results = handler.scannerGet(scanner8);
    handler.scannerClose(scanner8);
    assertEquals(1, results.size());
    assertEquals(ByteBuffer.wrap(results.get(0).getRow()), rowBname);

    // Teardown
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
  }

  /**
   * For HBASE-2556
   * Tests for GetTableRegions
   *
   * @throws Exception
   */
  public void doTestGetTableRegions() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    doTestGetTableRegions(handler);
  }

  public static void doTestGetTableRegions(Hbase.Iface handler)
      throws Exception {
    assertEquals(0, handler.getTableNames().size());
    handler.createTable(tableAname, getColumnDescriptors());
    assertEquals(1, handler.getTableNames().size());
    List<TRegionInfo> regions = handler.getTableRegions(tableAname);
    int regionCount = regions.size();
    assertEquals("empty table should have only 1 region, " +
            "but found " + regionCount, 1, regionCount);
    LOG.info("Region found:" + regions.get(0));
    handler.disableTable(tableAname);
    handler.deleteTable(tableAname);
    regionCount = handler.getTableRegions(tableAname).size();
    assertEquals("non-existing table should have 0 region, " +
            "but found " + regionCount, 0, regionCount);
  }

  public void doTestFilterRegistration() throws Exception {
    Configuration conf = UTIL.getConfiguration();

    conf.set("hbase.thrift.filters", "MyFilter:filterclass");

    ThriftServer.registerFilters(conf);

    Map<String, String> registeredFilters = ParseFilter.getAllFilters();

    assertEquals("filterclass", registeredFilters.get("MyFilter"));
  }

  public void doTestGetRegionInfo() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    doTestGetRegionInfo(handler);
  }

  public static void doTestGetRegionInfo(Hbase.Iface handler) throws Exception {
    // Create tableA and add two columns to rowA
    handler.createTable(tableAname, getColumnDescriptors());
    try {
      handler.mutateRow(tableAname, rowAname, getMutations(), null);
      byte[] searchRow = HRegionInfo.createRegionName(
          TableName.valueOf(tableAname.array()), rowAname.array(),
          HConstants.NINES, false);
      TRegionInfo regionInfo = handler.getRegionInfo(ByteBuffer.wrap(searchRow));
      assertTrue(Bytes.toStringBinary(regionInfo.getName()).startsWith(
            Bytes.toStringBinary(tableAname)));
    } finally {
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  /**
   * Appends the value to a cell and checks that the cell value is updated properly.
   *
   * @throws Exception
   */
  public static void doTestAppend() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    handler.createTable(tableAname, getColumnDescriptors());
    try {
      List<Mutation> mutations = new ArrayList<>(1);
      mutations.add(new Mutation(false, columnAname, valueAname, true));
      handler.mutateRow(tableAname, rowAname, mutations, null);

      List<ByteBuffer> columnList = new ArrayList<>(1);
      columnList.add(columnAname);
      List<ByteBuffer> valueList = new ArrayList<>(1);
      valueList.add(valueBname);

      TAppend append = new TAppend(tableAname, rowAname, columnList, valueList);
      handler.append(append);

      TRowResult rowResult = handler.getRow(tableAname, rowAname, null).get(0);
      assertEquals(rowAname, rowResult.row);
      assertArrayEquals(Bytes.add(valueAname.array(), valueBname.array()),
        rowResult.columns.get(columnAname).value.array());
    } finally {
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  /**
   * Check that checkAndPut fails if the cell does not exist, then put in the cell, then check that
   * the checkAndPut succeeds.
   *
   * @throws Exception
   */
  public static void doTestCheckAndPut() throws Exception {
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    handler.createTable(tableAname, getColumnDescriptors());
    try {
      List<Mutation> mutations = new ArrayList<>(1);
      mutations.add(new Mutation(false, columnAname, valueAname, true));
      Mutation putB = (new Mutation(false, columnBname, valueBname, true));

      assertFalse(handler.checkAndPut(tableAname, rowAname, columnAname, valueAname, putB, null));

      handler.mutateRow(tableAname, rowAname, mutations, null);

      assertTrue(handler.checkAndPut(tableAname, rowAname, columnAname, valueAname, putB, null));

      TRowResult rowResult = handler.getRow(tableAname, rowAname, null).get(0);
      assertEquals(rowAname, rowResult.row);
      assertEquals(valueBname, rowResult.columns.get(columnBname).value);
    } finally {
      handler.disableTable(tableAname);
      handler.deleteTable(tableAname);
    }
  }

  @Test
  public void testGetTableNamesWithStatus() throws Exception{
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));

    createTestTables(handler);

    assertEquals(2, handler.getTableNamesWithIsTableEnabled().size());
    assertEquals(2, countTablesByStatus(true, handler));
    handler.disableTable(tableBname);
    assertEquals(1, countTablesByStatus(true, handler));
    assertEquals(1, countTablesByStatus(false, handler));
    assertEquals(2, handler.getTableNamesWithIsTableEnabled().size());
    handler.enableTable(tableBname);
    assertEquals(2, countTablesByStatus(true, handler));

    dropTestTables(handler);
  }

  private static int countTablesByStatus(Boolean isEnabled, Hbase.Iface handler) throws Exception {
    AtomicInteger counter = new AtomicInteger(0);
    handler.getTableNamesWithIsTableEnabled().forEach(
      (table, tableStatus) -> {
        if (tableStatus.equals(isEnabled)) counter.getAndIncrement();
      });
    return counter.get();
  }

  @Test
  public void testMetricsWithException() throws Exception {
    String rowkey = "row1";
    String family = "f";
    String col = "c";
    // create a table which will throw exceptions for requests
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      tableDesc.addCoprocessor(ErrorThrowingGetObserver.class.getName());
      tableDesc.addFamily(new HColumnDescriptor(family));

      Table table = UTIL.createTable(tableDesc, null);
      long now = System.currentTimeMillis();
      table.put(new Put(Bytes.toBytes(rowkey))
        .addColumn(Bytes.toBytes(family), Bytes.toBytes(col), now, Bytes.toBytes("val1")));

      Configuration conf = UTIL.getConfiguration();
      ThriftMetrics metrics = getMetrics(conf);
      ThriftHBaseServiceHandler hbaseHandler =
        new ThriftHBaseServiceHandler(UTIL.getConfiguration(), UserProvider.instantiate(UTIL.getConfiguration()));
      Hbase.Iface handler = HbaseHandlerMetricsProxy.newInstance(hbaseHandler, metrics, conf);

      ByteBuffer tTableName = asByteBuffer(tableName.getNameAsString());

      // check metrics increment with a successful get
      long preGetCounter = metricsHelper.checkCounterExists("getRow_num_ops", metrics.getSource()) ?
        metricsHelper.getCounter("getRow_num_ops", metrics.getSource()) :
        0;
      List<TRowResult> tRowResult = handler.getRow(tTableName, asByteBuffer(rowkey), null);
      assertEquals(1, tRowResult.size());
      TRowResult tResult = tRowResult.get(0);

      TCell expectedColumnValue = new TCell(asByteBuffer("val1"), now);

      assertArrayEquals(Bytes.toBytes(rowkey), tResult.getRow());
      Collection<TCell> returnedColumnValues = tResult.getColumns().values();
      assertEquals(1, returnedColumnValues.size());
      assertEquals(expectedColumnValue, returnedColumnValues.iterator().next());

      metricsHelper.assertCounter("getRow_num_ops", preGetCounter + 1, metrics.getSource());

      // check metrics increment when the get throws each exception type
      for (ErrorThrowingGetObserver.ErrorType type : ErrorThrowingGetObserver.ErrorType.values()) {
        testExceptionType(handler, metrics, tTableName, rowkey, type);
      }
    } finally {
      UTIL.deleteTable(tableName);
    }
  }

  private void testExceptionType(Hbase.Iface handler, ThriftMetrics metrics,
                                 ByteBuffer tTableName, String rowkey,
                                 ErrorThrowingGetObserver.ErrorType errorType) throws Exception {
    long preGetCounter = metricsHelper.getCounter("getRow_num_ops", metrics.getSource());
    String exceptionKey = errorType.getMetricName();
    long preExceptionCounter = metricsHelper.checkCounterExists(exceptionKey, metrics.getSource()) ?
        metricsHelper.getCounter(exceptionKey, metrics.getSource()) :
        0;
    Map<ByteBuffer, ByteBuffer> attributes = new HashMap<>();
    attributes.put(asByteBuffer(ErrorThrowingGetObserver.SHOULD_ERROR_ATTRIBUTE),
        asByteBuffer(errorType.name()));
    try {
      List<TRowResult> tRowResult = handler.getRow(tTableName, asByteBuffer(rowkey), attributes);
      fail("Get with error attribute should have thrown an exception");
    } catch (IOError e) {
      LOG.info("Received exception: ", e);
      metricsHelper.assertCounter("getRow_num_ops", preGetCounter + 1, metrics.getSource());
      metricsHelper.assertCounter(exceptionKey, preExceptionCounter + 1, metrics.getSource());
    }
  }

  /**
   *
   * @return a List of ColumnDescriptors for use in creating a table.  Has one
   * default ColumnDescriptor and one ColumnDescriptor with fewer versions
   */
  private static List<ColumnDescriptor> getColumnDescriptors() {
    ArrayList<ColumnDescriptor> cDescriptors = new ArrayList<>(2);

    // A default ColumnDescriptor
    ColumnDescriptor cDescA = new ColumnDescriptor();
    cDescA.name = columnAname;
    cDescriptors.add(cDescA);

    // A slightly customized ColumnDescriptor (only 2 versions)
    ColumnDescriptor cDescB = new ColumnDescriptor(columnBname, 2, "NONE",
        false, "NONE", 0, 0, false, -1);
    cDescriptors.add(cDescB);

    return cDescriptors;
  }

  /**
   *
   * @param includeA whether or not to include columnA
   * @param includeB whether or not to include columnB
   * @return a List of column names for use in retrieving a scanner
   */
  private List<ByteBuffer> getColumnList(boolean includeA, boolean includeB) {
    List<ByteBuffer> columnList = new ArrayList<>();
    if (includeA) columnList.add(columnAname);
    if (includeB) columnList.add(columnBname);
    return columnList;
  }

  /**
   *
   * @return a List of Mutations for a row, with columnA having valueA
   * and columnB having valueB
   */
  private static List<Mutation> getMutations() {
    List<Mutation> mutations = new ArrayList<>(2);
    mutations.add(new Mutation(false, columnAname, valueAname, true));
    mutations.add(new Mutation(false, columnBname, valueBname, true));
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
  private static List<BatchMutation> getBatchMutations() {
    List<BatchMutation> batchMutations = new ArrayList<>(3);

    // Mutations to rowA.  You can't mix delete and put anymore.
    List<Mutation> rowAmutations = new ArrayList<>(1);
    rowAmutations.add(new Mutation(true, columnAname, null, true));
    batchMutations.add(new BatchMutation(rowAname, rowAmutations));

    rowAmutations = new ArrayList<>(1);
    rowAmutations.add(new Mutation(false, columnBname, valueCname, true));
    batchMutations.add(new BatchMutation(rowAname, rowAmutations));

    // Mutations to rowB
    List<Mutation> rowBmutations = new ArrayList<>(2);
    rowBmutations.add(new Mutation(false, columnAname, valueCname, true));
    rowBmutations.add(new Mutation(false, columnBname, valueDname, true));
    batchMutations.add(new BatchMutation(rowBname, rowBmutations));

    return batchMutations;
  }

  /**
   * Asserts that the passed scanner is exhausted, and then closes
   * the scanner.
   *
   * @param scannerId the scanner to close
   * @param handler the HBaseServiceHandler interfacing to HBase
   * @throws Exception
   */
  private void closeScanner(
      int scannerId, ThriftHBaseServiceHandler handler) throws Exception {
    handler.scannerGet(scannerId);
    handler.scannerClose(scannerId);
  }

  @Test
  public void testGetThriftServerType() throws Exception {
    ThriftHBaseServiceHandler handler =
        new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
            UserProvider.instantiate(UTIL.getConfiguration()));
    assertEquals(TThriftServerType.ONE, handler.getThriftServerType());
  }

  /**
   * Verify that thrift client calling thrift2 server can get the thrift2 server type correctly.
   */
  @Test
  public void testGetThriftServerOneType() throws Exception {
    // start a thrift2 server
    HBaseThriftTestingUtility THRIFT_TEST_UTIL = new HBaseThriftTestingUtility();

    LOG.info("Starting HBase Thrift Server Two");
    THRIFT_TEST_UTIL.startThriftServer(UTIL.getConfiguration(), ThriftServerType.TWO);
    try (TTransport transport = new TSocket(InetAddress.getLocalHost().getHostName(),
        THRIFT_TEST_UTIL.getServerPort())){
      TProtocol protocol = new TBinaryProtocol(transport);
      // This is our thrift client.
      Hbase.Client client = new Hbase.Client(protocol);
      // open the transport
      transport.open();
      assertEquals(TThriftServerType.TWO.name(), client.getThriftServerType().name());
    } finally {
      THRIFT_TEST_UTIL.stopThriftServer();
    }
  }
}
