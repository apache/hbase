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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the rowcounter map reduce job.
 */
@Category({MapReduceTests.class, LargeTests.class})
public class TestRowCounter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCounter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRowCounter.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String TABLE_NAME = "testRowCounter";
  private final static String TABLE_NAME_TS_RANGE = "testRowCounter_ts_range";
  private final static String COL_FAM = "col_fam";
  private final static String COL1 = "c1";
  private final static String COL2 = "c2";
  private final static String COMPOSITE_COLUMN = "C:A:A";
  private final static int TOTAL_ROWS = 10;
  private final static int ROWS_WITH_ONE_COL = 2;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    Table table = TEST_UTIL.createTable(TableName.valueOf(TABLE_NAME), Bytes.toBytes(COL_FAM));
    writeRows(table, TOTAL_ROWS, ROWS_WITH_ONE_COL);
    table.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test a case when no column was specified in command line arguments.
   *
   * @throws Exception
   */
  @Test
  public void testRowCounterNoColumn() throws Exception {
    String[] args = new String[] {
      TABLE_NAME
    };
    runRowCount(args, 10);
  }

  /**
   * Test a case when the column specified in command line arguments is
   * exclusive for few rows.
   *
   * @throws Exception
   */
  @Test
  public void testRowCounterExclusiveColumn() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, COL_FAM + ":" + COL1
    };
    runRowCount(args, 8);
  }

  /**
   * Test a case when the column specified in command line arguments is
   * one for which the qualifier contains colons.
   *
   * @throws Exception
   */
  @Test
  public void testRowCounterColumnWithColonInQualifier() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, COL_FAM + ":" + COMPOSITE_COLUMN
    };
    runRowCount(args, 8);
  }

  /**
   * Test a case when the column specified in command line arguments is not part
   * of first KV for a row.
   *
   * @throws Exception
   */
  @Test
  public void testRowCounterHiddenColumn() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, COL_FAM + ":" + COL2
    };
    runRowCount(args, 10);
  }


  /**
   * Test a case when the column specified in command line arguments is
   * exclusive for few rows and also a row range filter is specified
   *
   * @throws Exception
   */
  @Test
  public void testRowCounterColumnAndRowRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00rov,\\x00rox", COL_FAM + ":" + COL1
    };
    runRowCount(args, 8);
  }

  /**
   * Test a case when a range is specified with single range of start-end keys
   * @throws Exception
   */
  @Test
  public void testRowCounterRowSingleRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00row1,\\x00row3"
    };
    runRowCount(args, 2);
  }

  /**
   * Test a case when a range is specified with single range with end key only
   * @throws Exception
   */
  @Test
  public void testRowCounterRowSingleRangeUpperBound() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=,\\x00row3"
    };
    runRowCount(args, 3);
  }

  /**
   * Test a case when a range is specified with two ranges where one range is with end key only
   * @throws Exception
   */
  @Test
  public void testRowCounterRowMultiRangeUpperBound() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=,\\x00row3;\\x00row5,\\x00row7"
    };
    runRowCount(args, 5);
  }

  /**
   * Test a case when a range is specified with multiple ranges of start-end keys
   * @throws Exception
   */
  @Test
  public void testRowCounterRowMultiRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00row1,\\x00row3;\\x00row5,\\x00row8"
    };
    runRowCount(args, 5);
  }

  /**
   * Test a case when a range is specified with multiple ranges of start-end keys;
   * one range is filled, another two are not
   * @throws Exception
   */
  @Test
  public void testRowCounterRowMultiEmptyRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00row1,\\x00row3;;"
    };
    runRowCount(args, 2);
  }

  @Test
  public void testRowCounter10kRowRange() throws Exception {
    String tableName = TABLE_NAME + "10k";

    try (Table table = TEST_UTIL.createTable(
      TableName.valueOf(tableName), Bytes.toBytes(COL_FAM))) {
      writeRows(table, 10000, 0);
    }
    String[] args = new String[] {
      tableName, "--range=\\x00row9872,\\x00row9875"
    };
    runRowCount(args, 3);
  }

  /**
   * Test a case when the timerange is specified with --starttime and --endtime options
   *
   * @throws Exception
   */
  @Test
  public void testRowCounterTimeRange() throws Exception {
    final byte[] family = Bytes.toBytes(COL_FAM);
    final byte[] col1 = Bytes.toBytes(COL1);
    Put put1 = new Put(Bytes.toBytes("row_timerange_" + 1));
    Put put2 = new Put(Bytes.toBytes("row_timerange_" + 2));
    Put put3 = new Put(Bytes.toBytes("row_timerange_" + 3));

    long ts;

    // clean up content of TABLE_NAME
    Table table = TEST_UTIL.createTable(TableName.valueOf(TABLE_NAME_TS_RANGE), Bytes.toBytes(COL_FAM));

    ts = System.currentTimeMillis();
    put1.addColumn(family, col1, ts, Bytes.toBytes("val1"));
    table.put(put1);
    Thread.sleep(100);

    ts = System.currentTimeMillis();
    put2.addColumn(family, col1, ts, Bytes.toBytes("val2"));
    put3.addColumn(family, col1, ts, Bytes.toBytes("val3"));
    table.put(put2);
    table.put(put3);
    table.close();

    String[] args = new String[] {
      TABLE_NAME_TS_RANGE, COL_FAM + ":" + COL1,
      "--starttime=" + 0,
      "--endtime=" + ts
    };
    runRowCount(args, 1);

    args = new String[] {
      TABLE_NAME_TS_RANGE, COL_FAM + ":" + COL1,
      "--starttime=" + 0,
      "--endtime=" + (ts - 10)
    };
    runRowCount(args, 1);

    args = new String[] {
      TABLE_NAME_TS_RANGE, COL_FAM + ":" + COL1,
      "--starttime=" + ts,
      "--endtime=" + (ts + 1000)
    };
    runRowCount(args, 2);

    args = new String[] {
      TABLE_NAME_TS_RANGE, COL_FAM + ":" + COL1,
      "--starttime=" + (ts - 30 * 1000),
      "--endtime=" + (ts + 30 * 1000),
    };
    runRowCount(args, 3);
  }

  /**
   * Run the RowCounter map reduce job and verify the row count.
   *
   * @param args the command line arguments to be used for rowcounter job.
   * @param expectedCount the expected row count (result of map reduce job).
   * @throws Exception
   */
  private void runRowCount(String[] args, int expectedCount) throws Exception {
    RowCounter rowCounter = new RowCounter();
    rowCounter.setConf(TEST_UTIL.getConfiguration());
    args = Arrays.copyOf(args, args.length+1);
    args[args.length-1]="--expectedCount=" + expectedCount;
    long start = System.currentTimeMillis();
    int result = rowCounter.run(args);
    long duration = System.currentTimeMillis() - start;
    LOG.debug("row count duration (ms): " + duration);
    assertTrue(result==0);
  }

  /**
   * Run the RowCounter map reduce job and verify the row count.
   *
   * @param args the command line arguments to be used for rowcounter job.
   * @param expectedCount the expected row count (result of map reduce job).
   * @throws Exception in case of any unexpected error.
   */
  private void runCreateSubmittableJobWithArgs(String[] args, int expectedCount) throws Exception {
    Job job = RowCounter.createSubmittableJob(TEST_UTIL.getConfiguration(), args);
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long duration = System.currentTimeMillis() - start;
    LOG.debug("row count duration (ms): " + duration);
    assertTrue(job.isSuccessful());
    Counter counter = job.getCounters().findCounter(RowCounter.RowCounterMapper.Counters.ROWS);
    assertEquals(expectedCount, counter.getValue());
  }

  @Test
  public void testCreateSubmittableJobWithArgsNoColumn() throws Exception {
    String[] args = new String[] {
      TABLE_NAME
    };
    runCreateSubmittableJobWithArgs(args, 10);
  }

  /**
   * Test a case when the column specified in command line arguments is
   * exclusive for few rows.
   *
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsExclusiveColumn() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, COL_FAM + ":" + COL1
    };
    runCreateSubmittableJobWithArgs(args, 8);
  }

  /**
   * Test a case when the column specified in command line arguments is
   * one for which the qualifier contains colons.
   *
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsColumnWithColonInQualifier() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, COL_FAM + ":" + COMPOSITE_COLUMN
    };
    runCreateSubmittableJobWithArgs(args, 8);
  }

  /**
   * Test a case when the column specified in command line arguments is not part
   * of first KV for a row.
   *
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsHiddenColumn() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, COL_FAM + ":" + COL2
    };
    runCreateSubmittableJobWithArgs(args, 10);
  }


  /**
   * Test a case when the column specified in command line arguments is
   * exclusive for few rows and also a row range filter is specified
   *
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsColumnAndRowRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00rov,\\x00rox", COL_FAM + ":" + COL1
    };
    runCreateSubmittableJobWithArgs(args, 8);
  }

  /**
   * Test a case when a range is specified with single range of start-end keys
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsRowSingleRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00row1,\\x00row3"
    };
    runCreateSubmittableJobWithArgs(args, 2);
  }

  /**
   * Test a case when a range is specified with single range with end key only
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsRowSingleRangeUpperBound() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=,\\x00row3"
    };
    runCreateSubmittableJobWithArgs(args, 3);
  }

  /**
   * Test a case when a range is specified with two ranges where one range is with end key only
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsRowMultiRangeUpperBound() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=,\\x00row3;\\x00row5,\\x00row7"
    };
    runCreateSubmittableJobWithArgs(args, 5);
  }

  /**
   * Test a case when a range is specified with multiple ranges of start-end keys
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsRowMultiRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00row1,\\x00row3;\\x00row5,\\x00row8"
    };
    runCreateSubmittableJobWithArgs(args, 5);
  }

  /**
   * Test a case when a range is specified with multiple ranges of start-end keys;
   * one range is filled, another two are not
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsRowMultiEmptyRange() throws Exception {
    String[] args = new String[] {
      TABLE_NAME, "--range=\\x00row1,\\x00row3;;"
    };
    runCreateSubmittableJobWithArgs(args, 2);
  }

  @Test
  public void testCreateSubmittableJobWithArgs10kRowRange() throws Exception {
    String tableName = TABLE_NAME + "CreateSubmittableJobWithArgs10kRowRange";

    try (Table table = TEST_UTIL.createTable(
      TableName.valueOf(tableName), Bytes.toBytes(COL_FAM))) {
      writeRows(table, 10000, 0);
    }
    String[] args = new String[] {
      tableName, "--range=\\x00row9872,\\x00row9875"
    };
    runCreateSubmittableJobWithArgs(args, 3);
  }

  /**
   * Test a case when the timerange is specified with --starttime and --endtime options
   *
   * @throws Exception in case of any unexpected error.
   */
  @Test
  public void testCreateSubmittableJobWithArgsTimeRange() throws Exception {
    final byte[] family = Bytes.toBytes(COL_FAM);
    final byte[] col1 = Bytes.toBytes(COL1);
    Put put1 = new Put(Bytes.toBytes("row_timerange_" + 1));
    Put put2 = new Put(Bytes.toBytes("row_timerange_" + 2));
    Put put3 = new Put(Bytes.toBytes("row_timerange_" + 3));

    long ts;

    String tableName = TABLE_NAME_TS_RANGE+"CreateSubmittableJobWithArgs";
    // clean up content of TABLE_NAME
    Table table = TEST_UTIL.createTable(TableName.valueOf(tableName), Bytes.toBytes(COL_FAM));

    ts = System.currentTimeMillis();
    put1.addColumn(family, col1, ts, Bytes.toBytes("val1"));
    table.put(put1);
    Thread.sleep(100);

    ts = System.currentTimeMillis();
    put2.addColumn(family, col1, ts, Bytes.toBytes("val2"));
    put3.addColumn(family, col1, ts, Bytes.toBytes("val3"));
    table.put(put2);
    table.put(put3);
    table.close();

    String[] args = new String[] {
      tableName, COL_FAM + ":" + COL1,
      "--starttime=" + 0,
      "--endtime=" + ts
    };
    runCreateSubmittableJobWithArgs(args, 1);

    args = new String[] {
      tableName, COL_FAM + ":" + COL1,
      "--starttime=" + 0,
      "--endtime=" + (ts - 10)
    };
    runCreateSubmittableJobWithArgs(args, 1);

    args = new String[] {
      tableName, COL_FAM + ":" + COL1,
      "--starttime=" + ts,
      "--endtime=" + (ts + 1000)
    };
    runCreateSubmittableJobWithArgs(args, 2);

    args = new String[] {
      tableName, COL_FAM + ":" + COL1,
      "--starttime=" + (ts - 30 * 1000),
      "--endtime=" + (ts + 30 * 1000),
    };
    runCreateSubmittableJobWithArgs(args, 3);
  }

  /**
   * Writes TOTAL_ROWS number of distinct rows in to the table. Few rows have
   * two columns, Few have one.
   *
   * @param table
   * @throws IOException
   */
  private static void writeRows(Table table, int totalRows, int rowsWithOneCol) throws IOException {
    final byte[] family = Bytes.toBytes(COL_FAM);
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] col1 = Bytes.toBytes(COL1);
    final byte[] col2 = Bytes.toBytes(COL2);
    final byte[] col3 = Bytes.toBytes(COMPOSITE_COLUMN);
    ArrayList<Put> rowsUpdate = new ArrayList<>();
    // write few rows with two columns
    int i = 0;
    for (; i < totalRows - rowsWithOneCol; i++) {
      // Use binary rows values to test for HBASE-15287.
      byte[] row = Bytes.toBytesBinary("\\x00row" + i);
      Put put = new Put(row);
      put.addColumn(family, col1, value);
      put.addColumn(family, col2, value);
      put.addColumn(family, col3, value);
      rowsUpdate.add(put);
    }

    // write few rows with only one column
    for (; i < totalRows; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.addColumn(family, col2, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
  }

  /**
   * test main method. Import should print help and call System.exit
   */
  @Test
  public void testImportMain() throws Exception {
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    String[] args = {};
    try {
      try {
        RowCounter.main(args);
        fail("should be SecurityException");
      } catch (SecurityException e) {
        assertEquals(RowCounter.EXIT_FAILURE, newSecurityManager.getExitCode());
      }
      try {
        args = new String[2];
        args[0] = "table";
        args[1] = "--range=1";
        RowCounter.main(args);
        fail("should be SecurityException");
      } catch (SecurityException e) {
        assertEquals(RowCounter.EXIT_FAILURE, newSecurityManager.getExitCode());
      }

    } finally {
      System.setSecurityManager(SECURITY_MANAGER);
    }
  }

  @Test
  public void testHelp() throws Exception {
    PrintStream oldPrintStream = System.out;
    try {
      ByteArrayOutputStream data = new ByteArrayOutputStream();
      PrintStream stream = new PrintStream(data);
      System.setOut(stream);
      String[] args = {"-h"};
      runRowCount(args, 0);
      assertUsageContent(data.toString());
      args = new String[]{"--help"};
      runRowCount(args, 0);
      assertUsageContent(data.toString());
    }finally {
      System.setOut(oldPrintStream);
    }
  }

  @Test
  public void testInvalidTable() throws Exception {
    try {
      String[] args = {"invalid"};
      runRowCount(args, 0);
      fail("RowCounter should had failed with invalid table.");
    }catch (Throwable e){
      assertTrue(e instanceof AssertionError);
    }
  }

  private void assertUsageContent(String usage) {
    assertTrue(usage.contains("usage: hbase rowcounter "
      + "<tablename> [options] [<column1> <column2>...]"));
    assertTrue(usage.contains("Options:\n"));
    assertTrue(usage.contains("--starttime=<arg>       "
      + "starting time filter to start counting rows from.\n"));
    assertTrue(usage.contains("--endtime=<arg>         "
      + "end time filter limit, to only count rows up to this timestamp.\n"));
    assertTrue(usage.contains("--range=<arg>           "
      + "[startKey],[endKey][;[startKey],[endKey]...]]\n"));
    assertTrue(usage.contains("--expectedCount=<arg>   expected number of rows to be count.\n"));
    assertTrue(usage.contains("For performance, "
      + "consider the following configuration properties:\n"));
    assertTrue(usage.contains("-Dhbase.client.scanner.caching=100\n"));
    assertTrue(usage.contains("-Dmapreduce.map.speculative=false\n"));
  }

}
