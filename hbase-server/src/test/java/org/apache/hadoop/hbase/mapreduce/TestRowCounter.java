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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TestAdmin;
import org.apache.hadoop.hbase.mapreduce.RowCounter.RowCounterMapper;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the rowcounter map reduce job.
 */
@Category(MediumTests.class)
public class TestRowCounter {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String TABLE_NAME = "testRowCounter";
  private final static String COL_FAM = "col_fam";
  private final static String COL1 = "c1";
  private final static String COL2 = "c2";
  private final static int NUM_ONLINE_CHANGES = 4;
  private final static int TOTAL_ROWS = 100;
  private final static int ROWS_WITH_ONE_COL = 20;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.startMiniMapReduceCluster();
    HTable table = TEST_UTIL.createTable(Bytes.toBytes(TABLE_NAME),
        Bytes.toBytes(COL_FAM));
    writeRows(table);
    table.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
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
    runRowCount(args, TOTAL_ROWS);
  }

  @Test
  public void testRowCounterWithOnlineSchemaChange() throws Exception {

    String[] args = new String[] { TABLE_NAME };
    final TableName tableName = TableName.valueOf(TABLE_NAME);
    HTableDescriptor htd = TEST_UTIL.getHBaseAdmin().getTableDescriptor(
        tableName);
    final int INITAL_MAX_VERSIONS = htd.getFamilies().iterator().next()
        .getMaxVersions();
    final int EXPECTED_NUM_REGIONS = TEST_UTIL.getHBaseAdmin()
        .getTableRegions(tableName).size();

    runRowCounterWithOnlineSchemaChange(args, TOTAL_ROWS);
    final int FINAL_MAX_VERSIONS = TestAdmin.waitForColumnSchemasToSettle(
        TEST_UTIL.getMiniHBaseCluster(), tableName, EXPECTED_NUM_REGIONS)
        .getMaxVersions();
    assertEquals(
        "There was a mismatch in the number of online schema modifications that were created",
        FINAL_MAX_VERSIONS, INITAL_MAX_VERSIONS + NUM_ONLINE_CHANGES);

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
    runRowCount(args, TOTAL_ROWS - ROWS_WITH_ONE_COL);
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
    runRowCount(args, TOTAL_ROWS);
  }

  /**
   * Run the RowCounter map reduce job and verify the row count.
   * 
   * @param args the command line arguments to be used for rowcounter job.
   * @param expectedCount the expected row count (result of map reduce job).
   * @throws Exception
   */
  private void runRowCount(String[] args, int expectedCount) throws Exception {
    GenericOptionsParser opts = new GenericOptionsParser(
        TEST_UTIL.getConfiguration(), args);
    Configuration conf = opts.getConfiguration();
    args = opts.getRemainingArgs();
    Job job = RowCounter.createSubmittableJob(conf, args);
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    Counter counter = job.getCounters().findCounter(
        RowCounterMapper.Counters.ROWS);
    assertEquals(expectedCount, counter.getValue());
  }

  private void runRowCounterWithOnlineSchemaChange(String[] args,
      int expectedCount) throws Exception {

    GenericOptionsParser opts = new GenericOptionsParser(
        TEST_UTIL.getConfiguration(), args);
    Configuration conf = opts.getConfiguration();
    args = opts.getRemainingArgs();
    Job job = RowCounter.createSubmittableJob(conf, args);

    // This is where we'd want to start a background operation to make change on
    // the table

    BackgroundSchemaChangeThread schemaChangeThread = new BackgroundSchemaChangeThread(
        TEST_UTIL.getHBaseAdmin(), TableName.valueOf(TABLE_NAME),
        NUM_ONLINE_CHANGES);
    schemaChangeThread.start();

    job.waitForCompletion(true);
    String trackingURL = job.getHistoryUrl();
    String trackingURL2 = job.getTrackingURL();
    System.out.println("Tracking URL is: " + trackingURL2);
    schemaChangeThread.join();
    // this is where we'd have the thread returning

   //might be a timing issue - if it takes too long, then that service is just down. stupid.
    //it might also be an issue of asking for the tracking url. that may kill the history server (nope. it's a time thing).

    assertTrue(job.isSuccessful());
    Counter counter = job.getCounters().findCounter(
        RowCounterMapper.Counters.ROWS);
    assertEquals(expectedCount, counter.getValue());
  }

  /**
   * Writes TOTAL_ROWS number of distinct rows in to the table. Few rows have
   * two columns, Few have one.
   *
   * @param table
   * @throws IOException
   */
  private static void writeRows(HTable table) throws IOException {
    final byte[] family = Bytes.toBytes(COL_FAM);
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] col1 = Bytes.toBytes(COL1);
    final byte[] col2 = Bytes.toBytes(COL2);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    // write few rows with two columns
    int i = 0;
    for (; i < TOTAL_ROWS - ROWS_WITH_ONE_COL; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(family, col1, value);
      put.add(family, col2, value);
      rowsUpdate.add(put);
    }

    // write few rows with only one column
    for (; i < TOTAL_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(family, col2, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
  }

  /**
   * test main method. Import should print help and call System.exit
   */
  @Test
  public void testImportMain() throws Exception {
    PrintStream oldPrintStream = System.err;
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    String[] args = {};
    System.setErr(new PrintStream(data));
    try {
      System.setErr(new PrintStream(data));

      try {
        RowCounter.main(args);
        fail("should be SecurityException");
      } catch (SecurityException e) {
        assertEquals(-1, newSecurityManager.getExitCode());
        assertTrue(data.toString().contains("Wrong number of parameters:"));
        assertTrue(data.toString().contains(
            "Usage: RowCounter [options] <tablename> [--range=[startKey],[endKey]] " +
            "[<column1> <column2>...]"));
        assertTrue(data.toString().contains("-Dhbase.client.scanner.caching=100"));
        assertTrue(data.toString().contains("-Dmapred.map.tasks.speculative.execution=false"));
      }
      data.reset();
      try {
        args = new String[2];
        args[0] = "table";
        args[1] = "--range=1";
        RowCounter.main(args);
        fail("should be SecurityException");
      } catch (SecurityException e) {
        assertEquals(-1, newSecurityManager.getExitCode());
        assertTrue(data.toString().contains(
            "Please specify range in such format as \"--range=a,b\" or, with only one boundary," +
            " \"--range=,b\" or \"--range=a,\""));
        assertTrue(data.toString().contains(
            "Usage: RowCounter [options] <tablename> [--range=[startKey],[endKey]] " +
            "[<column1> <column2>...]"));
      }

    } finally {
      System.setErr(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }

  }

  public class BackgroundSchemaChangeThread extends Thread {
    private int numOnlineChanges;
    HBaseAdmin admin;
    TableName tableName;

    public BackgroundSchemaChangeThread(HBaseAdmin admin, TableName tableName,
        int numOnlineChanges) throws IOException {
      this.admin = admin;
      this.tableName = tableName;
      this.numOnlineChanges = numOnlineChanges;

      if (admin == null) {
        throw new IllegalArgumentException(
            "[Test Error]: Provided admin should not be null");
      }
    }

    @Override
    public void run() {
      final long START_TIME = System.currentTimeMillis();
      final int ONLINE_CHANGE_TIMEOUT = 200000;

      HTableDescriptor htd = null;
      try {
        htd = admin.getTableDescriptor(tableName);
      } catch (IOException ioe) {

        ioe.printStackTrace();
        fail("Fail: Issue pulling table descriptor");
      }

      HColumnDescriptor hcd = null;
      assertTrue(htd != null);
      final int countOfFamilies = htd.getFamilies().size();
      assertTrue(countOfFamilies > 0);
      boolean expectedException = false;

      int numIterations = 0;


      while (numIterations < numOnlineChanges) {

        if (System.currentTimeMillis() - START_TIME > ONLINE_CHANGE_TIMEOUT) {
          fail("Fail: Timed out reaching before required snapshot count. Only had "
              + numIterations + " updates");
        }

        hcd = htd.getFamilies().iterator().next();
        int maxversions = hcd.getMaxVersions();
        int newMaxVersions = maxversions + 1;
        System.out.println("Setting max versions on CF to " + newMaxVersions);

        hcd.setMaxVersions(newMaxVersions);
        final byte[] hcdName = hcd.getName();
        expectedException = false;

        try {
          this.admin.modifyColumn(tableName, hcd);
        } catch (TableNotDisabledException re) {
          expectedException = true;
        } catch (IOException e) {
          e.printStackTrace();
          fail("Fail: IO Issue while modifying column");
        }
        assertFalse(expectedException);

        try {
          int EXPECTED_NUM_REGIONS = TEST_UTIL.getHBaseAdmin().getTableRegions(tableName).size();
          assertEquals("The max version count was not updated", newMaxVersions, TestAdmin.waitForColumnSchemasToSettle(TEST_UTIL.getMiniHBaseCluster(), tableName, EXPECTED_NUM_REGIONS).getMaxVersions());
          Thread.sleep(2000);
        } catch (TableNotFoundException e) {
          e.printStackTrace();
          fail("Fail: Table not found.");
        } catch (IOException e) {
          e.printStackTrace();
          fail("Fail: IO Issue while modifying column");
        } catch (InterruptedException e) {
          LOG.warn("Sleep was interrupted. This is unusual, but not grounds for TF");
          e.printStackTrace();
        }

        numIterations++;
      }
    }
  }
}
