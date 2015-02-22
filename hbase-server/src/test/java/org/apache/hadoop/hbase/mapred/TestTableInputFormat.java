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
package org.apache.hadoop.hbase.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.MapreduceTestingShim;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This tests the TableInputFormat and its recovery semantics
 * 
 */
@Category(LargeTests.class)
public class TestTableInputFormat {

  private static final Log LOG = LogFactory.getLog(TestTableInputFormat.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static MiniMRCluster mrCluster;
  static final byte[] FAMILY = Bytes.toBytes("family");

  private static final byte[][] columns = new byte[][] { FAMILY };

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    mrCluster = UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniMapReduceCluster();
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    LOG.info("before");
    UTIL.ensureSomeRegionServersAvailable(1);
    LOG.info("before done");
  }

  /**
   * Setup a table with two rows and values.
   * 
   * @param tableName
   * @return
   * @throws IOException
   */
  public static HTable createTable(byte[] tableName) throws IOException {
    return createTable(tableName, new byte[][] { FAMILY });
  }

  /**
   * Setup a table with two rows and values per column family.
   * 
   * @param tableName
   * @return
   * @throws IOException
   */
  public static HTable createTable(byte[] tableName, byte[][] families) throws IOException {
    HTable table = UTIL.createTable(tableName, families);
    Put p = new Put("aaa".getBytes());
    for (byte[] family : families) {
      p.add(family, null, "value aaa".getBytes());
    }
    table.put(p);
    p = new Put("bbb".getBytes());
    for (byte[] family : families) {
      p.add(family, null, "value bbb".getBytes());
    }
    table.put(p);
    return table;
  }

  /**
   * Verify that the result and key have expected values.
   * 
   * @param r
   * @param key
   * @param expectedKey
   * @param expectedValue
   * @return
   */
  static boolean checkResult(Result r, ImmutableBytesWritable key,
      byte[] expectedKey, byte[] expectedValue) {
    assertEquals(0, key.compareTo(expectedKey));
    Map<byte[], byte[]> vals = r.getFamilyMap(FAMILY);
    byte[] value = vals.values().iterator().next();
    assertTrue(Arrays.equals(value, expectedValue));
    return true; // if succeed
  }

  /**
   * Create table data and run tests on specified htable using the
   * o.a.h.hbase.mapred API.
   * 
   * @param table
   * @throws IOException
   */
  static void runTestMapred(HTable table) throws IOException {
    org.apache.hadoop.hbase.mapred.TableRecordReader trr = 
        new org.apache.hadoop.hbase.mapred.TableRecordReader();
    trr.setStartRow("aaa".getBytes());
    trr.setEndRow("zzz".getBytes());
    trr.setHTable(table);
    trr.setInputColumns(columns);

    trr.init();
    Result r = new Result();
    ImmutableBytesWritable key = new ImmutableBytesWritable();

    boolean more = trr.next(key, r);
    assertTrue(more);
    checkResult(r, key, "aaa".getBytes(), "value aaa".getBytes());

    more = trr.next(key, r);
    assertTrue(more);
    checkResult(r, key, "bbb".getBytes(), "value bbb".getBytes());

    // no more data
    more = trr.next(key, r);
    assertFalse(more);
  }

  /**
   * Create a table that IOE's on first scanner next call
   * 
   * @throws IOException
   */
  static HTable createIOEScannerTable(byte[] name, final int failCnt)
      throws IOException {
    // build up a mock scanner stuff to fail the first time
    Answer<ResultScanner> a = new Answer<ResultScanner>() {
      int cnt = 0;

      @Override
      public ResultScanner answer(InvocationOnMock invocation) throws Throwable {
        // first invocation return the busted mock scanner
        if (cnt++ < failCnt) {
          // create mock ResultScanner that always fails.
          Scan scan = mock(Scan.class);
          doReturn("bogus".getBytes()).when(scan).getStartRow(); // avoid npe
          ResultScanner scanner = mock(ResultScanner.class);
          // simulate TimeoutException / IOException
          doThrow(new IOException("Injected exception")).when(scanner).next();
          return scanner;
        }

        // otherwise return the real scanner.
        return (ResultScanner) invocation.callRealMethod();
      }
    };

    HTable htable = spy(createTable(name));
    doAnswer(a).when(htable).getScanner((Scan) anyObject());
    return htable;
  }

  /**
   * Create a table that throws a DoNoRetryIOException on first scanner next
   * call
   * 
   * @throws IOException
   */
  static HTable createDNRIOEScannerTable(byte[] name, final int failCnt)
      throws IOException {
    // build up a mock scanner stuff to fail the first time
    Answer<ResultScanner> a = new Answer<ResultScanner>() {
      int cnt = 0;

      @Override
      public ResultScanner answer(InvocationOnMock invocation) throws Throwable {
        // first invocation return the busted mock scanner
        if (cnt++ < failCnt) {
          // create mock ResultScanner that always fails.
          Scan scan = mock(Scan.class);
          doReturn("bogus".getBytes()).when(scan).getStartRow(); // avoid npe
          ResultScanner scanner = mock(ResultScanner.class);

          invocation.callRealMethod(); // simulate UnknownScannerException
          doThrow(
              new UnknownScannerException("Injected simulated TimeoutException"))
              .when(scanner).next();
          return scanner;
        }

        // otherwise return the real scanner.
        return (ResultScanner) invocation.callRealMethod();
      }
    };

    HTable htable = spy(createTable(name));
    doAnswer(a).when(htable).getScanner((Scan) anyObject());
    return htable;
  }

  /**
   * Run test assuming no errors using mapred api.
   * 
   * @throws IOException
   */
  @Test
  public void testTableRecordReader() throws IOException {
    HTable table = createTable("table1".getBytes());
    runTestMapred(table);
  }

  /**
   * Run test assuming Scanner IOException failure using mapred api,
   * 
   * @throws IOException
   */
  @Test
  public void testTableRecordReaderScannerFail() throws IOException {
    HTable htable = createIOEScannerTable("table2".getBytes(), 1);
    runTestMapred(htable);
  }

  /**
   * Run test assuming Scanner IOException failure using mapred api,
   * 
   * @throws IOException
   */
  @Test(expected = IOException.class)
  public void testTableRecordReaderScannerFailTwice() throws IOException {
    HTable htable = createIOEScannerTable("table3".getBytes(), 2);
    runTestMapred(htable);
  }

  /**
   * Run test assuming UnknownScannerException (which is a type of
   * DoNotRetryIOException) using mapred api.
   * 
   * @throws org.apache.hadoop.hbase.DoNotRetryIOException
   */
  @Test
  public void testTableRecordReaderScannerTimeout() throws IOException {
    HTable htable = createDNRIOEScannerTable("table4".getBytes(), 1);
    runTestMapred(htable);
  }

  /**
   * Run test assuming UnknownScannerException (which is a type of
   * DoNotRetryIOException) using mapred api.
   * 
   * @throws org.apache.hadoop.hbase.DoNotRetryIOException
   */
  @Test(expected = org.apache.hadoop.hbase.DoNotRetryIOException.class)
  public void testTableRecordReaderScannerTimeoutTwice() throws IOException {
    HTable htable = createDNRIOEScannerTable("table5".getBytes(), 2);
    runTestMapred(htable);
  }

  /**
   * Verify the example we present in javadocs on TableInputFormatBase
   */
  @Test
  public void testExtensionOfTableInputFormatBase() throws IOException {
    LOG.info("testing use of an InputFormat taht extends InputFormatBase");
    final HTable htable = createTable(Bytes.toBytes("exampleTable"),
      new byte[][] { Bytes.toBytes("columnA"), Bytes.toBytes("columnB") });
    final JobConf job = MapreduceTestingShim.getJobConf(mrCluster);
    job.setInputFormat(ExampleTIF.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setMapperClass(ExampleVerifier.class);
    job.setNumReduceTasks(0);
    LOG.debug("submitting job.");
    final RunningJob run = JobClient.runJob(job);
    assertTrue("job failed!", run.isSuccessful());
    assertEquals("Saw the wrong number of instances of the filtered-for row.", 2, run.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":row", "aaa").getCounter());
    assertEquals("Saw any instances of the filtered out row.", 0, run.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":row", "bbb").getCounter());
    assertEquals("Saw the wrong number of instances of columnA.", 1, run.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":family", "columnA").getCounter());
    assertEquals("Saw the wrong number of instances of columnB.", 1, run.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":family", "columnB").getCounter());
    assertEquals("Saw the wrong count of values for the filtered-for row.", 2, run.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":value", "value aaa").getCounter());
    assertEquals("Saw the wrong count of values for the filtered-out row.", 0, run.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":value", "value bbb").getCounter());
  }

  public static class ExampleVerifier implements TableMap<Text, Text> {

    @Override
    public void configure(JobConf conf) {
    }

    @Override
    public void map(ImmutableBytesWritable key, Result value,
        OutputCollector<Text,Text> output,
        Reporter reporter) throws IOException {
      for (Cell cell : value.listCells()) {
        reporter.getCounter(TestTableInputFormat.class.getName() + ":row",
            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()))
            .increment(1l);
        reporter.getCounter(TestTableInputFormat.class.getName() + ":family",
            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
            .increment(1l);
        reporter.getCounter(TestTableInputFormat.class.getName() + ":value",
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
            .increment(1l);
      }
    }

    @Override
    public void close() {
    }

  }

  public static class ExampleTIF extends TableInputFormatBase implements JobConfigurable {

    @Override
    public void configure(JobConf job) {
      try {
        HTable exampleTable = new HTable(HBaseConfiguration.create(job),
          Bytes.toBytes("exampleTable"));
        // mandatory
        setHTable(exampleTable);
        byte[][] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
          Bytes.toBytes("columnB") };
        // mandatory
        setInputColumns(inputColumns);
        Filter exampleFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("aa.*"));
        // optional
        setRowFilter(exampleFilter);
      } catch (IOException exception) {
        throw new RuntimeException("Failed to configure for job.", exception);
      }
    }

  }

}

