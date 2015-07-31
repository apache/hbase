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
package org.apache.hadoop.hbase.mapreduce;

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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
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
  public static Table createTable(byte[] tableName) throws IOException {
    return createTable(tableName, new byte[][] { FAMILY });
  }

  /**
   * Setup a table with two rows and values per column family.
   *
   * @param tableName
   * @return
   * @throws IOException
   */
  public static Table createTable(byte[] tableName, byte[][] families) throws IOException {
    Table table = UTIL.createTable(TableName.valueOf(tableName), families);
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
   * o.a.h.hbase.mapreduce API.
   *
   * @param table
   * @throws IOException
   * @throws InterruptedException
   */
  static void runTestMapreduce(Table table) throws IOException,
      InterruptedException {
    org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl trr =
        new org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl();
    Scan s = new Scan();
    s.setStartRow("aaa".getBytes());
    s.setStopRow("zzz".getBytes());
    s.addFamily(FAMILY);
    trr.setScan(s);
    trr.setHTable(table);

    trr.initialize(null, null);
    Result r = new Result();
    ImmutableBytesWritable key = new ImmutableBytesWritable();

    boolean more = trr.nextKeyValue();
    assertTrue(more);
    key = trr.getCurrentKey();
    r = trr.getCurrentValue();
    checkResult(r, key, "aaa".getBytes(), "value aaa".getBytes());

    more = trr.nextKeyValue();
    assertTrue(more);
    key = trr.getCurrentKey();
    r = trr.getCurrentValue();
    checkResult(r, key, "bbb".getBytes(), "value bbb".getBytes());

    // no more data
    more = trr.nextKeyValue();
    assertFalse(more);
  }

  /**
   * Create a table that IOE's on first scanner next call
   *
   * @throws IOException
   */
  static Table createIOEScannerTable(byte[] name, final int failCnt)
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

    Table htable = spy(createTable(name));
    doAnswer(a).when(htable).getScanner((Scan) anyObject());
    return htable;
  }

  /**
   * Create a table that throws a NotServingRegionException on first scanner 
   * next call
   *
   * @throws IOException
   */
  static Table createDNRIOEScannerTable(byte[] name, final int failCnt)
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

          invocation.callRealMethod(); // simulate NotServingRegionException 
          doThrow(
              new NotServingRegionException("Injected simulated TimeoutException"))
              .when(scanner).next();
          return scanner;
        }

        // otherwise return the real scanner.
        return (ResultScanner) invocation.callRealMethod();
      }
    };

    Table htable = spy(createTable(name));
    doAnswer(a).when(htable).getScanner((Scan) anyObject());
    return htable;
  }

  /**
   * Run test assuming no errors using newer mapreduce api
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testTableRecordReaderMapreduce() throws IOException,
      InterruptedException {
    Table table = createTable("table1-mr".getBytes());
    runTestMapreduce(table);
  }

  /**
   * Run test assuming Scanner IOException failure using newer mapreduce api
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testTableRecordReaderScannerFailMapreduce() throws IOException,
      InterruptedException {
    Table htable = createIOEScannerTable("table2-mr".getBytes(), 1);
    runTestMapreduce(htable);
  }

  /**
   * Run test assuming Scanner IOException failure using newer mapreduce api
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(expected = IOException.class)
  public void testTableRecordReaderScannerFailMapreduceTwice() throws IOException,
      InterruptedException {
    Table htable = createIOEScannerTable("table3-mr".getBytes(), 2);
    runTestMapreduce(htable);
  }

  /**
   * Run test assuming NotServingRegionException using newer mapreduce api
   * 
   * @throws InterruptedException
   * @throws org.apache.hadoop.hbase.DoNotRetryIOException
   */
  @Test
  public void testTableRecordReaderScannerTimeoutMapreduce()
      throws IOException, InterruptedException {
    Table htable = createDNRIOEScannerTable("table4-mr".getBytes(), 1);
    runTestMapreduce(htable);
  }

  /**
   * Run test assuming NotServingRegionException using newer mapreduce api
   * 
   * @throws InterruptedException
   * @throws org.apache.hadoop.hbase.NotServingRegionException
   */
  @Test(expected = org.apache.hadoop.hbase.NotServingRegionException.class)
  public void testTableRecordReaderScannerTimeoutMapreduceTwice()
      throws IOException, InterruptedException {
    Table htable = createDNRIOEScannerTable("table5-mr".getBytes(), 2);
    runTestMapreduce(htable);
  }

  /**
   * Verify the example we present in javadocs on TableInputFormatBase
   */
  @Test
  public void testExtensionOfTableInputFormatBase()
      throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("testing use of an InputFormat taht extends InputFormatBase");
    final Table htable = createTable(Bytes.toBytes("exampleTable"),
      new byte[][] { Bytes.toBytes("columnA"), Bytes.toBytes("columnB") });
    testInputFormat(ExampleTIF.class);
  }

  @Test
  public void testJobConfigurableExtensionOfTableInputFormatBase()
      throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("testing use of an InputFormat taht extends InputFormatBase, " +
        "using JobConfigurable.");
    final Table htable = createTable(Bytes.toBytes("exampleJobConfigurableTable"),
      new byte[][] { Bytes.toBytes("columnA"), Bytes.toBytes("columnB") });
    testInputFormat(ExampleJobConfigurableTIF.class);
  }

  @Test
  public void testDeprecatedExtensionOfTableInputFormatBase()
      throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("testing use of an InputFormat taht extends InputFormatBase, " +
        "using the approach documented in 0.98.");
    final Table htable = createTable(Bytes.toBytes("exampleDeprecatedTable"),
      new byte[][] { Bytes.toBytes("columnA"), Bytes.toBytes("columnB") });
    testInputFormat(ExampleDeprecatedTIF.class);
  }

  void testInputFormat(Class<? extends InputFormat> clazz)
      throws IOException, InterruptedException, ClassNotFoundException {
    final Job job = MapreduceTestingShim.createJob(UTIL.getConfiguration());
    job.setInputFormatClass(clazz);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(ExampleVerifier.class);
    job.setNumReduceTasks(0);

    LOG.debug("submitting job.");
    assertTrue("job failed!", job.waitForCompletion(true));
    assertEquals("Saw the wrong number of instances of the filtered-for row.", 2, job.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":row", "aaa").getValue());
    assertEquals("Saw any instances of the filtered out row.", 0, job.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":row", "bbb").getValue());
    assertEquals("Saw the wrong number of instances of columnA.", 1, job.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":family", "columnA").getValue());
    assertEquals("Saw the wrong number of instances of columnB.", 1, job.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":family", "columnB").getValue());
    assertEquals("Saw the wrong count of values for the filtered-for row.", 2, job.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":value", "value aaa").getValue());
    assertEquals("Saw the wrong count of values for the filtered-out row.", 0, job.getCounters()
        .findCounter(TestTableInputFormat.class.getName() + ":value", "value bbb").getValue());
  }

  public static class ExampleVerifier extends TableMapper<NullWritable, NullWritable> {

    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException {
      for (Cell cell : value.listCells()) {
        context.getCounter(TestTableInputFormat.class.getName() + ":row",
            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()))
            .increment(1l);
        context.getCounter(TestTableInputFormat.class.getName() + ":family",
            Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
            .increment(1l);
        context.getCounter(TestTableInputFormat.class.getName() + ":value",
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
            .increment(1l);
      }
    }

  }

  public static class ExampleDeprecatedTIF extends TableInputFormatBase implements JobConfigurable {

    @Override
    public void configure(JobConf job) {
      try {
        HTable exampleTable = new HTable(HBaseConfiguration.create(job),
          Bytes.toBytes("exampleDeprecatedTable"));
        // mandatory
        setHTable(exampleTable);
        byte[][] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
          Bytes.toBytes("columnB") };
        // optional
        Scan scan = new Scan();
        for (byte[] family : inputColumns) {
          scan.addFamily(family);
        }
        Filter exampleFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("aa.*"));
        scan.setFilter(exampleFilter);
        setScan(scan);
      } catch (IOException exception) {
        throw new RuntimeException("Failed to configure for job.", exception);
      }
    }

  }


  public static class ExampleJobConfigurableTIF extends TableInputFormatBase
      implements JobConfigurable {

    @Override
    public void configure(JobConf job) {
      try {
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(job));
        TableName tableName = TableName.valueOf("exampleJobConfigurableTable");
        // mandatory
        initializeTable(connection, tableName);
        byte[][] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
          Bytes.toBytes("columnB") };
        //optional
        Scan scan = new Scan();
        for (byte[] family : inputColumns) {
          scan.addFamily(family);
        }
        Filter exampleFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("aa.*"));
        scan.setFilter(exampleFilter);
        setScan(scan);
      } catch (IOException exception) {
        throw new RuntimeException("Failed to initialize.", exception);
      }
    }
  }


  public static class ExampleTIF extends TableInputFormatBase {

    @Override
    protected void initialize(JobContext job) throws IOException {
      Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(
          job.getConfiguration()));
      TableName tableName = TableName.valueOf("exampleTable");
      // mandatory
      initializeTable(connection, tableName);
      byte[][] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
        Bytes.toBytes("columnB") };
      //optional
      Scan scan = new Scan();
      for (byte[] family : inputColumns) {
        scan.addFamily(family);
      }
      Filter exampleFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("aa.*"));
      scan.setFilter(exampleFilter);
      setScan(scan);
    }

  }
}

