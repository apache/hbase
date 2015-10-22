/*
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

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.DoubleMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test class to test DoubleColumnInterpreter for AggregateProtocol
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestDoubleColumnInterpreter {
  protected static Log myLog = LogFactory.getLog(TestDoubleColumnInterpreter.class);

  /**
   * Creating the test infrastructure.
   */
  private static final TableName TEST_TABLE = TableName.valueOf("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static final byte[] TEST_MULTI_CQ = Bytes.toBytes("TestMultiCQ");

  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static Configuration conf = util.getConfiguration();

  /**
   * A set up method to start the test cluster. AggregateProtocolImpl is registered and will be
   * loaded during region startup.
   * @throws Exception
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.coprocessor.AggregateImplementation");

    util.startMiniCluster(2);
    final byte[][] SPLIT_KEYS = new byte[][] { ROWS[rowSeperator1], ROWS[rowSeperator2] };
    Table table = util.createTable(TEST_TABLE, TEST_FAMILY, SPLIT_KEYS);
    /**
     * The testtable has one CQ which is always populated and one variable CQ for each row rowkey1:
     * CF:CQ CF:CQ1 rowKey2: CF:CQ CF:CQ2
     */
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      Double d = new Double(i);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(d));
      table.put(put);
      Put p2 = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      p2.add(TEST_FAMILY, Bytes.add(TEST_MULTI_CQ, Bytes.toBytes(d)), Bytes.toBytes(d * 0.10));
      table.put(p2);
    }
    table.close();
  }

  /**
   * Shutting down the cluster
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * an infrastructure method to prepare rows for the testtable.
   * @param base
   * @param n
   * @return
   */
  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(i));
    }
    return ret;
  }

  /**
   * ****************** Test cases for Median **********************
   */
  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMedianWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double median = aClient.median(TEST_TABLE, ci, scan);
    assertEquals(8.00, median, 0.00);
  }

  /**
   * ***************Test cases for Maximum *******************
   */

  /**
   * give max for the entire table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMaxWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(19.00, maximum, 0.00);
  }

  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMaxWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(14.00, max, 0.00);
  }

  @Test(timeout = 300000)
  public void testMaxWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(19.00, maximum, 0.00);
  }

  @Test(timeout = 300000)
  public void testMaxWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(6.00, max, 0.00);
  }

  @Test(timeout = 300000)
  public void testMaxWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Scan scan = new Scan();
    Double max = null;
    try {
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      max = null;
    }
    assertEquals(null, max);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test(timeout = 300000)
  public void testMaxWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Scan scan = new Scan();
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[2]);
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    double max = Double.MIN_VALUE;
    ;
    try {
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      max = 0.00;
    }
    assertEquals(0.00, max, 0.00);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testMaxWithInvalidRange2() throws Throwable {
    double max = Double.MIN_VALUE;
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[4]);
    try {
      AggregationClient aClient = new AggregationClient(conf);
      final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
          new DoubleColumnInterpreter();
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Exception e) {
      max = 0.00;
    }
    assertEquals(0.00, max, 0.00);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testMaxWithFilter() throws Throwable {
    Double max = 0.00d;
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(null, max);
  }

  /**
   * **************************Test cases for Minimum ***********************
   */

  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMinWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    scan.setStopRow(HConstants.EMPTY_END_ROW);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(0.00, min, 0.00);
  }

  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMinWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(5.00, min, 0.00);
  }

  @Test(timeout = 300000)
  public void testMinWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    scan.setStopRow(HConstants.EMPTY_END_ROW);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(0.00, min, 0.00);
  }

  @Test(timeout = 300000)
  public void testMinWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(0.60, min, 0.001);
  }

  @Test(timeout = 300000)
  public void testMinWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double min = null;
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      min = null;
    }
    assertEquals(null, min);// CP will throw an IOException about the
    // null column family, and min will be set to 0
  }

  @Test(timeout = 300000)
  public void testMinWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Double min = null;
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[2]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testMinWithInvalidRange2() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[6]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double min = null;
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testMinWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double min = null;
    min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(null, min);
  }

  /**
   * *************** Test cases for Sum *********************
   */
  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testSumWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(190.00, sum, 0.00);
  }

  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testSumWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(95.00, sum, 0.00);
  }

  @Test(timeout = 300000)
  public void testSumWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(209.00, sum, 0.00); // 190 + 19
  }

  @Test(timeout = 300000)
  public void testSumWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(6.60, sum, 0.00); // 6 + 60
  }

  @Test(timeout = 300000)
  public void testSumWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double sum = null;
    try {
      sum = aClient.sum(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, sum);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test(timeout = 300000)
  public void testSumWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[2]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double sum = null;
    try {
      sum = aClient.sum(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, sum);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testSumWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setFilter(f);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double sum = null;
    sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(null, sum);
  }

  /**
   * ****************************** Test Cases for Avg **************
   */
  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testAvgWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(9.5, avg, 0);
  }

  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testAvgWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci =
        new DoubleColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(9.5, avg, 0);
  }

  @Test(timeout = 300000)
  public void testAvgWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(10.45, avg, 0.01);
  }

  @Test(timeout = 300000)
  public void testAvgWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(6 + 0.60, avg, 0);
  }

  @Test(timeout = 300000)
  public void testAvgWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double avg = null;
    try {
      avg = aClient.avg(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, avg);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test(timeout = 300000)
  public void testAvgWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[1]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double avg = null;
    try {
      avg = aClient.avg(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, avg);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testAvgWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double avg = null;
    avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(Double.NaN, avg, 0);
  }

  /**
   * ****************** Test cases for STD **********************
   */
  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testStdWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(5.766, std, 0.05d);
  }

  /**
   * need to change this
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testStdWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(2.87, std, 0.05d);
  }

  /**
   * need to change this
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testStdWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(6.342, std, 0.05d);
  }

  @Test(timeout = 300000)
  public void testStdWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    System.out.println("std is:" + std);
    assertEquals(0, std, 0.05d);
  }

  @Test(timeout = 300000)
  public void testStdWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[17]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double std = null;
    try {
      std = aClient.std(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, std);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test
  public void testStdWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[1]);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double std = null;
    try {
      std = aClient.std(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, std);// control should go to the catch block
  }

  @Test(timeout = 300000)
  public void testStdWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setFilter(f);
    final ColumnInterpreter<Double, Double, EmptyMsg, DoubleMsg, DoubleMsg> ci = 
        new DoubleColumnInterpreter();
    Double std = null;
    std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(Double.NaN, std, 0);
  }
}
