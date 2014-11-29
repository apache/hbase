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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import java.math.BigDecimal;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.BigDecimalColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BigDecimalMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test class to test BigDecimalColumnInterpreter for AggregationsProtocol
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestBigDecimalColumnInterpreter {
  protected static Log myLog = LogFactory.getLog(TestBigDecimalColumnInterpreter.class);

  /**
   * Creating the test infrastructure.
   */
  private static final TableName TEST_TABLE =
      TableName.valueOf("TestTable");
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
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY, new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY, ROWS[rowSeperator1], ROWS[rowSeperator2] });
    /**
     * The testtable has one CQ which is always populated and one variable CQ for each row rowkey1:
     * CF:CQ CF:CQ1 rowKey2: CF:CQ CF:CQ2
     */
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      BigDecimal bd = new BigDecimal(i);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(bd));
      table.put(put);
      Put p2 = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      p2.add(TEST_FAMILY, Bytes.add(TEST_MULTI_CQ, Bytes.toBytes(bd)),
        Bytes.toBytes(bd.multiply(new BigDecimal("0.10"))));
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
  @Test (timeout=300000)
  public void testMedianWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal median = aClient.median(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("8.00"), median);
  }

  /**
   * ***************Test cases for Maximum *******************
   */

  /**
   * give max for the entire table.
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testMaxWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("19.00"), maximum);
  }

  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testMaxWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
        new BigDecimalColumnInterpreter();
    BigDecimal max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("14.00"), max);
  }

  @Test (timeout=300000)
  public void testMaxWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
        new BigDecimalColumnInterpreter();
    BigDecimal maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("19.00"), maximum);
  }

  @Test (timeout=300000)
  public void testMaxWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
        new BigDecimalColumnInterpreter();
    BigDecimal max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("6.00"), max);
  }

  @Test (timeout=300000)
  public void testMaxWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    Scan scan = new Scan();
    BigDecimal max = null;
    try {
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      max = null;
    }
    assertEquals(null, max);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test (timeout=300000)
  public void testMaxWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    Scan scan = new Scan();
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[2]);
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    BigDecimal max = new BigDecimal(Long.MIN_VALUE);
    ;
    try {
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
      max = BigDecimal.ZERO;
    }
    assertEquals(BigDecimal.ZERO, max);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testMaxWithInvalidRange2() throws Throwable {
    BigDecimal max = new BigDecimal(Long.MIN_VALUE);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[4]);
    try {
      AggregationClient aClient = new AggregationClient(conf);
      final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
        new BigDecimalColumnInterpreter();
      max = aClient.max(TEST_TABLE, ci, scan);
    } catch (Exception e) {
      max = BigDecimal.ZERO;
    }
    assertEquals(BigDecimal.ZERO, max);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testMaxWithFilter() throws Throwable {
    BigDecimal max = BigDecimal.ZERO;
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    max = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(null, max);
  }

  /**
   * **************************Test cases for Minimum ***********************
   */

  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testMinWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    scan.setStopRow(HConstants.EMPTY_END_ROW);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("0.00"), min);
  }

  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testMinWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("5.00"), min);
  }

  @Test (timeout=300000)
  public void testMinWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    scan.setStopRow(HConstants.EMPTY_END_ROW);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("0.00"), min);
  }

  @Test (timeout=300000)
  public void testMinWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("0.60"), min);
  }

  @Test (timeout=300000)
  public void testMinWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = null;
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test (timeout=300000)
  public void testMinWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    BigDecimal min = null;
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[4]);
    scan.setStopRow(ROWS[2]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testMinWithInvalidRange2() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[6]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = null;
    try {
      min = aClient.min(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, min);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testMinWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal min = null;
    min = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(null, min);
  }

  /**
   * *************** Test cases for Sum *********************
   */
  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testSumWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("190.00"), sum);
  }

  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testSumWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("95.00"), sum);
  }

  @Test (timeout=300000)
  public void testSumWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("209.00"), sum); // 190 + 19
  }

  @Test (timeout=300000)
  public void testSumWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(new BigDecimal("6.60"), sum); // 6 + 60
  }

  @Test (timeout=300000)
  public void testSumWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = null;
    try {
      sum = aClient.sum(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, sum);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test (timeout=300000)
  public void testSumWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[2]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = null;
    try {
      sum = aClient.sum(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, sum);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testSumWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setFilter(f);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    BigDecimal sum = null;
    sum = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(null, sum);
  }

  /**
   * ****************************** Test Cases for Avg **************
   */
  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testAvgWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(9.5, avg, 0);
  }

  /**
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testAvgWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(9.5, avg, 0);
  }

  @Test (timeout=300000)
  public void testAvgWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(10.45, avg, 0.01);
  }

  @Test (timeout=300000)
  public void testAvgWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double avg = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(6 + 0.60, avg, 0);
  }

  @Test (timeout=300000)
  public void testAvgWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    Double avg = null;
    try {
      avg = aClient.avg(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, avg);// CP will throw an IOException about the
    // null column family, and max will be set to 0
  }

  @Test (timeout=300000)
  public void testAvgWithInvalidRange() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[1]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    Double avg = null;
    try {
      avg = aClient.avg(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, avg);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testAvgWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    scan.setFilter(f);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
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
  @Test (timeout=300000)
  public void testStdWithValidRange() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(5.766, std, 0.05d);
  }

  /**
   * need to change this
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testStdWithValidRange2() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    scan.setStartRow(ROWS[5]);
    scan.setStopRow(ROWS[15]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(2.87, std, 0.05d);
  }

  /**
   * need to change this
   * @throws Throwable
   */
  @Test (timeout=300000)
  public void testStdWithValidRangeWithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(6.342, std, 0.05d);
  }

  @Test (timeout=300000)
  public void testStdWithValidRange2WithNoCQ() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[7]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    double std = aClient.std(TEST_TABLE, ci, scan);
    System.out.println("std is:" + std);
    assertEquals(0, std, 0.05d);
  }

  @Test (timeout=300000)
  public void testStdWithValidRangeWithNullCF() {
    AggregationClient aClient = new AggregationClient(conf);
    Scan scan = new Scan();
    scan.setStartRow(ROWS[6]);
    scan.setStopRow(ROWS[17]);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
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
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
      new BigDecimalColumnInterpreter();
    Double std = null;
    try {
      std = aClient.std(TEST_TABLE, ci, scan);
    } catch (Throwable e) {
    }
    assertEquals(null, std);// control should go to the catch block
  }

  @Test (timeout=300000)
  public void testStdWithFilter() throws Throwable {
    AggregationClient aClient = new AggregationClient(conf);
    Filter f = new PrefixFilter(Bytes.toBytes("foo:bar"));
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setFilter(f);
    final ColumnInterpreter<BigDecimal, BigDecimal, EmptyMsg, BigDecimalMsg, BigDecimalMsg> ci =
        new BigDecimalColumnInterpreter();
    Double std = null;
    std = aClient.std(TEST_TABLE, ci, scan);
    assertEquals(Double.NaN, std, 0);
  }
}