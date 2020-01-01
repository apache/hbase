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
package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.rest.HBaseRESTTestingUtility;
import org.apache.hadoop.hbase.rest.RESTServlet;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestRemoteTable {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRemoteTable.class);

  // Verify that invalid URL characters and arbitrary bytes are escaped when
  // constructing REST URLs per HBASE-7621. RemoteHTable should support row keys
  // and qualifiers containing any byte for all table operations.
  private static final String INVALID_URL_CHARS_1 =
      "|\"\\^{}\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0009\u000B\u000C";

  // HColumnDescriptor prevents certain characters in column names.  The following
  // are examples of characters are allowed in column names but are not valid in
  // URLs.
  private static final String INVALID_URL_CHARS_2 = "|^{}\u0242";

  // Besides alphanumeric these characters can also be present in table names.
  private static final String VALID_TABLE_NAME_CHARS = "_-.";

  private static final TableName TABLE =
      TableName.valueOf("TestRemoteTable" + VALID_TABLE_NAME_CHARS);

  private static final byte[] ROW_1 = Bytes.toBytes("testrow1" + INVALID_URL_CHARS_1);
  private static final byte[] ROW_2 = Bytes.toBytes("testrow2" + INVALID_URL_CHARS_1);
  private static final byte[] ROW_3 = Bytes.toBytes("testrow3" + INVALID_URL_CHARS_1);
  private static final byte[] ROW_4 = Bytes.toBytes("testrow4"+ INVALID_URL_CHARS_1);

  private static final byte[] COLUMN_1 = Bytes.toBytes("a" + INVALID_URL_CHARS_2);
  private static final byte[] COLUMN_2 = Bytes.toBytes("b" + INVALID_URL_CHARS_2);
  private static final byte[] COLUMN_3 = Bytes.toBytes("c" + INVALID_URL_CHARS_2);

  private static final byte[] QUALIFIER_1 = Bytes.toBytes("1" + INVALID_URL_CHARS_1);
  private static final byte[] QUALIFIER_2 = Bytes.toBytes("2" + INVALID_URL_CHARS_1);
  private static final byte[] VALUE_1 = Bytes.toBytes("testvalue1");
  private static final byte[] VALUE_2 = Bytes.toBytes("testvalue2");

  private static final long ONE_HOUR = 60 * 60 * 1000;
  private static final long TS_2 = System.currentTimeMillis();
  private static final long TS_1 = TS_2 - ONE_HOUR;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private RemoteHTable remoteTable;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
  }

  @Before
  public void before() throws Exception  {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(TABLE)) {
      if (admin.isTableEnabled(TABLE)) {
        admin.disableTable(TABLE);
      }

      admin.deleteTable(TABLE);
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(COLUMN_1).setMaxVersions(3));
    htd.addFamily(new HColumnDescriptor(COLUMN_2).setMaxVersions(3));
    htd.addFamily(new HColumnDescriptor(COLUMN_3).setMaxVersions(3));
    admin.createTable(htd);
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE)) {
      Put put = new Put(ROW_1);
      put.addColumn(COLUMN_1, QUALIFIER_1, TS_2, VALUE_1);
      table.put(put);
      put = new Put(ROW_2);
      put.addColumn(COLUMN_1, QUALIFIER_1, TS_1, VALUE_1);
      put.addColumn(COLUMN_1, QUALIFIER_1, TS_2, VALUE_2);
      put.addColumn(COLUMN_2, QUALIFIER_2, TS_2, VALUE_2);
      table.put(put);
    }
    remoteTable = new RemoteHTable(
      new Client(new Cluster().add("localhost",
          REST_TEST_UTIL.getServletPort())),
        TEST_UTIL.getConfiguration(), TABLE.toBytes());
  }

  @After
  public void after() throws Exception {
    remoteTable.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetTableDescriptor() throws IOException {
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(TABLE);
      HTableDescriptor local = table.getTableDescriptor();
      assertEquals(remoteTable.getTableDescriptor(), local);
    } finally {
      if (null != table) table.close();
    }
  }

  @Test
  public void testGet() throws IOException {
    Get get = new Get(ROW_1);
    Result result = remoteTable.get(get);
    byte[] value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    byte[] value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    get = new Get(ROW_1);
    get.addFamily(COLUMN_3);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNull(value1);
    assertNull(value2);

    get = new Get(ROW_1);
    get.addColumn(COLUMN_1, QUALIFIER_1);
    get.addColumn(COLUMN_2, QUALIFIER_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    get = new Get(ROW_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));

    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
    assertNull(value2);

    get = new Get(ROW_2);
    get.addColumn(COLUMN_1, QUALIFIER_1);
    get.addColumn(COLUMN_2, QUALIFIER_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));

    // test timestamp
    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    get.setTimestamp(TS_1);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1)); // @TS_1
    assertNull(value2);

    // test timerange
    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    get.setTimeRange(0, TS_1 + 1);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1)); // @TS_1
    assertNull(value2);

    // test maxVersions
    get = new Get(ROW_2);
    get.addFamily(COLUMN_1);
    get.setMaxVersions(2);
    result = remoteTable.get(get);
    int count = 0;
    for (Cell kv: result.listCells()) {
      if (CellUtil.matchingFamily(kv, COLUMN_1) && TS_1 == kv.getTimestamp()) {
        assertTrue(CellUtil.matchingValue(kv, VALUE_1)); // @TS_1
        count++;
      }
      if (CellUtil.matchingFamily(kv, COLUMN_1) && TS_2 == kv.getTimestamp()) {
        assertTrue(CellUtil.matchingValue(kv, VALUE_2)); // @TS_2
        count++;
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testMultiGet() throws Exception {
    ArrayList<Get> gets = new ArrayList<>(2);
    gets.add(new Get(ROW_1));
    gets.add(new Get(ROW_2));
    Result[] results = remoteTable.get(gets);
    assertNotNull(results);
    assertEquals(2, results.length);
    assertEquals(1, results[0].size());
    assertEquals(2, results[1].size());

    //Test Versions
    gets = new ArrayList<>(2);
    Get g = new Get(ROW_1);
    g.setMaxVersions(3);
    gets.add(g);
    gets.add(new Get(ROW_2));
    results = remoteTable.get(gets);
    assertNotNull(results);
    assertEquals(2, results.length);
    assertEquals(1, results[0].size());
    assertEquals(3, results[1].size());

    //404
    gets = new ArrayList<>(1);
    gets.add(new Get(Bytes.toBytes("RESALLYREALLYNOTTHERE")));
    results = remoteTable.get(gets);
    assertNotNull(results);
    assertEquals(0, results.length);

    gets = new ArrayList<>(3);
    gets.add(new Get(Bytes.toBytes("RESALLYREALLYNOTTHERE")));
    gets.add(new Get(ROW_1));
    gets.add(new Get(ROW_2));
    results = remoteTable.get(gets);
    assertNotNull(results);
    assertEquals(2, results.length);
  }

  @Test
  public void testPut() throws IOException {
    Put put = new Put(ROW_3);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    remoteTable.put(put);

    Get get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    Result result = remoteTable.get(get);
    byte[] value = result.getValue(COLUMN_1, QUALIFIER_1);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_1, value));

    // multiput
    List<Put> puts = new ArrayList<>(3);
    put = new Put(ROW_3);
    put.addColumn(COLUMN_2, QUALIFIER_2, VALUE_2);
    puts.add(put);
    put = new Put(ROW_4);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.addColumn(COLUMN_2, QUALIFIER_2, VALUE_2);
    puts.add(put);
    remoteTable.put(puts);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_2, value));
    get = new Get(ROW_4);
    result = remoteTable.get(get);
    value = result.getValue(COLUMN_1, QUALIFIER_1);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_1, value));
    value = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_2, value));

    assertTrue(Bytes.equals(Bytes.toBytes("TestRemoteTable" + VALID_TABLE_NAME_CHARS),
        remoteTable.getTableName()));
  }

  @Test
  public void testDelete() throws IOException {
    Put put = new Put(ROW_3);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    put.addColumn(COLUMN_2, QUALIFIER_2, VALUE_2);
    put.addColumn(COLUMN_3, QUALIFIER_1, VALUE_1);
    put.addColumn(COLUMN_3, QUALIFIER_2, VALUE_2);
    remoteTable.put(put);

    Get get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    get.addFamily(COLUMN_3);
    Result result = remoteTable.get(get);
    byte[] value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    byte[] value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    byte[] value3 = result.getValue(COLUMN_3, QUALIFIER_1);
    byte[] value4 = result.getValue(COLUMN_3, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));
    assertNotNull(value3);
    assertTrue(Bytes.equals(VALUE_1, value3));
    assertNotNull(value4);
    assertTrue(Bytes.equals(VALUE_2, value4));

    Delete delete = new Delete(ROW_3);
    delete.addColumn(COLUMN_2, QUALIFIER_2);
    remoteTable.delete(delete);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    delete = new Delete(ROW_3);
    delete.setTimestamp(1L);
    remoteTable.delete(delete);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);

    // Delete column family from row
    delete = new Delete(ROW_3);
    delete.addFamily(COLUMN_3);
    remoteTable.delete(delete);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_3);
    result = remoteTable.get(get);
    value3 = result.getValue(COLUMN_3, QUALIFIER_1);
    value4 = result.getValue(COLUMN_3, QUALIFIER_2);
    assertNull(value3);
    assertNull(value4);

    delete = new Delete(ROW_3);
    remoteTable.delete(delete);

    get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    result = remoteTable.get(get);
    value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNull(value1);
    assertNull(value2);
  }

  /**
   * Test RemoteHTable.Scanner
   */
  @Test
  public void testScanner() throws IOException {
    List<Put> puts = new ArrayList<>(4);
    Put put = new Put(ROW_1);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_2);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_3);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    remoteTable.put(puts);

    ResultScanner scanner = remoteTable.getScanner(new Scan());

    Result[] results = scanner.next(1);
    assertNotNull(results);
    assertEquals(1, results.length);
    assertTrue(Bytes.equals(ROW_1, results[0].getRow()));

    Result result = scanner.next();
    assertNotNull(result);
    assertTrue(Bytes.equals(ROW_2, result.getRow()));

    results = scanner.next(2);
    assertNotNull(results);
    assertEquals(2, results.length);
    assertTrue(Bytes.equals(ROW_3, results[0].getRow()));
    assertTrue(Bytes.equals(ROW_4, results[1].getRow()));

    results = scanner.next(1);
    assertNull(results);
    scanner.close();

    scanner = remoteTable.getScanner(COLUMN_1);
    results = scanner.next(4);
    assertNotNull(results);
    assertEquals(4, results.length);
    assertTrue(Bytes.equals(ROW_1, results[0].getRow()));
    assertTrue(Bytes.equals(ROW_2, results[1].getRow()));
    assertTrue(Bytes.equals(ROW_3, results[2].getRow()));
    assertTrue(Bytes.equals(ROW_4, results[3].getRow()));

    scanner.close();

    scanner = remoteTable.getScanner(COLUMN_1,QUALIFIER_1);
    results = scanner.next(4);
    assertNotNull(results);
    assertEquals(4, results.length);
    assertTrue(Bytes.equals(ROW_1, results[0].getRow()));
    assertTrue(Bytes.equals(ROW_2, results[1].getRow()));
    assertTrue(Bytes.equals(ROW_3, results[2].getRow()));
    assertTrue(Bytes.equals(ROW_4, results[3].getRow()));
    scanner.close();
    assertTrue(remoteTable.isAutoFlush());
  }

  @Test
  public void testCheckAndDelete() throws IOException {
    Get get = new Get(ROW_1);
    Result result = remoteTable.get(get);
    byte[] value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    byte[] value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNull(value2);
    assertTrue(remoteTable.exists(get));
    assertEquals(1, remoteTable.existsAll(Collections.singletonList(get)).length);
    Delete delete = new Delete(ROW_1);

    remoteTable.checkAndMutate(ROW_1, COLUMN_1).qualifier(QUALIFIER_1)
        .ifEquals(VALUE_1).thenDelete(delete);
    assertFalse(remoteTable.exists(get));

    Put put = new Put(ROW_1);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    remoteTable.put(put);

    assertTrue(remoteTable.checkAndMutate(ROW_1, COLUMN_1).qualifier(QUALIFIER_1)
        .ifEquals(VALUE_1).thenPut(put));
    assertFalse(remoteTable.checkAndMutate(ROW_1, COLUMN_1).qualifier(QUALIFIER_1)
        .ifEquals(VALUE_2).thenPut(put));
  }

  /**
   * Test RemoteHable.Scanner.iterator method
   */
  @Test
  public void testIteratorScaner() throws IOException {
    List<Put> puts = new ArrayList<>(4);
    Put put = new Put(ROW_1);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_2);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_3);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.addColumn(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    remoteTable.put(puts);

    ResultScanner scanner = remoteTable.getScanner(new Scan());
    Iterator<Result> iterator = scanner.iterator();
    assertTrue(iterator.hasNext());
    int counter = 0;
    while (iterator.hasNext()) {
      iterator.next();
      counter++;
    }
    assertEquals(4, counter);
  }

  /**
   * Test a some methods of class Response.
   */
  @Test
  public void testResponse(){
    Response response = new Response(200);
    assertEquals(200, response.getCode());
    Header[] headers = new Header[2];
    headers[0] = new BasicHeader("header1", "value1");
    headers[1] = new BasicHeader("header2", "value2");
    response = new Response(200, headers);
    assertEquals("value1", response.getHeader("header1"));
    assertFalse(response.hasBody());
    response.setCode(404);
    assertEquals(404, response.getCode());
    headers = new Header[2];
    headers[0] = new BasicHeader("header1", "value1.1");
    headers[1] = new BasicHeader("header2", "value2");
    response.setHeaders(headers);
    assertEquals("value1.1", response.getHeader("header1"));
    response.setBody(Bytes.toBytes("body"));
    assertTrue(response.hasBody());
  }

  /**
   * Tests keeping a HBase scanner alive for long periods of time. Each call to next() should reset
   * the ConnectionCache timeout for the scanner's connection.
   *
   * @throws Exception if starting the servlet container or disabling or truncating the table fails
   */
  @Test
  public void testLongLivedScan() throws Exception {
    int numTrials = 6;
    int trialPause = 1000;
    int cleanUpInterval = 100;

    // Shutdown the Rest Servlet container
    REST_TEST_UTIL.shutdownServletContainer();

    // Set the ConnectionCache timeout to trigger halfway through the trials
    TEST_UTIL.getConfiguration().setLong(RESTServlet.MAX_IDLETIME, (numTrials / 2) * trialPause);
    TEST_UTIL.getConfiguration().setLong(RESTServlet.CLEANUP_INTERVAL, cleanUpInterval);

    // Start the Rest Servlet container
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());

    // Truncate the test table for inserting test scenarios rows keys
    TEST_UTIL.getHBaseAdmin().disableTable(TABLE);
    TEST_UTIL.getHBaseAdmin().truncateTable(TABLE, false);

    remoteTable = new RemoteHTable(
        new Client(new Cluster().add("localhost", REST_TEST_UTIL.getServletPort())),
        TEST_UTIL.getConfiguration(), TABLE.toBytes());

    String row = "testrow";

    try (Table table = TEST_UTIL.getConnection().getTable(TABLE)) {
      List<Put> puts = new ArrayList<Put>();
      Put put = null;
      for (int i = 1; i <= numTrials; i++) {
        put = new Put(Bytes.toBytes(row + i));
        put.addColumn(COLUMN_1, QUALIFIER_1, TS_2, Bytes.toBytes("testvalue" + i));
        puts.add(put);
      }
      table.put(puts);
    }

    Scan scan = new Scan();
    scan.setCaching(1);
    scan.setBatch(1);

    ResultScanner scanner = remoteTable.getScanner(scan);
    Result result = null;
    // get scanner and rows
    for (int i = 1; i <= numTrials; i++) {
      // Make sure that the Scanner doesn't throw an exception after the ConnectionCache timeout
      result = scanner.next();
      assertEquals(row + i, Bytes.toString(result.getRow()));
      Thread.sleep(trialPause);
    }
  }
}
