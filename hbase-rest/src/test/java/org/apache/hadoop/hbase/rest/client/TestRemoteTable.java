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

package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.HBaseRESTTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRemoteTable {
  private static final String TABLE = "TestRemoteTable";
  private static final byte[] ROW_1 = Bytes.toBytes("testrow1");
  private static final byte[] ROW_2 = Bytes.toBytes("testrow2");
  private static final byte[] ROW_3 = Bytes.toBytes("testrow3");
  private static final byte[] ROW_4 = Bytes.toBytes("testrow4");
  private static final byte[] COLUMN_1 = Bytes.toBytes("a");
  private static final byte[] COLUMN_2 = Bytes.toBytes("b");
  private static final byte[] COLUMN_3 = Bytes.toBytes("c");
  private static final byte[] QUALIFIER_1 = Bytes.toBytes("1");
  private static final byte[] QUALIFIER_2 = Bytes.toBytes("2");
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
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TABLE)) {
      if (admin.isTableEnabled(TABLE)) admin.disableTable(TABLE);
      admin.deleteTable(TABLE);
    }
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    htd.addFamily(new HColumnDescriptor(COLUMN_1).setMaxVersions(3));
    htd.addFamily(new HColumnDescriptor(COLUMN_2).setMaxVersions(3));
    htd.addFamily(new HColumnDescriptor(COLUMN_3).setMaxVersions(3));
    admin.createTable(htd);
    HTable table = null;
    try {
      table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
      Put put = new Put(ROW_1);
      put.add(COLUMN_1, QUALIFIER_1, TS_2, VALUE_1);
      table.put(put);
      put = new Put(ROW_2);
      put.add(COLUMN_1, QUALIFIER_1, TS_1, VALUE_1);
      put.add(COLUMN_1, QUALIFIER_1, TS_2, VALUE_2);
      put.add(COLUMN_2, QUALIFIER_2, TS_2, VALUE_2);
      table.put(put);
      table.flushCommits();
    } finally {
      if (null != table) table.close();
    }
    remoteTable = new RemoteHTable(
      new Client(new Cluster().add("localhost", 
          REST_TEST_UTIL.getServletPort())),
        TEST_UTIL.getConfiguration(), TABLE);
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
    HTable table = null;
    try {
      table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
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
    get.setTimeStamp(TS_1);
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
    ArrayList<Get> gets = new ArrayList<Get>();
    gets.add(new Get(ROW_1));
    gets.add(new Get(ROW_2));
    Result[] results = remoteTable.get(gets);
    assertNotNull(results);
    assertEquals(2, results.length);
    assertEquals(1, results[0].size());
    assertEquals(2, results[1].size());

    //Test Versions
    gets = new ArrayList<Get>();
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
    gets = new ArrayList<Get>();
    gets.add(new Get(Bytes.toBytes("RESALLYREALLYNOTTHERE")));
    results = remoteTable.get(gets);
    assertNotNull(results);
    assertEquals(0, results.length);

    gets = new ArrayList<Get>();
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
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    remoteTable.put(put);

    Get get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    Result result = remoteTable.get(get);
    byte[] value = result.getValue(COLUMN_1, QUALIFIER_1);
    assertNotNull(value);
    assertTrue(Bytes.equals(VALUE_1, value));

    // multiput

    List<Put> puts = new ArrayList<Put>();
    put = new Put(ROW_3);
    put.add(COLUMN_2, QUALIFIER_2, VALUE_2);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_2, QUALIFIER_2, VALUE_2);
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
    
    assertTrue(Bytes.equals(Bytes.toBytes("TestRemoteTable"), remoteTable.getTableName()));
  }

  @Test
  public void testDelete() throws IOException {
    Put put = new Put(ROW_3);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    put.add(COLUMN_2, QUALIFIER_2, VALUE_2);
    remoteTable.put(put);

    Get get = new Get(ROW_3);
    get.addFamily(COLUMN_1);
    get.addFamily(COLUMN_2);
    Result result = remoteTable.get(get);
    byte[] value1 = result.getValue(COLUMN_1, QUALIFIER_1);
    byte[] value2 = result.getValue(COLUMN_2, QUALIFIER_2);
    assertNotNull(value1);
    assertTrue(Bytes.equals(VALUE_1, value1));
    assertNotNull(value2);
    assertTrue(Bytes.equals(VALUE_2, value2));

    Delete delete = new Delete(ROW_3);
    delete.deleteColumn(COLUMN_2, QUALIFIER_2);
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
    List<Put> puts = new ArrayList<Put>();
    Put put = new Put(ROW_1);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_2);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_3);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
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
    assertEquals(1, remoteTable.exists(Collections.singletonList(get)).length);
    Delete delete = new Delete(ROW_1);

    remoteTable.checkAndDelete(ROW_1, COLUMN_1, QUALIFIER_1, VALUE_1, delete);
    assertFalse(remoteTable.exists(get));

    Put put = new Put(ROW_1);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    remoteTable.put(put);

    assertTrue(remoteTable.checkAndPut(ROW_1, COLUMN_1, QUALIFIER_1, VALUE_1,
        put));
    assertFalse(remoteTable.checkAndPut(ROW_1, COLUMN_1, QUALIFIER_1, VALUE_2,
        put));
  }
  
  /**
   * Test RemoteHable.Scanner.iterator method  
   */
  @Test
  public void testIteratorScaner() throws IOException {
    List<Put> puts = new ArrayList<Put>();
    Put put = new Put(ROW_1);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_2);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_3);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
    puts.add(put);
    put = new Put(ROW_4);
    put.add(COLUMN_1, QUALIFIER_1, VALUE_1);
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
    headers[0] = new Header("header1", "value1");
    headers[1] = new Header("header2", "value2");
    response = new Response(200, headers);
    assertEquals("value1", response.getHeader("header1"));
    assertFalse(response.hasBody());
    response.setCode(404);
    assertEquals(404, response.getCode());
    headers = new Header[2];
    headers[0] = new Header("header1", "value1.1");
    headers[1] = new Header("header2", "value2");
    response.setHeaders(headers);
    assertEquals("value1.1", response.getHeader("header1"));
    response.setBody(Bytes.toBytes("body"));
    assertTrue(response.hasBody());    
  }
  
}

