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
package org.apache.hadoop.hbase.thrift2;

import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_INFO_SERVER_PORT;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.thrift.Constants;
import org.apache.hadoop.hbase.thrift2.client.ThriftConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RestTests.class, MediumTests.class})

public class TestThriftConnection {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestThriftConnection.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftConnection.class);

  private static final byte[] FAMILYA = Bytes.toBytes("fa");
  private static final byte[] FAMILYB = Bytes.toBytes("fb");
  private static final byte[] FAMILYC = Bytes.toBytes("fc");
  private static final byte[] FAMILYD = Bytes.toBytes("fd");

  private static final byte[] ROW_1 = Bytes.toBytes("testrow1");
  private static final byte[] ROW_2 = Bytes.toBytes("testrow2");
  private static final byte[] ROW_3 = Bytes.toBytes("testrow3");
  private static final byte[] ROW_4 = Bytes.toBytes("testrow4");

  private static final byte[] QUALIFIER_1 = Bytes.toBytes("1");
  private static final byte[] QUALIFIER_2 = Bytes.toBytes("2");
  private static final byte[] VALUE_1 = Bytes.toBytes("testvalue1");
  private static final byte[] VALUE_2 = Bytes.toBytes("testvalue2");

  private static final long ONE_HOUR = 60 * 60 * 1000;
  private static final long TS_2 = System.currentTimeMillis();
  private static final long TS_1 = TS_2 - ONE_HOUR;


  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static ThriftServer thriftServer;

  protected static ThriftServer thriftHttpServer;

  protected static int thriftPort;
  protected static int httpPort;

  protected static Connection thriftConnection;
  protected static Connection thriftHttpConnection;

  private static Admin thriftAdmin;

  private static ThriftServer startThriftServer(int port, boolean useHttp) {
    Configuration thriftServerConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    thriftServerConf.setInt(Constants.PORT_CONF_KEY, port);
    if (useHttp) {
      thriftServerConf.setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    }
    ThriftServer server = new ThriftServer(thriftServerConf);
    Thread thriftServerThread = new Thread(() -> {
      try{
        server.run();
      } catch (Exception t) {
        LOG.error("Thrift Server failed", t);
      }
    });
    thriftServerThread.setDaemon(true);
    thriftServerThread.start();
    if (useHttp) {
      TEST_UTIL.waitFor(10000, () -> server.getHttpServer() != null);
    } else {
      TEST_UTIL.waitFor(10000, () -> server.getTserver() != null);
    }
    return server;
  }

  private static Connection createConnection(int port, boolean useHttp) throws IOException {
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL,
        ThriftConnection.class.getName());
    if (useHttp) {
      conf.set(Constants.HBASE_THRIFT_CLIENT_BUIDLER_CLASS,
          ThriftConnection.HTTPThriftClientBuilder.class.getName());
    }
    String host = HConstants.LOCALHOST;
    if (useHttp) {
      host = "http://" + host;
    }
    conf.set(Constants.HBASE_THRIFT_SERVER_NAME, host);
    conf.setInt(Constants.HBASE_THRIFT_SERVER_PORT, port);
    return ConnectionFactory.createConnection(conf);
  }


  @BeforeClass
  public static void setUp() throws Exception {
    // Do not start info server
    TEST_UTIL.getConfiguration().setInt(THRIFT_INFO_SERVER_PORT , -1);
    TEST_UTIL.startMiniCluster();
    thriftPort = HBaseTestingUtility.randomFreePort();
    httpPort = HBaseTestingUtility.randomFreePort();
    // Start a thrift server
    thriftServer = startThriftServer(thriftPort, false);
    // Start an HTTP thrift server
    thriftHttpServer = startThriftServer(httpPort, true);
    thriftConnection = createConnection(thriftPort, false);
    thriftHttpConnection = createConnection(httpPort, true);
    thriftAdmin = thriftConnection.getAdmin();
    LOG.info("TS_1=" + TS_1);
    LOG.info("TS_2=" + TS_1);

  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (thriftAdmin != null) {
      thriftAdmin.close();
    }
    if (thriftHttpConnection != null) {
      thriftHttpConnection.close();
    }
    if (thriftConnection != null) {
      thriftConnection.close();
    }
    if (thriftHttpServer != null) {
      thriftHttpServer.stop();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testThrfitAdmin() throws Exception {
    testThriftAdmin(thriftConnection, "testThrfitAdminNamesapce", "testThrfitAdminTable");
    testThriftAdmin(thriftHttpConnection, "testThrfitHttpAdminNamesapce",
        "testThrfitHttpAdminTable");
  }

  @Test
  public void testGet() throws Exception {
    testGet(thriftConnection, "testGetTable");
    testGet(thriftHttpConnection, "testGetHttpTable");

  }

  public void testGet(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      Get get = new Get(ROW_1);
      Result result = table.get(get);
      byte[] value1 = result.getValue(FAMILYA, QUALIFIER_1);
      byte[] value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1));
      assertNull(value2);

      get = new Get(ROW_1);
      get.addFamily(FAMILYC);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNull(value1);
      assertNull(value2);

      get = new Get(ROW_1);
      get.addColumn(FAMILYA, QUALIFIER_1);
      get.addColumn(FAMILYB, QUALIFIER_2);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1));
      assertNull(value2);

      get = new Get(ROW_2);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
      assertNotNull(value2);
      assertTrue(Bytes.equals(VALUE_2, value2));

      get = new Get(ROW_2);
      get.addFamily(FAMILYA);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
      assertNull(value2);

      get = new Get(ROW_2);
      get.addColumn(FAMILYA, QUALIFIER_1);
      get.addColumn(FAMILYB, QUALIFIER_2);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_2, value1)); // @TS_2
      assertNotNull(value2);
      assertTrue(Bytes.equals(VALUE_2, value2));

      // test timestamp

      get = new Get(ROW_2);
      get.addFamily(FAMILYA);
      get.addFamily(FAMILYB);
      get.setTimestamp(TS_1);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1)); // @TS_1
      assertNull(value2);

      // test timerange

      get = new Get(ROW_2);
      get.addFamily(FAMILYA);
      get.addFamily(FAMILYB);
      get.setTimeRange(0, TS_1 + 1);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1)); // @TS_1
      assertNull(value2);

      // test maxVersions

      get = new Get(ROW_2);
      get.addFamily(FAMILYA);
      get.setMaxVersions(2);
      result = table.get(get);
      int count = 0;
      for (Cell kv: result.listCells()) {
        if (CellUtil.matchingFamily(kv, FAMILYA) && TS_1 == kv.getTimestamp()) {
          assertTrue(CellUtil.matchingValue(kv, VALUE_1)); // @TS_1
          count++;
        }
        if (CellUtil.matchingFamily(kv, FAMILYA) && TS_2 == kv.getTimestamp()) {
          assertTrue(CellUtil.matchingValue(kv, VALUE_2)); // @TS_2
          count++;
        }
      }
      assertEquals(2, count);
    }

  }

  @Test
  public void testHBASE22011()throws Exception{
    testHBASE22011(thriftConnection, "testHBASE22011Table");
    testHBASE22011(thriftHttpConnection, "testHBASE22011HttpTable");
  }

  public void testHBASE22011(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      Get get = new Get(ROW_2);
      Result result = table.get(get);
      assertEquals(2, result.listCells().size());

      ColumnCountGetFilter filter = new ColumnCountGetFilter(1);
      get.setFilter(filter);
      result = table.get(get);
      assertEquals(1, result.listCells().size());
    }
  }

  @Test
  public void testMultiGet() throws Exception {
    testMultiGet(thriftConnection, "testMultiGetTable");
    testMultiGet(thriftHttpConnection, "testMultiGetHttpTable");
  }

  public void testMultiGet(Connection connection, String tableName) throws Exception {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      ArrayList<Get> gets = new ArrayList<>(2);
      gets.add(new Get(ROW_1));
      gets.add(new Get(ROW_2));
      Result[] results = table.get(gets);
      assertNotNull(results);
      assertEquals(2, results.length);
      assertEquals(1, results[0].size());
      assertEquals(2, results[1].size());

      //Test Versions
      gets = new ArrayList<>(2);
      Get g = new Get(ROW_1);
      g.setMaxVersions(3);
      gets.add(g);
      Get get2 = new Get(ROW_2);
      get2.setMaxVersions(3);
      gets.add(get2);
      results = table.get(gets);
      assertNotNull(results);
      assertEquals(2, results.length);
      assertEquals(1, results[0].size());
      assertEquals(3, results[1].size());

      gets = new ArrayList<>(1);
      gets.add(new Get(Bytes.toBytes("RESALLYREALLYNOTTHERE")));
      results = table.get(gets);
      assertNotNull(results);
      assertTrue(results[0].isEmpty());

      gets = new ArrayList<>(3);
      gets.add(new Get(Bytes.toBytes("RESALLYREALLYNOTTHERE")));
      gets.add(new Get(ROW_1));
      gets.add(new Get(ROW_2));
      results = table.get(gets);
      assertNotNull(results);
      assertEquals(3, results.length);
      assertTrue(results[0].isEmpty());
    }

  }

  @Test
  public void testPut() throws Exception {
    testPut(thriftConnection, "testPutTable");
    testPut(thriftHttpConnection, "testPutHttpTable");
  }

  public void testPut(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      Put put = new Put(ROW_3);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      table.put(put);

      Get get = new Get(ROW_3);
      get.addFamily(FAMILYA);
      Result result = table.get(get);
      byte[] value = result.getValue(FAMILYA, QUALIFIER_1);
      assertNotNull(value);
      assertTrue(Bytes.equals(VALUE_1, value));

      // multiput

      List<Put> puts = new ArrayList<>(3);
      put = new Put(ROW_3);
      put.addColumn(FAMILYB, QUALIFIER_2, VALUE_2);
      puts.add(put);
      put = new Put(ROW_4);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_4);
      put.addColumn(FAMILYB, QUALIFIER_2, VALUE_2);
      puts.add(put);
      table.put(puts);

      get = new Get(ROW_3);
      get.addFamily(FAMILYB);
      result = table.get(get);
      value = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value);
      assertTrue(Bytes.equals(VALUE_2, value));
      get = new Get(ROW_4);
      result = table.get(get);
      value = result.getValue(FAMILYA, QUALIFIER_1);
      assertNotNull(value);
      assertTrue(Bytes.equals(VALUE_1, value));
      value = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value);
      assertTrue(Bytes.equals(VALUE_2, value));
    }
  }

  @Test
  public void testDelete() throws Exception {
    testDelete(thriftConnection, "testDeleteTable");
    testDelete(thriftHttpConnection, "testDeleteHttpTable");
  }

  public void testDelete(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      Put put = new Put(ROW_3);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      put.addColumn(FAMILYB, QUALIFIER_2, VALUE_2);
      put.addColumn(FAMILYC, QUALIFIER_1, VALUE_1);
      put.addColumn(FAMILYC, QUALIFIER_2, VALUE_2);
      table.put(put);

      Get get = new Get(ROW_3);
      get.addFamily(FAMILYA);
      get.addFamily(FAMILYB);
      get.addFamily(FAMILYC);
      Result result = table.get(get);
      byte[] value1 = result.getValue(FAMILYA, QUALIFIER_1);
      byte[] value2 = result.getValue(FAMILYB, QUALIFIER_2);
      byte[] value3 = result.getValue(FAMILYC, QUALIFIER_1);
      byte[] value4 = result.getValue(FAMILYC, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1));
      assertNotNull(value2);
      assertTrue(Bytes.equals(VALUE_2, value2));
      assertNotNull(value3);
      assertTrue(Bytes.equals(VALUE_1, value3));
      assertNotNull(value4);
      assertTrue(Bytes.equals(VALUE_2, value4));

      Delete delete = new Delete(ROW_3);
      delete.addColumn(FAMILYB, QUALIFIER_2);
      table.delete(delete);

      get = new Get(ROW_3);
      get.addFamily(FAMILYA);
      get.addFamily(FAMILYB);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1));
      assertNull(value2);

      delete = new Delete(ROW_3);
      delete.setTimestamp(1L);
      table.delete(delete);

      get = new Get(ROW_3);
      get.addFamily(FAMILYA);
      get.addFamily(FAMILYB);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1));
      assertNull(value2);

      // Delete column family from row
      delete = new Delete(ROW_3);
      delete.addFamily(FAMILYC);
      table.delete(delete);

      get = new Get(ROW_3);
      get.addFamily(FAMILYC);
      result = table.get(get);
      value3 = result.getValue(FAMILYC, QUALIFIER_1);
      value4 = result.getValue(FAMILYC, QUALIFIER_2);
      assertNull(value3);
      assertNull(value4);

      delete = new Delete(ROW_3);
      table.delete(delete);

      get = new Get(ROW_3);
      get.addFamily(FAMILYA);
      get.addFamily(FAMILYB);
      result = table.get(get);
      value1 = result.getValue(FAMILYA, QUALIFIER_1);
      value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNull(value1);
      assertNull(value2);
    }

  }

  @Test
  public void testScanner() throws Exception {
    testScanner(thriftConnection, "testScannerTable");
    testScanner(thriftHttpConnection, "testScannerHttpTable");
  }

  public void testScanner(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      List<Put> puts = new ArrayList<>(4);
      Put put = new Put(ROW_1);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_2);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_3);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_4);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      table.put(puts);

      ResultScanner scanner = table.getScanner(new Scan());

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
      assertTrue(results == null || results.length == 0);
      scanner.close();

      scanner = table.getScanner(FAMILYA);
      results = scanner.next(4);
      assertNotNull(results);
      assertEquals(4, results.length);
      assertTrue(Bytes.equals(ROW_1, results[0].getRow()));
      assertTrue(Bytes.equals(ROW_2, results[1].getRow()));
      assertTrue(Bytes.equals(ROW_3, results[2].getRow()));
      assertTrue(Bytes.equals(ROW_4, results[3].getRow()));

      scanner.close();

      scanner = table.getScanner(FAMILYA,QUALIFIER_1);
      results = scanner.next(4);
      assertNotNull(results);
      assertEquals(4, results.length);
      assertTrue(Bytes.equals(ROW_1, results[0].getRow()));
      assertTrue(Bytes.equals(ROW_2, results[1].getRow()));
      assertTrue(Bytes.equals(ROW_3, results[2].getRow()));
      assertTrue(Bytes.equals(ROW_4, results[3].getRow()));
      scanner.close();
    }

  }

  @Test
  public void testCheckAndDelete() throws Exception {
    testCheckAndDelete(thriftConnection, "testCheckAndDeleteTable");
    testCheckAndDelete(thriftHttpConnection, "testCheckAndDeleteHttpTable");
  }


  public void testCheckAndDelete(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      Get get = new Get(ROW_1);
      Result result = table.get(get);
      byte[] value1 = result.getValue(FAMILYA, QUALIFIER_1);
      byte[] value2 = result.getValue(FAMILYB, QUALIFIER_2);
      assertNotNull(value1);
      assertTrue(Bytes.equals(VALUE_1, value1));
      assertNull(value2);
      assertTrue(table.exists(get));
      assertEquals(1, table.existsAll(Collections.singletonList(get)).length);
      Delete delete = new Delete(ROW_1);

      table.checkAndMutate(ROW_1, FAMILYA).qualifier(QUALIFIER_1)
          .ifEquals(VALUE_1).thenDelete(delete);
      assertFalse(table.exists(get));

      Put put = new Put(ROW_1);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      table.put(put);

      assertTrue(table.checkAndMutate(ROW_1, FAMILYA).qualifier(QUALIFIER_1)
          .ifEquals(VALUE_1).thenPut(put));
      assertFalse(table.checkAndMutate(ROW_1, FAMILYA).qualifier(QUALIFIER_1)
          .ifEquals(VALUE_2).thenPut(put));
    }

  }

  @Test
  public void testIteratorScaner() throws Exception {
    testIteratorScanner(thriftConnection, "testIteratorScanerTable");
    testIteratorScanner(thriftHttpConnection, "testIteratorScanerHttpTable");
  }

  public void testIteratorScanner(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      List<Put> puts = new ArrayList<>(4);
      Put put = new Put(ROW_1);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_2);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_3);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_4);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      table.put(puts);
      Scan scan = new Scan();
      scan.setCaching(1);
      ResultScanner scanner = table.getScanner(scan);
      Iterator<Result> iterator = scanner.iterator();
      assertTrue(iterator.hasNext());
      int counter = 0;
      while (iterator.hasNext()) {
        iterator.next();
        counter++;
      }
      assertEquals(4, counter);
    }

  }

  @Test
  public void testReverseScan() throws Exception {
    testReverseScan(thriftConnection, "testReverseScanTable");
    testReverseScan(thriftHttpConnection, "testReverseScanHttpTable");
  }

  public void testReverseScan(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      List<Put> puts = new ArrayList<>(4);
      Put put = new Put(ROW_1);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_2);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_3);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      put = new Put(ROW_4);
      put.addColumn(FAMILYA, QUALIFIER_1, VALUE_1);
      puts.add(put);
      table.put(puts);
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.setCaching(1);
      ResultScanner scanner = table.getScanner(scan);
      Iterator<Result> iterator = scanner.iterator();
      assertTrue(iterator.hasNext());
      int counter = 0;
      Result lastResult = null;
      while (iterator.hasNext()) {
        Result current = iterator.next();
        if (lastResult != null) {
          assertTrue(Bytes.compareTo(lastResult.getRow(), current.getRow()) > 0);
        }
        lastResult = current;
        counter++;
      }
      assertEquals(4, counter);
    }

  }


  @Test
  public void testScanWithFilters() throws Exception {
    testScanWithFilters(thriftConnection, "testScanWithFiltersTable");
    testScanWithFilters(thriftHttpConnection, "testScanWithFiltersHttpTable");
  }

  private void testScanWithFilters(Connection connection, String tableName) throws IOException {
    createTable(thriftAdmin, tableName);
    try (Table table = connection.getTable(TableName.valueOf(tableName))){
      FilterList filterList = new FilterList();
      PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("testrow"));
      ColumnValueFilter columnValueFilter = new ColumnValueFilter(FAMILYA, QUALIFIER_1,
          CompareOperator.EQUAL, VALUE_1);
      filterList.addFilter(prefixFilter);
      filterList.addFilter(columnValueFilter);
      Scan scan = new Scan();
      scan.setMaxVersions(2);
      scan.setFilter(filterList);
      ResultScanner scanner = table.getScanner(scan);
      Iterator<Result> iterator = scanner.iterator();
      assertTrue(iterator.hasNext());
      int counter = 0;
      while (iterator.hasNext()) {
        Result result = iterator.next();
        counter += result.size();
      }
      assertEquals(2, counter);
    }
  }


  private TableDescriptor createTable(Admin admin, String tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder
        .newBuilder(TableName.valueOf(tableName));
    ColumnFamilyDescriptorBuilder familyABuilder = ColumnFamilyDescriptorBuilder
        .newBuilder(FAMILYA);
    familyABuilder.setMaxVersions(3);
    ColumnFamilyDescriptorBuilder familyBBuilder = ColumnFamilyDescriptorBuilder
        .newBuilder(FAMILYB);
    familyBBuilder.setMaxVersions(3);
    ColumnFamilyDescriptorBuilder familyCBuilder = ColumnFamilyDescriptorBuilder
        .newBuilder(FAMILYC);
    familyCBuilder.setMaxVersions(3);
    builder.setColumnFamily(familyABuilder.build());
    builder.setColumnFamily(familyBBuilder.build());
    builder.setColumnFamily(familyCBuilder.build());
    TableDescriptor tableDescriptor = builder.build();
    admin.createTable(tableDescriptor);
    try (Table table = TEST_UTIL.getConnection().getTable(TableName.valueOf(tableName))) {
      Put put = new Put(ROW_1);
      put.addColumn(FAMILYA, QUALIFIER_1, TS_2, VALUE_1);
      table.put(put);
      put = new Put(ROW_2);
      put.addColumn(FAMILYA, QUALIFIER_1, TS_1, VALUE_1);
      put.addColumn(FAMILYA, QUALIFIER_1, TS_2, VALUE_2);
      put.addColumn(FAMILYB, QUALIFIER_2, TS_2, VALUE_2);
      table.put(put);

    }
    return tableDescriptor;

  }

  private void testThriftAdmin(Connection connection, String namespace, String table)
      throws Exception {
    try (Admin admin = connection.getAdmin()){
      //create name space
      NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
      namespaceDescriptor.setConfiguration("key1", "value1");
      namespaceDescriptor.setConfiguration("key2", "value2");
      admin.createNamespace(namespaceDescriptor);
      //list namespace
      NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
      boolean found = false;
      for (NamespaceDescriptor nd : namespaceDescriptors) {
        if (nd.getName().equals(namespace)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
      //modify namesapce
      namespaceDescriptor.setConfiguration("kye3", "value3");
      admin.modifyNamespace(namespaceDescriptor);
      //get namespace
      NamespaceDescriptor namespaceDescriptorReturned = admin.getNamespaceDescriptor(namespace);
      assertTrue(namespaceDescriptorReturned.getConfiguration().size() == 3);
      //create table
      TableDescriptor tableDescriptor = createTable(admin, table);
      //modify table
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor);
      builder.setDurability(Durability.ASYNC_WAL);
      admin.modifyTable(builder.build());
      //modify column family
      ColumnFamilyDescriptor familyA = tableDescriptor.getColumnFamily(FAMILYA);
      ColumnFamilyDescriptorBuilder familyABuilder = ColumnFamilyDescriptorBuilder
          .newBuilder(familyA);
      familyABuilder.setInMemory(true);
      admin.modifyColumnFamily(tableDescriptor.getTableName(), familyABuilder.build());
      //add column family
      ColumnFamilyDescriptorBuilder familyDBuilder = ColumnFamilyDescriptorBuilder
          .newBuilder(FAMILYD);
      familyDBuilder.setDataBlockEncoding(DataBlockEncoding.PREFIX);
      admin.addColumnFamily(tableDescriptor.getTableName(), familyDBuilder.build());
      //get table descriptor
      TableDescriptor tableDescriptorReturned = admin.getDescriptor(tableDescriptor.getTableName());
      assertTrue(tableDescriptorReturned.getColumnFamilies().length == 4);
      assertTrue(tableDescriptorReturned.getDurability() ==  Durability.ASYNC_WAL);
      ColumnFamilyDescriptor columnFamilyADescriptor1Returned = tableDescriptorReturned
          .getColumnFamily(FAMILYA);
      assertTrue(columnFamilyADescriptor1Returned.isInMemory() == true);
      //delete column family
      admin.deleteColumnFamily(tableDescriptor.getTableName(), FAMILYA);
      tableDescriptorReturned = admin.getDescriptor(tableDescriptor.getTableName());
      assertTrue(tableDescriptorReturned.getColumnFamilies().length == 3);
      //disable table
      admin.disableTable(tableDescriptor.getTableName());
      assertTrue(admin.isTableDisabled(tableDescriptor.getTableName()));
      //enable table
      admin.enableTable(tableDescriptor.getTableName());
      assertTrue(admin.isTableEnabled(tableDescriptor.getTableName()));
      assertTrue(admin.isTableAvailable(tableDescriptor.getTableName()));
      //truncate table
      admin.disableTable(tableDescriptor.getTableName());
      admin.truncateTable(tableDescriptor.getTableName(), true);
      assertTrue(admin.isTableAvailable(tableDescriptor.getTableName()));
      //delete table
      admin.disableTable(tableDescriptor.getTableName());
      admin.deleteTable(tableDescriptor.getTableName());
      assertFalse(admin.tableExists(tableDescriptor.getTableName()));
      //delete namespace
      admin.deleteNamespace(namespace);
      namespaceDescriptors = admin.listNamespaceDescriptors();
      // should have 2 namespace, default and hbase
      found = false;
      for (NamespaceDescriptor nd : namespaceDescriptors) {
        if (nd.getName().equals(namespace)) {
          found = true;
          break;
        }
      }
      assertTrue(found == false);
    }
  }
}
