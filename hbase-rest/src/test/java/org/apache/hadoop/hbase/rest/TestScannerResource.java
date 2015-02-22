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

package org.apache.hadoop.hbase.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestScannerResource {
  private static final TableName TABLE = TableName.valueOf("TestScannerResource");
  private static final String NONEXISTENT_TABLE = "ThisTableDoesNotExist";
  private static final String CFA = "a";
  private static final String CFB = "b";
  private static final String COLUMN_1 = CFA + ":1";
  private static final String COLUMN_2 = CFB + ":2";

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Marshaller marshaller;
  private static Unmarshaller unmarshaller;
  private static int expectedRows1;
  private static int expectedRows2;
  private static Configuration conf;

  static int insertData(Configuration conf, TableName tableName, String column, double prob)
      throws IOException {
    Random rng = new Random();
    byte[] k = new byte[3];
    byte [][] famAndQf = KeyValue.parseColumn(Bytes.toBytes(column));
    List<Put> puts = new ArrayList<>();
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          if (rng.nextDouble() < prob) {
            k[0] = b1;
            k[1] = b2;
            k[2] = b3;
            Put put = new Put(k);
            put.setDurability(Durability.SKIP_WAL);
            put.add(famAndQf[0], famAndQf[1], k);
            puts.add(put);
          }
        }
      }
    }
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(tableName)) {
      table.put(puts);
    }
    return puts.size();
  }

  static int countCellSet(CellSetModel model) {
    int count = 0;
    Iterator<RowModel> rows = model.getRows().iterator();
    while (rows.hasNext()) {
      RowModel row = rows.next();
      Iterator<CellModel> cells = row.getCells().iterator();
      while (cells.hasNext()) {
        cells.next();
        count++;
      }
    }
    return count;
  }

  private static int fullTableScan(ScannerModel model) throws IOException {
    model.setBatch(100);
    Response response = client.put("/" + TABLE + "/scanner",
      Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);
    int count = 0;
    while (true) {
      response = client.get(scannerURI, Constants.MIMETYPE_PROTOBUF);
      assertTrue(response.getCode() == 200 || response.getCode() == 204);
      if (response.getCode() == 200) {
        assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
        CellSetModel cellSet = new CellSetModel();
        cellSet.getObjectFromMessage(response.getBody());
        Iterator<RowModel> rows = cellSet.getRows().iterator();
        while (rows.hasNext()) {
          RowModel row = rows.next();
          Iterator<CellModel> cells = row.getCells().iterator();
          while (cells.hasNext()) {
            cells.next();
            count++;
          }
        }
      } else {
        break;
      }
    }
    // delete the scanner
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
    return count;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(
      CellModel.class,
      CellSetModel.class,
      RowModel.class,
      ScannerModel.class);
    marshaller = context.createMarshaller();
    unmarshaller = context.createUnmarshaller();
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(CFA));
    htd.addFamily(new HColumnDescriptor(CFB));
    admin.createTable(htd);
    expectedRows1 = insertData(TEST_UTIL.getConfiguration(), TABLE, COLUMN_1, 1.0);
    expectedRows2 = insertData(TEST_UTIL.getConfiguration(), TABLE, COLUMN_2, 0.5);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSimpleScannerXML() throws IOException, JAXBException {
    final int BATCH_SIZE = 5;
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(BATCH_SIZE);
    model.addColumn(Bytes.toBytes(COLUMN_1));
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    byte[] body = Bytes.toBytes(writer.toString());

    // test put operation is forbidden in read-only mode
    conf.set("hbase.rest.readonly", "true");
    Response response = client.put("/" + TABLE + "/scanner",
      Constants.MIMETYPE_XML, body);
    assertEquals(response.getCode(), 403);
    String scannerURI = response.getLocation();
    assertNull(scannerURI);

    // recall previous put operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    response = client.put("/" + TABLE + "/scanner", Constants.MIMETYPE_XML,
      body);
    assertEquals(response.getCode(), 201);
    scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel)
      unmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    // confirm batch size conformance
    assertEquals(countCellSet(cellSet), BATCH_SIZE);

    // test delete scanner operation is forbidden in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 403);

    // recall previous delete scanner operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSimpleScannerPB() throws IOException {
    final int BATCH_SIZE = 10;
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(BATCH_SIZE);
    model.addColumn(Bytes.toBytes(COLUMN_1));

    // test put operation is forbidden in read-only mode
    conf.set("hbase.rest.readonly", "true");
    Response response = client.put("/" + TABLE + "/scanner",
      Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 403);
    String scannerURI = response.getLocation();
    assertNull(scannerURI);

    // recall previous put operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    response = client.put("/" + TABLE + "/scanner",
      Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 201);
    scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    CellSetModel cellSet = new CellSetModel();
    cellSet.getObjectFromMessage(response.getBody());
    // confirm batch size conformance
    assertEquals(countCellSet(cellSet), BATCH_SIZE);

    // test delete scanner operation is forbidden in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 403);

    // recall previous delete scanner operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSimpleScannerBinary() throws IOException {
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(1);
    model.addColumn(Bytes.toBytes(COLUMN_1));

    // test put operation is forbidden in read-only mode
    conf.set("hbase.rest.readonly", "true");
    Response response = client.put("/" + TABLE + "/scanner",
      Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 403);
    String scannerURI = response.getLocation();
    assertNull(scannerURI);

    // recall previous put operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    response = client.put("/" + TABLE + "/scanner",
      Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
    assertEquals(response.getCode(), 201);
    scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell
    response = client.get(scannerURI, Constants.MIMETYPE_BINARY);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_BINARY, response.getHeader("content-type"));
    // verify that data was returned
    assertTrue(response.getBody().length > 0);
    // verify that the expected X-headers are present
    boolean foundRowHeader = false, foundColumnHeader = false,
      foundTimestampHeader = false;
    for (Header header: response.getHeaders()) {
      if (header.getName().equals("X-Row")) {
        foundRowHeader = true;
      } else if (header.getName().equals("X-Column")) {
        foundColumnHeader = true;
      } else if (header.getName().equals("X-Timestamp")) {
        foundTimestampHeader = true;
      }
    }
    assertTrue(foundRowHeader);
    assertTrue(foundColumnHeader);
    assertTrue(foundTimestampHeader);

    // test delete scanner operation is forbidden in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 403);

    // recall previous delete scanner operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    response = client.delete(scannerURI);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testFullTableScan() throws IOException {
    ScannerModel model = new ScannerModel();
    model.addColumn(Bytes.toBytes(COLUMN_1));
    assertEquals(fullTableScan(model), expectedRows1);

    model = new ScannerModel();
    model.addColumn(Bytes.toBytes(COLUMN_2));
    assertEquals(fullTableScan(model), expectedRows2);
  }

  @Test
  public void testTableDoesNotExist() throws IOException, JAXBException {
    ScannerModel model = new ScannerModel();
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    byte[] body = Bytes.toBytes(writer.toString());
    Response response = client.put("/" + NONEXISTENT_TABLE +
      "/scanner", Constants.MIMETYPE_XML, body);
    assertEquals(response.getCode(), 404);
  }

}

