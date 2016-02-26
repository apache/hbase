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
package org.apache.hadoop.hbase.rest;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.collections.keyvalue.AbstractMapEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.provider.JacksonProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class RowResourceBase {

  protected static final String TABLE = "TestRowResource";
  protected static final String CFA = "a";
  protected static final String CFB = "b";
  protected static final String COLUMN_1 = CFA + ":1";
  protected static final String COLUMN_2 = CFB + ":2";
  protected static final String COLUMN_3 = CFA + ":";
  protected static final String ROW_1 = "testrow1";
  protected static final String VALUE_1 = "testvalue1";
  protected static final String ROW_2 = "testrow2";
  protected static final String VALUE_2 = "testvalue2";
  protected static final String ROW_3 = "testrow3";
  protected static final String VALUE_3 = "testvalue3";
  protected static final String ROW_4 = "testrow4";
  protected static final String VALUE_4 = "testvalue4";

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  protected static Client client;
  protected static JAXBContext context;
  protected static Marshaller xmlMarshaller;
  protected static Unmarshaller xmlUnmarshaller;
  protected static Configuration conf;
  protected static ObjectMapper jsonMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster(3);
    REST_TEST_UTIL.startServletContainer(conf);
    context = JAXBContext.newInstance(
        CellModel.class,
        CellSetModel.class,
        RowModel.class);
    xmlMarshaller = context.createMarshaller();
    xmlUnmarshaller = context.createUnmarshaller();
    jsonMapper = new JacksonProvider()
    .locateMapper(CellSetModel.class, MediaType.APPLICATION_JSON_TYPE);
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeMethod() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TableName.valueOf(TABLE))) {
      TEST_UTIL.deleteTable(Bytes.toBytes(TABLE));
    }
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    htd.addFamily(new HColumnDescriptor(CFA));
    htd.addFamily(new HColumnDescriptor(CFB));
    admin.createTable(htd);
  }

  @After
  public void afterMethod() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TableName.valueOf(TABLE))) {
      TEST_UTIL.deleteTable(Bytes.toBytes(TABLE));
    }
  }

  static Response putValuePB(String table, String row, String column,
      String value) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return putValuePB(path.toString(), table, row, column, value);
  }

  static Response putValuePB(String url, String table, String row,
      String column, String value) throws IOException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    Response response = client.put(url, Constants.MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();
    return response;
  }

  protected static void checkValueXML(String url, String table, String row,
      String column, String value) throws IOException, JAXBException {
    Response response = getValueXML(url);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel)
      xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  protected static void checkValueXML(String table, String row, String column,
      String value) throws IOException, JAXBException {
    Response response = getValueXML(table, row, column);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel)
      xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  protected static Response getValuePB(String url) throws IOException {
    Response response = client.get(url, Constants.MIMETYPE_PROTOBUF); 
    return response;
  }

  protected static Response putValueXML(String table, String row, String column,
      String value) throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return putValueXML(path.toString(), table, row, column, value);
  }

  protected static Response putValueXML(String url, String table, String row,
      String column, String value) throws IOException, JAXBException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);
    Response response = client.put(url, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();
    return response;
  }

  protected static Response getValuePB(String table, String row, String column)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return getValuePB(path.toString());
  }

  protected static void checkValuePB(String table, String row, String column,
      String value) throws IOException {
    Response response = getValuePB(table, row, column);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    CellSetModel cellSet = new CellSetModel();
    cellSet.getObjectFromMessage(response.getBody());
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  protected static Response checkAndPutValuePB(String url, String table,
      String row, String column, String valueToCheck, String valueToPut, HashMap<String,String> otherCells)
        throws IOException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(valueToPut)));

    if(otherCells != null) {
      for (Map.Entry<String,String> entry :otherCells.entrySet()) {
        rowModel.addCell(new CellModel(Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue())));
      }
    }

    // This Cell need to be added as last cell.
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(valueToCheck)));

    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    Response response = client.put(url, Constants.MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();
    return response;
  }

  protected static Response checkAndPutValuePB(String table, String row,
      String column, String valueToCheck, String valueToPut) throws IOException {
    return checkAndPutValuePB(table,row,column,valueToCheck,valueToPut,null);
  }
    protected static Response checkAndPutValuePB(String table, String row,
      String column, String valueToCheck, String valueToPut, HashMap<String,String> otherCells) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append("?check=put");
    return checkAndPutValuePB(path.toString(), table, row, column,
      valueToCheck, valueToPut, otherCells);
  }

  protected static Response checkAndPutValueXML(String url, String table,
      String row, String column, String valueToCheck, String valueToPut, HashMap<String,String> otherCells)
        throws IOException, JAXBException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(valueToPut)));

    if(otherCells != null) {
      for (Map.Entry<String,String> entry :otherCells.entrySet()) {
        rowModel.addCell(new CellModel(Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue())));
      }
    }

    // This Cell need to be added as last cell.
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(valueToCheck)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);
    Response response = client.put(url, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();
    return response;
  }

  protected static Response checkAndPutValueXML(String table, String row,
                                                String column, String valueToCheck, String valueToPut)
          throws IOException, JAXBException {
    return checkAndPutValueXML(table,row,column,valueToCheck,valueToPut, null);
  }

  protected static Response checkAndPutValueXML(String table, String row,
      String column, String valueToCheck, String valueToPut, HashMap<String,String> otherCells)
        throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append("?check=put");
    return checkAndPutValueXML(path.toString(), table, row, column,
      valueToCheck, valueToPut, otherCells);
  }

  protected static Response checkAndDeleteXML(String url, String table,
      String row, String column, String valueToCheck)
        throws IOException, JAXBException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(valueToCheck)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);
    Response response = client.put(url, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();
    return response;
  }

  protected static Response checkAndDeleteXML(String table, String row,
      String column, String valueToCheck) throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append("?check=delete");
    return checkAndDeleteXML(path.toString(), table, row, column, valueToCheck);
  }

  protected static Response checkAndDeleteJson(String table, String row,
      String column, String valueToCheck) throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append("?check=delete");
    return checkAndDeleteJson(path.toString(), table, row, column, valueToCheck);
  }

  protected static Response checkAndDeleteJson(String url, String table,
      String row, String column, String valueToCheck)
        throws IOException, JAXBException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(valueToCheck)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    String jsonString = jsonMapper.writeValueAsString(cellSetModel);
    Response response = client.put(url, Constants.MIMETYPE_JSON,
      Bytes.toBytes(jsonString));
    Thread.yield();
    return response;
  }

  protected static Response checkAndDeletePB(String table, String row,
      String column, String value) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append("?check=delete");
    return checkAndDeleteValuePB(path.toString(), table, row, column, value);
  }

  protected static Response checkAndDeleteValuePB(String url, String table,
      String row, String column, String valueToCheck)
      throws IOException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column), Bytes
        .toBytes(valueToCheck)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    Response response = client.put(url, Constants.MIMETYPE_PROTOBUF,
        cellSetModel.createProtobufOutput());
    Thread.yield();
    return response;
  }

  protected static Response getValueXML(String table, String startRow,
      String endRow, String column) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(startRow);
    path.append(",");
    path.append(endRow);
    path.append('/');
    path.append(column);
    return getValueXML(path.toString());
  }

  protected static Response getValueXML(String url) throws IOException {
    Response response = client.get(url, Constants.MIMETYPE_XML);
    return response;
  }

  protected static Response getValueJson(String url) throws IOException {
    Response response = client.get(url, Constants.MIMETYPE_JSON);
    return response;
  }

  protected static Response deleteValue(String table, String row, String column)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    Response response = client.delete(path.toString());
    Thread.yield();
    return response;
  }

  protected static Response getValueXML(String table, String row, String column)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return getValueXML(path.toString());
  }

  protected static Response deleteRow(String table, String row)
      throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    Response response = client.delete(path.toString());
    Thread.yield();
    return response;
  }

  protected static Response getValueJson(String table, String row,
      String column) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return getValueJson(path.toString());
  }

  protected static void checkValueJSON(String table, String row, String column,
      String value) throws IOException, JAXBException {
    Response response = getValueJson(table, row, column);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    ObjectMapper mapper = new JacksonProvider()
    .locateMapper(CellSetModel.class, MediaType.APPLICATION_JSON_TYPE);
    CellSetModel cellSet = mapper.readValue(response.getBody(), CellSetModel.class);
    RowModel rowModel = cellSet.getRows().get(0);
    CellModel cell = rowModel.getCells().get(0);
    assertEquals(Bytes.toString(cell.getColumn()), column);
    assertEquals(Bytes.toString(cell.getValue()), value);
  }

  protected static Response putValueJson(String table, String row, String column,
      String value) throws IOException, JAXBException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(table);
    path.append('/');
    path.append(row);
    path.append('/');
    path.append(column);
    return putValueJson(path.toString(), table, row, column, value);
  }

  protected static Response putValueJson(String url, String table, String row, String column,
      String value) throws IOException, JAXBException {
    RowModel rowModel = new RowModel(row);
    rowModel.addCell(new CellModel(Bytes.toBytes(column),
      Bytes.toBytes(value)));
    CellSetModel cellSetModel = new CellSetModel();
    cellSetModel.addRow(rowModel);
    String jsonString = jsonMapper.writeValueAsString(cellSetModel);
    Response response = client.put(url, Constants.MIMETYPE_JSON,
      Bytes.toBytes(jsonString));
    Thread.yield();
    return response;
  }

}
