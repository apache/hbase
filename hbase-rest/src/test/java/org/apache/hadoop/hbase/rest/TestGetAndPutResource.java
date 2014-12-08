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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestGetAndPutResource extends RowResourceBase {

  private static final MetricsAssertHelper METRICS_ASSERT =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testForbidden() throws IOException, JAXBException {
    conf.set("hbase.rest.readonly", "true");

    Response response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 403);
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 403);
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_2);
    assertEquals(response.getCode(), 403);
    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_2);
    assertEquals(response.getCode(), 403);
    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 403);
    response = checkAndDeletePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 403);
    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 403);

    conf.set("hbase.rest.readonly", "false");

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_2);
    assertEquals(response.getCode(), 200);
    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2, VALUE_3);
    assertEquals(response.getCode(), 200);
    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSingleCellGetPutXML() throws IOException, JAXBException {
    Response response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2, VALUE_3);
    assertEquals(response.getCode(), 200);
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_3);
    response = checkAndDeleteXML(TABLE, ROW_1, COLUMN_1, VALUE_3);
    assertEquals(response.getCode(), 200);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSingleCellGetPutPB() throws IOException, JAXBException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(response.getCode(), 404);
    
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2);

    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2, VALUE_3);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_3);
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_3, VALUE_4);
    assertEquals(response.getCode(), 200);
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSingleCellGetPutBinary() throws IOException {
    final String path = "/" + TABLE + "/" + ROW_3 + "/" + COLUMN_1;
    final byte[] body = Bytes.toBytes(VALUE_3);
    Response response = client.put(path, Constants.MIMETYPE_BINARY, body);
    assertEquals(response.getCode(), 200);
    Thread.yield();

    response = client.get(path, Constants.MIMETYPE_BINARY);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_BINARY, response.getHeader("content-type"));
    assertTrue(Bytes.equals(response.getBody(), body));
    boolean foundTimestampHeader = false;
    for (Header header: response.getHeaders()) {
      if (header.getName().equals("X-Timestamp")) {
        foundTimestampHeader = true;
        break;
      }
    }
    assertTrue(foundTimestampHeader);

    response = deleteRow(TABLE, ROW_3);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSingleCellGetJSON() throws IOException, JAXBException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    Response response = client.put(path, Constants.MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_4));
    assertEquals(response.getCode(), 200);
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    response = deleteRow(TABLE, ROW_4);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testLatestCellGetJSON() throws IOException, JAXBException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_4);
    CellModel cellOne = new CellModel(Bytes.toBytes(COLUMN_1), 1L,
      Bytes.toBytes(VALUE_1));
    CellModel cellTwo = new CellModel(Bytes.toBytes(COLUMN_1), 2L,
      Bytes.toBytes(VALUE_2));
    rowModel.addCell(cellOne);
    rowModel.addCell(cellTwo);
    cellSetModel.addRow(rowModel);
    String jsonString = jsonMapper.writeValueAsString(cellSetModel);
    Response response = client.put(path, Constants.MIMETYPE_JSON,
      Bytes.toBytes(jsonString));
    assertEquals(response.getCode(), 200);
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    CellSetModel cellSet = jsonMapper.readValue(response.getBody(), CellSetModel.class);
    assertTrue(cellSet.getRows().size() == 1);
    assertTrue(cellSet.getRows().get(0).getCells().size() == 1);
    CellModel cell = cellSet.getRows().get(0).getCells().get(0);
    assertEquals(VALUE_2 , Bytes.toString(cell.getValue()));
    assertEquals(2L , cell.getTimestamp());
    response = deleteRow(TABLE, ROW_4);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testURLEncodedKey() throws IOException, JAXBException {
    String urlKey = "http://example.com/foo";
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(TABLE);
    path.append('/');
    path.append(URLEncoder.encode(urlKey, HConstants.UTF8_ENCODING));
    path.append('/');
    path.append(COLUMN_1);
    Response response;
    response = putValueXML(path.toString(), TABLE, urlKey, COLUMN_1,
      VALUE_1);
    assertEquals(response.getCode(), 200);
    checkValueXML(path.toString(), TABLE, urlKey, COLUMN_1, VALUE_1);
  }

  @Test
  public void testNoSuchCF() throws IOException, JAXBException {
    final String goodPath = "/" + TABLE + "/" + ROW_1 + "/" + CFA+":";
    final String badPath = "/" + TABLE + "/" + ROW_1 + "/" + "BAD";
    Response response = client.post(goodPath, Constants.MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_1));
    assertEquals(response.getCode(), 200);
    assertEquals(client.get(goodPath, Constants.MIMETYPE_BINARY).getCode(),
      200);
    assertEquals(client.get(badPath, Constants.MIMETYPE_BINARY).getCode(),
      404);
    assertEquals(client.get(goodPath, Constants.MIMETYPE_BINARY).getCode(),
      200);
  }

  @Test
  public void testMultiCellGetPutXML() throws IOException, JAXBException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);
    Response response = client.put(path, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValueXML(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValueXML(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testMultiCellGetPutPB() throws IOException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    Response response = client.put(path, Constants.MIMETYPE_PROTOBUF,
      cellSetModel.createProtobufOutput());
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValuePB(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValuePB(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testStartEndRowGetPutXML() throws IOException, JAXBException {
    String[] rows = { ROW_1, ROW_2, ROW_3 };
    String[] values = { VALUE_1, VALUE_2, VALUE_3 };
    Response response = null;
    for (int i = 0; i < rows.length; i++) {
      response = putValueXML(TABLE, rows[i], COLUMN_1, values[i]);
      assertEquals(200, response.getCode());
      checkValueXML(TABLE, rows[i], COLUMN_1, values[i]);
    }
    response = getValueXML(TABLE, rows[0], rows[2], COLUMN_1);
    assertEquals(200, response.getCode());
    CellSetModel cellSet = (CellSetModel)
      xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    assertEquals(2, cellSet.getRows().size());
    for (int i = 0; i < cellSet.getRows().size()-1; i++) {
      RowModel rowModel = cellSet.getRows().get(i);
      for (CellModel cell: rowModel.getCells()) {
        assertEquals(COLUMN_1, Bytes.toString(cell.getColumn()));
        assertEquals(values[i], Bytes.toString(cell.getValue()));
      }
    }
    for (String row : rows) {
      response = deleteRow(TABLE, row);
      assertEquals(200, response.getCode());
    }
  }

  @Test
  public void testInvalidCheckParam() throws IOException, JAXBException {
    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);

    final String path = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1 + "?check=blah";

    Response response = client.put(path, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    assertEquals(response.getCode(), 400);
  }

  @Test
  public void testInvalidColumnPut() throws IOException, JAXBException {
    String dummyColumn = "doesnot:exist";
    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(dummyColumn),
      Bytes.toBytes(VALUE_1)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);

    final String path = "/" + TABLE + "/" + ROW_1 + "/" + dummyColumn;

    Response response = client.put(path, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    assertEquals(response.getCode(), 404);
  }

  @Test
  public void testMultiCellGetJson() throws IOException, JAXBException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    String jsonString = jsonMapper.writeValueAsString(cellSetModel);

    Response response = client.put(path, Constants.MIMETYPE_JSON,
      Bytes.toBytes(jsonString));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    checkValueJSON(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueJSON(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValueJSON(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValueJSON(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }
  
  @Test
  public void testMetrics() throws IOException, JAXBException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    Response response = client.put(path, Constants.MIMETYPE_BINARY,
        Bytes.toBytes(VALUE_4));
    assertEquals(response.getCode(), 200);
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    response = deleteRow(TABLE, ROW_4);
    assertEquals(response.getCode(), 200);

    UserProvider userProvider = UserProvider.instantiate(conf);
    METRICS_ASSERT.assertCounterGt("requests", 2l,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());

    METRICS_ASSERT.assertCounterGt("successfulGet", 0l,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());

    METRICS_ASSERT.assertCounterGt("successfulPut", 0l,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());

    METRICS_ASSERT.assertCounterGt("successfulDelete", 0l,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());
  }
  
  @Test
  public void testMultiColumnGetXML() throws Exception {
    String path = "/" + TABLE + "/fakerow";
    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1), Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2), Bytes.toBytes(VALUE_2)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_3), Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);

    Response response = client.put(path, Constants.MIMETYPE_XML, Bytes.toBytes(writer.toString()));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 404);

    // Try getting all the column values at once.
    path = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1 + "," + COLUMN_2 + "," + COLUMN_3;
    response = client.get(path, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    CellSetModel cellSet = (CellSetModel) xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response
        .getBody()));
    assertTrue(cellSet.getRows().size() == 1);
    assertTrue(cellSet.getRows().get(0).getCells().size() == 3);
    List<CellModel> cells = cellSet.getRows().get(0).getCells();

    assertTrue(containsCellModel(cells, COLUMN_1, VALUE_1));
    assertTrue(containsCellModel(cells, COLUMN_2, VALUE_2));
    assertTrue(containsCellModel(cells, COLUMN_3, VALUE_2));
    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
  }

  private boolean containsCellModel(List<CellModel> cells, String column, String value) {
    boolean contains = false;
    for (CellModel cell : cells) {
      if (Bytes.toString(cell.getColumn()).equals(column)
          && Bytes.toString(cell.getValue()).equals(value)) {
        contains = true;
        return contains;
      }
    }
    return contains;
  }

  @Test
  public void testSuffixGlobbingXMLWithNewScanner() throws IOException, JAXBException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);
    Response response = client.put(path, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    StringBuilder query = new StringBuilder();
    query.append('/');
    query.append(TABLE);
    query.append('/');
    query.append("testrow*");
    response = client.get(query.toString(), Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel)
      xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    assertTrue(cellSet.getRows().size() == 2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }

  @Test
  public void testSuffixGlobbingXML() throws IOException, JAXBException {
    String path = "/" + TABLE + "/fakerow";  // deliberate nonexistent row

    CellSetModel cellSetModel = new CellSetModel();
    RowModel rowModel = new RowModel(ROW_1);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_1)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_2)));
    cellSetModel.addRow(rowModel);
    rowModel = new RowModel(ROW_2);
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_1),
      Bytes.toBytes(VALUE_3)));
    rowModel.addCell(new CellModel(Bytes.toBytes(COLUMN_2),
      Bytes.toBytes(VALUE_4)));
    cellSetModel.addRow(rowModel);
    StringWriter writer = new StringWriter();
    xmlMarshaller.marshal(cellSetModel, writer);
    Response response = client.put(path, Constants.MIMETYPE_XML,
      Bytes.toBytes(writer.toString()));
    Thread.yield();

    // make sure the fake row was not actually created
    response = client.get(path, Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 404);

    // check that all of the values were created
    StringBuilder query = new StringBuilder();
    query.append('/');
    query.append(TABLE);
    query.append('/');
    query.append("testrow*");
    query.append('/');
    query.append(COLUMN_1);
    response = client.get(query.toString(), Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel)
      xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    List<RowModel> rows = cellSet.getRows();
    assertTrue(rows.size() == 2);
    for (RowModel row : rows) {
      assertTrue(row.getCells().size() == 1);
      assertEquals(COLUMN_1, Bytes.toString(row.getCells().get(0).getColumn()));
    }
    response = deleteRow(TABLE, ROW_1);
    assertEquals(response.getCode(), 200);
    response = deleteRow(TABLE, ROW_2);
    assertEquals(response.getCode(), 200);
  }
}

