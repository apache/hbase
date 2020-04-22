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
import java.util.HashMap;
import java.util.List;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.Header;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestGetAndPutResource extends RowResourceBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGetAndPutResource.class);

  private static final MetricsAssertHelper METRICS_ASSERT =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testForbidden() throws IOException, JAXBException {
    conf.set("hbase.rest.readonly", "true");

    Response response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(403, response.getCode());
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(403, response.getCode());
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_2);
    assertEquals(403, response.getCode());
    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_2);
    assertEquals(403, response.getCode());
    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(403, response.getCode());
    response = checkAndDeletePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(403, response.getCode());
    response = deleteRow(TABLE, ROW_1);
    assertEquals(403, response.getCode());

    conf.set("hbase.rest.readonly", "false");

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_2);
    assertEquals(200, response.getCode());
    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2, VALUE_3);
    assertEquals(200, response.getCode());
    response = deleteValue(TABLE, ROW_1, COLUMN_1);
    assertEquals(200, response.getCode());
    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testSingleCellGetPutXML() throws IOException, JAXBException {
    Response response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2, VALUE_3);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_3);
    response = checkAndDeleteXML(TABLE, ROW_1, COLUMN_1, VALUE_3);
    assertEquals(200, response.getCode());

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testSingleCellGetPutPB() throws IOException, JAXBException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2);

    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2, VALUE_3);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_3);
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_3, VALUE_4);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testMultipleCellCheckPutPB() throws IOException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    // Add 2 Columns to setup the test
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);

    response = putValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);

    HashMap<String,String> otherCells = new HashMap<>();
    otherCells.put(COLUMN_2,VALUE_3);

    // On Success update both the cells
    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_3, otherCells);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_3);
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_3);

    // On Failure, we dont update any cells
    response = checkAndPutValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_4, otherCells);
    assertEquals(304, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_3);
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_3);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testMultipleCellCheckPutXML() throws IOException, JAXBException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    // Add 2 Columns to setup the test
    response = putValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);

    response = putValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);

    HashMap<String,String> otherCells = new HashMap<>();
    otherCells.put(COLUMN_2,VALUE_3);

    // On Success update both the cells
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_3, otherCells);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_3);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_3);

    // On Failure, we dont update any cells
    response = checkAndPutValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1, VALUE_4, otherCells);
    assertEquals(304, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_3);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_3);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testMultipleCellCheckDeletePB() throws IOException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    // Add 3 Columns to setup the test
    response = putValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);

    response = putValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);

    response = putValuePB(TABLE, ROW_1, COLUMN_3, VALUE_3);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_3, VALUE_3);

    // Deletes the following columns based on Column1 check
    HashMap<String,String> cellsToDelete = new HashMap<>();
    cellsToDelete.put(COLUMN_2,VALUE_2); // Value does not matter
    cellsToDelete.put(COLUMN_3,VALUE_3); // Value does not matter

    // On Success update both the cells
    response = checkAndDeletePB(TABLE, ROW_1, COLUMN_1, VALUE_1, cellsToDelete);
    assertEquals(200, response.getCode());

    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);

    response = getValuePB(TABLE, ROW_1, COLUMN_2);
    assertEquals(404, response.getCode());

    response = getValuePB(TABLE, ROW_1, COLUMN_3);
    assertEquals(404, response.getCode());

    response = putValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);

    response = putValuePB(TABLE, ROW_1, COLUMN_3, VALUE_3);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_3, VALUE_3);

    // On Failure, we dont update any cells
    response = checkAndDeletePB(TABLE, ROW_1, COLUMN_1, VALUE_3, cellsToDelete);
    assertEquals(304, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValuePB(TABLE, ROW_1, COLUMN_3, VALUE_3);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testSingleCellGetPutBinary() throws IOException {
    final String path = "/" + TABLE + "/" + ROW_3 + "/" + COLUMN_1;
    final byte[] body = Bytes.toBytes(VALUE_3);
    Response response = client.put(path, Constants.MIMETYPE_BINARY, body);
    assertEquals(200, response.getCode());
    Thread.yield();

    response = client.get(path, Constants.MIMETYPE_BINARY);
    assertEquals(200, response.getCode());
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
    assertEquals(200, response.getCode());
  }

  @Test
  public void testSingleCellGetJSON() throws IOException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    Response response = client.put(path, Constants.MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_4));
    assertEquals(200, response.getCode());
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    response = deleteRow(TABLE, ROW_4);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testLatestCellGetJSON() throws IOException {
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
    assertEquals(200, response.getCode());
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    CellSetModel cellSet = jsonMapper.readValue(response.getBody(), CellSetModel.class);
    assertTrue(cellSet.getRows().size() == 1);
    assertTrue(cellSet.getRows().get(0).getCells().size() == 1);
    CellModel cell = cellSet.getRows().get(0).getCells().get(0);
    assertEquals(VALUE_2 , Bytes.toString(cell.getValue()));
    assertEquals(2L , cell.getTimestamp());
    response = deleteRow(TABLE, ROW_4);
    assertEquals(200, response.getCode());
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
    assertEquals(200, response.getCode());
    checkValueXML(path.toString(), TABLE, urlKey, COLUMN_1, VALUE_1);
  }

  @Test
  public void testNoSuchCF() throws IOException {
    final String goodPath = "/" + TABLE + "/" + ROW_1 + "/" + CFA+":";
    final String badPath = "/" + TABLE + "/" + ROW_1 + "/" + "BAD";
    Response response = client.post(goodPath, Constants.MIMETYPE_BINARY,
      Bytes.toBytes(VALUE_1));
    assertEquals(200, response.getCode());
    assertEquals(200, client.get(goodPath, Constants.MIMETYPE_BINARY).getCode());
    assertEquals(404, client.get(badPath, Constants.MIMETYPE_BINARY).getCode());
    assertEquals(200, client.get(goodPath, Constants.MIMETYPE_BINARY).getCode());
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
    assertEquals(404, response.getCode());

    // check that all of the values were created
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueXML(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValueXML(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValueXML(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
    response = deleteRow(TABLE, ROW_2);
    assertEquals(200, response.getCode());
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
    assertEquals(404, response.getCode());

    // check that all of the values were created
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValuePB(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValuePB(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValuePB(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
    response = deleteRow(TABLE, ROW_2);
    assertEquals(200, response.getCode());
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
    assertEquals(400, response.getCode());
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
    assertEquals(404, response.getCode());
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
    assertEquals(404, response.getCode());

    // check that all of the values were created
    checkValueJSON(TABLE, ROW_1, COLUMN_1, VALUE_1);
    checkValueJSON(TABLE, ROW_1, COLUMN_2, VALUE_2);
    checkValueJSON(TABLE, ROW_2, COLUMN_1, VALUE_3);
    checkValueJSON(TABLE, ROW_2, COLUMN_2, VALUE_4);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
    response = deleteRow(TABLE, ROW_2);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testMetrics() throws IOException {
    final String path = "/" + TABLE + "/" + ROW_4 + "/" + COLUMN_1;
    Response response = client.put(path, Constants.MIMETYPE_BINARY,
        Bytes.toBytes(VALUE_4));
    assertEquals(200, response.getCode());
    Thread.yield();
    response = client.get(path, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    response = deleteRow(TABLE, ROW_4);
    assertEquals(200, response.getCode());

    UserProvider userProvider = UserProvider.instantiate(conf);
    METRICS_ASSERT.assertCounterGt("requests", 2L,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());

    METRICS_ASSERT.assertCounterGt("successfulGet", 0L,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());

    METRICS_ASSERT.assertCounterGt("successfulPut", 0L,
      RESTServlet.getInstance(conf, userProvider).getMetrics().getSource());

    METRICS_ASSERT.assertCounterGt("successfulDelete", 0L,
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
    assertEquals(404, response.getCode());

    // Try getting all the column values at once.
    path = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1 + "," + COLUMN_2 + "," + COLUMN_3;
    response = client.get(path, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    CellSetModel cellSet =
        (CellSetModel) xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    assertTrue(cellSet.getRows().size() == 1);
    assertTrue(cellSet.getRows().get(0).getCells().size() == 3);
    List<CellModel> cells = cellSet.getRows().get(0).getCells();

    assertTrue(containsCellModel(cells, COLUMN_1, VALUE_1));
    assertTrue(containsCellModel(cells, COLUMN_2, VALUE_2));
    assertTrue(containsCellModel(cells, COLUMN_3, VALUE_2));
    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
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
    assertEquals(404, response.getCode());

    // check that all of the values were created
    StringBuilder query = new StringBuilder();
    query.append('/');
    query.append(TABLE);
    query.append('/');
    query.append("testrow*");
    response = client.get(query.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel)
      xmlUnmarshaller.unmarshal(new ByteArrayInputStream(response.getBody()));
    assertTrue(cellSet.getRows().size() == 2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
    response = deleteRow(TABLE, ROW_2);
    assertEquals(200, response.getCode());
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
    assertEquals(404, response.getCode());

    // check that all of the values were created
    StringBuilder query = new StringBuilder();
    query.append('/');
    query.append(TABLE);
    query.append('/');
    query.append("testrow*");
    query.append('/');
    query.append(COLUMN_1);
    response = client.get(query.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
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
    assertEquals(200, response.getCode());
    response = deleteRow(TABLE, ROW_2);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testAppendXML() throws IOException, JAXBException {
    Response response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    //append cell
    response = appendValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = appendValueXML(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(200, response.getCode());
    checkValueXML(TABLE, ROW_1, COLUMN_1, VALUE_1 + VALUE_2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testAppendPB() throws IOException, JAXBException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    //append cell
    response = appendValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = appendValuePB(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(200, response.getCode());
    checkValuePB(TABLE, ROW_1, COLUMN_1, VALUE_1 + VALUE_2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testAppendJSON() throws IOException, JAXBException {
    Response response = getValueJson(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    //append cell
    response = appendValueJson(TABLE, ROW_1, COLUMN_1, VALUE_1);
    assertEquals(200, response.getCode());
    putValueJson(TABLE, ROW_1, COLUMN_1, VALUE_1);
    response = appendValueJson(TABLE, ROW_1, COLUMN_1, VALUE_2);
    assertEquals(200, response.getCode());
    putValueJson(TABLE, ROW_1, COLUMN_1, VALUE_1 + VALUE_2);

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testIncrementXML() throws IOException, JAXBException {
    Response response = getValueXML(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    //append single cell
    response = incrementValueXML(TABLE, ROW_1, COLUMN_1, VALUE_5);
    assertEquals(200, response.getCode());
    checkIncrementValueXML(TABLE, ROW_1, COLUMN_1, Long.parseLong(VALUE_5));
    response = incrementValueXML(TABLE, ROW_1, COLUMN_1, VALUE_6);
    assertEquals(200, response.getCode());
    checkIncrementValueXML(TABLE, ROW_1, COLUMN_1,
        Long.parseLong(VALUE_5) + Long.parseLong(VALUE_6));

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testIncrementPB() throws IOException, JAXBException {
    Response response = getValuePB(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    //append cell
    response = incrementValuePB(TABLE, ROW_1, COLUMN_1, VALUE_5);
    assertEquals(200, response.getCode());
    checkIncrementValuePB(TABLE, ROW_1, COLUMN_1, Long.parseLong(VALUE_5));
    response = incrementValuePB(TABLE, ROW_1, COLUMN_1, VALUE_6);
    assertEquals(200, response.getCode());
    checkIncrementValuePB(TABLE, ROW_1, COLUMN_1,
        Long.parseLong(VALUE_5) + Long.parseLong(VALUE_6));

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }

  @Test
  public void testIncrementJSON() throws IOException, JAXBException {
    Response response = getValueJson(TABLE, ROW_1, COLUMN_1);
    assertEquals(404, response.getCode());

    //append cell
    response = incrementValueJson(TABLE, ROW_1, COLUMN_1, VALUE_5);
    assertEquals(200, response.getCode());
    checkIncrementValueJSON(TABLE, ROW_1, COLUMN_1, Long.parseLong(VALUE_5));
    response = incrementValueJson(TABLE, ROW_1, COLUMN_1, VALUE_6);
    assertEquals(200, response.getCode());
    checkIncrementValueJSON(TABLE, ROW_1, COLUMN_1,
        Long.parseLong(VALUE_5) + Long.parseLong(VALUE_6));

    response = deleteRow(TABLE, ROW_1);
    assertEquals(200, response.getCode());
  }
}
