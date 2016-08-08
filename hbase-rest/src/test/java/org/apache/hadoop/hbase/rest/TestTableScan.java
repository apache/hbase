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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.provider.JacksonProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

@Category(MediumTests.class)
public class TestTableScan {

  private static final TableName TABLE = TableName.valueOf("TestScanResource");
  private static final String CFA = "a";
  private static final String CFB = "b";
  private static final String COLUMN_1 = CFA + ":1";
  private static final String COLUMN_2 = CFB + ":2";
  private static Client client;
  private static int expectedRows1;
  private static int expectedRows2;
  private static Configuration conf;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.set(Constants.CUSTOM_FILTERS, "CustomFilter:" + CustomFilter.class.getName()); 
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (!admin.tableExists(TABLE)) {
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(CFA));
    htd.addFamily(new HColumnDescriptor(CFB));
    admin.createTable(htd);
    expectedRows1 = TestScannerResource.insertData(conf, TABLE, COLUMN_1, 1.0);
    expectedRows2 = TestScannerResource.insertData(conf, TABLE, COLUMN_2, 0.5);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.getHBaseAdmin().disableTable(TABLE);
    TEST_UTIL.getHBaseAdmin().deleteTable(TABLE);
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSimpleScannerXML() throws IOException, JAXBException, XMLStreamException {
    // Test scanning particular columns
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_LIMIT + "=10");
    Response response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    JAXBContext ctx = JAXBContext.newInstance(CellSetModel.class);
    Unmarshaller ush = ctx.createUnmarshaller();
    CellSetModel model = (CellSetModel) ush.unmarshal(response.getStream());
    int count = TestScannerResource.countCellSet(model);
    assertEquals(10, count);
    checkRowsNotNull(model);

    //Test with no limit.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type")); 
    model = (CellSetModel) ush.unmarshal(response.getStream());
    count = TestScannerResource.countCellSet(model);
    assertEquals(expectedRows1, count);
    checkRowsNotNull(model);

    //Test with start and end row.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aay");
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = (CellSetModel) ush.unmarshal(response.getStream());
    count = TestScannerResource.countCellSet(model);
    RowModel startRow = model.getRows().get(0);
    assertEquals("aaa", Bytes.toString(startRow.getKey()));
    RowModel endRow = model.getRows().get(model.getRows().size() - 1);
    assertEquals("aax", Bytes.toString(endRow.getKey()));
    assertEquals(24, count);
    checkRowsNotNull(model);

    //Test with start row and limit.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_LIMIT + "=15");
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    model = (CellSetModel) ush.unmarshal(response.getStream());
    startRow = model.getRows().get(0);
    assertEquals("aaa", Bytes.toString(startRow.getKey()));
    count = TestScannerResource.countCellSet(model);
    assertEquals(15, count);
    checkRowsNotNull(model);
  }

  @Test
  public void testSimpleScannerJson() throws IOException, JAXBException {
    // Test scanning particular columns with limit.
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_LIMIT + "=20");
    Response response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    ObjectMapper mapper = new JacksonProvider()
        .locateMapper(CellSetModel.class, MediaType.APPLICATION_JSON_TYPE);
    CellSetModel model = mapper.readValue(response.getStream(), CellSetModel.class);
    int count = TestScannerResource.countCellSet(model);
    assertEquals(20, count);
    checkRowsNotNull(model);

    //Test scanning with no limit.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_2);
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    model = mapper.readValue(response.getStream(), CellSetModel.class);
    count = TestScannerResource.countCellSet(model);
    assertEquals(expectedRows2, count);
    checkRowsNotNull(model);

    //Test with start row and end row.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aay");
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = mapper.readValue(response.getStream(), CellSetModel.class);
    RowModel startRow = model.getRows().get(0);
    assertEquals("aaa", Bytes.toString(startRow.getKey()));
    RowModel endRow = model.getRows().get(model.getRows().size() - 1);
    assertEquals("aax", Bytes.toString(endRow.getKey()));
    count = TestScannerResource.countCellSet(model);
    assertEquals(24, count);
    checkRowsNotNull(model);
  }

  /**
   * An example to scan using listener in unmarshaller for XML.
   * @throws Exception the exception
   */
  @Test
  public void testScanUsingListenerUnmarshallerXML() throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_LIMIT + "=10");
    Response response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    JAXBContext context = JAXBContext.newInstance(ClientSideCellSetModel.class, RowModel.class,
      CellModel.class);
    Unmarshaller unmarshaller = context.createUnmarshaller();

    final ClientSideCellSetModel.Listener listener = new ClientSideCellSetModel.Listener() {
      @Override
      public void handleRowModel(ClientSideCellSetModel helper, RowModel row) {
        assertTrue(row.getKey() != null);
        assertTrue(row.getCells().size() > 0);
      }
    };

    // install the callback on all ClientSideCellSetModel instances
    unmarshaller.setListener(new Unmarshaller.Listener() {
        public void beforeUnmarshal(Object target, Object parent) {
            if (target instanceof ClientSideCellSetModel) {
                ((ClientSideCellSetModel) target).setCellSetModelListener(listener);
            }
        }

        public void afterUnmarshal(Object target, Object parent) {
            if (target instanceof ClientSideCellSetModel) {
                ((ClientSideCellSetModel) target).setCellSetModelListener(null);
            }
        }
    });

    // create a new XML parser
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setNamespaceAware(true);
    XMLReader reader = factory.newSAXParser().getXMLReader();
    reader.setContentHandler(unmarshaller.getUnmarshallerHandler());
    assertFalse(ClientSideCellSetModel.listenerInvoked);
    reader.parse(new InputSource(response.getStream()));
    assertTrue(ClientSideCellSetModel.listenerInvoked);

  }

  @Test
  public void testStreamingJSON() throws Exception {
    // Test scanning particular columns with limit.
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_LIMIT + "=20");
    Response response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    ObjectMapper mapper = new JacksonProvider()
        .locateMapper(CellSetModel.class, MediaType.APPLICATION_JSON_TYPE);
    CellSetModel model = mapper.readValue(response.getStream(), CellSetModel.class);
    int count = TestScannerResource.countCellSet(model);
    assertEquals(20, count);
    checkRowsNotNull(model);

    //Test scanning with no limit.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_2);
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    model = mapper.readValue(response.getStream(), CellSetModel.class);
    count = TestScannerResource.countCellSet(model);
    assertEquals(expectedRows2, count);
    checkRowsNotNull(model);

    //Test with start row and end row.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aay");
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());

    count = 0;
    JsonFactory jfactory = new JsonFactory(mapper);
    JsonParser jParser = jfactory.createJsonParser(response.getStream());
    boolean found = false;
    while (jParser.nextToken() != JsonToken.END_OBJECT) {
      if(jParser.getCurrentToken() == JsonToken.START_OBJECT && found) {
        RowModel row = jParser.readValueAs(RowModel.class);
        assertNotNull(row.getKey());
        for (int i = 0; i < row.getCells().size(); i++) {
          if (count == 0) {
            assertEquals("aaa", Bytes.toString(row.getKey()));
          }
          if (count == 23) {
            assertEquals("aax", Bytes.toString(row.getKey()));
          }
          count++;
        }
        jParser.skipChildren();
      } else {
        found = jParser.getCurrentToken() == JsonToken.START_ARRAY;
      }
    }
    assertEquals(24, count);
  }

  @Test
  public void testSimpleScannerProtobuf() throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_LIMIT + "=15");
    Response response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    int rowCount = readProtobufStream(response.getStream());
    assertEquals(15, rowCount);

  //Test with start row and end row.
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aay");
    response = client.get("/" + TABLE + builder.toString(),
      Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    rowCount = readProtobufStream(response.getStream());
    assertEquals(24, rowCount);
  }

  private void checkRowsNotNull(CellSetModel model) {
    for (RowModel row: model.getRows()) {
      assertTrue(row.getKey() != null);
      assertTrue(row.getCells().size() > 0);
    }
  }

  /**
   * Read protobuf stream.
   * @param inputStream the input stream
   * @return The number of rows in the cell set model.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public int readProtobufStream(InputStream inputStream) throws IOException{
    DataInputStream stream = new DataInputStream(inputStream);
    CellSetModel model = null;
    int rowCount = 0;
    try {
      while (true) {
        byte[] lengthBytes = new byte[2];
        int readBytes = stream.read(lengthBytes);
        if (readBytes == -1) {
          break;
        }
        assertEquals(2, readBytes);
        int length = Bytes.toShort(lengthBytes);
        byte[] cellset = new byte[length];
        stream.read(cellset);
        model = new CellSetModel();
        model.getObjectFromMessage(cellset);
        checkRowsNotNull(model);
        rowCount = rowCount + TestScannerResource.countCellSet(model);
      }
    } catch (EOFException exp) {
      exp.printStackTrace();
    } finally {
      stream.close();
    }
    return rowCount;
  }

  @Test
  public void testScanningUnknownColumnJson() throws IOException, JAXBException {
    // Test scanning particular columns with limit.
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=a:test");
    Response response = client.get("/" + TABLE  + builder.toString(),
      Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    ObjectMapper mapper = new JacksonProvider().locateMapper(CellSetModel.class,
      MediaType.APPLICATION_JSON_TYPE);
    CellSetModel model = mapper.readValue(response.getStream(), CellSetModel.class);
    int count = TestScannerResource.countCellSet(model);
    assertEquals(0, count);
  }
  
  @Test
  public void testSimpleFilter() throws IOException, JAXBException {
    StringBuilder builder = new StringBuilder();
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aay");
    builder.append("&");
    builder.append(Constants.SCAN_FILTER + "=" + URLEncoder.encode("PrefixFilter('aab')", "UTF-8"));
    Response response =
        client.get("/" + TABLE + builder.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    JAXBContext ctx = JAXBContext.newInstance(CellSetModel.class);
    Unmarshaller ush = ctx.createUnmarshaller();
    CellSetModel model = (CellSetModel) ush.unmarshal(response.getStream());
    int count = TestScannerResource.countCellSet(model);
    assertEquals(1, count);
    assertEquals("aab", new String(model.getRows().get(0).getCells().get(0).getValue()));
  }

  @Test
  public void testCompoundFilter() throws IOException, JAXBException {
    StringBuilder builder = new StringBuilder();
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_FILTER + "="
        + URLEncoder.encode("PrefixFilter('abc') AND QualifierFilter(=,'binary:1')", "UTF-8"));
    Response response =
        client.get("/" + TABLE + builder.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    JAXBContext ctx = JAXBContext.newInstance(CellSetModel.class);
    Unmarshaller ush = ctx.createUnmarshaller();
    CellSetModel model = (CellSetModel) ush.unmarshal(response.getStream());
    int count = TestScannerResource.countCellSet(model);
    assertEquals(1, count);
    assertEquals("abc", new String(model.getRows().get(0).getCells().get(0).getValue()));
  }

  @Test
  public void testCustomFilter() throws IOException, JAXBException {
    StringBuilder builder = new StringBuilder();
    builder = new StringBuilder();
    builder.append("/a*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_FILTER + "=" + URLEncoder.encode("CustomFilter('abc')", "UTF-8"));
    Response response =
        client.get("/" + TABLE + builder.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    JAXBContext ctx = JAXBContext.newInstance(CellSetModel.class);
    Unmarshaller ush = ctx.createUnmarshaller();
    CellSetModel model = (CellSetModel) ush.unmarshal(response.getStream());
    int count = TestScannerResource.countCellSet(model);
    assertEquals(1, count);
    assertEquals("abc", new String(model.getRows().get(0).getCells().get(0).getValue()));
  }
  
  @Test
  public void testNegativeCustomFilter() throws IOException, JAXBException {
    StringBuilder builder = new StringBuilder();
    builder = new StringBuilder();
    builder.append("/b*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_FILTER + "=" + URLEncoder.encode("CustomFilter('abc')", "UTF-8"));
    Response response =
        client.get("/" + TABLE + builder.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    JAXBContext ctx = JAXBContext.newInstance(CellSetModel.class);
    Unmarshaller ush = ctx.createUnmarshaller();
    CellSetModel model = (CellSetModel) ush.unmarshal(response.getStream());
    int count = TestScannerResource.countCellSet(model);
    // Should return no rows as the filters conflict
    assertEquals(0, count);
  }

  @Test
  public void testReversed() throws IOException, JAXBException {
    StringBuilder builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aay");
    Response response = client.get("/" + TABLE + builder.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    JAXBContext ctx = JAXBContext.newInstance(CellSetModel.class);
    Unmarshaller ush = ctx.createUnmarshaller();
    CellSetModel model = (CellSetModel) ush.unmarshal(response.getStream());
    int count = TestScannerResource.countCellSet(model);
    assertEquals(24, count);
    List<RowModel> rowModels = model.getRows().subList(1, count);

    //reversed
    builder = new StringBuilder();
    builder.append("/*");
    builder.append("?");
    builder.append(Constants.SCAN_COLUMN + "=" + COLUMN_1);
    builder.append("&");
    builder.append(Constants.SCAN_START_ROW + "=aay");
    builder.append("&");
    builder.append(Constants.SCAN_END_ROW + "=aaa");
    builder.append("&");
    builder.append(Constants.SCAN_REVERSED + "=true");
    response = client.get("/" + TABLE + builder.toString(), Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = (CellSetModel) ush.unmarshal(response.getStream());
    count = TestScannerResource.countCellSet(model);
    assertEquals(24, count);
    List<RowModel> reversedRowModels = model.getRows().subList(1, count);

    Collections.reverse(reversedRowModels);
    assertEquals(rowModels.size(), reversedRowModels.size());
    for (int i = 0; i < rowModels.size(); i++) {
      RowModel rowModel = rowModels.get(i);
      RowModel reversedRowModel = reversedRowModels.get(i);

      assertEquals(new String(rowModel.getKey(), "UTF-8"),
          new String(reversedRowModel.getKey(), "UTF-8"));
      assertEquals(new String(rowModel.getCells().get(0).getValue(), "UTF-8"),
          new String(reversedRowModel.getCells().get(0).getValue(), "UTF-8"));
    }
  }

  public static class CustomFilter extends PrefixFilter {
    private byte[] key = null;

    public CustomFilter(byte[] key) {
      super(key);
    }
    
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
      int cmp = Bytes.compareTo(buffer, offset, length, this.key, 0, this.key.length);
      return cmp != 0;
    }

    public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
      byte[] prefix = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
      return new CustomFilter(prefix);
    }
  }

  /**
   * The Class ClientSideCellSetModel which mimics cell set model, and contains listener to perform
   * user defined operations on the row model.
   */
  @XmlRootElement(name = "CellSet")
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class ClientSideCellSetModel implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * This list is not a real list; instead it will notify a listener whenever JAXB has
     * unmarshalled the next row.
     */
    @XmlElement(name="Row")
    private List<RowModel> row;

    static boolean listenerInvoked = false;

    /**
     * Install a listener for row model on this object. If l is null, the listener
     * is removed again.
     */
    public void setCellSetModelListener(final Listener l) {
        row = (l == null) ? null : new ArrayList<RowModel>() {
        private static final long serialVersionUID = 1L;

            public boolean add(RowModel o) {
                l.handleRowModel(ClientSideCellSetModel.this, o);
                listenerInvoked = true;
                return false;
            }
        };
    }

    /**
     * This listener is invoked every time a new row model is unmarshalled.
     */
    public static interface Listener {
        void handleRowModel(ClientSideCellSetModel helper, RowModel rowModel);
    }
  }
}



