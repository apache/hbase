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
package org.apache.hadoop.hbase.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.ScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.SimpleScanLabelGenerator;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestScannersWithLabels {
  private static final TableName TABLE = TableName.valueOf("TestScannersWithLabels");
  private static final String CFA = "a";
  private static final String CFB = "b";
  private static final String COLUMN_1 = CFA + ":1";
  private static final String COLUMN_2 = CFB + ":2";
  private final static String TOPSECRET = "topsecret";
  private final static String PUBLIC = "public";
  private final static String PRIVATE = "private";
  private final static String CONFIDENTIAL = "confidential";
  private final static String SECRET = "secret";
  private static User SUPERUSER;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Marshaller marshaller;
  private static Unmarshaller unmarshaller;
  private static Configuration conf;

  private static int insertData(TableName tableName, String column, double prob) throws IOException {
    byte[] k = new byte[3];
    byte[][] famAndQf = KeyValue.parseColumn(Bytes.toBytes(column));

    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.setDurability(Durability.SKIP_WAL);
      put.add(famAndQf[0], famAndQf[1], k);
      put.setCellVisibility(new CellVisibility("(" + SECRET + "|" + CONFIDENTIAL + ")" + "&" + "!"
          + TOPSECRET));
      puts.add(put);
    }
    try (Table table = new HTable(TEST_UTIL.getConfiguration(), tableName)) {
      table.put(puts);
    }
    return puts.size();
  }

  private static int countCellSet(CellSetModel model) {
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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SUPERUSER = User.createUserForTesting(conf, "admin",
        new String[] { "supergroup" });
    conf = TEST_UTIL.getConfiguration();
    conf.setClass(VisibilityUtils.VISIBILITY_LABEL_GENERATOR_CLASS,
        SimpleScanLabelGenerator.class, ScanLabelGenerator.class);
    conf.setInt("hfile.format.version", 3);
    conf.set("hbase.superuser", SUPERUSER.getShortName());
    conf.set("hbase.coprocessor.master.classes", VisibilityController.class.getName());
    conf.set("hbase.coprocessor.region.classes", VisibilityController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(VisibilityConstants.LABELS_TABLE_NAME.getName(), 50000);
    createLabels();
    setAuths();
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost", REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(CellModel.class, CellSetModel.class, RowModel.class,
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
    insertData(TABLE, COLUMN_1, 1.0);
    insertData(TABLE, COLUMN_2, 0.5);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void createLabels() throws IOException, InterruptedException {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action = new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, CONFIDENTIAL, PRIVATE, PUBLIC, TOPSECRET };
        try {
          VisibilityClient.addLabels(conf, labels);
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }
  private static void setAuths() throws Exception {
    String[] labels = { SECRET, CONFIDENTIAL, PRIVATE, PUBLIC, TOPSECRET };
    try {
      VisibilityClient.setAuths(conf, labels, User.getCurrent().getShortName());
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
  @Test
  public void testSimpleScannerXMLWithLabelsThatReceivesNoData() throws IOException, JAXBException {
    final int BATCH_SIZE = 5;
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(BATCH_SIZE);
    model.addColumn(Bytes.toBytes(COLUMN_1));
    model.addLabel(PUBLIC);
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    byte[] body = Bytes.toBytes(writer.toString());
    // recall previous put operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    Response response = client.put("/" + TABLE + "/scanner", Constants.MIMETYPE_XML, body);
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, Constants.MIMETYPE_XML);
    // Respond with 204 as there are no cells to be retrieved
    assertEquals(response.getCode(), 204);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
  }

  @Test
  public void testSimpleScannerXMLWithLabelsThatReceivesData() throws IOException, JAXBException {
    // new scanner
    ScannerModel model = new ScannerModel();
    model.setBatch(5);
    model.addColumn(Bytes.toBytes(COLUMN_1));
    model.addLabel(SECRET);
    StringWriter writer = new StringWriter();
    marshaller.marshal(model, writer);
    byte[] body = Bytes.toBytes(writer.toString());

    // recall previous put operation with read-only off
    conf.set("hbase.rest.readonly", "false");
    Response response = client.put("/" + TABLE + "/scanner", Constants.MIMETYPE_XML, body);
    assertEquals(response.getCode(), 201);
    String scannerURI = response.getLocation();
    assertNotNull(scannerURI);

    // get a cell set
    response = client.get(scannerURI, Constants.MIMETYPE_XML);
    // Respond with 204 as there are no cells to be retrieved
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    CellSetModel cellSet = (CellSetModel) unmarshaller.unmarshal(new ByteArrayInputStream(response
        .getBody()));
    assertEquals(countCellSet(cellSet), 5);
  }

}
