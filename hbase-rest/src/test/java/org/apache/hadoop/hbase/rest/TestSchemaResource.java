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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.ColumnSchemaModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.rest.model.TestTableSchemaModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RestTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class TestSchemaResource {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSchemaResource.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaResource.class);

  private static String TABLE1 = "TestSchemaResource1";
  private static String TABLE2 = "TestSchemaResource2";

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Configuration conf;
  private static TestTableSchemaModel testTableSchemaModel;
  private static Header extraHdr = null;

  private static boolean csrfEnabled = true;

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return HBaseCommonTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  public TestSchemaResource(Boolean csrf) {
    csrfEnabled = csrf;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(RESTServer.REST_CSRF_ENABLED_KEY, csrfEnabled);
    extraHdr = new BasicHeader(RESTServer.REST_CSRF_CUSTOM_HEADER_DEFAULT, "");
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    testTableSchemaModel = new TestTableSchemaModel();
    context = JAXBContext.newInstance(
      ColumnSchemaModel.class,
      TableSchemaModel.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();

    for (String table : new String[] {TABLE1, TABLE2}) {
      TableName t = TableName.valueOf(table);
      if (admin.tableExists(t)) {
        admin.disableTable(t);
        admin.deleteTable(t);
      }
    }

    conf.set("hbase.rest.readonly", "false");
  }

  private static byte[] toXML(TableSchemaModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return Bytes.toBytes(writer.toString());
  }

  private static TableSchemaModel fromXML(byte[] content)
      throws JAXBException {
    return (TableSchemaModel) context.createUnmarshaller()
      .unmarshal(new ByteArrayInputStream(content));
  }

  @Test
  public void testTableCreateAndDeleteXML() throws IOException, JAXBException {
    String schemaPath = "/" + TABLE1 + "/schema";
    TableSchemaModel model;
    Response response;

    Admin admin = TEST_UTIL.getAdmin();
    assertFalse("Table " + TABLE1 + " should not exist", admin.tableExists(TableName.valueOf(TABLE1)));

    // create the table
    model = testTableSchemaModel.buildTestModel(TABLE1);
    testTableSchemaModel.checkModel(model, TABLE1);
    if (csrfEnabled) {
      // test put operation is forbidden without custom header
      response = client.put(schemaPath, Constants.MIMETYPE_XML, toXML(model));
      assertEquals(400, response.getCode());
    }

    response = client.put(schemaPath, Constants.MIMETYPE_XML, toXML(model), extraHdr);
    assertEquals("put failed with csrf " + (csrfEnabled ? "enabled" : "disabled"),
       201, response.getCode());

    // recall the same put operation but in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.put(schemaPath, Constants.MIMETYPE_XML, toXML(model), extraHdr);
    assertEquals(403, response.getCode());

    // retrieve the schema and validate it
    response = client.get(schemaPath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    model = fromXML(response.getBody());
    testTableSchemaModel.checkModel(model, TABLE1);

    // with json retrieve the schema and validate it
    response = client.get(schemaPath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    model = testTableSchemaModel.fromJSON(Bytes.toString(response.getBody()));
    testTableSchemaModel.checkModel(model, TABLE1);

    if (csrfEnabled) {
      // test delete schema operation is forbidden without custom header
      response = client.delete(schemaPath);
      assertEquals(400, response.getCode());
    }

    // test delete schema operation is forbidden in read-only mode
    response = client.delete(schemaPath, extraHdr);
    assertEquals(403, response.getCode());

    // return read-only setting back to default
    conf.set("hbase.rest.readonly", "false");

    // delete the table and make sure HBase concurs
    response = client.delete(schemaPath, extraHdr);
    assertEquals(200, response.getCode());
    assertFalse(admin.tableExists(TableName.valueOf(TABLE1)));
  }

  @Test
  public void testTableCreateAndDeletePB() throws IOException, JAXBException {
    String schemaPath = "/" + TABLE2 + "/schema";
    TableSchemaModel model;
    Response response;

    Admin admin = TEST_UTIL.getAdmin();
    assertFalse(admin.tableExists(TableName.valueOf(TABLE2)));

    // create the table
    model = testTableSchemaModel.buildTestModel(TABLE2);
    testTableSchemaModel.checkModel(model, TABLE2);

    if (csrfEnabled) {
      // test put operation is forbidden without custom header
      response = client.put(schemaPath, Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
      assertEquals(400, response.getCode());
    }
    response = client.put(schemaPath, Constants.MIMETYPE_PROTOBUF,
      model.createProtobufOutput(), extraHdr);
    assertEquals("put failed with csrf " + (csrfEnabled ? "enabled" : "disabled"),
        201, response.getCode());

    // recall the same put operation but in read-only mode
    conf.set("hbase.rest.readonly", "true");
    response = client.put(schemaPath, Constants.MIMETYPE_PROTOBUF,
      model.createProtobufOutput(), extraHdr);
    assertNotNull(extraHdr);
    assertEquals(403, response.getCode());

    // retrieve the schema and validate it
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    model = new TableSchemaModel();
    model.getObjectFromMessage(response.getBody());
    testTableSchemaModel.checkModel(model, TABLE2);

    // retrieve the schema and validate it with alternate pbuf type
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF_IETF);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_PROTOBUF_IETF, response.getHeader("content-type"));
    model = new TableSchemaModel();
    model.getObjectFromMessage(response.getBody());
    testTableSchemaModel.checkModel(model, TABLE2);

    if (csrfEnabled) {
      // test delete schema operation is forbidden without custom header
      response = client.delete(schemaPath);
      assertEquals(400, response.getCode());
    }

    // test delete schema operation is forbidden in read-only mode
    response = client.delete(schemaPath, extraHdr);
    assertEquals(403, response.getCode());

    // return read-only setting back to default
    conf.set("hbase.rest.readonly", "false");

    // delete the table and make sure HBase concurs
    response = client.delete(schemaPath, extraHdr);
    assertEquals(200, response.getCode());
    assertFalse(admin.tableExists(TableName.valueOf(TABLE2)));
  }

}

