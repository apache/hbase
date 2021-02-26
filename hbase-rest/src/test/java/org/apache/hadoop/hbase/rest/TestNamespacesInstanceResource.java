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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.NamespacesInstanceModel;
import org.apache.hadoop.hbase.rest.model.TableListModel;
import org.apache.hadoop.hbase.rest.model.TableModel;
import org.apache.hadoop.hbase.rest.model.TestNamespacesInstanceModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.Header;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestNamespacesInstanceResource {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespacesInstanceResource.class);

  private static String NAMESPACE1 = "TestNamespacesInstanceResource1";
  private static Map<String,String> NAMESPACE1_PROPS = new HashMap<>();
  private static String NAMESPACE2 = "TestNamespacesInstanceResource2";
  private static Map<String,String> NAMESPACE2_PROPS = new HashMap<>();
  private static String NAMESPACE3 = "TestNamespacesInstanceResource3";
  private static Map<String,String> NAMESPACE3_PROPS = new HashMap<>();
  private static String NAMESPACE4 = "TestNamespacesInstanceResource4";
  private static Map<String,String> NAMESPACE4_PROPS = new HashMap<>();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Configuration conf;
  private static TestNamespacesInstanceModel testNamespacesInstanceModel;
  protected static ObjectMapper jsonMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    testNamespacesInstanceModel = new TestNamespacesInstanceModel();
    context = JAXBContext.newInstance(NamespacesInstanceModel.class, TableListModel.class);
    jsonMapper = new JacksonJaxbJsonProvider()
      .locateMapper(NamespacesInstanceModel.class, MediaType.APPLICATION_JSON_TYPE);
    NAMESPACE1_PROPS.put("key1", "value1");
    NAMESPACE2_PROPS.put("key2a", "value2a");
    NAMESPACE2_PROPS.put("key2b", "value2b");
    NAMESPACE3_PROPS.put("key3", "value3");
    NAMESPACE4_PROPS.put("key4a", "value4a");
    NAMESPACE4_PROPS.put("key4b", "value4b");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static byte[] toXML(NamespacesInstanceModel model) throws JAXBException {
    StringWriter writer = new StringWriter();
    context.createMarshaller().marshal(model, writer);
    return Bytes.toBytes(writer.toString());
  }

  @SuppressWarnings("unchecked")
  private static <T> T fromXML(byte[] content)
      throws JAXBException {
    return (T) context.createUnmarshaller().unmarshal(new ByteArrayInputStream(content));
  }

  private NamespaceDescriptor findNamespace(Admin admin, String namespaceName) throws IOException{
    NamespaceDescriptor[] nd = admin.listNamespaceDescriptors();
    for (NamespaceDescriptor namespaceDescriptor : nd) {
      if (namespaceDescriptor.getName().equals(namespaceName)) {
        return namespaceDescriptor;
      }
    }
    return null;
  }

  private void checkNamespaceProperties(NamespaceDescriptor nd, Map<String,String> testProps){
    checkNamespaceProperties(nd.getConfiguration(), testProps);
  }

  private void checkNamespaceProperties(Map<String,String> namespaceProps,
      Map<String,String> testProps){
    assertTrue(namespaceProps.size() == testProps.size());
    for (String key: testProps.keySet()) {
      assertEquals(testProps.get(key), namespaceProps.get(key));
    }
  }

  private void checkNamespaceTables(List<TableModel> namespaceTables, List<String> testTables){
    assertEquals(namespaceTables.size(), testTables.size());
    for (TableModel namespaceTable : namespaceTables) {
      String tableName = namespaceTable.getName();
      assertTrue(testTables.contains(tableName));
    }
  }

  @Test
  public void testCannotDeleteDefaultAndHbaseNamespaces() throws IOException {
    String defaultPath = "/namespaces/default";
    String hbasePath = "/namespaces/hbase";
    Response response;

    // Check that doesn't exist via non-REST call.
    Admin admin = TEST_UTIL.getAdmin();
    assertNotNull(findNamespace(admin, "default"));
    assertNotNull(findNamespace(admin, "hbase"));

    // Try (but fail) to delete namespaces via REST.
    response = client.delete(defaultPath);
    assertEquals(503, response.getCode());
    response = client.delete(hbasePath);
    assertEquals(503, response.getCode());

    assertNotNull(findNamespace(admin, "default"));
    assertNotNull(findNamespace(admin, "hbase"));
  }

  @Test
  public void testGetNamespaceTablesAndCannotDeleteNamespace() throws IOException, JAXBException {
    Admin admin = TEST_UTIL.getAdmin();
    String nsName = "TestNamespacesInstanceResource5";
    Response response;

    // Create namespace via admin.
    NamespaceDescriptor.Builder nsBuilder = NamespaceDescriptor.create(nsName);
    NamespaceDescriptor nsd = nsBuilder.build();
    nsd.setConfiguration("key1", "value1");
    admin.createNamespace(nsd);

    // Create two tables via admin.
    HColumnDescriptor colDesc = new HColumnDescriptor("cf1");
    TableName tn1 = TableName.valueOf(nsName + ":table1");
    HTableDescriptor table = new HTableDescriptor(tn1);
    table.addFamily(colDesc);
    admin.createTable(table);
    TableName tn2 = TableName.valueOf(nsName + ":table2");
    table = new HTableDescriptor(tn2);
    table.addFamily(colDesc);
    admin.createTable(table);

    Map<String, String> nsProperties = new HashMap<>();
    nsProperties.put("key1", "value1");
    List<String> nsTables = Arrays.asList("table1", "table2");

    // Check get namespace properties as XML, JSON and Protobuf.
    String namespacePath = "/namespaces/" + nsName;
    response = client.get(namespacePath);
    assertEquals(200, response.getCode());

    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    NamespacesInstanceModel model = fromXML(response.getBody());
    checkNamespaceProperties(model.getProperties(), nsProperties);

    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = jsonMapper.readValue(response.getBody(), NamespacesInstanceModel.class);
    checkNamespaceProperties(model.getProperties(), nsProperties);

    response = client.get(namespacePath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    checkNamespaceProperties(model.getProperties(), nsProperties);

    // Check get namespace tables as XML, JSON and Protobuf.
    namespacePath = "/namespaces/" + nsName + "/tables";
    response = client.get(namespacePath);
    assertEquals(200, response.getCode());

    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    TableListModel tablemodel = fromXML(response.getBody());
    checkNamespaceTables(tablemodel.getTables(), nsTables);

    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    tablemodel = jsonMapper.readValue(response.getBody(), TableListModel.class);
    checkNamespaceTables(tablemodel.getTables(), nsTables);

    response = client.get(namespacePath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    tablemodel.setTables(new ArrayList<>());
    tablemodel.getObjectFromMessage(response.getBody());
    checkNamespaceTables(tablemodel.getTables(), nsTables);

    // Check cannot delete namespace via REST because it contains tables.
    response = client.delete(namespacePath);
    namespacePath = "/namespaces/" + nsName;
    assertEquals(503, response.getCode());
  }

  @Ignore("HBASE-19210")
  @Test
  public void testInvalidNamespacePostsAndPuts() throws IOException, JAXBException {
    String namespacePath1 = "/namespaces/" + NAMESPACE1;
    String namespacePath2 = "/namespaces/" + NAMESPACE2;
    String namespacePath3 = "/namespaces/" + NAMESPACE3;
    NamespacesInstanceModel model1;
    NamespacesInstanceModel model2;
    NamespacesInstanceModel model3;
    Response response;

    // Check that namespaces don't exist via non-REST call.
    Admin admin = TEST_UTIL.getAdmin();
    assertNull(findNamespace(admin, NAMESPACE1));
    assertNull(findNamespace(admin, NAMESPACE2));
    assertNull(findNamespace(admin, NAMESPACE3));

    model1 = testNamespacesInstanceModel.buildTestModel(NAMESPACE1, NAMESPACE1_PROPS);
    testNamespacesInstanceModel.checkModel(model1, NAMESPACE1, NAMESPACE1_PROPS);
    model2 = testNamespacesInstanceModel.buildTestModel(NAMESPACE2, NAMESPACE2_PROPS);
    testNamespacesInstanceModel.checkModel(model2, NAMESPACE2, NAMESPACE2_PROPS);
    model3 = testNamespacesInstanceModel.buildTestModel(NAMESPACE3, NAMESPACE3_PROPS);
    testNamespacesInstanceModel.checkModel(model3, NAMESPACE3, NAMESPACE3_PROPS);

    // Try REST post and puts with invalid content.
    response = client.post(namespacePath1, Constants.MIMETYPE_JSON, toXML(model1));
    assertEquals(500, response.getCode());
    String jsonString = jsonMapper.writeValueAsString(model2);
    response = client.put(namespacePath2, Constants.MIMETYPE_XML, Bytes.toBytes(jsonString));
    assertEquals(400, response.getCode());
    response = client.post(namespacePath3, Constants.MIMETYPE_PROTOBUF, toXML(model3));
    assertEquals(500, response.getCode());

    NamespaceDescriptor nd1 = findNamespace(admin, NAMESPACE1);
    NamespaceDescriptor nd2 = findNamespace(admin, NAMESPACE2);
    NamespaceDescriptor nd3 = findNamespace(admin, NAMESPACE3);
    assertNull(nd1);
    assertNull(nd2);
    assertNull(nd3);
  }

  @Test
  public void testNamespaceCreateAndDeleteXMLAndJSON() throws IOException, JAXBException {
    String namespacePath1 = "/namespaces/" + NAMESPACE1;
    String namespacePath2 = "/namespaces/" + NAMESPACE2;
    NamespacesInstanceModel model1;
    NamespacesInstanceModel model2;
    Response response;

    // Check that namespaces don't exist via non-REST call.
    Admin admin = TEST_UTIL.getAdmin();
    assertNull(findNamespace(admin, NAMESPACE1));
    assertNull(findNamespace(admin, NAMESPACE2));

    model1 = testNamespacesInstanceModel.buildTestModel(NAMESPACE1, NAMESPACE1_PROPS);
    testNamespacesInstanceModel.checkModel(model1, NAMESPACE1, NAMESPACE1_PROPS);
    model2 = testNamespacesInstanceModel.buildTestModel(NAMESPACE2, NAMESPACE2_PROPS);
    testNamespacesInstanceModel.checkModel(model2, NAMESPACE2, NAMESPACE2_PROPS);

    // Test cannot PUT (alter) non-existent namespace.
    response = client.put(namespacePath1, Constants.MIMETYPE_XML, toXML(model1));
    assertEquals(403, response.getCode());
    String jsonString = jsonMapper.writeValueAsString(model2);
    response = client.put(namespacePath2, Constants.MIMETYPE_JSON, Bytes.toBytes(jsonString));
    assertEquals(403, response.getCode());

    // Test cannot create tables when in read only mode.
    conf.set("hbase.rest.readonly", "true");
    response = client.post(namespacePath1, Constants.MIMETYPE_XML, toXML(model1));
    assertEquals(403, response.getCode());
    jsonString = jsonMapper.writeValueAsString(model2);
    response = client.post(namespacePath2, Constants.MIMETYPE_JSON, Bytes.toBytes(jsonString));
    assertEquals(403, response.getCode());
    NamespaceDescriptor nd1 = findNamespace(admin, NAMESPACE1);
    NamespaceDescriptor nd2 = findNamespace(admin, NAMESPACE2);
    assertNull(nd1);
    assertNull(nd2);
    conf.set("hbase.rest.readonly", "false");

    // Create namespace via XML and JSON.
    response = client.post(namespacePath1, Constants.MIMETYPE_XML, toXML(model1));
    assertEquals(201, response.getCode());
    jsonString = jsonMapper.writeValueAsString(model2);
    response = client.post(namespacePath2, Constants.MIMETYPE_JSON, Bytes.toBytes(jsonString));
    assertEquals(201, response.getCode());
    //check passing null content-type with a payload returns 415
    Header[] nullHeaders = null;
    response = client.post(namespacePath1, nullHeaders, toXML(model1));
    assertEquals(415, response.getCode());
    response = client.post(namespacePath1, nullHeaders, Bytes.toBytes(jsonString));
    assertEquals(415, response.getCode());

    // Check that created namespaces correctly.
    nd1 = findNamespace(admin, NAMESPACE1);
    nd2 = findNamespace(admin, NAMESPACE2);
    assertNotNull(nd1);
    assertNotNull(nd2);
    checkNamespaceProperties(nd1, NAMESPACE1_PROPS);
    checkNamespaceProperties(nd1, NAMESPACE1_PROPS);

    // Test cannot delete tables when in read only mode.
    conf.set("hbase.rest.readonly", "true");
    response = client.delete(namespacePath1);
    assertEquals(403, response.getCode());
    response = client.delete(namespacePath2);
    assertEquals(403, response.getCode());
    nd1 = findNamespace(admin, NAMESPACE1);
    nd2 = findNamespace(admin, NAMESPACE2);
    assertNotNull(nd1);
    assertNotNull(nd2);
    conf.set("hbase.rest.readonly", "false");

    // Delete namespaces via XML and JSON.
    response = client.delete(namespacePath1);
    assertEquals(200, response.getCode());
    response = client.delete(namespacePath2);
    assertEquals(200, response.getCode());
    nd1 = findNamespace(admin, NAMESPACE1);
    nd2 = findNamespace(admin, NAMESPACE2);
    assertNull(nd1);
    assertNull(nd2);
  }

  @Test
  public void testNamespaceCreateAndDeletePBAndNoBody() throws IOException {
    String namespacePath3 = "/namespaces/" + NAMESPACE3;
    String namespacePath4 = "/namespaces/" + NAMESPACE4;
    NamespacesInstanceModel model3;
    NamespacesInstanceModel model4;
    Response response;

    // Check that namespaces don't exist via non-REST call.
    Admin admin = TEST_UTIL.getAdmin();
    assertNull(findNamespace(admin, NAMESPACE3));
    assertNull(findNamespace(admin, NAMESPACE4));

    model3 = testNamespacesInstanceModel.buildTestModel(NAMESPACE3, NAMESPACE3_PROPS);
    testNamespacesInstanceModel.checkModel(model3, NAMESPACE3, NAMESPACE3_PROPS);
    model4 = testNamespacesInstanceModel.buildTestModel(NAMESPACE4, NAMESPACE4_PROPS);
    testNamespacesInstanceModel.checkModel(model4, NAMESPACE4, NAMESPACE4_PROPS);

    //Defines null headers for use in tests where no body content is provided, so that we set
    // no content-type in the request
    Header[] nullHeaders = null;

    // Test cannot PUT (alter) non-existent namespace.
    response = client.put(namespacePath3, nullHeaders, new byte[]{});
    assertEquals(403, response.getCode());
    response = client.put(namespacePath4, Constants.MIMETYPE_PROTOBUF,
      model4.createProtobufOutput());
    assertEquals(403, response.getCode());

    // Test cannot create tables when in read only mode.
    conf.set("hbase.rest.readonly", "true");
    response = client.post(namespacePath3, nullHeaders, new byte[]{});
    assertEquals(403, response.getCode());
    response = client.put(namespacePath4, Constants.MIMETYPE_PROTOBUF,
      model4.createProtobufOutput());
    assertEquals(403, response.getCode());
    NamespaceDescriptor nd3 = findNamespace(admin, NAMESPACE3);
    NamespaceDescriptor nd4 = findNamespace(admin, NAMESPACE4);
    assertNull(nd3);
    assertNull(nd4);
    conf.set("hbase.rest.readonly", "false");

    // Create namespace with no body and binary content type.
    response = client.post(namespacePath3, nullHeaders, new byte[]{});
    assertEquals(201, response.getCode());
    // Create namespace with protobuf content-type.
    response = client.post(namespacePath4, Constants.MIMETYPE_PROTOBUF,
      model4.createProtobufOutput());
    assertEquals(201, response.getCode());
    //check setting unsupported content-type returns 415
    response = client.post(namespacePath3, Constants.MIMETYPE_BINARY, new byte[]{});
    assertEquals(415, response.getCode());

    // Check that created namespaces correctly.
    nd3 = findNamespace(admin, NAMESPACE3);
    nd4 = findNamespace(admin, NAMESPACE4);
    assertNotNull(nd3);
    assertNotNull(nd4);
    checkNamespaceProperties(nd3, new HashMap<>());
    checkNamespaceProperties(nd4, NAMESPACE4_PROPS);

    // Check cannot post tables that already exist.
    response = client.post(namespacePath3, nullHeaders, new byte[]{});
    assertEquals(403, response.getCode());
    response = client.post(namespacePath4, Constants.MIMETYPE_PROTOBUF,
      model4.createProtobufOutput());
    assertEquals(403, response.getCode());

    // Check cannot post tables when in read only mode.
    conf.set("hbase.rest.readonly", "true");
    response = client.delete(namespacePath3);
    assertEquals(403, response.getCode());
    response = client.delete(namespacePath4);
    assertEquals(403, response.getCode());
    nd3 = findNamespace(admin, NAMESPACE3);
    nd4 = findNamespace(admin, NAMESPACE4);
    assertNotNull(nd3);
    assertNotNull(nd4);
    conf.set("hbase.rest.readonly", "false");

    // Delete namespaces via XML and JSON.
    response = client.delete(namespacePath3);
    assertEquals(200, response.getCode());
    response = client.delete(namespacePath4);
    assertEquals(200, response.getCode());
    nd3 = findNamespace(admin, NAMESPACE3);
    nd4 = findNamespace(admin, NAMESPACE4);
    assertNull(nd3);
    assertNull(nd4);
  }
}
