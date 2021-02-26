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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.NamespacesModel;
import org.apache.hadoop.hbase.rest.model.TestNamespacesModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestNamespacesResource {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespacesResource.class);

  private static String NAMESPACE1 = "TestNamespacesInstanceResource1";
  private static String NAMESPACE2 = "TestNamespacesInstanceResource2";

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  private static Configuration conf;
  private static TestNamespacesModel testNamespacesModel;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(conf);
    client = new Client(new Cluster().add("localhost", REST_TEST_UTIL.getServletPort()));
    testNamespacesModel = new TestNamespacesModel();
    context = JAXBContext.newInstance(NamespacesModel.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static NamespacesModel fromXML(byte[] content) throws JAXBException {
    return (NamespacesModel) context.createUnmarshaller()
      .unmarshal(new ByteArrayInputStream(content));
  }

  private boolean doesNamespaceExist(Admin admin, String namespaceName) throws IOException {
    NamespaceDescriptor[] nd = admin.listNamespaceDescriptors();
    for (NamespaceDescriptor namespaceDescriptor : nd) {
      if (namespaceDescriptor.getName().equals(namespaceName)) {
        return true;
      }
    }
    return false;
  }

  private void createNamespaceViaAdmin(Admin admin, String name) throws IOException {
    NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(name);
    NamespaceDescriptor nsd = builder.build();
    admin.createNamespace(nsd);
  }

  @Test
  public void testNamespaceListXMLandJSON() throws IOException, JAXBException {
    String namespacePath = "/namespaces/";
    NamespacesModel model;
    Response response;

    // Check that namespace does not yet exist via non-REST call.
    Admin admin = TEST_UTIL.getAdmin();
    assertFalse(doesNamespaceExist(admin, NAMESPACE1));
    model = testNamespacesModel.buildTestModel();
    testNamespacesModel.checkModel(model);

    // Check that REST GET finds only default namespaces via XML and JSON responses.
    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = fromXML(response.getBody());
    testNamespacesModel.checkModel(model, "hbase", "default");
    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = testNamespacesModel.fromJSON(Bytes.toString(response.getBody()));
    testNamespacesModel.checkModel(model, "hbase", "default");

    // Create namespace and check that REST GET finds one additional namespace.
    createNamespaceViaAdmin(admin, NAMESPACE1);
    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = fromXML(response.getBody());
    testNamespacesModel.checkModel(model, NAMESPACE1, "hbase", "default");
    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = testNamespacesModel.fromJSON(Bytes.toString(response.getBody()));
    testNamespacesModel.checkModel(model, NAMESPACE1, "hbase", "default");

    // Create another namespace and check that REST GET finds one additional namespace.
    createNamespaceViaAdmin(admin, NAMESPACE2);
    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = fromXML(response.getBody());
    testNamespacesModel.checkModel(model, NAMESPACE1, NAMESPACE2, "hbase", "default");
    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = testNamespacesModel.fromJSON(Bytes.toString(response.getBody()));
    testNamespacesModel.checkModel(model, NAMESPACE1, NAMESPACE2, "hbase", "default");

    // Delete namespace and check that REST still finds correct namespaces.
    admin.deleteNamespace(NAMESPACE1);
    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = fromXML(response.getBody());
    testNamespacesModel.checkModel(model, NAMESPACE2, "hbase", "default");
    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = testNamespacesModel.fromJSON(Bytes.toString(response.getBody()));
    testNamespacesModel.checkModel(model, NAMESPACE2, "hbase", "default");

    admin.deleteNamespace(NAMESPACE2);
  }

  @Test
  public void testNamespaceListPBandDefault() throws IOException {
    String schemaPath = "/namespaces/";
    NamespacesModel model;
    Response response;

    // Check that namespace does not yet exist via non-REST call.
    Admin admin = TEST_UTIL.getAdmin();
    assertFalse(doesNamespaceExist(admin, NAMESPACE1));
    model = testNamespacesModel.buildTestModel();
    testNamespacesModel.checkModel(model);

    // Check that REST GET finds only default namespaces via PB and default Accept header.
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    testNamespacesModel.checkModel(model, "hbase", "default");
    response = client.get(schemaPath);
    assertEquals(200, response.getCode());

    // Create namespace and check that REST GET finds one additional namespace.
    createNamespaceViaAdmin(admin, NAMESPACE1);
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    testNamespacesModel.checkModel(model, NAMESPACE1, "hbase", "default");
    response = client.get(schemaPath);
    assertEquals(200, response.getCode());

    // Create another namespace and check that REST GET finds one additional namespace.
    createNamespaceViaAdmin(admin, NAMESPACE2);
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    testNamespacesModel.checkModel(model, NAMESPACE1, NAMESPACE2, "hbase", "default");
    response = client.get(schemaPath);
    assertEquals(200, response.getCode());

    // Delete namespace and check that REST GET still finds correct namespaces.
    admin.deleteNamespace(NAMESPACE1);
    response = client.get(schemaPath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    testNamespacesModel.checkModel(model, NAMESPACE2, "hbase", "default");
    response = client.get(schemaPath);
    assertEquals(200, response.getCode());

    admin.deleteNamespace(NAMESPACE2);
  }
}
