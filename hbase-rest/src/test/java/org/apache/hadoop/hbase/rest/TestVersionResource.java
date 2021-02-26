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
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.StorageClusterVersionModel;
import org.apache.hadoop.hbase.rest.model.VersionModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RestTests.class, MediumTests.class})
public class TestVersionResource {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVersionResource.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestVersionResource.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(
      VersionModel.class,
      StorageClusterVersionModel.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void validate(VersionModel model) {
    assertNotNull(model);
    assertNotNull(model.getRESTVersion());
    assertEquals(RESTServlet.VERSION_STRING, model.getRESTVersion());
    String osVersion = model.getOSVersion();
    assertNotNull(osVersion);
    assertTrue(osVersion.contains(System.getProperty("os.name")));
    assertTrue(osVersion.contains(System.getProperty("os.version")));
    assertTrue(osVersion.contains(System.getProperty("os.arch")));
    String jvmVersion = model.getJVMVersion();
    assertNotNull(jvmVersion);
    assertTrue(jvmVersion.contains(System.getProperty("java.vm.vendor")));
    assertTrue(jvmVersion.contains(System.getProperty("java.version")));
    assertTrue(jvmVersion.contains(System.getProperty("java.vm.version")));
    assertNotNull(model.getServerVersion());
    String jerseyVersion = model.getJerseyVersion();
    assertNotNull(jerseyVersion);
    // TODO: fix when we actually get a jersey version
    // assertEquals(jerseyVersion, ServletContainer.class.getPackage().getImplementationVersion());
  }

  @Test
  public void testGetStargateVersionText() throws IOException {
    Response response = client.get("/version", Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_TEXT, response.getHeader("content-type"));
    String body = Bytes.toString(response.getBody());
    assertTrue(body.length() > 0);
    assertTrue(body.contains(RESTServlet.VERSION_STRING));
    assertTrue(body.contains(System.getProperty("java.vm.vendor")));
    assertTrue(body.contains(System.getProperty("java.version")));
    assertTrue(body.contains(System.getProperty("java.vm.version")));
    assertTrue(body.contains(System.getProperty("os.name")));
    assertTrue(body.contains(System.getProperty("os.version")));
    assertTrue(body.contains(System.getProperty("os.arch")));
    // TODO: fix when we actually get a jersey version
    // assertTrue(body.contains(ServletContainer.class.getPackage().getImplementationVersion()));
  }

  @Test
  public void testGetStargateVersionXML() throws IOException, JAXBException {
    Response response = client.get("/version", Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    VersionModel model = (VersionModel)
      context.createUnmarshaller().unmarshal(
        new ByteArrayInputStream(response.getBody()));
    validate(model);
    LOG.info("success retrieving Stargate version as XML");
  }

  @Test
  public void testGetStargateVersionJSON() throws IOException {
    Response response = client.get("/version", Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    ObjectMapper mapper = new JacksonJaxbJsonProvider()
            .locateMapper(VersionModel.class, MediaType.APPLICATION_JSON_TYPE);
    VersionModel model
            = mapper.readValue(response.getBody(), VersionModel.class);
    validate(model);
    LOG.info("success retrieving Stargate version as JSON");
  }

  @Test
  public void testGetStargateVersionPB() throws IOException {
    Response response = client.get("/version", Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    VersionModel model = new VersionModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
    response = client.get("/version", Constants.MIMETYPE_PROTOBUF_IETF);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_PROTOBUF_IETF, response.getHeader("content-type"));
    model = new VersionModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
  }

  @Test
  public void testGetStorageClusterVersionText() throws IOException {
    Response response = client.get("/version/cluster", Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_TEXT, response.getHeader("content-type"));
  }

  @Test
  public void testGetStorageClusterVersionXML() throws IOException,
      JAXBException {
    Response response = client.get("/version/cluster",Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    StorageClusterVersionModel clusterVersionModel =
      (StorageClusterVersionModel)
        context.createUnmarshaller().unmarshal(
          new ByteArrayInputStream(response.getBody()));
    assertNotNull(clusterVersionModel);
    assertNotNull(clusterVersionModel.getVersion());
    LOG.info("success retrieving storage cluster version as XML");
  }

  @Test
  public void testGetStorageClusterVersionJSON() throws IOException {
    Response response = client.get("/version/cluster", Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
    ObjectMapper mapper = new JacksonJaxbJsonProvider()
            .locateMapper(StorageClusterVersionModel.class, MediaType.APPLICATION_JSON_TYPE);
    StorageClusterVersionModel clusterVersionModel
            = mapper.readValue(response.getBody(), StorageClusterVersionModel.class);
    assertNotNull(clusterVersionModel);
    assertNotNull(clusterVersionModel.getVersion());
    LOG.info("success retrieving storage cluster version as JSON");
  }
}

