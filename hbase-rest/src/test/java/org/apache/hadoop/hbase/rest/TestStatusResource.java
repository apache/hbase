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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.StorageClusterStatusModel;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestStatusResource {
  private static final byte[] META_REGION_NAME = Bytes.toBytes(TableName.META_TABLE_NAME+",,1");

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = 
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;
  
  private static void validate(StorageClusterStatusModel model) {
    assertNotNull(model);
    assertTrue(model.getRegions() + ">= 1", model.getRegions() >= 1);
    assertTrue(model.getRequests() >= 0);
    assertTrue(model.getAverageLoad() >= 0.0);
    assertNotNull(model.getLiveNodes());
    assertNotNull(model.getDeadNodes());
    assertFalse(model.getLiveNodes().isEmpty());
    boolean foundMeta = false;
    for (StorageClusterStatusModel.Node node: model.getLiveNodes()) {
      assertNotNull(node.getName());
      assertTrue(node.getStartCode() > 0L);
      assertTrue(node.getRequests() >= 0);
      for (StorageClusterStatusModel.Node.Region region: node.getRegions()) {
        if (Bytes.equals(region.getName(), META_REGION_NAME)) {
          foundMeta = true;
        }
      }
    }
    assertTrue(foundMeta);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost", 
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(StorageClusterStatusModel.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetClusterStatusXML() throws IOException, JAXBException {
    Response response = client.get("/status/cluster", Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    StorageClusterStatusModel model = (StorageClusterStatusModel)
      context.createUnmarshaller().unmarshal(
        new ByteArrayInputStream(response.getBody()));
    validate(model);
  }

  @Test
  public void testGetClusterStatusPB() throws IOException {
    Response response = client.get("/status/cluster", Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    StorageClusterStatusModel model = new StorageClusterStatusModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
    response = client.get("/status/cluster", Constants.MIMETYPE_PROTOBUF_IETF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF_IETF, response.getHeader("content-type"));
    model = new StorageClusterStatusModel();
    model.getObjectFromMessage(response.getBody());
    validate(model);
  }
}
