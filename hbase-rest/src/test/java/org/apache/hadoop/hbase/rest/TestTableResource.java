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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.TableInfoModel;
import org.apache.hadoop.hbase.rest.model.TableListModel;
import org.apache.hadoop.hbase.rest.model.TableModel;
import org.apache.hadoop.hbase.rest.model.TableRegionModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestTableResource {
  private static final Log LOG = LogFactory.getLog(TestTableResource.class);

  private static TableName TABLE = TableName.valueOf("TestTableResource");
  private static String COLUMN_FAMILY = "test";
  private static String COLUMN = COLUMN_FAMILY + ":qualifier";
  private static List<HRegionLocation> regionMap;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost",
      REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(
        TableModel.class,
        TableInfoModel.class,
        TableListModel.class,
        TableRegionModel.class);
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    admin.createTable(htd);
    byte[] k = new byte[3];
    byte [][] famAndQf = KeyValue.parseColumn(Bytes.toBytes(COLUMN));
    List<Put> puts = new ArrayList<>();
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.setDurability(Durability.SKIP_WAL);
          put.add(famAndQf[0], famAndQf[1], k);
          puts.add(put);
        }
      }
    }
    Connection connection = TEST_UTIL.getConnection();
    
    Table table =  connection.getTable(TABLE);
    table.put(puts);
    table.close();
    // get the initial layout (should just be one region)
    
    RegionLocator regionLocator = connection.getRegionLocator(TABLE);
    List<HRegionLocation> m = regionLocator.getAllRegionLocations();
    assertEquals(m.size(), 1);
    // tell the master to split the table
    admin.split(TABLE);
    // give some time for the split to happen

    long timeout = System.currentTimeMillis() + (15 * 1000);
    while (System.currentTimeMillis() < timeout && m.size()!=2){
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
      // check again
      m = regionLocator.getAllRegionLocations();
    }

    // should have two regions now
    assertEquals(m.size(), 2);
    regionMap = m;
    LOG.info("regions: " + regionMap);
    regionLocator.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void checkTableList(TableListModel model) {
    boolean found = false;
    Iterator<TableModel> tables = model.getTables().iterator();
    assertTrue(tables.hasNext());
    while (tables.hasNext()) {
      TableModel table = tables.next();
      if (table.getName().equals(TABLE.getNameAsString())) {
        found = true;
        break;
      }
    }
    assertTrue(found);
  }

  void checkTableInfo(TableInfoModel model) {
    assertEquals(model.getName(), TABLE.getNameAsString());
    Iterator<TableRegionModel> regions = model.getRegions().iterator();
    assertTrue(regions.hasNext());
    while (regions.hasNext()) {
      TableRegionModel region = regions.next();
      boolean found = false;
      for (HRegionLocation e: regionMap) {
        HRegionInfo hri = e.getRegionInfo();
        String hriRegionName = hri.getRegionNameAsString();
        String regionName = region.getName();
        if (hriRegionName.equals(regionName)) {
          found = true;
          byte[] startKey = hri.getStartKey();
          byte[] endKey = hri.getEndKey();
          ServerName serverName = e.getServerName();
          InetSocketAddress sa =
              new InetSocketAddress(serverName.getHostname(), serverName.getPort());
          String location = sa.getHostName() + ":" +
            Integer.valueOf(sa.getPort());
          assertEquals(hri.getRegionId(), region.getId());
          assertTrue(Bytes.equals(startKey, region.getStartKey()));
          assertTrue(Bytes.equals(endKey, region.getEndKey()));
          assertEquals(location, region.getLocation());
          break;
        }
      }
      assertTrue(found);
    }
  }

  @Test
  public void testTableListText() throws IOException {
    Response response = client.get("/", Constants.MIMETYPE_TEXT);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_TEXT, response.getHeader("content-type"));
  }

  @Test
  public void testTableListXML() throws IOException, JAXBException {
    Response response = client.get("/", Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    TableListModel model = (TableListModel)
      context.createUnmarshaller()
        .unmarshal(new ByteArrayInputStream(response.getBody()));
    checkTableList(model);
  }

  @Test
  public void testTableListJSON() throws IOException {
    Response response = client.get("/", Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
  }

  @Test
  public void testTableListPB() throws IOException, JAXBException {
    Response response = client.get("/", Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    TableListModel model = new TableListModel();
    model.getObjectFromMessage(response.getBody());
    checkTableList(model);
    response = client.get("/", Constants.MIMETYPE_PROTOBUF_IETF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF_IETF, response.getHeader("content-type"));
    model = new TableListModel();
    model.getObjectFromMessage(response.getBody());
    checkTableList(model);
  }

  @Test
  public void testTableInfoText() throws IOException {
    Response response = client.get("/" + TABLE + "/regions", Constants.MIMETYPE_TEXT);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_TEXT, response.getHeader("content-type"));
  }

  @Test
  public void testTableInfoXML() throws IOException, JAXBException {
    Response response = client.get("/" + TABLE + "/regions",  Constants.MIMETYPE_XML);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_XML, response.getHeader("content-type"));
    TableInfoModel model = (TableInfoModel)
      context.createUnmarshaller()
        .unmarshal(new ByteArrayInputStream(response.getBody()));
    checkTableInfo(model);
  }

  @Test
  public void testTableInfoJSON() throws IOException {
    Response response = client.get("/" + TABLE + "/regions", Constants.MIMETYPE_JSON);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_JSON, response.getHeader("content-type"));
  }

  @Test
  public void testTableInfoPB() throws IOException, JAXBException {
    Response response = client.get("/" + TABLE + "/regions", Constants.MIMETYPE_PROTOBUF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF, response.getHeader("content-type"));
    TableInfoModel model = new TableInfoModel();
    model.getObjectFromMessage(response.getBody());
    checkTableInfo(model);
    response = client.get("/" + TABLE + "/regions", Constants.MIMETYPE_PROTOBUF_IETF);
    assertEquals(response.getCode(), 200);
    assertEquals(Constants.MIMETYPE_PROTOBUF_IETF, response.getHeader("content-type"));
    model = new TableInfoModel();
    model.getObjectFromMessage(response.getBody());
    checkTableInfo(model);
  }

}

