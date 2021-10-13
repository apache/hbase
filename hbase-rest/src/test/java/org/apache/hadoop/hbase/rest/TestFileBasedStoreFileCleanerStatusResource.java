/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.FileBasedStoreFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.FileBasedStoreFileCleanerStatusesModel;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RestTests.class, MediumTests.class })
public class TestFileBasedStoreFileCleanerStatusResource {
  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFileBasedStoreFileCleanerStatusResource.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL = new HBaseRESTTestingUtility();
  private static Client client;
  private static JAXBContext context;

  private final static byte[] fam = Bytes.toBytes("cf_1");
  private final static byte[] qual1 = Bytes.toBytes("qf_1");
  private final static byte[] val = Bytes.toBytes("val");
  private final static String junkFileName = "409fad9a751c4e8c86d7f32581bdc156";
  TableName tableName;

  @BeforeClass public static void setUpBeforeClass() throws Exception {

    TEST_UTIL.getConfiguration().set(StoreFileTrackerFactory.TRACKER_IMPL,
      "org.apache.hadoop.hbase.regionserver.storefiletracker.FileBasedStoreFileTracker");
    TEST_UTIL.getConfiguration()
      .set(FileBasedStoreFileCleaner.FILEBASED_STOREFILE_CLEANER_ENABLED, "true");
    TEST_UTIL.getConfiguration()
      .set(FileBasedStoreFileCleaner.FILEBASED_STOREFILE_CLEANER_TTL, "0");
    TEST_UTIL.getConfiguration()
      .set(FileBasedStoreFileCleaner.FILEBASED_STOREFILE_CLEANER_PERIOD, "15000000");
    TEST_UTIL.getConfiguration()
      .set(FileBasedStoreFileCleaner.FILEBASED_STOREFILE_CLEANER_DELAY, "0");
    TEST_UTIL.startMiniCluster(1);

    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost", REST_TEST_UTIL.getServletPort()));
    context = JAXBContext.newInstance(FileBasedStoreFileCleanerStatusesModel.class);
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test public void testDeletingJunkFile() throws Exception {
    tableName = TableName.valueOf(getClass().getSimpleName() + "testDeletingJunkFile");
    createTableWithData(tableName);

    String namespacePath = "/status/fileBasedStoreFileCleaner/";
    FileBasedStoreFileCleanerStatusesModel model;
    Response response;

    HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
    ServerName sn = TEST_UTIL.getMiniHBaseCluster()
      .getServerHoldingRegion(tableName, region.getRegionInfo().getRegionName());
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(sn);
    FileBasedStoreFileCleaner cleaner = rs.getFileBasedStoreFileCleaner();

    //create junk file
    HStore store = region.getStore(fam);
    Path cfPath = store.getRegionFileSystem().getStoreDir(store.getColumnFamilyName());
    Path junkFilePath = new Path(cfPath, junkFileName);

    FSDataOutputStream junkFileOS = store.getFileSystem().create(junkFilePath);
    junkFileOS.writeUTF("hello");
    junkFileOS.close();

    int storeFiles = store.getStorefilesCount();
    assertTrue(storeFiles > 0);

    //verify the file exist before the chore and missing afterwards
    assertTrue(store.getFileSystem().exists(junkFilePath));
    cleaner.chore();
    assertFalse(store.getFileSystem().exists(junkFilePath));

    //verify no storefile got deleted
    int currentStoreFiles = store.getStorefilesCount();
    assertEquals(currentStoreFiles, storeFiles);

    //testing XML
    response = client.get(namespacePath, Constants.MIMETYPE_XML);
    assertEquals(200, response.getCode());
    model = fromXML(response.getBody());
    assertEquals(2, model.getFileBasedFileStoreCleanerStatuses().size());
    assertNotNull(model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME));
    assertEquals(1, model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME).getDeletedFiles());

    //testing JSON
    ObjectMapper jsonMapper =
      new JacksonJaxbJsonProvider().locateMapper(FileBasedStoreFileCleanerStatusesModel.class,
        MediaType.APPLICATION_JSON_TYPE);
    response = client.get(namespacePath, Constants.MIMETYPE_JSON);
    assertEquals(200, response.getCode());
    model = jsonMapper.readValue(response.getBody(), FileBasedStoreFileCleanerStatusesModel.class);
    assertEquals(2, model.getFileBasedFileStoreCleanerStatuses().size());
    assertNotNull(model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME));
    assertEquals(1, model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME).getDeletedFiles());

    //test PROTOBUF
    response = client.get(namespacePath, Constants.MIMETYPE_PROTOBUF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    assertEquals(2, model.getFileBasedFileStoreCleanerStatuses().size());
    assertNotNull(model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME));
    assertEquals(1, model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME).getDeletedFiles());

    //test MIMETYPE_PROTOBUF_IETF
    response = client.get(namespacePath, Constants.MIMETYPE_PROTOBUF_IETF);
    assertEquals(200, response.getCode());
    model.getObjectFromMessage(response.getBody());
    assertEquals(2, model.getFileBasedFileStoreCleanerStatuses().size());
    assertNotNull(model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME));
    assertEquals(1, model.getFileBasedFileStoreCleanerStatuses()
      .get(HMaster.AGGREGATED_FILEBASED_STOREFILE_CLEANER_NAME).getDeletedFiles());

    //test TEXT
    response = client.get(namespacePath, Constants.MIMETYPE_TEXT);
    assertEquals(200, response.getCode());

  }

  private Table createTableWithData(TableName tableName) throws IOException {
    Table table = TEST_UTIL.createTable(tableName, fam);
    try {
      for (int i = 1; i < 10; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, qual1, val);
        table.put(p);
      }
      // flush them
      TEST_UTIL.getAdmin().flush(tableName);
      for (int i = 11; i < 20; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, qual1, val);
        table.put(p);
      }
      // flush them
      TEST_UTIL.getAdmin().flush(tableName);
      for (int i = 21; i < 30; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, qual1, val);
        table.put(p);
      }
      // flush them
      TEST_UTIL.getAdmin().flush(tableName);
    } catch (IOException e) {
      table.close();
      throw e;
    }
    return table;
  }

  private static FileBasedStoreFileCleanerStatusesModel fromXML(byte[] content) throws JAXBException {
    return (FileBasedStoreFileCleanerStatusesModel) context.createUnmarshaller()
      .unmarshal(new ByteArrayInputStream(content));
  }
}
