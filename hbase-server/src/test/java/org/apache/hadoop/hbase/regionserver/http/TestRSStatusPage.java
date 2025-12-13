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
package org.apache.hadoop.hbase.regionserver.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.TestServerHttpUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests for the region server status page and its template.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestRSStatusPage {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSStatusPage.class);

  private static LocalHBaseCluster CLUSTER;

  private final static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  public static final String TEST_TABLE_NAME_1 = "TEST_TABLE_1";
  public static final String TEST_TABLE_NAME_2 = "TEST_TABLE_2";

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    UTIL.startMiniZKCluster();

    UTIL.startMiniDFSCluster(1);
    Path rootdir = UTIL.getDataTestDirOnTestFS("TestRSStatusPage");
    CommonFSUtils.setRootDir(conf, rootdir);

    // The info servers do not run in tests by default.
    // Set them to ephemeral ports so they will start
    // setup configuration
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);

    CLUSTER = new LocalHBaseCluster(conf, 1);
    CLUSTER.startup();
    CLUSTER.getActiveMaster().waitForMetaOnline();
  }

  /**
   * Helper method to shut down the cluster (if running)
   */
  @AfterClass
  public static void shutDownMiniCluster() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
      CLUSTER.join();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStatusPage() throws Exception {
    HMaster master = CLUSTER.getActiveMaster();

    int masterPort = master.getActiveMasterManager().getActiveMasterInfoPort();
    ServerName activeMaster =
      master.getActiveMaster().orElseThrow(() -> new IllegalStateException("No active master"));
    String masterHostname = activeMaster.getHostname();

    createTestTables(master);

    ServerManager serverManager = master.getServerManager();
    List<ServerName> onlineServersList = serverManager.getOnlineServersList();

    assertEquals(1, onlineServersList.size());

    ServerName firstServerName = onlineServersList.get(0);
    int infoPort = master.getRegionServerInfoPort(firstServerName);
    String hostname = firstServerName.getHostname();
    int port = firstServerName.getPort();

    URL url = new URL("http://" + hostname + ":" + infoPort + "/regionserver.jsp");
    String page = TestServerHttpUtils.getPageContent(url, "text/html;charset=utf-8");

    assertTrue(page.contains("<title>HBase Region Server: " + masterHostname + "</title>"));

    String expectedPageHeader = "<h1>RegionServer <small>" + hostname + "," + port + ","
      + firstServerName.getStartCode() + "</small></h1>";
    assertTrue(page.contains(expectedPageHeader));
    assertTrue(page.contains("<h2>Server Metrics</h2>"));
    assertTrue(page.contains("<th>Requests Per Second</th>"));
    assertTrue(page.contains("<h2>Block Cache</h2>"));
    assertTrue(page.contains("<h2>Regions</h2>"));
    assertTrue(page.contains("<h2>Replication Status</h2>"));
    assertTrue(page.contains("<h2>Software Attributes</h2>"));

    // Should have a link to master
    String expectedMasterLink = "<a href=\"//" + masterHostname + ":" + masterPort
      + "/master.jsp\">" + masterHostname + ":" + masterPort + "</a>";
    assertTrue(page.contains(expectedMasterLink));
  }

  private static void createTestTables(HMaster master) throws IOException {
    ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of("CF");
    TableDescriptor tableDescriptor1 = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(TEST_TABLE_NAME_1)).setColumnFamily(cf).build();
    master.createTable(tableDescriptor1, null, 0, 0);
    TableDescriptor tableDescriptor2 = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(TEST_TABLE_NAME_2)).setColumnFamily(cf).build();
    master.createTable(tableDescriptor2, null, 0, 0);
    master.flushMasterStore();
  }
}
