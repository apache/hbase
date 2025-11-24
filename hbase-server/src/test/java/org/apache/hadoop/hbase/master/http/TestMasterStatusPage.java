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
package org.apache.hadoop.hbase.master.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterStatusPage {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterStatusPage.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  public static final String TEST_TABLE_NAME_1 = "TEST_TABLE_1";
  public static final String TEST_TABLE_NAME_2 = "TEST_TABLE_2";

  private static LocalHBaseCluster CLUSTER;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    UTIL.startMiniZKCluster();

    UTIL.startMiniDFSCluster(1);
    Path rootdir = UTIL.getDataTestDirOnTestFS("TestMasterStatusPage");
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
  public void testMasterStatusPage() throws Exception {
    HMaster master = CLUSTER.getActiveMaster();

    createTestTables(master);

    String page = getMasterStatusPageContent();

    String hostname = master.getServerName().getHostname();
    assertTrue(page.contains("<h1>Master <small>" + hostname + "</small></h1>"));
    assertTrue(page.contains("<h2><a name=\"regionservers\">Region Servers</a></h2>"));
    assertRegionServerLinks(master, page);

    assertTrue(page.contains("<h2>Backup Masters</h2>"));
    assertTrue(page.contains("<h2><a name=\"tables\">Tables</a></h2>"));
    assertTableLinks(master, page);

    assertTrue(page.contains("<h2><a name=\"region_visualizer\"></a>Region Visualizer</h2>"));
    assertTrue(page.contains("<h2><a name=\"peers\">Peers</a></h2>"));
    assertTrue(page.contains("<h2><a name=\"tasks\">Tasks</a></h2>"));
    assertTrue(page.contains("<h2><a name=\"attributes\">Software Attributes</a></h2>"));

    assertTrue(page.contains(VersionInfo.getVersion()));
  }

  private String getMasterStatusPageContent() throws IOException {
    URL url = new URL(getInfoServerHostAndPort() + "/master-status");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();

    assertEquals(200, conn.getResponseCode());
    assertEquals("text/html;charset=utf-8", conn.getContentType());

    return getResponseBody(conn);
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

  private String getInfoServerHostAndPort() {
    return "http://localhost:" + CLUSTER.getActiveMaster().getInfoServer().getPort();
  }

  private static String getResponseBody(HttpURLConnection conn) throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String output;
    while ((output = br.readLine()) != null) {
      sb.append(output);
    }
    return sb.toString();
  }

  private static void assertRegionServerLinks(HMaster master, String responseBody) {
    ServerManager serverManager = master.getServerManager();
    List<ServerName> servers = serverManager.getOnlineServersList();
    assertEquals(1, servers.size());
    for (ServerName serverName : servers) {
      String expectedRsLink = MasterStatusUtil.serverNameLink(master, serverName);
      assertTrue(responseBody.contains(expectedRsLink));
    }
  }

  private static void assertTableLinks(HMaster master, String responseBody) {
    List<TableDescriptor> tables = new ArrayList<>();
    String errorMessage = MasterStatusUtil.getUserTables(master, tables);
    assertNull(errorMessage);
    assertEquals(2, tables.size());
    for (TableDescriptor table : tables) {
      String tableName = table.getTableName().getNameAsString();
      String expectedTableLink = "<a href=table.jsp?name=" + tableName + ">" + tableName + "</a>";
      assertTrue(responseBody.contains(expectedTableLink));
    }
  }
}
