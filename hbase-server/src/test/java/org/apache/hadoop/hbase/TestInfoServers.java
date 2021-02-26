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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing, info servers are disabled.  This test enables then and checks that
 * they serve pages.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestInfoServers {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestInfoServers.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestInfoServers.class);
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // The info servers do not run in tests by default.
    // Set them to ephemeral ports so they will start
    UTIL.getConfiguration().setInt(HConstants.MASTER_INFO_PORT, 0);
    UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_INFO_PORT, 0);

    //We need to make sure that the server can be started as read only.
    UTIL.getConfiguration().setBoolean("hbase.master.ui.readonly", true);
    UTIL.startMiniCluster();
    if (!UTIL.getHBaseCluster().waitForActiveAndReadyMaster(30000)) {
      throw new RuntimeException("Active master not ready");
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetMasterInfoPort() throws Exception {
    try (Admin admin = UTIL.getAdmin()) {
      assertEquals(UTIL.getHBaseCluster().getMaster().getInfoServer().getPort(),
        admin.getMasterInfoPort());
    }
  }

  /**
   * Ensure when we go to top level index pages that we get redirected to an info-server specific
   * status page.
   */
  @Test
  public void testInfoServersRedirect() throws Exception {
    // give the cluster time to start up
    UTIL.getConnection().getTable(TableName.META_TABLE_NAME).close();
    int port = UTIL.getHBaseCluster().getMaster().getInfoServer().getPort();
    assertContainsContent(new URL("http://localhost:" + port + "/index.html"), "master-status");
    port = UTIL.getHBaseCluster().getRegionServerThreads().get(0).getRegionServer()
        .getInfoServer().getPort();
    assertContainsContent(new URL("http://localhost:" + port + "/index.html"), "rs-status");
  }

  /**
   * Test that the status pages in the minicluster load properly.
   *
   * This is somewhat a duplicate of TestRSStatusServlet and
   * TestMasterStatusServlet, but those are true unit tests
   * whereas this uses a cluster.
   */
  @Test
  public void testInfoServersStatusPages() throws Exception {
    int port = UTIL.getHBaseCluster().getMaster().getInfoServer().getPort();
    assertContainsContent(new URL("http://localhost:" + port + "/master-status"), "meta");
    port = UTIL.getHBaseCluster().getRegionServerThreads().get(0).getRegionServer()
        .getInfoServer().getPort();
    assertContainsContent(new URL("http://localhost:" + port + "/rs-status"), "meta");
  }

  @Test
  public void testMasterServerReadOnly() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] cf = Bytes.toBytes("d");
    UTIL.createTable(tableName, cf);
    UTIL.waitTableAvailable(tableName);
    HMaster master = UTIL.getHBaseCluster().getMaster();
    int port = master.getRegionServerInfoPort(master.getServerName());
    assertDoesNotContainContent(new URL("http://localhost:" + port + "/table.jsp?name=" +
      tableName + "&action=split&key="), "Table action request accepted");
    assertDoesNotContainContent(
      new URL("http://localhost:" + port + "/table.jsp?name=" + tableName), "Actions:");
  }

  private void assertContainsContent(final URL u, final String expected) throws IOException {
    LOG.info("Testing " + u.toString() + " has " + expected);
    String content = getUrlContent(u);
    assertTrue("expected=" + expected + ", content=" + content, content.contains(expected));
  }

  private void assertDoesNotContainContent(final URL u, final String expected) throws IOException {
    LOG.info("Testing " + u.toString() + " does not have " + expected);
    String content = getUrlContent(u);
    assertFalse("Does Not Contain =" + expected + ", content=" + content,
      content.contains(expected));
  }

  private String getUrlContent(URL u) throws IOException {
    java.net.URLConnection c = u.openConnection();
    c.setConnectTimeout(20000);
    c.setReadTimeout(20000);
    c.connect();
    try (InputStream in = c.getInputStream()) {
      return IOUtils.toString(in, HConstants.UTF8_ENCODING);
    }
  }
}
