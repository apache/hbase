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
package org.apache.hadoop.hbase.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.TestServerHttpUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This class performs tests that ensure sensitive config values found in the HBase UI's Debug Dump
 * are redacted. A config property name must follow one of the regex patterns specified in
 * hadoop.security.sensitive-config-keys in order to have its value redacted.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestDebugDumpRedaction {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String XML_CONFIGURATION_START_TAG = "<configuration>";
  private static final String XML_CONFIGURATION_END_TAG = "</configuration>";
  private static final int SUBSTRING_OFFSET = XML_CONFIGURATION_END_TAG.length();
  private static final String PLAIN_TEXT_UTF8 = "text/plain;charset=utf-8";
  private static final String REDACTED = "******";
  private static final List<String> SENSITIVE_CONF_PROPERTIES =
    List.of("hbase.zookeeper.property.ssl.trustStore.password", "ssl.client.truststore.password",
      "hbase.rpc.tls.truststore.password", "ssl.server.keystore.password",
      "fs.s3a.server-side-encryption.key", "fs.s3a.encryption.algorithm", "fs.s3a.encryption.key",
      "fs.s3a.secret.key", "fs.s3a.important.secret.key", "fs.s3a.session.key",
      "fs.s3a.secret.session.key", "fs.s3a.session.token", "fs.s3a.secret.session.token",
      "fs.azure.account.key.importantKey", "fs.azure.oauth2.token", "fs.adl.oauth2.token",
      "fs.gs.encryption.sensitive", "fs.gs.proxy.important", "fs.gs.auth.sensitive.info",
      "sensitive.credential", "oauth.important.secret", "oauth.important.password",
      "oauth.important.token");
  private static LocalHBaseCluster CLUSTER;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDebugDumpRedaction.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();

    // Add various config properties with sensitive information that should be redacted
    // when the Debug Dump is performed in the UI. These properties are following the
    // regexes specified by the hadoop.security.sensitive-config-keys property.
    for (String property : SENSITIVE_CONF_PROPERTIES) {
      conf.set(property, "testPassword");
    }

    UTIL.startMiniZKCluster();

    UTIL.startMiniDFSCluster(1);
    Path rootdir = UTIL.getDataTestDirOnTestFS("TestDebugDumpServlet");
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

  @AfterClass
  public static void afterClass() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
      CLUSTER.join();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterPasswordsAreRedacted() throws IOException {
    URL debugDumpUrl =
      new URL(TestServerHttpUtils.getMasterInfoServerHostAndPort(CLUSTER) + "/dump");
    String response = TestServerHttpUtils.getPageContent(debugDumpUrl, PLAIN_TEXT_UTF8);

    // Verify this is the master server's debug dump
    assertTrue(
      response.startsWith("Master status for " + CLUSTER.getActiveMaster().getServerName()));

    verifyDebugDumpResponseConfig(response);
  }

  @Test
  public void testRegionServerPasswordsAreRedacted() throws IOException {
    HMaster master = CLUSTER.getActiveMaster();

    ServerManager serverManager = master.getServerManager();
    List<ServerName> onlineServersList = serverManager.getOnlineServersList();

    assertEquals(1, onlineServersList.size());

    ServerName regionServerName = onlineServersList.get(0);
    int regionServerInfoPort = master.getRegionServerInfoPort(regionServerName);
    String regionServerHostname = regionServerName.getHostname();

    URL debugDumpUrl =
      new URL("http://" + regionServerHostname + ":" + regionServerInfoPort + "/dump");
    String response = TestServerHttpUtils.getPageContent(debugDumpUrl, PLAIN_TEXT_UTF8);

    // Verify this is the region server's debug dump
    assertTrue(response.startsWith("RegionServer status for " + regionServerName));

    verifyDebugDumpResponseConfig(response);
  }

  private void verifyDebugDumpResponseConfig(String response) throws IOException {
    // Grab the server's config from the Debug Dump.
    String xmlString = response.substring(response.indexOf(XML_CONFIGURATION_START_TAG),
      response.indexOf(XML_CONFIGURATION_END_TAG) + SUBSTRING_OFFSET);

    // Convert the XML string into an InputStream
    Configuration conf = new Configuration(false);
    try (InputStream is = new ByteArrayInputStream(xmlString.getBytes(StandardCharsets.UTF_8))) {
      // Add the InputStream as a resource to the Configuration object
      conf.addResource(is, "DebugDumpXmlConfig");
    }

    // Verify all sensitive properties have had their values redacted
    for (String property : SENSITIVE_CONF_PROPERTIES) {
      assertEquals(REDACTED, conf.get(property));
    }
  }
}
