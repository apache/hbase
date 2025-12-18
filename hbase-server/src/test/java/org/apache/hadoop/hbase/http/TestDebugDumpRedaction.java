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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs tests that ensure sensitive config values found in the HBase UI's Debug Dump
 * are redacted. A config property name must follow one of the regex patterns specified in
 * hadoop.security.sensitive-config-keys in order to have its value redacted.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestDebugDumpRedaction {
  private static final Logger LOG = LoggerFactory.getLogger(TestDebugDumpRedaction.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String XML_CONFIGURATION_START_TAG = "<configuration>";
  private static final String XML_CONFIGURATION_END_TAG = "</configuration>";
  private static final int SUBSTRING_OFFSET = XML_CONFIGURATION_END_TAG.length();
  private static final String REDACTED_TEXT = "******";

  // These are typical configuration properties whose values we would want to see redacted.
  private static final List<String> SENSITIVE_CONF_PROPERTIES =
    Arrays.asList("hbase.zookeeper.property.ssl.trustStore.password",
      "ssl.client.truststore.password", "hbase.rpc.tls.truststore.password",
      "ssl.server.keystore.password", "fs.s3a.server-side-encryption.key",
      "fs.s3a.encryption.algorithm", "fs.s3a.encryption.key", "fs.s3a.secret.key",
      "fs.s3a.important.secret.key", "fs.s3a.session.key", "fs.s3a.secret.session.key",
      "fs.s3a.session.token", "fs.s3a.secret.session.token", "fs.azure.account.key.importantKey",
      "fs.azure.oauth2.token", "fs.adl.oauth2.token", "fs.gs.encryption.sensitive",
      "fs.gs.proxy.important", "fs.gs.auth.sensitive.info", "sensitive.credential",
      "oauth.important.secret", "oauth.important.password", "oauth.important.token",
      "fs.adl.oauth2.access.token.provider.type", "hadoop.security.sensitive-config-keys");

  // These are not typical configuration properties whose values we would want to see redacted,
  // but we are testing their redaction anyway because we want to see how the redaction behaves
  // with booleans and ints.
  private static final List<String> NON_SENSITIVE_KEYS_WITH_DEFAULT_VALUES = Arrays.asList(
    "hbase.zookeeper.quorum", "hbase.cluster.distributed", "hbase.master.logcleaner.ttl",
    "hbase.master.hfilecleaner.plugins", "hbase.master.infoserver.redirect",
    "hbase.thrift.minWorkerThreads", "hbase.table.lock.enable");

  // We also want to verify the behavior for a string with value "null" and an empty string.
  // (giving a config property an actual null value will throw an error)
  private static final String NULL_CONFIG_KEY = "null.key";
  private static final String EMPTY_CONFIG_KEY = "empty.key";

  // Combine all properties we want to redact into one list
  private static final List<String> REDACTED_PROPS =
    Stream.of(SENSITIVE_CONF_PROPERTIES, NON_SENSITIVE_KEYS_WITH_DEFAULT_VALUES,
      List.of(NULL_CONFIG_KEY, EMPTY_CONFIG_KEY)).flatMap(Collection::stream).toList();

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

    // Also verify a null string and empty string will get redacted.
    // Setting the config to use an actual null value throws an error.
    conf.set(NULL_CONFIG_KEY, "null");
    conf.set(EMPTY_CONFIG_KEY, "");

    // Config properties following these regex patterns will have their values redacted in the
    // Debug Dump
    String sensitiveKeyRegexes = "secret$,password$,ssl.keystore.pass$,"
      + "fs.s3a.server-side-encryption.key,fs.s3a.*.server-side-encryption.key,"
      + "fs.s3a.encryption.algorithm,fs.s3a.encryption.key,fs.s3a.secret.key,"
      + "fs.s3a.*.secret.key,fs.s3a.session.key,fs.s3a.*.session.key,fs.s3a.session.token,"
      + "fs.s3a.*.session.token,fs.azure.account.key.*,fs.azure.oauth2.*,fs.adl.oauth2.*,"
      + "fs.gs.encryption.*,fs.gs.proxy.*,fs.gs.auth.*,credential$,oauth.*secret,"
      + "oauth.*password,oauth.*token,hadoop.security.sensitive-config-keys,"
      + String.join(",", NON_SENSITIVE_KEYS_WITH_DEFAULT_VALUES) + "," + NULL_CONFIG_KEY + ","
      + EMPTY_CONFIG_KEY;

    conf.set("hadoop.security.sensitive-config-keys", sensitiveKeyRegexes);

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
    String response = TestServerHttpUtils.getMasterPageContent(CLUSTER);

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

    String response =
      TestServerHttpUtils.getRegionServerPageContent(regionServerHostname, regionServerInfoPort);

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

    // Verify the expected properties had their values redacted
    for (String property : REDACTED_PROPS) {
      LOG.info("Verifying property has been redacted: {}", property);
      assertEquals("Expected " + property + " to have its value redacted", REDACTED_TEXT,
        conf.get(property));
    }

    String propertyName;
    for (Map.Entry<String, String> property : conf) {
      propertyName = property.getKey();
      if (!REDACTED_PROPS.contains(propertyName)) {
        LOG.info("Verifying {} property has not had its value redacted", propertyName);
        assertNotEquals("Expected property " + propertyName + " to not have its value redacted",
          REDACTED_TEXT, conf.get(propertyName));
      }
    }
  }
}
