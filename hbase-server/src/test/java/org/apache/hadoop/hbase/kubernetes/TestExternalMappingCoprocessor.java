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
package org.apache.hadoop.hbase.kubernetes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MediumTests.TAG)
public class TestExternalMappingCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(TestExternalMappingCoprocessor.class);

  private static final String HOST_NAME;
  private static final String HOST_ADDRESS;

  private static HBaseTestingUtil TEST_UTIL;
  private static TableName TEST_TABLE_NAME = TableName.valueOf("test");

  static {
    try {
      HOST_NAME = InetAddress.getLocalHost().getHostName();
      HOST_ADDRESS = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public TestExternalMappingCoprocessor() {
  }

  @BeforeAll
  public static void setupBeforeClass(@TempDir Path tempDir) throws Exception {
    /*
     * Creating a mapping for hostname -> hostaddress. HBase always advertises itself with the name,
     * not the address. So we can add a simple IP address replacement.
     */
    Properties properties = new Properties();
    properties.setProperty(HOST_NAME, HOST_ADDRESS);

    StringWriter writer = new StringWriter();
    properties.store(writer, null);

    Path mappingPath = tempDir.resolve("mapping.properties");
    Files.writeString(mappingPath, writer.toString());

    Configuration configuration = HBaseConfiguration.create();

    String className = ExternalKubernetesCoprocessor.class.getName();
    configuration.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, className);
    configuration.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, className);
    configuration.set(CoprocessorHost.CLIENT_META_COPROCESSOR_CONF_KEY, className);
    configuration.set("hbase.kubernetes.external.mapping", mappingPath.toString());

    TEST_UTIL = new HBaseTestingUtil(configuration);
    TEST_UTIL.startMiniCluster();

    TEST_UTIL.createTable(TEST_TABLE_NAME, "family");
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNoReplacement() throws IOException {
    ServerName serverName = TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName();

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.masters", serverName.getHostname() + ":" + serverName.getPort());

    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      List<HRegionLocation> locations =
        connection.getTable(TEST_TABLE_NAME).getRegionLocator().getAllRegionLocations();

      for (HRegionLocation location : locations) {
        assertEquals(HOST_NAME, location.getHostname());
      }

      ClusterMetrics clusterMetrics = connection.getAdmin().getClusterMetrics();

      ServerName masterServerName = clusterMetrics.getMasterName();
      assertEquals(HOST_NAME, masterServerName.getHostname());

      for (ServerName regionServerName : clusterMetrics.getLiveServerMetrics().keySet()) {
        assertEquals(HOST_NAME, regionServerName.getHostname());
      }
    }
  }

  @Test
  public void testReplacement() throws IOException {
    ServerName serverName = TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName();

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.masters", serverName.getHostname() + ":" + serverName.getPort());
    configuration.set("hbase.client.header.kubernetes", "true");

    /*
     * This test implicitly tests ClientMetaService replacement, otherwise we won't be able to find
     * master server and meta region server.
     */

    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      List<HRegionLocation> locations =
        connection.getTable(TEST_TABLE_NAME).getRegionLocator().getAllRegionLocations();

      for (HRegionLocation location : locations) {
        assertEquals(HOST_ADDRESS, location.getHostname());
      }

      ClusterMetrics clusterMetrics = connection.getAdmin().getClusterMetrics();

      ServerName masterServerName = clusterMetrics.getMasterName();
      assertEquals(HOST_ADDRESS, masterServerName.getHostname());

      for (ServerName regionServerName : clusterMetrics.getLiveServerMetrics().keySet()) {
        assertEquals(HOST_ADDRESS, regionServerName.getHostname());
      }
    }
  }
}
