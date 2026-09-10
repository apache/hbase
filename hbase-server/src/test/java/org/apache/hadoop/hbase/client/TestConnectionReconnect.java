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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Test of that unmanaged HConnections are able to reconnect properly (see HBASE-5058)
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: registryImpl={0}")
public class TestConnectionReconnect {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName NAME = TableName.valueOf("reconnect");

  private Class<? extends ConnectionRegistry> registryImpl;

  public TestConnectionReconnect(Class<? extends ConnectionRegistry> registryImpl) {
    this.registryImpl = registryImpl;
  }

  @SuppressWarnings("deprecation")
  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(RpcConnectionRegistry.class),
      Arguments.of(ZKConnectionRegistry.class));
  }

  @BeforeAll
  public static void setUpBeforeAll() throws Exception {
    UTIL.startMiniCluster(1);
    UTIL.createTable(NAME, HConstants.CATALOG_FAMILY);
    UTIL.waitTableAvailable(NAME);
  }

  @AfterAll
  public static void tearDownAfterAll() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private Connection getConnection() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, registryImpl,
      ConnectionRegistry.class);
    conf.set(RpcConnectionRegistry.BOOTSTRAP_NODES,
      UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName().getAddress().toString());
    return ConnectionFactory.createConnection(conf);
  }

  @TestTemplate
  public void testReconnect() throws Exception {
    try (Connection conn = getConnection()) {
      try (Table t = conn.getTable(NAME); Admin admin = conn.getAdmin()) {
        assertTrue(admin.tableExists(NAME));
        assertTrue(t.get(new Get(Bytes.toBytes(0))).isEmpty());
      }

      // stop the master
      SingleProcessHBaseCluster cluster = UTIL.getHBaseCluster();

      cluster.stopMaster(0, false);
      cluster.waitOnMaster(0);

      // start up a new master
      cluster.startMaster();
      assertTrue(cluster.waitForActiveAndReadyMaster());

      // test that the same unmanaged connection works with a new
      // Admin and can connect to the new master;
      try (Admin admin = conn.getAdmin()) {
        assertTrue(admin.tableExists(NAME));
        assertEquals(
          admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().size(),
          1);
      }
    }
  }
}
