/**
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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hbase.HConstants.META_REPLICAS_NUM;
import static org.junit.Assert.assertEquals;

@Category({ LargeTests.class, ClientTests.class })
public class TestHMasterAsyncRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TestHMasterAsyncRegistry.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TEST_TABLE = TableName.valueOf("foo");
  private static final byte[] COL_FAM = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("col");

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHMasterAsyncRegistry.class);

  // Automatically cleaned up on cluster shutdown.
  private static AsyncClusterConnection customConnection;
  private static HMasterAsyncRegistry REGISTRY;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(META_REPLICAS_NUM, 3);
    TEST_UTIL.startMiniCluster(3);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    List<HostAndPort> hostAndPorts = new ArrayList<>();
    String masterHostName =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName().getHostname();
    int masterPort = TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName().getPort();
    // Add some invalid hostAndPort configs. The implementation should be resilient enough to
    // skip those and probe for the only working master.
    // 1. Valid hostname but invalid port
    hostAndPorts.add(HostAndPort.fromParts(masterHostName, 10001));
    // 2. Invalid hostname but valid port
    hostAndPorts.add(HostAndPort.fromParts("foo.bar", masterPort));
    // 3. Invalid hostname and port.
    hostAndPorts.add(HostAndPort.fromParts("foo.bar", 10003));
    // 4. Finally valid host:port
    hostAndPorts.add(HostAndPort.fromParts(masterHostName, masterPort));
    final String config = Joiner.on(",").join(hostAndPorts);
    conf.set(HMasterAsyncRegistry.CONF_KEY, config);
    conf.set(AsyncRegistryFactory.REGISTRY_IMPL_CONF_KEY, HMasterAsyncRegistry.class.getName());
    // make sure that we do not depend on this config when getting locations for meta replicas, see
    // HBASE-21658.
    conf.setInt(META_REPLICAS_NUM, 1);
    REGISTRY = new HMasterAsyncRegistry(conf);
    customConnection = TEST_UTIL.getCustomConnection(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRegistryImpl() throws Exception {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    assertEquals(REGISTRY.getClusterId().get(), master.getClusterId());
    assertEquals(REGISTRY.getMasterAddress().get(), master.getServerName());
  }

  /**
   * Tests basic create, put, scan operations using the connection.
   */
  @Test
  public void testCustomConnectionBasicOps() throws Exception {
    // Verify that the right registry is in use.
    assertTrue(customConnection instanceof AsyncClusterConnectionImpl);
    assertTrue(((AsyncClusterConnectionImpl) customConnection).getRegistry()
        instanceof HMasterAsyncRegistry);
    Connection connection = customConnection.toConnection();
    // Create a test table.
    Admin admin = connection.getAdmin();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TEST_TABLE).
        setColumnFamily(ColumnFamilyDescriptorBuilder.of(COL_FAM));
    admin.createTable(builder.build());
    try (Table table = connection.getTable(TEST_TABLE)){
      // Insert one row each region
      int insertNum = 10;
      for (int i = 0; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("row" + String.format("%03d", i)));
        put.addColumn(COL_FAM, QUALIFIER, QUALIFIER);
        table.put(put);
      }
      // Verify the row count.
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          Assert.assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }
    }
  }
}
