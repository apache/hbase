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

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.BUFFER_SIZE_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * HBASE-19496 noticed that the RegionLoad/ServerLoad may be corrupted if rpc server
 * reuses the bytebuffer backed, so this test call the Admin#getLastMajorCompactionTimestamp() to
 * invoke HMaster to iterate all stored server/region loads.
 */
@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestServerLoadDurability {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerLoadDurability.class);

  private static final byte[] FAMILY = Bytes.toBytes("testFamily");

  @Parameterized.Parameter
  public Configuration conf;

  @Parameterized.Parameters
  public static final Collection<Object[]> parameters() {
    List<Object[]> configurations = new ArrayList<>();
    configurations.add(new Object[] { createConfigurationForSimpleRpcServer() });
    configurations.add(new Object[] { createConfigurationForNettyRpcServer() });
    return configurations;
  }

  private static Configuration createConfigurationForSimpleRpcServer() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, SimpleRpcServer.class.getName());
    conf.setInt(BUFFER_SIZE_KEY, 20);
    return conf;
  }

  private static Configuration createConfigurationForNettyRpcServer() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
        NettyRpcServer.class.getName());
    return conf;
  }

  protected HBaseTestingUtility utility;
  protected Connection conn;
  protected Admin admin;

  @Rule
  public TestName testName = new TestName();
  protected TableName tableName;

  @Before
  public void setUp() throws Exception {
    utility = new HBaseTestingUtility(conf);
    utility.startMiniCluster(2);
    conn = ConnectionFactory.createConnection(utility.getConfiguration());
    admin = conn.getAdmin();
    String methodName = testName.getMethodName();
    tableName = TableName.valueOf(methodName.substring(0, methodName.length() - 3));
  }

  @After
  public void tearDown() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Test
  public void testCompactionTimestamps() throws Exception {
    createTableWithDefaultConf(tableName);
    try (Table table = conn.getTable(tableName)) {
      long ts = admin.getLastMajorCompactionTimestamp(tableName);
    }
  }

  private void createTableWithDefaultConf(TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build());
  }

}
