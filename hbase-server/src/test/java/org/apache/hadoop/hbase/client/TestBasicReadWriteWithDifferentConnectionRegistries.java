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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic read write operation with different {@link ConnectionRegistry} implementations.
 */
@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestBasicReadWriteWithDifferentConnectionRegistries {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBasicReadWriteWithDifferentConnectionRegistries.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBasicReadWriteWithDifferentConnectionRegistries.class);

  protected static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  public enum RegistryImpl {
    ZK,
    RPC,
    ZK_URI,
    RPC_URI
  }

  @Parameter
  public RegistryImpl impl;

  @Rule
  public final TableNameTestRule name = new TableNameTestRule();

  private byte[] FAMILY = Bytes.toBytes("family");

  private Connection conn;

  @Parameters(name = "{index}: impl={0}")
  public static List<Object[]> data() {
    List<Object[]> data = new ArrayList<Object[]>();
    for (RegistryImpl impl : RegistryImpl.values()) {
      data.add(new Object[] { impl });
    }
    return data;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  protected Connection createConnectionFromUri(URI uri) throws Exception {
    return ConnectionFactory.createConnection(uri);
  }

  @Before
  public void setUp() throws Exception {
    switch (impl) {
      case ZK: {
        Configuration conf = HBaseConfiguration.create(UTIL.getConfiguration());
        conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
          ZKConnectionRegistry.class, ConnectionRegistry.class);
        String quorum = UTIL.getZkCluster().getAddress().toString();
        String path = UTIL.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT);
        conf.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, quorum);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, path);
        LOG.info("connect to cluster through zk quorum={} and parent={}", quorum, path);
        conn = ConnectionFactory.createConnection(conf);
        break;
      }
      case RPC: {
        Configuration conf = HBaseConfiguration.create(UTIL.getConfiguration());
        conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
          RpcConnectionRegistry.class, ConnectionRegistry.class);
        String bootstrapServers =
          UTIL.getMiniHBaseCluster().getMaster().getServerName().getAddress().toString();
        conf.set(RpcConnectionRegistry.BOOTSTRAP_NODES, bootstrapServers);
        LOG.info("connect to cluster through rpc bootstrap servers={}", bootstrapServers);
        conn = ConnectionFactory.createConnection(conf);
        break;
      }
      case ZK_URI: {
        String quorum = UTIL.getZkCluster().getAddress().toString();
        String path = UTIL.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT);
        URI connectionUri = new URI("hbase+zk://" + quorum + path);
        LOG.info("connect to cluster through connection url: {}", connectionUri);
        conn = createConnectionFromUri(connectionUri);
        break;
      }
      case RPC_URI: {
        URI connectionUri = new URI("hbase+rpc://"
          + UTIL.getMiniHBaseCluster().getMaster().getServerName().getAddress().toString());
        LOG.info("connect to cluster through connection url: {}", connectionUri);
        conn = createConnectionFromUri(connectionUri);
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown impl: " + impl);
    }
    try (Admin admin = conn.getAdmin()) {
      admin.createTable(TableDescriptorBuilder.newBuilder(name.getTableName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    }
  }

  @After
  public void tearDown() throws Exception {
    TableName tableName = name.getTableName();
    try (Admin admin = conn.getAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    conn.close();
  }

  @Test
  public void testReadWrite() throws Exception {
    byte[] row = Bytes.toBytes("row");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    try (Table table = conn.getTable(name.getTableName())) {
      Put put = new Put(row).addColumn(FAMILY, qualifier, value);
      table.put(put);
      Result result = table.get(new Get(row));
      assertArrayEquals(value, result.getValue(FAMILY, qualifier));
      table.delete(new Delete(row));
      assertFalse(table.exists(new Get(row)));
    }
  }
}
