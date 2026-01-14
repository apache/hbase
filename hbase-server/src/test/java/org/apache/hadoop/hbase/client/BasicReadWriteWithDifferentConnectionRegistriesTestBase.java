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

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicReadWriteWithDifferentConnectionRegistriesTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBasicReadWriteWithDifferentConnectionRegistries.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  public enum RegistryImpl {
    ZK,
    RPC,
    ZK_URI,
    RPC_URI
  }

  private TableName tableName;

  private byte[] FAMILY = Bytes.toBytes("family");

  private Connection conn;

  protected abstract Configuration getConf();

  protected abstract Connection createConn(URI uri) throws IOException;

  private void init(RegistryImpl impl) throws Exception {
    switch (impl) {
      case ZK: {
        Configuration conf = getConf();
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
        Configuration conf = getConf();
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
        conn = createConn(connectionUri);
        break;
      }
      case RPC_URI: {
        URI connectionUri = new URI("hbase+rpc://"
          + UTIL.getMiniHBaseCluster().getMaster().getServerName().getAddress().toString());
        LOG.info("connect to cluster through connection url: {}", connectionUri);
        conn = createConn(connectionUri);
        break;
      }
      default:
        throw new IllegalArgumentException("Unknown impl: " + impl);
    }
    try (Admin admin = conn.getAdmin()) {
      admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    }
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    String displayName = testInfo.getTestMethod().get().getName() + testInfo.getDisplayName();
    tableName = TableName.valueOf(displayName.replaceAll("[ \\[\\]]", "_"));
  }

  @AfterEach
  public void tearDown() throws Exception {
    try (Admin admin = conn.getAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    conn.close();
  }

  @ParameterizedTest
  @EnumSource(value = RegistryImpl.class)
  public void testReadWrite(RegistryImpl impl) throws Exception {
    init(impl);
    byte[] row = Bytes.toBytes("row");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    try (Table table = conn.getTable(tableName)) {
      Put put = new Put(row).addColumn(FAMILY, qualifier, value);
      table.put(put);
      Result result = table.get(new Get(row));
      assertArrayEquals(value, result.getValue(FAMILY, qualifier));
      table.delete(new Delete(row));
      assertFalse(table.exists(new Get(row)));
    }
  }
}
