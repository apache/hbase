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

import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous replication admin operations when more than 1 cluster
 */
@RunWith(Parameterized.class)
@Category({LargeTests.class, ClientTests.class})
public class TestAsyncReplicationAdminApiWithClusters extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncReplicationAdminApiWithClusters.class);

  private final static String ID_SECOND = "2";

  private static HBaseTestingUtility TEST_UTIL2;
  private static Configuration conf2;
  private static AsyncAdmin admin2;
  private static AsyncConnection connection;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster();
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    conf2 = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    TEST_UTIL2 = new HBaseTestingUtility(conf2);
    TEST_UTIL2.startMiniCluster();

    connection =
      ConnectionFactory.createAsyncConnection(TEST_UTIL2.getConfiguration()).get();
    admin2 = connection.getAdmin();

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(TEST_UTIL2.getClusterKey());
    ASYNC_CONN.getAdmin().addReplicationPeer(ID_SECOND, rpc).join();
  }

  @AfterClass
  public static void clearUp() throws IOException {
    connection.close();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    Pattern pattern = Pattern.compile(tableName.getNameAsString() + ".*");
    cleanupTables(admin, pattern);
    cleanupTables(admin2, pattern);
  }

  private void cleanupTables(AsyncAdmin admin, Pattern pattern) {
    admin.listTableNames(pattern, false).whenCompleteAsync((tables, err) -> {
      if (tables != null) {
        tables.forEach(table -> {
          try {
            admin.disableTable(table).join();
          } catch (Exception e) {
            LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
          }
          admin.deleteTable(table).join();
        });
      }
    }, ForkJoinPool.commonPool()).join();
  }

  private void createTableWithDefaultConf(AsyncAdmin admin, TableName tableName) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build()).join();
  }

  @Test
  public void testEnableAndDisableTableReplication() throws Exception {
    // default replication scope is local
    createTableWithDefaultConf(tableName);
    admin.enableTableReplication(tableName).join();
    TableDescriptor tableDesc = admin.getDescriptor(tableName).get();
    for (ColumnFamilyDescriptor fam : tableDesc.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, fam.getScope());
    }

    admin.disableTableReplication(tableName).join();
    tableDesc = admin.getDescriptor(tableName).get();
    for (ColumnFamilyDescriptor fam : tableDesc.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, fam.getScope());
    }
  }

  @Test
  public void testEnableReplicationWhenSlaveClusterDoesntHaveTable() throws Exception {
    // Only create table in source cluster
    createTableWithDefaultConf(tableName);
    assertFalse(admin2.tableExists(tableName).get());
    admin.enableTableReplication(tableName).join();
    assertTrue(admin2.tableExists(tableName).get());
  }

  @Test
  public void testEnableReplicationWhenTableDescriptorIsNotSameInClusters() throws Exception {
    createTableWithDefaultConf(admin, tableName);
    createTableWithDefaultConf(admin2, tableName);
    TableDescriptorBuilder builder =
        TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName).get());
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("newFamily"))
        .build());
    admin2.disableTable(tableName).join();
    admin2.modifyTable(builder.build()).join();
    admin2.enableTable(tableName).join();

    try {
      admin.enableTableReplication(tableName).join();
      fail("Exception should be thrown if table descriptors in the clusters are not same.");
    } catch (Exception ignored) {
      // ok
    }

    admin.disableTable(tableName).join();
    admin.modifyTable(builder.build()).join();
    admin.enableTable(tableName).join();
    admin.enableTableReplication(tableName).join();
    TableDescriptor tableDesc = admin.getDescriptor(tableName).get();
    for (ColumnFamilyDescriptor fam : tableDesc.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, fam.getScope());
    }
  }

  @Test
  public void testDisableReplicationForNonExistingTable() throws Exception {
    try {
      admin.disableTableReplication(tableName).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableNotFoundException);
    }
  }

  @Test
  public void testEnableReplicationForNonExistingTable() throws Exception {
    try {
      admin.enableTableReplication(tableName).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableNotFoundException);
    }
  }

  @Test
  public void testDisableReplicationWhenTableNameAsNull() throws Exception {
    try {
      admin.disableTableReplication(null).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testEnableReplicationWhenTableNameAsNull() throws Exception {
    try {
      admin.enableTableReplication(null).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  /*
   * Test enable table replication should create table only in user explicit specified table-cfs.
   * HBASE-14717
   */
  @Test
  public void testEnableReplicationForExplicitSetTableCfs() throws Exception {
    TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "2");
    // Only create table in source cluster
    createTableWithDefaultConf(tableName);
    createTableWithDefaultConf(tableName2);
    assertFalse("Table should not exists in the peer cluster",
      admin2.tableExists(tableName).get());
    assertFalse("Table should not exists in the peer cluster",
      admin2.tableExists(tableName2).get());

    Map<TableName, ? extends Collection<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, null);
    ReplicationPeerConfig rpc = admin.getReplicationPeerConfig(ID_SECOND).get();
    rpc.setReplicateAllUserTables(false);
    rpc.setTableCFsMap(tableCfs);
    try {
      // Only add tableName to replication peer config
      admin.updateReplicationPeerConfig(ID_SECOND, rpc).join();
      admin.enableTableReplication(tableName2).join();
      assertFalse("Table should not be created if user has set table cfs explicitly for the "
          + "peer and this is not part of that collection", admin2.tableExists(tableName2).get());

      // Add tableName2 to replication peer config, too
      tableCfs.put(tableName2, null);
      rpc.setTableCFsMap(tableCfs);
      admin.updateReplicationPeerConfig(ID_SECOND, rpc).join();
      admin.enableTableReplication(tableName2).join();
      assertTrue(
        "Table should be created if user has explicitly added table into table cfs collection",
        admin2.tableExists(tableName2).get());
    } finally {
      rpc.setTableCFsMap(null);
      rpc.setReplicateAllUserTables(true);
      admin.updateReplicationPeerConfig(ID_SECOND, rpc).join();
    }
  }
}
