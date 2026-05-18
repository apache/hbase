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

import static org.apache.hadoop.hbase.client.AsyncConnectionConfiguration.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

/**
 * Class to test asynchronous replication admin operations when more than 1 cluster
 */
@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: policy = {0}")
public class TestAsyncReplicationAdminApiWithClusters extends TestAsyncAdminBase {

  private final static String ID_SECOND = "2";

  private static HBaseTestingUtil TEST_UTIL2;
  private static Configuration conf2;
  private static AsyncAdmin admin2;
  private static AsyncConnection connection;

  public TestAsyncReplicationAdminApiWithClusters(Supplier<AsyncAdmin> admin) {
    super(admin);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster();
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    conf2 = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    TEST_UTIL2 = new HBaseTestingUtil(conf2);
    TEST_UTIL2.startMiniCluster();

    connection = ConnectionFactory.createAsyncConnection(TEST_UTIL2.getConfiguration()).get();
    admin2 = connection.getAdmin();

    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(TEST_UTIL2.getRpcConnnectionURI()).build();
    ASYNC_CONN.getAdmin().addReplicationPeer(ID_SECOND, rpc).join();
  }

  @AfterAll
  public static void clearUp() throws Exception {
    TestAsyncAdminBase.tearDownAfterClass();
    connection.close();
  }

  @Override
  @AfterEach
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

  @TestTemplate
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

  @TestTemplate
  public void testEnableReplicationWhenSlaveClusterDoesntHaveTable() throws Exception {
    // Only create table in source cluster
    createTableWithDefaultConf(tableName);
    assertFalse(admin2.tableExists(tableName).get());
    admin.enableTableReplication(tableName).join();
    assertTrue(admin2.tableExists(tableName).get());
  }

  @TestTemplate
  public void testEnableReplicationWhenTableDescriptorIsNotSameInClusters() throws Exception {
    createTableWithDefaultConf(admin, tableName);
    createTableWithDefaultConf(admin2, tableName);
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName).get());
    builder.setColumnFamily(
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("newFamily")).build());
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

  @TestTemplate
  public void testDisableReplicationForNonExistingTable() throws Exception {
    try {
      admin.disableTableReplication(tableName).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableNotFoundException);
    }
  }

  @TestTemplate
  public void testEnableReplicationForNonExistingTable() throws Exception {
    try {
      admin.enableTableReplication(tableName).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableNotFoundException);
    }
  }

  @TestTemplate
  public void testDisableReplicationWhenTableNameAsNull() throws Exception {
    try {
      admin.disableTableReplication(null).join();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @TestTemplate
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
  @TestTemplate
  public void testEnableReplicationForExplicitSetTableCfs() throws Exception {
    TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "2");
    // Only create table in source cluster
    createTableWithDefaultConf(tableName);
    createTableWithDefaultConf(tableName2);
    assertFalse(admin2.tableExists(tableName).get(), "Table should not exists in the peer cluster");
    assertFalse(admin2.tableExists(tableName2).get(),
      "Table should not exists in the peer cluster");

    Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, null);
    ReplicationPeerConfigBuilder rpcBuilder =
      ReplicationPeerConfig.newBuilder(admin.getReplicationPeerConfig(ID_SECOND).get())
        .setReplicateAllUserTables(false).setTableCFsMap(tableCfs);
    try {
      // Only add tableName to replication peer config
      admin.updateReplicationPeerConfig(ID_SECOND, rpcBuilder.build()).join();
      admin.enableTableReplication(tableName2).join();
      assertFalse(admin2.tableExists(tableName2).get(), "Table should not be created if user "
        + "has set table cfs explicitly for the peer and this is not part of that collection");

      // Add tableName2 to replication peer config, too
      tableCfs.put(tableName2, null);
      rpcBuilder.setTableCFsMap(tableCfs);
      admin.updateReplicationPeerConfig(ID_SECOND, rpcBuilder.build()).join();
      admin.enableTableReplication(tableName2).join();
      assertTrue(admin2.tableExists(tableName2).get(),
        "Table should be created if user has explicitly added table into table cfs collection");
    } finally {
      rpcBuilder.setTableCFsMap(null).setReplicateAllUserTables(true).build();
      admin.updateReplicationPeerConfig(ID_SECOND, rpcBuilder.build()).join();
    }
  }
}
