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
package org.apache.hadoop.hbase.client.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestExtension;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Unit testing of ReplicationAdmin with clusters
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestReplicationAdminWithClusters extends TestReplicationBase {

  static Connection connection1;
  static Connection connection2;
  static Admin admin1;
  static Admin admin2;
  static ReplicationAdmin adminExt;

  @RegisterExtension
  private final TableNameTestExtension tableNameExt = new TableNameTestExtension();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();
    connection1 = ConnectionFactory.createConnection(CONF1);
    connection2 = ConnectionFactory.createConnection(CONF2);
    admin1 = connection1.getAdmin();
    admin2 = connection2.getAdmin();
    adminExt = new ReplicationAdmin(CONF1);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    admin1.close();
    admin2.close();
    adminExt.close();
    connection1.close();
    connection2.close();
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void disableNotFullReplication() throws Exception {
    HTableDescriptor table = new HTableDescriptor(admin2.getTableDescriptor(tableName));
    HColumnDescriptor f = new HColumnDescriptor("notReplicatedFamily");
    table.addFamily(f);
    admin1.disableTable(tableName);
    admin1.modifyTable(tableName, table);
    admin1.enableTable(tableName);

    admin1.disableTableReplication(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, fam.getScope());
    }

    admin1.deleteColumnFamily(table.getTableName(), f.getName());
  }

  @Test
  public void testEnableReplicationWhenSlaveClusterDoesntHaveTable() throws Exception {
    admin1.disableTableReplication(tableName);
    admin2.disableTable(tableName);
    admin2.deleteTable(tableName);
    assertFalse(admin2.tableExists(tableName));
    admin1.enableTableReplication(tableName);
    assertTrue(admin2.tableExists(tableName));
  }

  @Test
  public void testEnableReplicationWhenReplicationNotEnabled() throws Exception {
    HTableDescriptor table = new HTableDescriptor(admin1.getTableDescriptor(tableName));
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      fam.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    }
    admin1.disableTable(tableName);
    admin1.modifyTable(tableName, table);
    admin1.enableTable(tableName);

    admin2.disableTable(tableName);
    admin2.modifyTable(tableName, table);
    admin2.enableTable(tableName);

    admin1.enableTableReplication(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, fam.getScope());
    }
  }

  @Test
  public void testEnableReplicationWhenTableDescriptorIsNotSameInClusters() throws Exception {
    HTableDescriptor table = new HTableDescriptor(admin2.getTableDescriptor(tableName));
    HColumnDescriptor f = new HColumnDescriptor("newFamily");
    table.addFamily(f);
    admin2.disableTable(tableName);
    admin2.modifyTable(tableName, table);
    admin2.enableTable(tableName);

    try {
      admin1.enableTableReplication(tableName);
      fail("Exception should be thrown if table descriptors in the clusters are not same.");
    } catch (RuntimeException ignored) {

    }
    admin1.disableTable(tableName);
    admin1.modifyTable(tableName, table);
    admin1.enableTable(tableName);
    admin1.enableTableReplication(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, fam.getScope());
    }

    admin1.deleteColumnFamily(tableName, f.getName());
    admin2.deleteColumnFamily(tableName, f.getName());
  }

  @Test
  public void testDisableAndEnableReplication() throws Exception {
    admin1.disableTableReplication(tableName);
    HTableDescriptor table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, fam.getScope());
    }
    admin1.enableTableReplication(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, fam.getScope());
    }
  }

  @Test
  public void testEnableReplicationForTableWithRegionReplica() throws Exception {
    TableName tn = tableNameExt.getTableName();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setRegionReplication(5)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(noRepfamName).build()).build();

    admin1.createTable(td);

    try {
      admin1.enableTableReplication(tn);
      td = admin1.getDescriptor(tn);
      for (ColumnFamilyDescriptor fam : td.getColumnFamilies()) {
        assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, fam.getScope());
      }
    } finally {
      UTIL1.deleteTable(tn);
      UTIL2.deleteTable(tn);
    }
  }

  @Test
  public void testDisableReplicationForNonExistingTable() throws Exception {
    assertThrows(TableNotFoundException.class,
      () -> admin1.disableTableReplication(tableNameExt.getTableName()));
  }

  @Test
  public void testEnableReplicationForNonExistingTable() throws Exception {
    assertThrows(TableNotFoundException.class,
      () -> admin1.enableTableReplication(tableNameExt.getTableName()));
  }

  @Test
  public void testDisableReplicationWhenTableNameAsNull() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> admin1.disableTableReplication(null));
  }

  @Test
  public void testEnableReplicationWhenTableNameAsNull() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> admin1.enableTableReplication(null));
  }

  /*
   * Test enable table replication should create table only in user explicit specified table-cfs.
   * HBASE-14717
   */
  @Test
  public void testEnableReplicationForExplicitSetTableCfs() throws Exception {
    final TableName tableName = tableNameExt.getTableName();
    String peerId = "2";
    if (admin2.isTableAvailable(TestReplicationBase.tableName)) {
      admin2.disableTable(TestReplicationBase.tableName);
      admin2.deleteTable(TestReplicationBase.tableName);
    }
    assertFalse(admin2.isTableAvailable(TestReplicationBase.tableName),
      "Table should not exists in the peer cluster");

    // update peer config
    ReplicationPeerConfig rpc = admin1.getReplicationPeerConfig(peerId);
    rpc.setReplicateAllUserTables(false);
    admin1.updateReplicationPeerConfig(peerId, rpc);

    Map<TableName, ? extends Collection<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, null);
    try {
      adminExt.setPeerTableCFs(peerId, tableCfs);
      admin1.enableTableReplication(TestReplicationBase.tableName);
      assertFalse(admin2.isTableAvailable(TestReplicationBase.tableName),
        "Table should not be created if user has set table cfs explicitly for the "
          + "peer and this is not part of that collection");

      tableCfs.put(TestReplicationBase.tableName, null);
      adminExt.setPeerTableCFs(peerId, tableCfs);
      admin1.enableTableReplication(TestReplicationBase.tableName);
      assertTrue(admin2.isTableAvailable(TestReplicationBase.tableName),
        "Table should be created if user has explicitly added table into table cfs collection");
    } finally {
      adminExt.removePeerTableCFs(peerId, adminExt.getPeerTableCFs(peerId));
      admin1.disableTableReplication(TestReplicationBase.tableName);

      rpc = admin1.getReplicationPeerConfig(peerId);
      rpc.setReplicateAllUserTables(true);
      admin1.updateReplicationPeerConfig(peerId, rpc);
    }
  }

  @Test
  public void testReplicationPeerConfigUpdateCallback() throws Exception {
    String peerId = "1";
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(UTIL2.getClusterKey());
    rpc.setReplicationEndpointImpl(TestUpdatableReplicationEndpoint.class.getName());
    rpc.getConfiguration().put("key1", "value1");

    admin1.addReplicationPeer(peerId, rpc);

    rpc.getConfiguration().put("key1", "value2");
    admin.updatePeerConfig(peerId, rpc);
    if (!TestUpdatableReplicationEndpoint.hasCalledBack()) {
      synchronized (TestUpdatableReplicationEndpoint.class) {
        TestUpdatableReplicationEndpoint.class.wait(2000L);
      }
    }

    assertEquals(true, TestUpdatableReplicationEndpoint.hasCalledBack());

    admin.removePeer(peerId);
  }

  public static class TestUpdatableReplicationEndpoint extends BaseReplicationEndpoint {
    private static boolean calledBack = false;

    public static boolean hasCalledBack() {
      return calledBack;
    }

    @Override
    public synchronized void peerConfigUpdated(ReplicationPeerConfig rpc) {
      calledBack = true;
      notifyAll();
    }

    @Override
    public void start() {
      startAsync();
    }

    @Override
    public void stop() {
      stopAsync();
    }

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      notifyStopped();
    }

    @Override
    public UUID getPeerUUID() {
      return UTIL1.getRandomUUID();
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      return false;
    }
  }
}
