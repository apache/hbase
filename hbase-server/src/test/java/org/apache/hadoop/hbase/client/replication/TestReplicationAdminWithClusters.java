/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.client.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit testing of ReplicationAdmin with clusters
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestReplicationAdminWithClusters extends TestReplicationBase {

  static Connection connection1;
  static Connection connection2;
  static Admin admin1;
  static Admin admin2;
  static ReplicationAdmin adminExt;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();
    connection1 = ConnectionFactory.createConnection(conf1);
    connection2 = ConnectionFactory.createConnection(conf2);
    admin1 = connection1.getAdmin();
    admin2 = connection2.getAdmin();
    adminExt = new ReplicationAdmin(conf1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin1.close();
    admin2.close();
    adminExt.close();
    connection1.close();
    connection2.close();
    TestReplicationBase.tearDownAfterClass();
  }

  @Test(timeout = 300000)
  public void testEnableReplicationWhenSlaveClusterDoesntHaveTable() throws Exception {
    admin2.disableTable(tableName);
    admin2.deleteTable(tableName);
    assertFalse(admin2.tableExists(tableName));
    adminExt.enableTableRep(tableName);
    assertTrue(admin2.tableExists(tableName));
  }

  @Test(timeout = 300000)
  public void testEnableReplicationWhenReplicationNotEnabled() throws Exception {
    HTableDescriptor table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      fam.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    }
    admin1.disableTable(tableName);
    admin1.modifyTable(tableName, table);
    admin1.enableTable(tableName);

    admin2.disableTable(tableName);
    admin2.modifyTable(tableName, table);
    admin2.enableTable(tableName);

    adminExt.enableTableRep(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_GLOBAL);
    }
  }

  @Test(timeout = 300000)
  public void testEnableReplicationWhenTableDescriptorIsNotSameInClusters() throws Exception {
    HTableDescriptor table = admin2.getTableDescriptor(tableName);
    HColumnDescriptor f = new HColumnDescriptor("newFamily");
    table.addFamily(f);
    admin2.disableTable(tableName);
    admin2.modifyTable(tableName, table);
    admin2.enableTable(tableName);

    try {
      adminExt.enableTableRep(tableName);
      fail("Exception should be thrown if table descriptors in the clusters are not same.");
    } catch (RuntimeException ignored) {

    }
    admin1.disableTable(tableName);
    admin1.modifyTable(tableName, table);
    admin1.enableTable(tableName);
    adminExt.enableTableRep(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_GLOBAL);
    }
  }

  @Test(timeout = 300000)
  public void testDisableAndEnableReplication() throws Exception {
    adminExt.disableTableRep(tableName);
    HTableDescriptor table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_LOCAL);
    }
    table = admin2.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_LOCAL);
    }
    adminExt.enableTableRep(tableName);
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_GLOBAL);
    }
  }

  @Test(timeout = 300000, expected = TableNotFoundException.class)
  public void testDisableReplicationForNonExistingTable() throws Exception {
    adminExt.disableTableRep(TableName.valueOf("nonExistingTable"));
  }

  @Test(timeout = 300000, expected = TableNotFoundException.class)
  public void testEnableReplicationForNonExistingTable() throws Exception {
    adminExt.enableTableRep(TableName.valueOf("nonExistingTable"));
  }

  @Test(timeout = 300000, expected = IllegalArgumentException.class)
  public void testDisableReplicationWhenTableNameAsNull() throws Exception {
    adminExt.disableTableRep(null);
  }

  @Test(timeout = 300000, expected = IllegalArgumentException.class)
  public void testEnableReplicationWhenTableNameAsNull() throws Exception {
    adminExt.enableTableRep(null);
  }
  
  /*
   * Test enable table replication should create table only in user explicit specified table-cfs.
   * HBASE-14717
   */
  @Test(timeout = 300000)
  public void testEnableReplicationForExplicitSetTableCfs() throws Exception {
    TableName tn = TableName.valueOf("testEnableReplicationForSetTableCfs");
    String peerId = "2";
    if (admin2.isTableAvailable(tableName)) {
      admin2.disableTable(tableName);
      admin2.deleteTable(tableName);
    }
    assertFalse("Table should not exists in the peer cluster", admin2.isTableAvailable(tableName));

    Map<TableName, ? extends Collection<String>> tableCfs =
        new HashMap<TableName, Collection<String>>();
    tableCfs.put(tn, null);
    try {
      adminExt.setPeerTableCFs(peerId, tableCfs);
      adminExt.enableTableRep(tableName);
      assertFalse("Table should not be created if user has set table cfs explicitly for the "
          + "peer and this is not part of that collection",
        admin2.isTableAvailable(tableName));

      tableCfs.put(tableName, null);
      adminExt.setPeerTableCFs(peerId, tableCfs);
      adminExt.enableTableRep(tableName);
      assertTrue(
        "Table should be created if user has explicitly added table into table cfs collection",
        admin2.isTableAvailable(tableName));
    } finally {
      adminExt.removePeerTableCFs(peerId, adminExt.getPeerTableCFs(peerId));
      adminExt.disableTableRep(tableName);
    }
  }

  @Test(timeout=300000)
  public void testReplicationPeerConfigUpdateCallback() throws Exception {
    String peerId = "1";
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicationEndpointImpl(TestUpdatableReplicationEndpoint.class.getName());
    rpc.getConfiguration().put("key1", "value1");

    admin.addPeer(peerId, rpc);
    admin.peerAdded(peerId);

    rpc.getConfiguration().put("key1", "value2");
    admin.updatePeerConfig(peerId, rpc);
    if (!TestUpdatableReplicationEndpoint.hasCalledBack()) {
      synchronized(TestUpdatableReplicationEndpoint.class) {
        TestUpdatableReplicationEndpoint.class.wait(2000L);
      }
    }

    assertEquals(true, TestUpdatableReplicationEndpoint.hasCalledBack());
  }

  public static class TestUpdatableReplicationEndpoint extends BaseReplicationEndpoint {
    private static boolean calledBack = false;
    public static boolean hasCalledBack(){
      return calledBack;
    }
    @Override
    public synchronized void peerConfigUpdated(ReplicationPeerConfig rpc){
      calledBack = true;
      notifyAll();
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
      return UUID.randomUUID();
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      return false;
    }
  }
}
