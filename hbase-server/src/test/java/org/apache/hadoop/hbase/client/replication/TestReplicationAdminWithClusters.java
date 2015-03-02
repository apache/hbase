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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit testing of ReplicationAdmin with clusters
 */
@Category({ MediumTests.class })
public class TestReplicationAdminWithClusters extends TestReplicationBase {

  static HConnection connection1;
  static HConnection connection2;
  static HBaseAdmin admin1;
  static HBaseAdmin admin2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();
    connection1 = HConnectionManager.createConnection(conf1);
    connection2 = HConnectionManager.createConnection(conf2);
    admin1 = new HBaseAdmin(connection1.getConfiguration());
    admin2 = new HBaseAdmin(connection2.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin1.close();
    admin2.close();
    connection1.close();
    connection2.close();
    TestReplicationBase.tearDownAfterClass();
  }

  @Test(timeout = 300000)
  public void testEnableReplicationWhenSlaveClusterDoesntHaveTable() throws Exception {
    admin2.disableTable(tableName);
    admin2.deleteTable(tableName);
    assertFalse(admin2.tableExists(tableName));
    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.enableTableRep(TableName.valueOf(tableName));
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

    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.enableTableRep(TableName.valueOf(tableName));
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

    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    try {
      adminExt.enableTableRep(TableName.valueOf(tableName));
      fail("Exception should be thrown if table descriptors in the clusters are not same.");
    } catch (RuntimeException ignored) {

    }
    admin1.disableTable(tableName);
    admin1.modifyTable(tableName, table);
    admin1.enableTable(tableName);
    adminExt.enableTableRep(TableName.valueOf(tableName));
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_GLOBAL);
    }
  }

  @Test(timeout = 300000)
  public void testDisableAndEnableReplication() throws Exception {
    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.disableTableRep(TableName.valueOf(tableName));
    HTableDescriptor table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_LOCAL);
    }
    table = admin2.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_LOCAL);
    }
    adminExt.enableTableRep(TableName.valueOf(tableName));
    table = admin1.getTableDescriptor(tableName);
    for (HColumnDescriptor fam : table.getColumnFamilies()) {
      assertEquals(fam.getScope(), HConstants.REPLICATION_SCOPE_GLOBAL);
    }
  }

  @Test(timeout = 300000, expected = TableNotFoundException.class)
  public void testDisableReplicationForNonExistingTable() throws Exception {
    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.disableTableRep(TableName.valueOf("nonExistingTable"));
  }

  @Test(timeout = 300000, expected = TableNotFoundException.class)
  public void testEnableReplicationForNonExistingTable() throws Exception {
    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.enableTableRep(TableName.valueOf("nonExistingTable"));
  }

  @Test(timeout = 300000, expected = IllegalArgumentException.class)
  public void testDisableReplicationWhenTableNameAsNull() throws Exception {
    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.disableTableRep(null);
  }

  @Test(timeout = 300000, expected = IllegalArgumentException.class)
  public void testEnableReplicationWhenTableNameAsNull() throws Exception {
    ReplicationAdmin adminExt = new ReplicationAdmin(conf1);
    adminExt.enableTableRep(null);
  }
}
