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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestReplicationAdminWithTwoDifferentZKClusters {

  private static Configuration conf1 = HBaseConfiguration.create();
  private static Configuration conf2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static ReplicationAdmin admin;

  private static final TableName tableName = TableName.valueOf("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final String peerId = "peer1";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniCluster();
    admin = new ReplicationAdmin(conf1);

    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2182);

    utility2 = new HBaseTestingUtility(conf2);
    utility2.startMiniCluster();

    ReplicationPeerConfig config = new ReplicationPeerConfig();
    config.setClusterKey(utility2.getClusterKey());
    admin.addPeer(peerId, config, null);

    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);

    utility1.getHBaseAdmin().createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    utility1.waitUntilAllRegionsAssigned(tableName);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin.removePeer(peerId);
    admin.close();
    utility1.deleteTable(tableName);
    utility2.deleteTable(tableName);
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  /*
   * Test for HBASE-15393
   */
  @Test
  public void testEnableTableReplication() throws Exception {
    admin.enableTableRep(tableName);
    assertTrue(utility2.getHBaseAdmin().tableExists(tableName));
  }

  @Test
  public void testDisableTableReplication() throws Exception {
    admin.disableTableRep(tableName);
    assertTrue(utility2.getHBaseAdmin().tableExists(tableName));
  }
}