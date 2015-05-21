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
package org.apache.hadoop.hbase.client.replication;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Unit testing of ReplicationAdmin
 */
@Category(MediumTests.class)
public class TestReplicationAdmin {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationAdmin.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final String ID_ONE = "1";
  private final String KEY_ONE = "127.0.0.1:2181:/hbase";
  private final String ID_SECOND = "2";
  private final String KEY_SECOND = "127.0.0.1:2181:/hbase2";

  private static ReplicationAdmin admin;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    admin = new ReplicationAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (admin != null) {
      admin.close();
    }
    TEST_UTIL.shutdownMiniZKCluster();
  }

  /**
   * Simple testing of adding and removing peers, basically shows that
   * all interactions with ZK work
   * @throws Exception
   */
  @Test
  public void testAddRemovePeer() throws Exception {
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE);
    // try adding the same (fails)
    try {
      admin.addPeer(ID_ONE, KEY_ONE);
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Try to remove an inexisting peer
    try {
      admin.removePeer(ID_SECOND);
      fail();
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Add a second since multi-slave is supported
    try {
      admin.addPeer(ID_SECOND, KEY_SECOND);
    } catch (IllegalStateException iae) {
      fail();
    }
    assertEquals(2, admin.getPeersCount());
    // Remove the first peer we added
    admin.removePeer(ID_ONE);
    assertEquals(1, admin.getPeersCount());
    admin.removePeer(ID_SECOND);
    assertEquals(0, admin.getPeersCount());
  }

  /**
   * basic checks that when we add a peer that it is enabled, and that we can disable
   * @throws Exception
   */
  @Test
  public void testEnableDisable() throws Exception {
    admin.addPeer(ID_ONE, KEY_ONE);
    assertEquals(1, admin.getPeersCount());
    assertTrue(admin.getPeerState(ID_ONE));
    admin.disablePeer(ID_ONE);

    assertFalse(admin.getPeerState(ID_ONE));
    try {
      admin.getPeerState(ID_SECOND);
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    admin.removePeer(ID_ONE);
  }

  @Test
  public void testGetTableCfsStr() {
    // opposite of TestPerTableCFReplication#testParseTableCFsFromConfig()

    Map<TableName, List<String>> tabCFsMap = null;

    // 1. null or empty string, result should be null
    assertEquals(null, ReplicationAdmin.getTableCfsStr(tabCFsMap));


    // 2. single table: "tab1" / "tab2:cf1" / "tab3:cf1,cf3"
    tabCFsMap = new TreeMap<TableName, List<String>>();
    tabCFsMap.put(TableName.valueOf("tab1"), null);   // its table name is "tab1"
    assertEquals("tab1", ReplicationAdmin.getTableCfsStr(tabCFsMap));

    tabCFsMap = new TreeMap<TableName, List<String>>();
    tabCFsMap.put(TableName.valueOf("tab1"), Lists.newArrayList("cf1"));
    assertEquals("tab1:cf1", ReplicationAdmin.getTableCfsStr(tabCFsMap));

    tabCFsMap = new TreeMap<TableName, List<String>>();
    tabCFsMap.put(TableName.valueOf("tab1"), Lists.newArrayList("cf1", "cf3"));
    assertEquals("tab1:cf1,cf3", ReplicationAdmin.getTableCfsStr(tabCFsMap));

    // 3. multiple tables: "tab1 ; tab2:cf1 ; tab3:cf1,cf3"
    tabCFsMap = new TreeMap<TableName, List<String>>();
    tabCFsMap.put(TableName.valueOf("tab1"), null);
    tabCFsMap.put(TableName.valueOf("tab2"), Lists.newArrayList("cf1"));
    tabCFsMap.put(TableName.valueOf("tab3"), Lists.newArrayList("cf1", "cf3"));
    assertEquals("tab1;tab2:cf1;tab3:cf1,cf3", ReplicationAdmin.getTableCfsStr(tabCFsMap));
  }

  @Test
  public void testAppendPeerTableCFs() throws Exception {
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE);

    admin.appendPeerTableCFs(ID_ONE, "t1");
    assertEquals("t1", admin.getPeerTableCFs(ID_ONE));

    // append table t2 to replication
    admin.appendPeerTableCFs(ID_ONE, "t2");
    String peerTablesOne = admin.getPeerTableCFs(ID_ONE);

    // Different jdk's return different sort order for the tables. ( Not sure on why exactly )
    //
    // So instead of asserting that the string is exactly we
    // assert that the string contains all tables and the needed separator.
    assertTrue("Should contain t1", peerTablesOne.contains("t1"));
    assertTrue("Should contain t2", peerTablesOne.contains("t2"));
    assertTrue("Should contain ; as the seperator", peerTablesOne.contains(";"));

    // append table column family: f1 of t3 to replication
    admin.appendPeerTableCFs(ID_ONE, "t3:f1");
    String peerTablesTwo = admin.getPeerTableCFs(ID_ONE);
    assertTrue("Should contain t1", peerTablesTwo.contains("t1"));
    assertTrue("Should contain t2", peerTablesTwo.contains("t2"));
    assertTrue("Should contain t3:f1", peerTablesTwo.contains("t3:f1"));
    assertTrue("Should contain ; as the seperator", peerTablesTwo.contains(";"));
    admin.removePeer(ID_ONE);
  }

  @Test
  public void testRemovePeerTableCFs() throws Exception {
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE);
    try {
      admin.removePeerTableCFs(ID_ONE, "t3");
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    assertEquals("", admin.getPeerTableCFs(ID_ONE));

    admin.setPeerTableCFs(ID_ONE, "t1;t2:cf1");
    try {
      admin.removePeerTableCFs(ID_ONE, "t3");
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    assertEquals("t1;t2:cf1", admin.getPeerTableCFs(ID_ONE));

    try {
      admin.removePeerTableCFs(ID_ONE, "t1:f1");
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    admin.removePeerTableCFs(ID_ONE, "t1");
    assertEquals("t2:cf1", admin.getPeerTableCFs(ID_ONE));

    try {
      admin.removePeerTableCFs(ID_ONE, "t2");
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    admin.removePeerTableCFs(ID_ONE, "t2:cf1");
    assertEquals("", admin.getPeerTableCFs(ID_ONE));
    admin.removePeer(ID_ONE);
  }
}
