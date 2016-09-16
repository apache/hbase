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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesArguments;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit testing of ReplicationAdmin
 */
@Category({MediumTests.class, ClientTests.class})
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
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(KEY_SECOND);
    // Add a valid peer
    admin.addPeer(ID_ONE, rpc1, null);
    // try adding the same (fails)
    try {
      admin.addPeer(ID_ONE, rpc1, null);
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
      admin.addPeer(ID_SECOND, rpc2, null);
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
   * Tests that the peer configuration used by ReplicationAdmin contains all
   * the peer's properties.
   */
  @Test
  public void testPeerConfig() throws Exception {
    ReplicationPeerConfig config = new ReplicationPeerConfig();
    config.setClusterKey(KEY_ONE);
    config.getConfiguration().put("key1", "value1");
    config.getConfiguration().put("key2", "value2");
    admin.addPeer(ID_ONE, config, null);

    List<ReplicationPeer> peers = admin.listReplicationPeers();
    assertEquals(1, peers.size());
    ReplicationPeer peerOne = peers.get(0);
    assertNotNull(peerOne);
    assertEquals("value1", peerOne.getConfiguration().get("key1"));
    assertEquals("value2", peerOne.getConfiguration().get("key2"));

    admin.removePeer(ID_ONE);
  }

  @Test
  public void testAddPeerWithUnDeletedQueues() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(KEY_SECOND);
    Configuration conf = TEST_UTIL.getConfiguration();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "Test HBaseAdmin", null);
    ReplicationQueues repQueues =
        ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, null, zkw));
    repQueues.init("server1");

    // add queue for ID_ONE
    repQueues.addLog(ID_ONE, "file1");
    try {
      admin.addPeer(ID_ONE, rpc1, null);
      fail();
    } catch (ReplicationException e) {
      // OK!
    }
    repQueues.removeQueue(ID_ONE);
    assertEquals(0, repQueues.getAllQueues().size());
    
    // add recovered queue for ID_ONE
    repQueues.addLog(ID_ONE + "-server2", "file1");
    try {
      admin.addPeer(ID_ONE, rpc2, null);
      fail();
    } catch (ReplicationException e) {
      // OK!
    }
    repQueues.removeAllQueues();
    zkw.close();
  }

  /**
   * basic checks that when we add a peer that it is enabled, and that we can disable
   * @throws Exception
   */
  @Test
  public void testEnableDisable() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    admin.addPeer(ID_ONE, rpc1, null);
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
  public void testAppendPeerTableCFs() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    TableName tab1 = TableName.valueOf("t1");
    TableName tab2 = TableName.valueOf("t2");
    TableName tab3 = TableName.valueOf("t3");
    TableName tab4 = TableName.valueOf("t4");
    TableName tab5 = TableName.valueOf("t5");
    TableName tab6 = TableName.valueOf("t6");

    // Add a valid peer
    admin.addPeer(ID_ONE, rpc1, null);

    Map<TableName, List<String>> tableCFs = new HashMap<>();

    tableCFs.put(tab1, null);
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    Map<TableName, List<String>> result =
      ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(1, result.size());
    assertEquals(true, result.containsKey(tab1));
    assertNull(result.get(tab1));

    // append table t2 to replication
    tableCFs.clear();
    tableCFs.put(tab2, null);
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(2, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertNull(result.get(tab1));
    assertNull(result.get(tab2));

    // append table column family: f1 of t3 to replication
    tableCFs.clear();
    tableCFs.put(tab3, new ArrayList<String>());
    tableCFs.get(tab3).add("f1");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(3, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertTrue("Should contain t3", result.containsKey(tab3));
    assertNull(result.get(tab1));
    assertNull(result.get(tab2));
    assertEquals(1, result.get(tab3).size());
    assertEquals("f1", result.get(tab3).get(0));

    tableCFs.clear();
    tableCFs.put(tab4, new ArrayList<String>());
    tableCFs.get(tab4).add("f1");
    tableCFs.get(tab4).add("f2");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(4, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertTrue("Should contain t3", result.containsKey(tab3));
    assertTrue("Should contain t4", result.containsKey(tab4));
    assertNull(result.get(tab1));
    assertNull(result.get(tab2));
    assertEquals(1, result.get(tab3).size());
    assertEquals("f1", result.get(tab3).get(0));
    assertEquals(2, result.get(tab4).size());
    assertEquals("f1", result.get(tab4).get(0));
    assertEquals("f2", result.get(tab4).get(1));

    // append "table5" => [], then append "table5" => ["f1"]
    tableCFs.clear();
    tableCFs.put(tab5, new ArrayList<String>());
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    tableCFs.clear();
    tableCFs.put(tab5, new ArrayList<String>());
    tableCFs.get(tab5).add("f1");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(5, result.size());
    assertTrue("Should contain t5", result.containsKey(tab5));
    // null means replication all cfs of tab5
    assertNull(result.get(tab5));

    // append "table6" => ["f1"], then append "table6" => []
    tableCFs.clear();
    tableCFs.put(tab6, new ArrayList<String>());
    tableCFs.get(tab6).add("f1");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    tableCFs.clear();
    tableCFs.put(tab6, new ArrayList<String>());
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(6, result.size());
    assertTrue("Should contain t6", result.containsKey(tab6));
    // null means replication all cfs of tab6
    assertNull(result.get(tab6));

    admin.removePeer(ID_ONE);
  }

  @Test
  public void testRemovePeerTableCFs() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    TableName tab1 = TableName.valueOf("t1");
    TableName tab2 = TableName.valueOf("t2");
    TableName tab3 = TableName.valueOf("t3");
    TableName tab4 = TableName.valueOf("t4");
    // Add a valid peer
    admin.addPeer(ID_ONE, rpc1, null);
    Map<TableName, List<String>> tableCFs = new HashMap<>();
    try {
      tableCFs.put(tab3, null);
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    assertNull(admin.getPeerTableCFs(ID_ONE));

    tableCFs.clear();
    tableCFs.put(tab1, null);
    tableCFs.put(tab2, new ArrayList<String>());
    tableCFs.get(tab2).add("cf1");
    admin.setPeerTableCFs(ID_ONE, tableCFs);
    try {
      tableCFs.clear();
      tableCFs.put(tab3, null);
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    Map<TableName, List<String>> result =
      ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(2, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertNull(result.get(tab1));
    assertEquals(1, result.get(tab2).size());
    assertEquals("cf1", result.get(tab2).get(0));

    try {
      tableCFs.clear();
      tableCFs.put(tab1, new ArrayList<String>());
      tableCFs.get(tab1).add("f1");
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    tableCFs.clear();
    tableCFs.put(tab1, null);
    admin.removePeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(1, result.size());
    assertEquals(1, result.get(tab2).size());
    assertEquals("cf1", result.get(tab2).get(0));

    try {
      tableCFs.clear();
      tableCFs.put(tab2, null);
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    tableCFs.clear();
    tableCFs.put(tab2, new ArrayList<String>());
    tableCFs.get(tab2).add("cf1");
    admin.removePeerTableCFs(ID_ONE, tableCFs);
    assertNull(admin.getPeerTableCFs(ID_ONE));

    tableCFs.clear();
    tableCFs.put(tab4, new ArrayList<String>());
    admin.setPeerTableCFs(ID_ONE, tableCFs);
    admin.removePeerTableCFs(ID_ONE, tableCFs);
    assertNull(admin.getPeerTableCFs(ID_ONE));

    admin.removePeer(ID_ONE);
  }

  @Test
  public void testSetPeerNamespaces() throws Exception {
    String ns1 = "ns1";
    String ns2 = "ns2";

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(KEY_ONE);
    admin.addPeer(ID_ONE, rpc);
    admin.peerAdded(ID_ONE);

    rpc = admin.getPeerConfig(ID_ONE);
    Set<String> namespaces = new HashSet<String>();
    namespaces.add(ns1);
    namespaces.add(ns2);
    rpc.setNamespaces(namespaces);
    admin.updatePeerConfig(ID_ONE, rpc);
    namespaces = admin.getPeerConfig(ID_ONE).getNamespaces();
    assertEquals(2, namespaces.size());
    assertTrue(namespaces.contains(ns1));
    assertTrue(namespaces.contains(ns2));

    rpc = admin.getPeerConfig(ID_ONE);
    namespaces.clear();
    namespaces.add(ns1);
    rpc.setNamespaces(namespaces);
    admin.updatePeerConfig(ID_ONE, rpc);
    namespaces = admin.getPeerConfig(ID_ONE).getNamespaces();
    assertEquals(1, namespaces.size());
    assertTrue(namespaces.contains(ns1));

    admin.removePeer(ID_ONE);
  }

  @Test
  public void testNamespacesAndTableCfsConfigConflict() throws ReplicationException {
    String ns1 = "ns1";
    String ns2 = "ns2";
    TableName tab1 = TableName.valueOf("ns1:tabl");
    TableName tab2 = TableName.valueOf("ns2:tab2");

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(KEY_ONE);
    admin.addPeer(ID_ONE, rpc);
    admin.peerAdded(ID_ONE);

    rpc = admin.getPeerConfig(ID_ONE);
    Set<String> namespaces = new HashSet<String>();
    namespaces.add(ns1);
    rpc.setNamespaces(namespaces);
    admin.updatePeerConfig(ID_ONE, rpc);
    rpc = admin.getPeerConfig(ID_ONE);
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tab1, new ArrayList<String>());
    rpc.setTableCFsMap(tableCfs);
    try {
      admin.updatePeerConfig(ID_ONE, rpc);
      fail("Should throw ReplicationException, because table " + tab1 + " conflict with namespace "
          + ns1);
    } catch (ReplicationException e) {
      // OK
    }

    rpc = admin.getPeerConfig(ID_ONE);
    tableCfs.clear();
    tableCfs.put(tab2, new ArrayList<String>());
    rpc.setTableCFsMap(tableCfs);
    admin.updatePeerConfig(ID_ONE, rpc);
    rpc = admin.getPeerConfig(ID_ONE);
    namespaces.clear();
    namespaces.add(ns2);
    rpc.setNamespaces(namespaces);
    try {
      admin.updatePeerConfig(ID_ONE, rpc);
      fail("Should throw ReplicationException, because namespace " + ns2 + " conflict with table "
          + tab2);
    } catch (ReplicationException e) {
      // OK
    }

    admin.removePeer(ID_ONE);
  }
}
