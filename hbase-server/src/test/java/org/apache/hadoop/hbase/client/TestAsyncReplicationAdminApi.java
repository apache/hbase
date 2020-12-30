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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.VerifyWALEntriesReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous replication admin operations.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncReplicationAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncReplicationAdminApi.class);

  private final String ID_ONE = "1";
  private static String KEY_ONE;
  private final String ID_TWO = "2";
  private static String KEY_TWO;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster();
    KEY_ONE = TEST_UTIL.getClusterKey() + "-test1";
    KEY_TWO = TEST_UTIL.getClusterKey() + "-test2";
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @After
  public void clearPeerAndQueues() throws IOException, ReplicationException {
    try {
      admin.removeReplicationPeer(ID_ONE).join();
    } catch (Exception e) {
    }
    try {
      admin.removeReplicationPeer(ID_TWO).join();
    } catch (Exception e) {
    }
    ReplicationQueueStorage queueStorage = ReplicationStorageFactory
      .getReplicationQueueStorage(TEST_UTIL.getZooKeeperWatcher(), TEST_UTIL.getConfiguration());
    for (ServerName serverName : queueStorage.getListOfReplicators()) {
      for (String queue : queueStorage.getAllQueues(serverName)) {
        queueStorage.removeQueue(serverName, queue);
      }
    }
  }

  @Test
  public void testAddRemovePeer() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(KEY_TWO);
    // Add a valid peer
    admin.addReplicationPeer(ID_ONE, rpc1).join();
    // try adding the same (fails)
    try {
      admin.addReplicationPeer(ID_ONE, rpc1).join();
      fail("Test case should fail as adding a same peer.");
    } catch (CompletionException e) {
      // OK!
    }
    assertEquals(1, admin.listReplicationPeers().get().size());
    // Try to remove an inexisting peer
    try {
      admin.removeReplicationPeer(ID_TWO).join();
      fail("Test case should fail as removing a inexisting peer.");
    } catch (CompletionException e) {
      // OK!
    }
    assertEquals(1, admin.listReplicationPeers().get().size());
    // Add a second since multi-slave is supported
    admin.addReplicationPeer(ID_TWO, rpc2).join();
    assertEquals(2, admin.listReplicationPeers().get().size());
    // Remove the first peer we added
    admin.removeReplicationPeer(ID_ONE).join();
    assertEquals(1, admin.listReplicationPeers().get().size());
    admin.removeReplicationPeer(ID_TWO).join();
    assertEquals(0, admin.listReplicationPeers().get().size());
  }

  @Test
  public void testPeerConfig() throws Exception {
    ReplicationPeerConfig config = new ReplicationPeerConfig();
    config.setClusterKey(KEY_ONE);
    config.getConfiguration().put("key1", "value1");
    config.getConfiguration().put("key2", "value2");
    admin.addReplicationPeer(ID_ONE, config).join();

    List<ReplicationPeerDescription> peers = admin.listReplicationPeers().get();
    assertEquals(1, peers.size());
    ReplicationPeerDescription peerOne = peers.get(0);
    assertNotNull(peerOne);
    assertEquals("value1", peerOne.getPeerConfig().getConfiguration().get("key1"));
    assertEquals("value2", peerOne.getPeerConfig().getConfiguration().get("key2"));

    admin.removeReplicationPeer(ID_ONE).join();
  }

  @Test
  public void testEnableDisablePeer() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    admin.addReplicationPeer(ID_ONE, rpc1).join();
    List<ReplicationPeerDescription> peers = admin.listReplicationPeers().get();
    assertEquals(1, peers.size());
    assertTrue(peers.get(0).isEnabled());

    admin.disableReplicationPeer(ID_ONE).join();
    peers = admin.listReplicationPeers().get();
    assertEquals(1, peers.size());
    assertFalse(peers.get(0).isEnabled());
    admin.removeReplicationPeer(ID_ONE).join();
  }

  @Test
  public void testAppendPeerTableCFs() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    final TableName tableName1 = TableName.valueOf(tableName.getNameAsString() + "t1");
    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "t2");
    final TableName tableName3 = TableName.valueOf(tableName.getNameAsString() + "t3");
    final TableName tableName4 = TableName.valueOf(tableName.getNameAsString() + "t4");
    final TableName tableName5 = TableName.valueOf(tableName.getNameAsString() + "t5");
    final TableName tableName6 = TableName.valueOf(tableName.getNameAsString() + "t6");

    // Add a valid peer
    admin.addReplicationPeer(ID_ONE, rpc1).join();
    rpc1.setReplicateAllUserTables(false);
    admin.updateReplicationPeerConfig(ID_ONE, rpc1).join();

    Map<TableName, List<String>> tableCFs = new HashMap<>();

    // append table t1 to replication
    tableCFs.put(tableName1, null);
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    Map<TableName, List<String>> result =
      admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(1, result.size());
    assertEquals(true, result.containsKey(tableName1));
    assertNull(result.get(tableName1));

    // append table t2 to replication
    tableCFs.clear();
    tableCFs.put(tableName2, null);
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    result = admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(2, result.size());
    assertTrue("Should contain t1", result.containsKey(tableName1));
    assertTrue("Should contain t2", result.containsKey(tableName2));
    assertNull(result.get(tableName1));
    assertNull(result.get(tableName2));

    // append table column family: f1 of t3 to replication
    tableCFs.clear();
    tableCFs.put(tableName3, new ArrayList<>());
    tableCFs.get(tableName3).add("f1");
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    result = admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(3, result.size());
    assertTrue("Should contain t1", result.containsKey(tableName1));
    assertTrue("Should contain t2", result.containsKey(tableName2));
    assertTrue("Should contain t3", result.containsKey(tableName3));
    assertNull(result.get(tableName1));
    assertNull(result.get(tableName2));
    assertEquals(1, result.get(tableName3).size());
    assertEquals("f1", result.get(tableName3).get(0));

    // append table column family: f1,f2 of t4 to replication
    tableCFs.clear();
    tableCFs.put(tableName4, new ArrayList<>());
    tableCFs.get(tableName4).add("f1");
    tableCFs.get(tableName4).add("f2");
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    result = admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(4, result.size());
    assertTrue("Should contain t1", result.containsKey(tableName1));
    assertTrue("Should contain t2", result.containsKey(tableName2));
    assertTrue("Should contain t3", result.containsKey(tableName3));
    assertTrue("Should contain t4", result.containsKey(tableName4));
    assertNull(result.get(tableName1));
    assertNull(result.get(tableName2));
    assertEquals(1, result.get(tableName3).size());
    assertEquals("f1", result.get(tableName3).get(0));
    assertEquals(2, result.get(tableName4).size());
    assertEquals("f1", result.get(tableName4).get(0));
    assertEquals("f2", result.get(tableName4).get(1));

    // append "table5" => [], then append "table5" => ["f1"]
    tableCFs.clear();
    tableCFs.put(tableName5, new ArrayList<>());
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    tableCFs.clear();
    tableCFs.put(tableName5, new ArrayList<>());
    tableCFs.get(tableName5).add("f1");
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    result = admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(5, result.size());
    assertTrue("Should contain t5", result.containsKey(tableName5));
    // null means replication all cfs of tab5
    assertNull(result.get(tableName5));

    // append "table6" => ["f1"], then append "table6" => []
    tableCFs.clear();
    tableCFs.put(tableName6, new ArrayList<>());
    tableCFs.get(tableName6).add("f1");
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    tableCFs.clear();
    tableCFs.put(tableName6, new ArrayList<>());
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    result = admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(6, result.size());
    assertTrue("Should contain t6", result.containsKey(tableName6));
    // null means replication all cfs of tab6
    assertNull(result.get(tableName6));

    admin.removeReplicationPeer(ID_ONE).join();
  }

  @Test
  public void testRemovePeerTableCFs() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    final TableName tableName1 = TableName.valueOf(tableName.getNameAsString() + "t1");
    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "t2");
    final TableName tableName3 = TableName.valueOf(tableName.getNameAsString() + "t3");
    final TableName tableName4 = TableName.valueOf(tableName.getNameAsString() + "t4");
    // Add a valid peer
    admin.addReplicationPeer(ID_ONE, rpc1).join();
    rpc1.setReplicateAllUserTables(false);
    admin.updateReplicationPeerConfig(ID_ONE, rpc1).join();

    Map<TableName, List<String>> tableCFs = new HashMap<>();
    try {
      tableCFs.put(tableName3, null);
      admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
      fail("Test case should fail as removing table-cfs from a peer whose table-cfs is null");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof ReplicationException);
    }
    assertNull(admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap());

    tableCFs.clear();
    tableCFs.put(tableName1, null);
    tableCFs.put(tableName2, new ArrayList<>());
    tableCFs.get(tableName2).add("cf1");
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    try {
      tableCFs.clear();
      tableCFs.put(tableName3, null);
      admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
      fail("Test case should fail as removing table-cfs from a peer whose" +
        " table-cfs didn't contain t3");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof ReplicationException);
    }
    Map<TableName, List<String>> result =
      admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(2, result.size());
    assertTrue("Should contain t1", result.containsKey(tableName1));
    assertTrue("Should contain t2", result.containsKey(tableName2));
    assertNull(result.get(tableName1));
    assertEquals(1, result.get(tableName2).size());
    assertEquals("cf1", result.get(tableName2).get(0));

    try {
      tableCFs.clear();
      tableCFs.put(tableName1, new ArrayList<>());
      tableCFs.get(tableName1).add("cf1");
      admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
      fail("Test case should fail, because table t1 didn't specify cfs in peer config");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof ReplicationException);
    }
    tableCFs.clear();
    tableCFs.put(tableName1, null);
    admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    result = admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap();
    assertEquals(1, result.size());
    assertEquals(1, result.get(tableName2).size());
    assertEquals("cf1", result.get(tableName2).get(0));

    try {
      tableCFs.clear();
      tableCFs.put(tableName2, null);
      admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
      fail("Test case should fail, because table t2 hase specified cfs in peer config");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof ReplicationException);
    }
    tableCFs.clear();
    tableCFs.put(tableName2, new ArrayList<>());
    tableCFs.get(tableName2).add("cf1");
    admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    assertNull(admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap());

    tableCFs.clear();
    tableCFs.put(tableName4, new ArrayList<>());
    admin.appendReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    admin.removeReplicationPeerTableCFs(ID_ONE, tableCFs).join();
    assertNull(admin.getReplicationPeerConfig(ID_ONE).get().getTableCFsMap());

    admin.removeReplicationPeer(ID_ONE);
  }

  @Test
  public void testSetPeerNamespaces() throws Exception {
    String ns1 = "ns1";
    String ns2 = "ns2";

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(KEY_ONE);
    admin.addReplicationPeer(ID_ONE, rpc).join();
    rpc.setReplicateAllUserTables(false);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).join();

    // add ns1 and ns2 to peer config
    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    Set<String> namespaces = new HashSet<>();
    namespaces.add(ns1);
    namespaces.add(ns2);
    rpc.setNamespaces(namespaces);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).join();
    namespaces = admin.getReplicationPeerConfig(ID_ONE).get().getNamespaces();
    assertEquals(2, namespaces.size());
    assertTrue(namespaces.contains(ns1));
    assertTrue(namespaces.contains(ns2));

    // update peer config only contains ns1
    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    namespaces = new HashSet<>();
    namespaces.add(ns1);
    rpc.setNamespaces(namespaces);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).join();
    namespaces = admin.getReplicationPeerConfig(ID_ONE).get().getNamespaces();
    assertEquals(1, namespaces.size());
    assertTrue(namespaces.contains(ns1));

    admin.removeReplicationPeer(ID_ONE).join();
  }

  @Test
  public void testNamespacesAndTableCfsConfigConflict() throws Exception {
    String ns1 = "ns1";
    String ns2 = "ns2";
    final TableName tableName1 = TableName.valueOf(ns1 + ":" + tableName.getNameAsString() + "1");
    final TableName tableName2 = TableName.valueOf(ns2 + ":" + tableName.getNameAsString() + "2");

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(KEY_ONE);
    admin.addReplicationPeer(ID_ONE, rpc).join();
    rpc.setReplicateAllUserTables(false);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).join();

    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    Set<String> namespaces = new HashSet<String>();
    namespaces.add(ns1);
    rpc.setNamespaces(namespaces);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).get();
    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName1, new ArrayList<>());
    rpc.setTableCFsMap(tableCfs);
    try {
      admin.updateReplicationPeerConfig(ID_ONE, rpc).join();
      fail(
        "Test case should fail, because table " + tableName1 + " conflict with namespace " + ns1);
    } catch (CompletionException e) {
      // OK
    }

    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    tableCfs.clear();
    tableCfs.put(tableName2, new ArrayList<>());
    rpc.setTableCFsMap(tableCfs);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).get();
    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    namespaces.clear();
    namespaces.add(ns2);
    rpc.setNamespaces(namespaces);
    try {
      admin.updateReplicationPeerConfig(ID_ONE, rpc).join();
      fail(
        "Test case should fail, because namespace " + ns2 + " conflict with table " + tableName2);
    } catch (CompletionException e) {
      // OK
    }

    admin.removeReplicationPeer(ID_ONE).join();
  }

  @Test
  public void testPeerBandwidth() throws Exception {
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(KEY_ONE);

    admin.addReplicationPeer(ID_ONE, rpc).join();
    rpc = admin.getReplicationPeerConfig(ID_ONE).get();
    assertEquals(0, rpc.getBandwidth());

    rpc.setBandwidth(2097152);
    admin.updateReplicationPeerConfig(ID_ONE, rpc).join();
    assertEquals(2097152, admin.getReplicationPeerConfig(ID_ONE).join().getBandwidth());

    admin.removeReplicationPeer(ID_ONE).join();
  }

  @Test
  public void testInvalidClusterKey() throws InterruptedException {
    try {
      admin.addReplicationPeer(ID_ONE,
        ReplicationPeerConfig.newBuilder().setClusterKey("whatever").build()).get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(DoNotRetryIOException.class));
    }
  }

  @Test
  public void testInvalidReplicationEndpoint() throws InterruptedException {
    try {
      admin.addReplicationPeer(ID_ONE,
        ReplicationPeerConfig.newBuilder().setReplicationEndpointImpl("whatever").build()).get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(DoNotRetryIOException.class));
      assertThat(e.getCause().getMessage(), startsWith("Can not instantiate"));
    }
  }

  @Test
  public void testSetReplicationEndpoint() throws InterruptedException, ExecutionException {
    // make sure that we do not need to set cluster key when we use customized ReplicationEndpoint
    admin
      .addReplicationPeer(ID_ONE,
        ReplicationPeerConfig.newBuilder()
          .setReplicationEndpointImpl(VerifyWALEntriesReplicationEndpoint.class.getName()).build())
      .get();

    // but we still need to check cluster key if we specify the default ReplicationEndpoint
    try {
      admin
        .addReplicationPeer(ID_TWO, ReplicationPeerConfig.newBuilder()
          .setReplicationEndpointImpl(HBaseInterClusterReplicationEndpoint.class.getName()).build())
        .get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(DoNotRetryIOException.class));
    }
  }
}
