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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.ZKReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager.NodeFailoverWorker;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;

/**
 * An abstract class that tests ReplicationSourceManager. Classes that extend this class should
 * set up the proper config for this class and initialize the proper cluster using
 * HBaseTestingUtility.
 */
@Category({ReplicationTests.class, MediumTests.class})
public abstract class TestReplicationSourceManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationSourceManager.class);

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationSourceManager.class);

  protected static Configuration conf;

  protected static HBaseTestingUtility utility;

  protected static Replication replication;

  protected static ReplicationSourceManager manager;

  protected static ReplicationSourceManager managerOfCluster;

  protected static ZKWatcher zkw;

  protected static TableDescriptor htd;

  protected static RegionInfo hri;

  protected static final byte[] r1 = Bytes.toBytes("r1");

  protected static final byte[] r2 = Bytes.toBytes("r2");

  protected static final byte[] f1 = Bytes.toBytes("f1");

  protected static final byte[] f2 = Bytes.toBytes("f2");

  protected static final TableName test =
      TableName.valueOf("test");

  protected static final String slaveId = "1";

  protected static FileSystem fs;

  protected static Path oldLogDir;

  protected static Path logDir;

  protected static Path remoteLogDir;

  protected static CountDownLatch latch;

  protected static List<String> files = new ArrayList<>();
  protected static NavigableMap<byte[], Integer> scopes;

  protected static void setupZkAndReplication() throws Exception {
    // The implementing class should set up the conf
    assertNotNull(conf);
    zkw = new ZKWatcher(conf, "test", null);
    ZKUtil.createWithParents(zkw, "/hbase/replication");
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1",
        Bytes.toBytes(conf.get(HConstants.ZOOKEEPER_QUORUM) + ":"
            + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT) + ":/1"));
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1/peer-state");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1/peer-state",
      ZKReplicationPeerStorage.ENABLED_ZNODE_BYTES);
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1/sync-rep-state");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1/sync-rep-state",
      ZKReplicationPeerStorage.NONE_STATE_ZNODE_BYTES);
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1/new-sync-rep-state");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1/new-sync-rep-state",
      ZKReplicationPeerStorage.NONE_STATE_ZNODE_BYTES);
    ZKUtil.createWithParents(zkw, "/hbase/replication/state");
    ZKUtil.setData(zkw, "/hbase/replication/state", ZKReplicationPeerStorage.ENABLED_ZNODE_BYTES);

    ZKClusterId.setClusterId(zkw, new ClusterId());
    FSUtils.setRootDir(utility.getConfiguration(), utility.getDataTestDir());
    fs = FileSystem.get(conf);
    oldLogDir = utility.getDataTestDir(HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = utility.getDataTestDir(HConstants.HREGION_LOGDIR_NAME);
    remoteLogDir = utility.getDataTestDir(ReplicationUtils.REMOTE_WAL_DIR_NAME);
    replication = new Replication();
    replication.initialize(new DummyServer(), fs, logDir, oldLogDir, null);
    managerOfCluster = getManagerFromCluster();
    if (managerOfCluster != null) {
      // After replication procedure, we need to add peer by hand (other than by receiving
      // notification from zk)
      managerOfCluster.addPeer(slaveId);
    }

    manager = replication.getReplicationManager();
    manager.addSource(slaveId);
    if (managerOfCluster != null) {
      waitPeer(slaveId, managerOfCluster, true);
    }
    waitPeer(slaveId, manager, true);

    htd = TableDescriptorBuilder.newBuilder(test)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(f1)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(f2)).build();

    scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    hri = RegionInfoBuilder.newBuilder(htd.getTableName()).setStartKey(r1).setEndKey(r2).build();
  }

  private static ReplicationSourceManager getManagerFromCluster() {
    // TestReplicationSourceManagerZkImpl won't start the mini hbase cluster.
    if (utility.getMiniHBaseCluster() == null) {
      return null;
    }
    return utility.getMiniHBaseCluster().getRegionServerThreads()
        .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .findAny()
        .map(HRegionServer::getReplicationSourceService)
        .map(r -> (Replication)r)
        .map(Replication::getReplicationManager)
        .get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (manager != null) {
      manager.join();
    }
    utility.shutdownMiniCluster();
  }

  @Rule
  public TestName testName = new TestName();

  private void cleanLogDir() throws IOException {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
    fs.delete(remoteLogDir, true);
  }

  @Before
  public void setUp() throws Exception {
    LOG.info("Start " + testName.getMethodName());
    cleanLogDir();
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("End " + testName.getMethodName());
    cleanLogDir();
    List<String> ids = manager.getSources().stream()
        .map(ReplicationSourceInterface::getPeerId).collect(Collectors.toList());
    for (String id : ids) {
      if (slaveId.equals(id)) {
        continue;
      }
      removePeerAndWait(id);
    }
  }

  @Test
  public void testLogRoll() throws Exception {
    long baseline = 1000;
    long time = baseline;
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    KeyValue kv = new KeyValue(r1, f1, r1);
    WALEdit edit = new WALEdit();
    edit.add(kv);

    WALFactory wals =
      new WALFactory(utility.getConfiguration(), URLEncoder.encode("regionserver:60020", "UTF8"));
    ReplicationSourceManager replicationManager = replication.getReplicationManager();
    wals.getWALProvider()
      .addWALActionsListener(new ReplicationSourceWALActionListener(conf, replicationManager));
    final WAL wal = wals.getWAL(hri);
    manager.init();
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf("tableame"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(f1)).build();
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    // Testing normal log rolling every 20
    for(long i = 1; i < 101; i++) {
      if(i > 1 && i % 20 == 0) {
        wal.rollWriter();
      }
      LOG.info(Long.toString(i));
      final long txid = wal.append(
          hri,
          new WALKeyImpl(hri.getEncodedNameAsBytes(), test, System.currentTimeMillis(), mvcc, scopes),
          edit,
          true);
      wal.sync(txid);
    }

    // Simulate a rapid insert that's followed
    // by a report that's still not totally complete (missing last one)
    LOG.info(baseline + " and " + time);
    baseline += 101;
    time = baseline;
    LOG.info(baseline + " and " + time);

    for (int i = 0; i < 3; i++) {
      wal.append(hri,
        new WALKeyImpl(hri.getEncodedNameAsBytes(), test, System.currentTimeMillis(), mvcc, scopes),
        edit, true);
    }
    wal.sync();

    int logNumber = 0;
    for (Map.Entry<String, NavigableSet<String>> entry : manager.getWALs().get(slaveId)
      .entrySet()) {
      logNumber += entry.getValue().size();
    }
    assertEquals(6, logNumber);

    wal.rollWriter();

    ReplicationSourceInterface source = mock(ReplicationSourceInterface.class);
    when(source.getQueueId()).thenReturn("1");
    when(source.isRecovered()).thenReturn(false);
    when(source.isSyncReplication()).thenReturn(false);
    manager.logPositionAndCleanOldLogs(source,
      new WALEntryBatch(0, manager.getSources().get(0).getCurrentPath()));

    wal.append(hri,
      new WALKeyImpl(hri.getEncodedNameAsBytes(), test, System.currentTimeMillis(), mvcc, scopes),
      edit, true);
    wal.sync();

    assertEquals(1, manager.getWALs().size());


    // TODO Need a case with only 2 WALs and we only want to delete the first one
  }

  @Test
  public void testClaimQueues() throws Exception {
    Server server = new DummyServer("hostname0.example.org");
    ReplicationQueueStorage rq = ReplicationStorageFactory
        .getReplicationQueueStorage(server.getZooKeeper(), server.getConfiguration());
    // populate some znodes in the peer znode
    files.add("log1");
    files.add("log2");
    for (String file : files) {
      rq.addWAL(server.getServerName(), "1", file);
    }
    // create 3 DummyServers
    Server s1 = new DummyServer("dummyserver1.example.org");
    Server s2 = new DummyServer("dummyserver2.example.org");
    Server s3 = new DummyServer("dummyserver3.example.org");

    // create 3 DummyNodeFailoverWorkers
    DummyNodeFailoverWorker w1 = new DummyNodeFailoverWorker(server.getServerName(), s1);
    DummyNodeFailoverWorker w2 = new DummyNodeFailoverWorker(server.getServerName(), s2);
    DummyNodeFailoverWorker w3 = new DummyNodeFailoverWorker(server.getServerName(), s3);

    latch = new CountDownLatch(3);
    // start the threads
    w1.start();
    w2.start();
    w3.start();
    // make sure only one is successful
    int populatedMap = 0;
    // wait for result now... till all the workers are done.
    latch.await();
    populatedMap += w1.isLogZnodesMapPopulated() + w2.isLogZnodesMapPopulated()
        + w3.isLogZnodesMapPopulated();
    assertEquals(1, populatedMap);
    server.abort("", null);
  }

  @Test
  public void testCleanupFailoverQueues() throws Exception {
    Server server = new DummyServer("hostname1.example.org");
    ReplicationQueueStorage rq = ReplicationStorageFactory
        .getReplicationQueueStorage(server.getZooKeeper(), server.getConfiguration());
    // populate some znodes in the peer znode
    SortedSet<String> files = new TreeSet<>();
    String group = "testgroup";
    String file1 = group + "." + EnvironmentEdgeManager.currentTime() + ".log1";
    String file2 = group + "." + EnvironmentEdgeManager.currentTime() + ".log2";
    files.add(file1);
    files.add(file2);
    for (String file : files) {
      rq.addWAL(server.getServerName(), "1", file);
    }
    Server s1 = new DummyServer("dummyserver1.example.org");
    ReplicationPeers rp1 =
        ReplicationFactory.getReplicationPeers(s1.getZooKeeper(), s1.getConfiguration());
    rp1.init();
    NodeFailoverWorker w1 =
        manager.new NodeFailoverWorker(server.getServerName());
    w1.run();
    assertEquals(1, manager.getWalsByIdRecoveredQueues().size());
    String id = "1-" + server.getServerName().getServerName();
    assertEquals(files, manager.getWalsByIdRecoveredQueues().get(id).get(group));
    ReplicationSourceInterface source = mock(ReplicationSourceInterface.class);
    when(source.getQueueId()).thenReturn(id);
    when(source.isRecovered()).thenReturn(true);
    when(source.isSyncReplication()).thenReturn(false);
    manager.cleanOldLogs(file2, false, source);
    // log1 should be deleted
    assertEquals(Sets.newHashSet(file2), manager.getWalsByIdRecoveredQueues().get(id).get(group));
  }

  @Test
  public void testCleanupUnknownPeerZNode() throws Exception {
    Server server = new DummyServer("hostname2.example.org");
    ReplicationQueueStorage rq = ReplicationStorageFactory
        .getReplicationQueueStorage(server.getZooKeeper(), server.getConfiguration());
    // populate some znodes in the peer znode
    // add log to an unknown peer
    String group = "testgroup";
    rq.addWAL(server.getServerName(), "2", group + ".log1");
    rq.addWAL(server.getServerName(), "2", group + ".log2");

    NodeFailoverWorker w1 = manager.new NodeFailoverWorker(server.getServerName());
    w1.run();

    // The log of the unknown peer should be removed from zk
    for (String peer : manager.getAllQueues()) {
      assertTrue(peer.startsWith("1"));
    }
  }

  /**
   * Test for HBASE-9038, Replication.scopeWALEdits would NPE if it wasn't filtering out the
   * compaction WALEdit.
   */
  @Test
  public void testCompactionWALEdits() throws Exception {
    TableName tableName = TableName.valueOf("testCompactionWALEdits");
    WALProtos.CompactionDescriptor compactionDescriptor =
      WALProtos.CompactionDescriptor.getDefaultInstance();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW).build();
    WALEdit edit = WALEdit.createCompaction(hri, compactionDescriptor);
    ReplicationSourceWALActionListener.scopeWALEdits(new WALKeyImpl(), edit, conf);
  }

  @Test
  public void testBulkLoadWALEditsWithoutBulkLoadReplicationEnabled() throws Exception {
    NavigableMap<byte[], Integer> scope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // 1. Get the bulk load wal edit event
    WALEdit logEdit = getBulkLoadWALEdit(scope);
    // 2. Create wal key
    WALKeyImpl logKey = new WALKeyImpl(scope);

    // 3. Get the scopes for the key
    ReplicationSourceWALActionListener.scopeWALEdits(logKey, logEdit, conf);

    // 4. Assert that no bulk load entry scopes are added if bulk load hfile replication is disabled
    assertNull("No bulk load entries scope should be added if bulk load replication is disabled.",
      logKey.getReplicationScopes());
  }

  @Test
  public void testBulkLoadWALEdits() throws Exception {
    // 1. Get the bulk load wal edit event
    NavigableMap<byte[], Integer> scope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    WALEdit logEdit = getBulkLoadWALEdit(scope);
    // 2. Create wal key
    WALKeyImpl logKey = new WALKeyImpl(scope);
    // 3. Enable bulk load hfile replication
    Configuration bulkLoadConf = HBaseConfiguration.create(conf);
    bulkLoadConf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);

    // 4. Get the scopes for the key
    ReplicationSourceWALActionListener.scopeWALEdits(logKey, logEdit, bulkLoadConf);

    NavigableMap<byte[], Integer> scopes = logKey.getReplicationScopes();
    // Assert family with replication scope global is present in the key scopes
    assertTrue("This family scope is set to global, should be part of replication key scopes.",
      scopes.containsKey(f1));
    // Assert family with replication scope local is not present in the key scopes
    assertFalse("This family scope is set to local, should not be part of replication key scopes",
      scopes.containsKey(f2));
  }

  /**
   * Test whether calling removePeer() on a ReplicationSourceManager that failed on initializing the
   * corresponding ReplicationSourceInterface correctly cleans up the corresponding
   * replication queue and ReplicationPeer.
   * See HBASE-16096.
   */
  @Test
  public void testPeerRemovalCleanup() throws Exception{
    String replicationSourceImplName = conf.get("replication.replicationsource.implementation");
    final String peerId = "FakePeer";
    final ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("localhost:" + utility.getZkCluster().getClientPort() + ":/hbase").build();
    try {
      DummyServer server = new DummyServer();
      ReplicationQueueStorage rq = ReplicationStorageFactory
          .getReplicationQueueStorage(server.getZooKeeper(), server.getConfiguration());
      // Purposely fail ReplicationSourceManager.addSource() by causing ReplicationSourceInterface
      // initialization to throw an exception.
      conf.set("replication.replicationsource.implementation",
          FailInitializeDummyReplicationSource.class.getName());
      manager.getReplicationPeers();
      // Set up the znode and ReplicationPeer for the fake peer
      // Don't wait for replication source to initialize, we know it won't.
      addPeerAndWait(peerId, peerConfig, false);

      // Sanity check
      assertNull(manager.getSource(peerId));

      // Create a replication queue for the fake peer
      rq.addWAL(server.getServerName(), peerId, "FakeFile");
      // Unregister peer, this should remove the peer and clear all queues associated with it
      // Need to wait for the ReplicationTracker to pick up the changes and notify listeners.
      removePeerAndWait(peerId);
      assertFalse(rq.getAllQueues(server.getServerName()).contains(peerId));
    } finally {
      conf.set("replication.replicationsource.implementation", replicationSourceImplName);
      removePeerAndWait(peerId);
    }
  }

  private static MetricsReplicationSourceSource getGlobalSource() throws Exception {
    ReplicationSourceInterface source = manager.getSource(slaveId);
    // Retrieve the global replication metrics source
    Field f = MetricsSource.class.getDeclaredField("globalSourceSource");
    f.setAccessible(true);
    return (MetricsReplicationSourceSource)f.get(source.getSourceMetrics());
  }

  private static long getSizeOfLatestPath() {
    // If no mini cluster is running, there are extra replication manager influencing the metrics.
    if (utility.getMiniHBaseCluster() == null) {
      return 0;
    }
    return utility.getMiniHBaseCluster().getRegionServerThreads()
        .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .map(HRegionServer::getReplicationSourceService)
        .map(r -> (Replication)r)
        .map(Replication::getReplicationManager)
        .mapToLong(ReplicationSourceManager::getSizeOfLatestPath)
        .sum();
  }

  @Test
  public void testRemovePeerMetricsCleanup() throws Exception {
    final String peerId = "DummyPeer";
    final ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("localhost:" + utility.getZkCluster().getClientPort() + ":/hbase").build();
    try {
      MetricsReplicationSourceSource globalSource = getGlobalSource();
      final int globalLogQueueSizeInitial = globalSource.getSizeOfLogQueue();
      final long sizeOfLatestPath = getSizeOfLatestPath();
      addPeerAndWait(peerId, peerConfig, true);
      assertEquals(sizeOfLatestPath + globalLogQueueSizeInitial, globalSource.getSizeOfLogQueue());
      ReplicationSourceInterface source = manager.getSource(peerId);
      // Sanity check
      assertNotNull(source);
      final int sizeOfSingleLogQueue = source.getSourceMetrics().getSizeOfLogQueue();
      // Enqueue log and check if metrics updated
      source.enqueueLog(new Path("abc"));
      assertEquals(1 + sizeOfSingleLogQueue, source.getSourceMetrics().getSizeOfLogQueue());
      assertEquals(source.getSourceMetrics().getSizeOfLogQueue() + globalLogQueueSizeInitial,
        globalSource.getSizeOfLogQueue());

      // Removing the peer should reset the global metrics
      removePeerAndWait(peerId);
      assertEquals(globalLogQueueSizeInitial, globalSource.getSizeOfLogQueue());

      // Adding the same peer back again should reset the single source metrics
      addPeerAndWait(peerId, peerConfig, true);
      source = manager.getSource(peerId);
      assertNotNull(source);
      assertEquals(source.getSourceMetrics().getSizeOfLogQueue() + globalLogQueueSizeInitial,
        globalSource.getSizeOfLogQueue());
    } finally {
      removePeerAndWait(peerId);
    }
  }

  private ReplicationSourceInterface mockReplicationSource(String peerId) {
    ReplicationSourceInterface source = mock(ReplicationSourceInterface.class);
    when(source.getPeerId()).thenReturn(peerId);
    when(source.getQueueId()).thenReturn(peerId);
    when(source.isRecovered()).thenReturn(false);
    when(source.isSyncReplication()).thenReturn(true);
    ReplicationPeerConfig config = mock(ReplicationPeerConfig.class);
    when(config.getRemoteWALDir())
      .thenReturn(remoteLogDir.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString());
    ReplicationPeer peer = mock(ReplicationPeer.class);
    when(peer.getPeerConfig()).thenReturn(config);
    when(source.getPeer()).thenReturn(peer);
    return source;
  }

  @Test
  public void testRemoveRemoteWALs() throws Exception {
    String peerId2 = slaveId + "_2";
    addPeerAndWait(peerId2,
      ReplicationPeerConfig.newBuilder()
        .setClusterKey("localhost:" + utility.getZkCluster().getClientPort() + ":/hbase").build(),
      true);
    try {
      // make sure that we can deal with files which does not exist
      String walNameNotExists =
        "remoteWAL-12345-" + slaveId + ".12345" + ReplicationUtils.SYNC_WAL_SUFFIX;
      Path wal = new Path(logDir, walNameNotExists);
      manager.preLogRoll(wal);
      manager.postLogRoll(wal);

      Path remoteLogDirForPeer = new Path(remoteLogDir, slaveId);
      fs.mkdirs(remoteLogDirForPeer);
      String walName =
        "remoteWAL-12345-" + slaveId + ".23456" + ReplicationUtils.SYNC_WAL_SUFFIX;
      Path remoteWAL =
        new Path(remoteLogDirForPeer, walName).makeQualified(fs.getUri(), fs.getWorkingDirectory());
      fs.create(remoteWAL).close();
      wal = new Path(logDir, walName);
      manager.preLogRoll(wal);
      manager.postLogRoll(wal);

      ReplicationSourceInterface source = mockReplicationSource(peerId2);
      manager.cleanOldLogs(walName, true, source);
      // still there if peer id does not match
      assertTrue(fs.exists(remoteWAL));

      source = mockReplicationSource(slaveId);
      manager.cleanOldLogs(walName, true, source);
      assertFalse(fs.exists(remoteWAL));
    } finally {
      removePeerAndWait(peerId2);
    }
  }

  @Test
  public void testSameWALPrefix() throws IOException {
    Set<String> latestWalsBefore =
      manager.getLastestPath().stream().map(Path::getName).collect(Collectors.toSet());
    String walName1 = "localhost,8080,12345-45678-Peer.34567";
    String walName2 = "localhost,8080,12345.56789";
    manager.preLogRoll(new Path(walName1));
    manager.preLogRoll(new Path(walName2));

    Set<String> latestWals = manager.getLastestPath().stream().map(Path::getName)
      .filter(n -> !latestWalsBefore.contains(n)).collect(Collectors.toSet());
    assertEquals(2, latestWals.size());
    assertTrue(latestWals.contains(walName1));
    assertTrue(latestWals.contains(walName2));
  }

  /**
   * Add a peer and wait for it to initialize
   * @param waitForSource Whether to wait for replication source to initialize
   */
  private void addPeerAndWait(final String peerId, final ReplicationPeerConfig peerConfig,
      final boolean waitForSource) throws Exception {
    final ReplicationPeers rp = manager.getReplicationPeers();
    rp.getPeerStorage().addPeer(peerId, peerConfig, true, SyncReplicationState.NONE);
    try {
      manager.addPeer(peerId);
    } catch (Exception e) {
      // ignore the failed exception, because we'll test both success & failed case.
    }
    waitPeer(peerId, manager, waitForSource);
    if (managerOfCluster != null) {
      managerOfCluster.addPeer(peerId);
      waitPeer(peerId, managerOfCluster, waitForSource);
    }
  }

  private static void waitPeer(final String peerId,
      ReplicationSourceManager manager, final boolean waitForSource) {
    ReplicationPeers rp = manager.getReplicationPeers();
    Waiter.waitFor(conf, 20000, () -> {
      if (waitForSource) {
        ReplicationSourceInterface rs = manager.getSource(peerId);
        if (rs == null) {
          return false;
        }
        if (rs instanceof ReplicationSourceDummy) {
          return ((ReplicationSourceDummy)rs).isStartup();
        }
        return true;
      } else {
        return (rp.getPeer(peerId) != null);
      }
    });
  }

  /**
   * Remove a peer and wait for it to get cleaned up
   * @param peerId
   * @throws Exception
   */
  private void removePeerAndWait(final String peerId) throws Exception {
    final ReplicationPeers rp = manager.getReplicationPeers();
    if (rp.getPeerStorage().listPeerIds().contains(peerId)) {
      rp.getPeerStorage().removePeer(peerId);
      try {
        manager.removePeer(peerId);
      } catch (Exception e) {
        // ignore the failed exception and continue.
      }
    }
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Collection<String> peers = rp.getPeerStorage().listPeerIds();
        return (!manager.getAllQueues().contains(peerId)) && (rp.getPeer(peerId) == null)
            && (!peers.contains(peerId)) && manager.getSource(peerId) == null;
      }
    });
  }

  private WALEdit getBulkLoadWALEdit(NavigableMap<byte[], Integer> scope) {
    // 1. Create store files for the families
    Map<byte[], List<Path>> storeFiles = new HashMap<>(1);
    Map<String, Long> storeFilesSize = new HashMap<>(1);
    List<Path> p = new ArrayList<>(1);
    Path hfilePath1 = new Path(Bytes.toString(f1));
    p.add(hfilePath1);
    try {
      storeFilesSize.put(hfilePath1.getName(), fs.getFileStatus(hfilePath1).getLen());
    } catch (IOException e) {
      LOG.debug("Failed to calculate the size of hfile " + hfilePath1);
      storeFilesSize.put(hfilePath1.getName(), 0L);
    }
    storeFiles.put(f1, p);
    scope.put(f1, 1);
    p = new ArrayList<>(1);
    Path hfilePath2 = new Path(Bytes.toString(f2));
    p.add(hfilePath2);
    try {
      storeFilesSize.put(hfilePath2.getName(), fs.getFileStatus(hfilePath2).getLen());
    } catch (IOException e) {
      LOG.debug("Failed to calculate the size of hfile " + hfilePath2);
      storeFilesSize.put(hfilePath2.getName(), 0L);
    }
    storeFiles.put(f2, p);
    // 2. Create bulk load descriptor
    BulkLoadDescriptor desc =
        ProtobufUtil.toBulkLoadDescriptor(hri.getTable(),
      UnsafeByteOperations.unsafeWrap(hri.getEncodedNameAsBytes()), storeFiles, storeFilesSize, 1);

    // 3. create bulk load wal edit event
    WALEdit logEdit = WALEdit.createBulkLoadEvent(hri, desc);
    return logEdit;
  }

  static class DummyNodeFailoverWorker extends Thread {
    private Map<String, Set<String>> logZnodesMap;
    Server server;
    private ServerName deadRS;
    ReplicationQueueStorage rq;

    public DummyNodeFailoverWorker(ServerName deadRS, Server s) throws Exception {
      this.deadRS = deadRS;
      this.server = s;
      this.rq = ReplicationStorageFactory.getReplicationQueueStorage(server.getZooKeeper(),
        server.getConfiguration());
    }

    @Override
    public void run() {
      try {
        logZnodesMap = new HashMap<>();
        List<String> queues = rq.getAllQueues(deadRS);
        for (String queue : queues) {
          Pair<String, SortedSet<String>> pair =
              rq.claimQueue(deadRS, queue, server.getServerName());
          if (pair != null) {
            logZnodesMap.put(pair.getFirst(), pair.getSecond());
          }
        }
        server.abort("Done with testing", null);
      } catch (Exception e) {
        LOG.error("Got exception while running NodeFailoverWorker", e);
      } finally {
        latch.countDown();
      }
    }

    /**
     * @return 1 when the map is not empty.
     */
    private int isLogZnodesMapPopulated() {
      Collection<Set<String>> sets = logZnodesMap.values();
      if (sets.size() > 1) {
        throw new RuntimeException("unexpected size of logZnodesMap: " + sets.size());
      }
      if (sets.size() == 1) {
        Set<String> s = sets.iterator().next();
        for (String file : files) {
          // at least one file was missing
          if (!s.contains(file)) {
            return 0;
          }
        }
        return 1; // we found all the files
      }
      return 0;
    }
  }

  static class FailInitializeDummyReplicationSource extends ReplicationSourceDummy {

    @Override
    public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
        ReplicationQueueStorage rq, ReplicationPeer rp, Server server, String peerClusterId,
        UUID clusterId, WALFileLengthProvider walFileLengthProvider, MetricsSource metrics)
        throws IOException {
      throw new IOException("Failing deliberately");
    }
  }

  static class DummyServer implements Server {
    String hostname;

    DummyServer() {
      hostname = "hostname.example.org";
    }

    DummyServer(String hostname) {
      this.hostname = hostname;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZKWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }
    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(hostname, 1234, 1L);
    }

    @Override
    public void abort(String why, Throwable e) {
      // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
      // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isStopped() {
      return false; // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }

    @Override
    public AsyncClusterConnection getAsyncClusterConnection() {
      return null;
    }
  }
}
