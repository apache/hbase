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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.replication.DummyReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationResult;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationSourceManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationSourceManager.class);

  public static final class ReplicationEndpointForTest extends DummyReplicationEndpoint {

    private String clusterKey;

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      // if you want to block the replication, for example, do not want the recovered source to be
      // removed
      if (clusterKey.endsWith("error")) {
        throw new RuntimeException("Inject error");
      }
      return true;
    }

    @Override
    public void init(Context context) throws IOException {
      super.init(context);
      this.clusterKey = context.getReplicationPeer().getPeerConfig().getClusterKey();
    }

  }

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static Configuration CONF;

  private static FileSystem FS;

  private static final byte[] F1 = Bytes.toBytes("f1");

  private static final byte[] F2 = Bytes.toBytes("f2");

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static RegionInfo RI;

  private static NavigableMap<byte[], Integer> SCOPES;

  @Rule
  public final TestName name = new TestName();

  private Path oldLogDir;

  private Path logDir;

  private Path remoteLogDir;

  private Server server;

  private Replication replication;

  private ReplicationSourceManager manager;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
    FS = UTIL.getTestFileSystem();
    CONF = new Configuration(UTIL.getConfiguration());
    CONF.setLong("replication.sleep.before.failover", 0);

    RI = RegionInfoBuilder.newBuilder(TABLE_NAME).build();
    SCOPES = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    SCOPES.put(F1, 1);
    SCOPES.put(F2, 0);
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    Path rootDir = UTIL.getDataTestDirOnTestFS(name.getMethodName());
    CommonFSUtils.setRootDir(CONF, rootDir);
    server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(CONF);
    when(server.getZooKeeper()).thenReturn(UTIL.getZooKeeperWatcher());
    when(server.getConnection()).thenReturn(UTIL.getConnection());
    ServerName sn = ServerName.valueOf("hostname.example.org", 1234, 1);
    when(server.getServerName()).thenReturn(sn);
    oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    FS.mkdirs(oldLogDir);
    logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    FS.mkdirs(logDir);
    remoteLogDir = new Path(rootDir, ReplicationUtils.REMOTE_WAL_DIR_NAME);
    FS.mkdirs(remoteLogDir);
    TableName tableName = TableName.valueOf("replication_" + name.getMethodName());
    UTIL.getAdmin()
      .createTable(ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName));
    CONF.set(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME, tableName.getNameAsString());

    replication = new Replication();
    replication.initialize(server, FS, new Path(logDir, sn.toString()), oldLogDir,
      new WALFactory(CONF, server.getServerName(), null));
    manager = replication.getReplicationManager();
  }

  @After
  public void tearDown() {
    replication.stopReplicationService();
  }

  /**
   * Add a peer and wait for it to initialize
   */
  private void addPeerAndWait(String peerId, String clusterKey, boolean syncRep)
    throws ReplicationException, IOException {
    ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL.getZkCluster().getAddress().toString() + ":/" + clusterKey)
      .setReplicationEndpointImpl(ReplicationEndpointForTest.class.getName());
    if (syncRep) {
      builder.setTableCFsMap(ImmutableMap.of(TABLE_NAME, Collections.emptyList()))
        .setRemoteWALDir(FS.makeQualified(remoteLogDir).toString());
    }

    manager.getReplicationPeers().getPeerStorage().addPeer(peerId, builder.build(), true,
      syncRep ? SyncReplicationState.DOWNGRADE_ACTIVE : SyncReplicationState.NONE);
    manager.addPeer(peerId);
    UTIL.waitFor(20000, () -> {
      ReplicationSourceInterface rs = manager.getSource(peerId);
      return rs != null && rs.isSourceActive();
    });
  }

  /**
   * Remove a peer and wait for it to get cleaned up
   */
  private void removePeerAndWait(String peerId) throws Exception {
    ReplicationPeers rp = manager.getReplicationPeers();
    rp.getPeerStorage().removePeer(peerId);
    manager.removePeer(peerId);
    UTIL.waitFor(20000, () -> {
      if (rp.getPeer(peerId) != null) {
        return false;
      }
      if (manager.getSource(peerId) != null) {
        return false;
      }
      return manager.getOldSources().stream().noneMatch(rs -> rs.getPeerId().equals(peerId));
    });
  }

  private void createWALFile(Path file) throws Exception {
    ProtobufLogWriter writer = new ProtobufLogWriter();
    try {
      writer.init(FS, file, CONF, false, FS.getDefaultBlockSize(file), null);
      WALKeyImpl key = new WALKeyImpl(RI.getEncodedNameAsBytes(), TABLE_NAME,
        EnvironmentEdgeManager.currentTime(), SCOPES);
      WALEdit edit = new WALEdit();
      WALEditInternalHelper.addExtendedCell(edit,
        ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(F1).setFamily(F1)
          .setQualifier(F1).setType(Cell.Type.Put).setValue(F1).build());
      WALEditInternalHelper.addExtendedCell(edit,
        ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(F2).setFamily(F2)
          .setQualifier(F2).setType(Cell.Type.Put).setValue(F2).build());
      writer.append(new WAL.Entry(key, edit));
      writer.sync(false);
    } finally {
      writer.close();
    }
  }

  @Test
  public void testClaimQueue() throws Exception {
    String peerId = "1";
    addPeerAndWait(peerId, "error", false);
    ServerName serverName = ServerName.valueOf("hostname0.example.org", 12345, 123);
    String walName1 = serverName.toString() + ".1";
    createWALFile(new Path(oldLogDir, walName1));
    ReplicationQueueId queueId = new ReplicationQueueId(serverName, peerId);
    ReplicationQueueStorage queueStorage = manager.getQueueStorage();
    queueStorage.setOffset(queueId, "", new ReplicationGroupOffset(peerId, 0),
      Collections.emptyMap());
    manager.claimQueue(queueId);
    assertThat(manager.getOldSources(), hasSize(1));
  }

  @Test
  public void testSameWALPrefix() throws IOException {
    String walName1 = "localhost,8080,12345-45678-Peer.34567";
    String walName2 = "localhost,8080,12345.56789";
    manager.postLogRoll(new Path(walName1));
    manager.postLogRoll(new Path(walName2));

    Set<String> latestWals =
      manager.getLastestPath().stream().map(Path::getName).collect(Collectors.toSet());
    assertThat(latestWals,
      Matchers.<Set<String>> both(hasSize(2)).and(hasItems(walName1, walName2)));
  }

  private MetricsReplicationSourceSource getGlobalSource() {
    return CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class)
      .getGlobalSource();
  }

  @Test
  public void testRemovePeerMetricsCleanup() throws Exception {
    MetricsReplicationSourceSource globalSource = getGlobalSource();
    int globalLogQueueSizeInitial = globalSource.getSizeOfLogQueue();
    String peerId = "DummyPeer";
    addPeerAndWait(peerId, "hbase", false);
    // there is no latestPaths so the size of log queue should not change
    assertEquals(globalLogQueueSizeInitial, globalSource.getSizeOfLogQueue());

    ReplicationSourceInterface source = manager.getSource(peerId);
    // Sanity check
    assertNotNull(source);
    int sizeOfSingleLogQueue = source.getSourceMetrics().getSizeOfLogQueue();
    // Enqueue log and check if metrics updated
    Path serverLogDir = new Path(logDir, server.getServerName().toString());
    source.enqueueLog(new Path(serverLogDir, server.getServerName() + ".1"));
    assertEquals(1 + sizeOfSingleLogQueue, source.getSourceMetrics().getSizeOfLogQueue());
    assertEquals(source.getSourceMetrics().getSizeOfLogQueue() + globalLogQueueSizeInitial,
      globalSource.getSizeOfLogQueue());

    // Removing the peer should reset the global metrics
    removePeerAndWait(peerId);
    assertEquals(globalLogQueueSizeInitial, globalSource.getSizeOfLogQueue());

    // Adding the same peer back again should reset the single source metrics
    addPeerAndWait(peerId, "hbase", false);
    source = manager.getSource(peerId);
    assertNotNull(source);
    assertEquals(source.getSourceMetrics().getSizeOfLogQueue() + globalLogQueueSizeInitial,
      globalSource.getSizeOfLogQueue());
  }

  @Test
  public void testDisablePeerMetricsCleanup() throws Exception {
    final String peerId = "DummyPeer";
    try {
      MetricsReplicationSourceSource globalSource = getGlobalSource();
      final int globalLogQueueSizeInitial = globalSource.getSizeOfLogQueue();
      addPeerAndWait(peerId, "hbase", false);
      assertEquals(globalLogQueueSizeInitial, globalSource.getSizeOfLogQueue());
      ReplicationSourceInterface source = manager.getSource(peerId);
      // Sanity check
      assertNotNull(source);
      final int sizeOfSingleLogQueue = source.getSourceMetrics().getSizeOfLogQueue();
      // Enqueue log and check if metrics updated
      Path serverLogDir = new Path(logDir, server.getServerName().toString());
      source.enqueueLog(new Path(serverLogDir, server.getServerName() + ".1"));
      assertEquals(1 + sizeOfSingleLogQueue, source.getSourceMetrics().getSizeOfLogQueue());
      assertEquals(source.getSourceMetrics().getSizeOfLogQueue() + globalLogQueueSizeInitial,
        globalSource.getSizeOfLogQueue());

      // Refreshing the peer should decrement the global and single source metrics
      manager.refreshSources(peerId);
      assertEquals(globalLogQueueSizeInitial, globalSource.getSizeOfLogQueue());

      source = manager.getSource(peerId);
      assertNotNull(source);
      assertEquals(sizeOfSingleLogQueue, source.getSourceMetrics().getSizeOfLogQueue());
      assertEquals(source.getSourceMetrics().getSizeOfLogQueue() + globalLogQueueSizeInitial,
        globalSource.getSizeOfLogQueue());
    } finally {
      removePeerAndWait(peerId);
    }
  }

  @Test
  public void testRemoveRemoteWALs() throws Exception {
    String peerId = "2";
    addPeerAndWait(peerId, "hbase", true);
    // make sure that we can deal with files which does not exist
    String walNameNotExists =
      "remoteWAL-12345-" + peerId + ".12345" + ReplicationUtils.SYNC_WAL_SUFFIX;
    Path wal = new Path(logDir, walNameNotExists);
    manager.postLogRoll(wal);

    Path remoteLogDirForPeer = new Path(remoteLogDir, peerId);
    FS.mkdirs(remoteLogDirForPeer);
    String walName = "remoteWAL-12345-" + peerId + ".23456" + ReplicationUtils.SYNC_WAL_SUFFIX;
    Path remoteWAL =
      new Path(remoteLogDirForPeer, walName).makeQualified(FS.getUri(), FS.getWorkingDirectory());
    FS.create(remoteWAL).close();
    wal = new Path(logDir, walName);
    manager.postLogRoll(wal);

    ReplicationSourceInterface source = manager.getSource(peerId);
    manager.cleanOldLogs(walName, true, source);
    assertFalse(FS.exists(remoteWAL));
  }
}
