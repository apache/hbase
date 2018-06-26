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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Base class for testing sync replication.
 */
public class SyncReplicationTestBase {

  protected static final HBaseZKTestingUtility ZK_UTIL = new HBaseZKTestingUtility();

  protected static final HBaseTestingUtility UTIL1 = new HBaseTestingUtility();

  protected static final HBaseTestingUtility UTIL2 = new HBaseTestingUtility();

  protected static TableName TABLE_NAME = TableName.valueOf("SyncRep");

  protected static byte[] CF = Bytes.toBytes("cf");

  protected static byte[] CQ = Bytes.toBytes("cq");

  protected static String PEER_ID = "1";

  protected static Path REMOTE_WAL_DIR1;

  protected static Path REMOTE_WAL_DIR2;

  protected static void initTestingUtility(HBaseTestingUtility util, String zkParent) {
    util.setZkCluster(ZK_UTIL.getZkCluster());
    Configuration conf = util.getConfiguration();
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkParent);
    conf.setInt("replication.source.size.capacity", 102400);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);
    conf.setFloat("replication.source.ratio", 1.0f);
    conf.setBoolean("replication.source.eof.autorecovery", true);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    ZK_UTIL.startMiniZKCluster();
    initTestingUtility(UTIL1, "/cluster1");
    initTestingUtility(UTIL2, "/cluster2");
    UTIL1.startMiniCluster(2,3);
    UTIL2.startMiniCluster(2,3);
    TableDescriptor td =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(CF).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build();
    UTIL1.getAdmin().createTable(td);
    UTIL2.getAdmin().createTable(td);
    FileSystem fs1 = UTIL1.getTestFileSystem();
    FileSystem fs2 = UTIL2.getTestFileSystem();
    REMOTE_WAL_DIR1 =
      new Path(UTIL1.getMiniHBaseCluster().getMaster().getMasterFileSystem().getWALRootDir(),
        "remoteWALs").makeQualified(fs1.getUri(), fs1.getWorkingDirectory());
    REMOTE_WAL_DIR2 =
      new Path(UTIL2.getMiniHBaseCluster().getMaster().getMasterFileSystem().getWALRootDir(),
        "remoteWALs").makeQualified(fs2.getUri(), fs2.getWorkingDirectory());
    UTIL1.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL2.getClusterKey())
        .setReplicateAllUserTables(false)
        .setTableCFsMap(ImmutableMap.of(TABLE_NAME, new ArrayList<>()))
        .setRemoteWALDir(REMOTE_WAL_DIR2.toUri().toString()).build());
    UTIL2.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL1.getClusterKey())
        .setReplicateAllUserTables(false)
        .setTableCFsMap(ImmutableMap.of(TABLE_NAME, new ArrayList<>()))
        .setRemoteWALDir(REMOTE_WAL_DIR1.toUri().toString()).build());
  }

  private static void shutdown(HBaseTestingUtility util) throws Exception {
    if (util.getHBaseCluster() == null) {
      return;
    }
    Admin admin = util.getAdmin();
    if (!admin.listReplicationPeers(Pattern.compile(PEER_ID)).isEmpty()) {
      if (admin
        .getReplicationPeerSyncReplicationState(PEER_ID) != SyncReplicationState.DOWNGRADE_ACTIVE) {
        admin.transitReplicationPeerSyncReplicationState(PEER_ID,
          SyncReplicationState.DOWNGRADE_ACTIVE);
      }
      admin.removeReplicationPeer(PEER_ID);
    }
    util.shutdownMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    shutdown(UTIL1);
    shutdown(UTIL2);
    ZK_UTIL.shutdownMiniZKCluster();
  }

  protected final void write(HBaseTestingUtility util, int start, int end) throws IOException {
    try (Table table = util.getConnection().getTable(TABLE_NAME)) {
      for (int i = start; i < end; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
  }

  protected final void verify(HBaseTestingUtility util, int start, int end) throws IOException {
    try (Table table = util.getConnection().getTable(TABLE_NAME)) {
      for (int i = start; i < end; i++) {
        assertEquals(i, Bytes.toInt(table.get(new Get(Bytes.toBytes(i))).getValue(CF, CQ)));
      }
    }
  }

  protected final void verifyThroughRegion(HBaseTestingUtility util, int start, int end)
      throws IOException {
    HRegion region = util.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0);
    for (int i = start; i < end; i++) {
      assertEquals(i, Bytes.toInt(region.get(new Get(Bytes.toBytes(i))).getValue(CF, CQ)));
    }
  }

  protected final void verifyNotReplicatedThroughRegion(HBaseTestingUtility util, int start,
      int end) throws IOException {
    HRegion region = util.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0);
    for (int i = start; i < end; i++) {
      assertTrue(region.get(new Get(Bytes.toBytes(i))).isEmpty());
    }
  }

  protected final void waitUntilReplicationDone(HBaseTestingUtility util, int end)
      throws Exception {
    // The reject check is in RSRpcService so we can still read through HRegion
    HRegion region = util.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0);
    util.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return !region.get(new Get(Bytes.toBytes(end - 1))).isEmpty();
      }

      @Override
      public String explainFailure() throws Exception {
        return "Replication has not been catched up yet";
      }
    });
  }

  protected final void writeAndVerifyReplication(HBaseTestingUtility util1,
      HBaseTestingUtility util2, int start, int end) throws Exception {
    write(util1, start, end);
    waitUntilReplicationDone(util2, end);
    verifyThroughRegion(util2, start, end);
  }

  protected final Path getRemoteWALDir(MasterFileSystem mfs, String peerId) {
    Path remoteWALDir = new Path(mfs.getWALRootDir(), ReplicationUtils.REMOTE_WAL_DIR_NAME);
    return getRemoteWALDir(remoteWALDir, peerId);
  }

  protected Path getRemoteWALDir(Path remoteWALDir, String peerId) {
    return new Path(remoteWALDir, peerId);
  }

  protected Path getReplayRemoteWALs(Path remoteWALDir, String peerId) {
    return new Path(remoteWALDir, peerId + "-replay");
  }

  protected void verifyRemovedPeer(String peerId, Path remoteWALDir, HBaseTestingUtility utility)
      throws Exception {
    ReplicationPeerStorage rps = ReplicationStorageFactory
      .getReplicationPeerStorage(utility.getZooKeeperWatcher(), utility.getConfiguration());
    try {
      rps.getPeerSyncReplicationState(peerId);
      fail("Should throw exception when get the sync replication state of a removed peer.");
    } catch (ReplicationException e) {
      // ignore.
    }
    try {
      rps.getPeerNewSyncReplicationState(peerId);
      fail("Should throw exception when get the new sync replication state of a removed peer");
    } catch (ReplicationException e) {
      // ignore.
    }
    try (FileSystem fs = utility.getTestFileSystem()) {
      assertFalse(fs.exists(getRemoteWALDir(remoteWALDir, peerId)));
      assertFalse(fs.exists(getReplayRemoteWALs(remoteWALDir, peerId)));
    }
  }

  protected void verifyReplicationRequestRejection(HBaseTestingUtility utility,
      boolean expectedRejection) throws Exception {
    HRegionServer regionServer = utility.getRSForFirstRegionInTable(TABLE_NAME);
    ClusterConnection connection = regionServer.getClusterConnection();
    Entry[] entries = new Entry[10];
    for (int i = 0; i < entries.length; i++) {
      entries[i] =
        new Entry(new WALKeyImpl(HConstants.EMPTY_BYTE_ARRAY, TABLE_NAME, 0), new WALEdit());
    }
    if (!expectedRejection) {
      ReplicationProtbufUtil.replicateWALEntry(connection.getAdmin(regionServer.getServerName()),
        entries, null, null, null);
    } else {
      try {
        ReplicationProtbufUtil.replicateWALEntry(connection.getAdmin(regionServer.getServerName()),
          entries, null, null, null);
        fail("Should throw IOException when sync-replication state is in A or DA");
      } catch (DoNotRetryIOException e) {
        assertTrue(e.getMessage().contains("Reject to apply to sink cluster"));
        assertTrue(e.getMessage().contains(TABLE_NAME.toString()));
      }
    }
  }
}
