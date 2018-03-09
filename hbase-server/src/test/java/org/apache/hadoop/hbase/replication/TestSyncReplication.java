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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSyncReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSyncReplication.class);

  private static final HBaseZKTestingUtility ZK_UTIL = new HBaseZKTestingUtility();

  private static final HBaseTestingUtility UTIL1 = new HBaseTestingUtility();

  private static final HBaseTestingUtility UTIL2 = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("SyncRep");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static String PEER_ID = "1";

  private static void initTestingUtility(HBaseTestingUtility util, String zkParent) {
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
    UTIL1.startMiniCluster(3);
    UTIL2.startMiniCluster(3);
    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(ColumnFamilyDescriptorBuilder
          .newBuilder(CF).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build();
    UTIL1.getAdmin().createTable(td);
    UTIL2.getAdmin().createTable(td);
    FileSystem fs1 = UTIL1.getTestFileSystem();
    FileSystem fs2 = UTIL2.getTestFileSystem();
    Path remoteWALDir1 =
        new Path(UTIL1.getMiniHBaseCluster().getMaster().getMasterFileSystem().getRootDir(),
          "remoteWALs").makeQualified(fs1.getUri(), fs1.getWorkingDirectory());
    Path remoteWALDir2 =
        new Path(UTIL2.getMiniHBaseCluster().getMaster().getMasterFileSystem().getRootDir(),
          "remoteWALs").makeQualified(fs2.getUri(), fs2.getWorkingDirectory());
    UTIL1.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL2.getClusterKey())
        .setReplicateAllUserTables(false)
        .setTableCFsMap(ImmutableMap.of(TABLE_NAME, new ArrayList<>()))
        .setRemoteWALDir(remoteWALDir2.toUri().toString()).build());
    UTIL2.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL1.getClusterKey())
        .setReplicateAllUserTables(false)
        .setTableCFsMap(ImmutableMap.of(TABLE_NAME, new ArrayList<>()))
        .setRemoteWALDir(remoteWALDir1.toUri().toString()).build());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL1.shutdownMiniCluster();
    UTIL2.shutdownMiniCluster();
    ZK_UTIL.shutdownMiniZKCluster();
  }

  @FunctionalInterface
  private interface TableAction {

    void call(Table table) throws IOException;
  }

  private void assertDisallow(Table table, TableAction action) throws IOException {
    try {
      action.call(table);
    } catch (DoNotRetryIOException | RetriesExhaustedException e) {
      // expected
      assertThat(e.getMessage(), containsString("STANDBY"));
    }
  }

  @Test
  public void testStandby() throws Exception {
    MasterFileSystem mfs = UTIL2.getHBaseCluster().getMaster().getMasterFileSystem();
    Path remoteWALDir = new Path(mfs.getWALRootDir(), ReplicationUtils.REMOTE_WAL_DIR_NAME);
    Path remoteWALDirForPeer = new Path(remoteWALDir, PEER_ID);
    assertFalse(mfs.getWALFileSystem().exists(remoteWALDirForPeer));
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    assertTrue(mfs.getWALFileSystem().exists(remoteWALDirForPeer));
    try (Table table = UTIL2.getConnection().getTable(TABLE_NAME)) {
      assertDisallow(table, t -> t.get(new Get(Bytes.toBytes("row"))));
      assertDisallow(table,
        t -> t.put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row"))));
      assertDisallow(table, t -> t.delete(new Delete(Bytes.toBytes("row"))));
      assertDisallow(table, t -> t.incrementColumnValue(Bytes.toBytes("row"), CF, CQ, 1));
      assertDisallow(table,
        t -> t.append(new Append(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row"))));
      assertDisallow(table,
        t -> t.get(Arrays.asList(new Get(Bytes.toBytes("row")), new Get(Bytes.toBytes("row1")))));
      assertDisallow(table,
        t -> t
          .put(Arrays.asList(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row")),
            new Put(Bytes.toBytes("row1")).addColumn(CF, CQ, Bytes.toBytes("row1")))));
      assertDisallow(table, t -> t.mutateRow(new RowMutations(Bytes.toBytes("row"))
        .add((Mutation) new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("row")))));
    }
    // But we should still allow replication writes
    try (Table table = UTIL1.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    // The reject check is in RSRpcService so we can still read through HRegion
    HRegion region = UTIL2.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0);
    UTIL2.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return !region.get(new Get(Bytes.toBytes(99))).isEmpty();
      }

      @Override
      public String explainFailure() throws Exception {
        return "Replication has not been catched up yet";
      }
    });
    for (int i = 0; i < 100; i++) {
      assertEquals(i, Bytes.toInt(region.get(new Get(Bytes.toBytes(i))).getValue(CF, CQ)));
    }
  }
}
