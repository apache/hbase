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
package org.apache.hadoop.hbase.wal;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.DualAsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogTestHelper;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.regionserver.SyncReplicationPeerInfoProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestSyncReplicationWALProvider {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationWALProvider.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String PEER_ID = "1";

  private static String REMOTE_WAL_DIR = "/RemoteWAL";

  private static TableName TABLE = TableName.valueOf("table");

  private static TableName TABLE_NO_REP = TableName.valueOf("table-no-rep");

  private static RegionInfo REGION = RegionInfoBuilder.newBuilder(TABLE).build();

  private static RegionInfo REGION_NO_REP = RegionInfoBuilder.newBuilder(TABLE_NO_REP).build();

  private static WALFactory FACTORY;

  public static final class InfoProvider implements SyncReplicationPeerInfoProvider {

    @Override
    public Optional<Pair<String, String>> getPeerIdAndRemoteWALDir(RegionInfo info) {
      if (info.getTable().equals(TABLE)) {
        return Optional.of(Pair.newPair(PEER_ID, REMOTE_WAL_DIR));
      } else {
        return Optional.empty();
      }
    }

    @Override
    public boolean isInState(RegionInfo info, SyncReplicationState state) {
      // TODO Implement SyncReplicationPeerInfoProvider.isInState
      return false;
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean(HConstants.SYNC_REPLICATION_ENABLED, true);
    UTIL.startMiniDFSCluster(3);
    FACTORY = new WALFactory(UTIL.getConfiguration(), "test");
    ((SyncReplicationWALProvider) FACTORY.getWALProvider()).setPeerInfoProvider(new InfoProvider());
    UTIL.getTestFileSystem().mkdirs(new Path(REMOTE_WAL_DIR, PEER_ID));
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    FACTORY.close();
    UTIL.shutdownMiniDFSCluster();
  }

  private void testReadWrite(DualAsyncFSWAL wal) throws Exception {
    int recordCount = 100;
    int columnCount = 10;
    byte[] row = Bytes.toBytes("testRow");
    long timestamp = System.currentTimeMillis();
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    ProtobufLogTestHelper.doWrite(wal, REGION, TABLE, columnCount, recordCount, row, timestamp,
      mvcc);
    Path localFile = wal.getCurrentFileName();
    Path remoteFile = new Path(REMOTE_WAL_DIR + "/" + PEER_ID, localFile.getName());
    try (ProtobufLogReader reader =
      (ProtobufLogReader) FACTORY.createReader(UTIL.getTestFileSystem(), localFile)) {
      ProtobufLogTestHelper.doRead(reader, false, REGION, TABLE, columnCount, recordCount, row,
        timestamp);
    }
    try (ProtobufLogReader reader =
      (ProtobufLogReader) FACTORY.createReader(UTIL.getTestFileSystem(), remoteFile)) {
      ProtobufLogTestHelper.doRead(reader, false, REGION, TABLE, columnCount, recordCount, row,
        timestamp);
    }
    wal.rollWriter();
    DistributedFileSystem dfs = (DistributedFileSystem) UTIL.getDFSCluster().getFileSystem();
    UTIL.waitFor(5000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return dfs.isFileClosed(localFile) && dfs.isFileClosed(remoteFile);
      }

      @Override
      public String explainFailure() throws Exception {
        StringBuilder sb = new StringBuilder();
        if (!dfs.isFileClosed(localFile)) {
          sb.append(localFile + " has not been closed yet.");
        }
        if (!dfs.isFileClosed(remoteFile)) {
          sb.append(remoteFile + " has not been closed yet.");
        }
        return sb.toString();
      }
    });
    try (ProtobufLogReader reader =
      (ProtobufLogReader) FACTORY.createReader(UTIL.getTestFileSystem(), localFile)) {
      ProtobufLogTestHelper.doRead(reader, true, REGION, TABLE, columnCount, recordCount, row,
        timestamp);
    }
    try (ProtobufLogReader reader =
      (ProtobufLogReader) FACTORY.createReader(UTIL.getTestFileSystem(), remoteFile)) {
      ProtobufLogTestHelper.doRead(reader, true, REGION, TABLE, columnCount, recordCount, row,
        timestamp);
    }
  }

  @Test
  public void test() throws Exception {
    WAL walNoRep = FACTORY.getWAL(REGION_NO_REP);
    assertThat(walNoRep, not(instanceOf(DualAsyncFSWAL.class)));
    DualAsyncFSWAL wal = (DualAsyncFSWAL) FACTORY.getWAL(REGION);
    assertEquals(2, FACTORY.getWALs().size());
    testReadWrite(wal);
    SyncReplicationWALProvider walProvider = (SyncReplicationWALProvider) FACTORY.getWALProvider();
    walProvider.peerSyncReplicationStateChange(PEER_ID, SyncReplicationState.ACTIVE,
      SyncReplicationState.DOWNGRADE_ACTIVE, 1);
    assertEquals(1, FACTORY.getWALs().size());
  }
}
