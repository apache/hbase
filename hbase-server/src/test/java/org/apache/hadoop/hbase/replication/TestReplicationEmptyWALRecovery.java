/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationEmptyWALRecovery extends TestReplicationBase {
  MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
  static final RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
  NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationEmptyWALRecovery.class);

  @Before
  public void setUp() throws IOException, InterruptedException {
    cleanUp();
    scopes.put(famName, HConstants.REPLICATION_SCOPE_GLOBAL);
    replicateCount.set(0);
    replicatedEntries.clear();
  }

  /**
   * Waits until there is only one log(the current writing one) in the replication queue
   *
   * @param numRs number of region servers
   */
  private void waitForLogAdvance(int numRs) {
    Waiter.waitFor(CONF1, 100000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        for (int i = 0; i < numRs; i++) {
          HRegionServer hrs = UTIL1.getHBaseCluster().getRegionServer(i);
          RegionInfo regionInfo =
            UTIL1.getHBaseCluster().getRegions(htable1.getName()).get(0).getRegionInfo();
          WAL wal = hrs.getWAL(regionInfo);
          Path currentFile = ((AbstractFSWAL<?>) wal).getCurrentFileName();
          Replication replicationService =
            (Replication) UTIL1.getHBaseCluster().getRegionServer(i).getReplicationSourceService();
          for (ReplicationSourceInterface rsi : replicationService.getReplicationManager()
            .getSources()) {
            ReplicationSource source = (ReplicationSource) rsi;
            // We are making sure that there is only one log queue and that is for the
            // current WAL of region server
            String logPrefix = source.getQueues().keySet().stream().findFirst().get();
            if (!currentFile.equals(source.getCurrentPath())
              || source.getQueues().keySet().size() != 1
              || source.getQueues().get(logPrefix).size() != 1) {
              return false;
            }
          }
        }
        return true;
      }
    });
  }

  private void verifyNumberOfLogsInQueue(int numQueues, int numRs) {
    Waiter.waitFor(CONF1, 10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() {
        for (int i = 0; i < numRs; i++) {
          Replication replicationService =
            (Replication) UTIL1.getHBaseCluster().getRegionServer(i).getReplicationSourceService();
          for (ReplicationSourceInterface rsi : replicationService.getReplicationManager()
            .getSources()) {
            ReplicationSource source = (ReplicationSource) rsi;
            String logPrefix = source.getQueues().keySet().stream().findFirst().get();
            if (source.getQueues().get(logPrefix).size() != numQueues) {
              return false;
            }
          }
        }
        return true;
      }
    });
  }

  @Test
  public void testEmptyWALRecovery() throws Exception {
    final int numRs = UTIL1.getHBaseCluster().getRegionServerThreads().size();
    // for each RS, create an empty wal with same walGroupId
    final List<Path> emptyWalPaths = new ArrayList<>();
    long ts = System.currentTimeMillis();
    for (int i = 0; i < numRs; i++) {
      RegionInfo regionInfo =
        UTIL1.getHBaseCluster().getRegions(htable1.getName()).get(0).getRegionInfo();
      WAL wal = UTIL1.getHBaseCluster().getRegionServer(i).getWAL(regionInfo);
      Path currentWalPath = AbstractFSWALProvider.getCurrentFileName(wal);
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(currentWalPath.getName());
      Path emptyWalPath = new Path(UTIL1.getDataTestDir(), walGroupId + "." + ts);
      UTIL1.getTestFileSystem().create(emptyWalPath).close();
      emptyWalPaths.add(emptyWalPath);
    }

    injectEmptyWAL(numRs, emptyWalPaths);

    // ReplicationSource should advance past the empty wal, or else the test will fail
    waitForLogAdvance(numRs);
    verifyNumberOfLogsInQueue(1, numRs);
    // we're now writing to the new wal
    // if everything works, the source should've stopped reading from the empty wal, and start
    // replicating from the new wal
    runSimplePutDeleteTest();
    rollWalsAndWaitForDeque(numRs);
  }

  /**
   * Test empty WAL along with non empty WALs in the same batch. This test is to make sure when we
   * see the empty and handle the EOF exception, we are able to ship the previous batch of entries
   * without loosing it. This test also tests the number of batches shipped
   * @throws Exception throws any exception
   */
  @Test
  public void testReplicationOfEmptyWALFollowingNonEmptyWAL() throws Exception {
    // Disable the replication peer to accumulate the non empty WAL followed by empty WAL
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    int numOfEntriesToReplicate = 20;

    final int numRs = UTIL1.getHBaseCluster().getRegionServerThreads().size();
    // for each RS, create an empty wal with same walGroupId
    final List<Path> emptyWalPaths = new ArrayList<>();
    long ts = System.currentTimeMillis();
    for (int i = 0; i < numRs; i++) {
      RegionInfo regionInfo =
        UTIL1.getHBaseCluster().getRegions(tableName.getName()).get(0).getRegionInfo();
      WAL wal = UTIL1.getHBaseCluster().getRegionServer(i).getWAL(regionInfo);
      Path currentWalPath = AbstractFSWALProvider.getCurrentFileName(wal);

      appendEntriesToWal(numOfEntriesToReplicate, wal);
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(currentWalPath.getName());
      Path emptyWalPath = new Path(UTIL1.getDefaultRootDirPath(), walGroupId + "." + ts);
      UTIL1.getTestFileSystem().create(emptyWalPath).close();
      emptyWalPaths.add(emptyWalPath);
    }

    injectEmptyWAL(numRs, emptyWalPaths);
    // There should be three WALs in queue
    // 1. non empty WAL
    // 2. empty WAL
    // 3. live WAL
    verifyNumberOfLogsInQueue(3, numRs);
    hbaseAdmin.enableReplicationPeer(PEER_ID2);
    // ReplicationSource should advance past the empty wal, or else the test will fail
    waitForLogAdvance(numRs);

    // Now we should expect numOfEntriesToReplicate entries
    // replicated from each region server. This makes sure we didn't loose data
    // from any previous batch when we encounter EOF exception for empty file.
    Assert.assertEquals("Replicated entries are not correct", numOfEntriesToReplicate * numRs,
      replicatedEntries.size());

    // We expect just one batch of replication which will
    // be from when we handle the EOF exception.
    Assert.assertEquals("Replicated batches are not correct", 1, replicateCount.intValue());
    verifyNumberOfLogsInQueue(1, numRs);
    // we're now writing to the new wal
    // if everything works, the source should've stopped reading from the empty wal, and start
    // replicating from the new wal
    runSimplePutDeleteTest();
    rollWalsAndWaitForDeque(numRs);
  }

  /**
   * Test empty WAL along with non empty WALs in the same batch. This test is to make sure when we
   * see the empty WAL and handle the EOF exception, we are able to proceed with next batch and
   * replicate it properly without missing data.
   * @throws Exception throws any exception
   */
  @Test
  public void testReplicationOfEmptyWALFollowedByNonEmptyWAL() throws Exception {
    // Disable the replication peer to accumulate the non empty WAL followed by empty WAL
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    int numOfEntriesToReplicate = 20;

    final int numRs = UTIL1.getHBaseCluster().getRegionServerThreads().size();
    // for each RS, create an empty wal with same walGroupId
    final List<Path> emptyWalPaths = new ArrayList<>();

    long ts = System.currentTimeMillis();
    WAL wal = null;
    for (int i = 0; i < numRs; i++) {
      RegionInfo regionInfo =
        UTIL1.getHBaseCluster().getRegions(tableName.getName()).get(0).getRegionInfo();
      wal = UTIL1.getHBaseCluster().getRegionServer(i).getWAL(regionInfo);
      Path currentWalPath = AbstractFSWALProvider.getCurrentFileName(wal);
      appendEntriesToWal(numOfEntriesToReplicate, wal);
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(currentWalPath.getName());
      Path emptyWalPath = new Path(UTIL1.getDataTestDir(), walGroupId + "." + ts);
      UTIL1.getTestFileSystem().create(emptyWalPath).close();
      emptyWalPaths.add(emptyWalPath);

    }
    injectEmptyWAL(numRs, emptyWalPaths);
    // roll the WAL now
    for (int i = 0; i < numRs; i++) {
      wal.rollWriter();
    }
    hbaseAdmin.enableReplicationPeer(PEER_ID2);
    // ReplicationSource should advance past the empty wal, or else the test will fail
    waitForLogAdvance(numRs);

    // Now we should expect numOfEntriesToReplicate entries
    // replicated from each region server. This makes sure we didn't loose data
    // from any previous batch when we encounter EOF exception for empty file.
    Assert.assertEquals("Replicated entries are not correct", numOfEntriesToReplicate * numRs,
      replicatedEntries.size());

    // We expect just one batch of replication to be shipped which will
    // for non empty WAL
    Assert.assertEquals("Replicated batches are not correct", 1, replicateCount.get());
    verifyNumberOfLogsInQueue(1, numRs);
    // we're now writing to the new wal
    // if everything works, the source should've stopped reading from the empty wal, and start
    // replicating from the new wal
    runSimplePutDeleteTest();
    rollWalsAndWaitForDeque(numRs);
  }

  /**
   * This test make sure we replicate all the enties from the non empty WALs which are surrounding
   * the empty WALs
   * @throws Exception throws exception
   */
  @Test
  public void testReplicationOfEmptyWALSurroundedNonEmptyWAL() throws Exception {
    // Disable the replication peer to accumulate the non empty WAL followed by empty WAL
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    int numOfEntriesToReplicate = 20;

    final int numRs = UTIL1.getHBaseCluster().getRegionServerThreads().size();
    // for each RS, create an empty wal with same walGroupId
    final List<Path> emptyWalPaths = new ArrayList<>();

    long ts = System.currentTimeMillis();
    WAL wal = null;
    for (int i = 0; i < numRs; i++) {
      RegionInfo regionInfo =
        UTIL1.getHBaseCluster().getRegions(tableName.getName()).get(0).getRegionInfo();
      wal = UTIL1.getHBaseCluster().getRegionServer(i).getWAL(regionInfo);
      Path currentWalPath = AbstractFSWALProvider.getCurrentFileName(wal);
      appendEntriesToWal(numOfEntriesToReplicate, wal);
      wal.rollWriter();
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(currentWalPath.getName());
      Path emptyWalPath = new Path(UTIL1.getDataTestDir(), walGroupId + "." + ts);
      UTIL1.getTestFileSystem().create(emptyWalPath).close();
      emptyWalPaths.add(emptyWalPath);
    }
    injectEmptyWAL(numRs, emptyWalPaths);

    // roll the WAL again with some entries
    for (int i = 0; i < numRs; i++) {
      appendEntriesToWal(numOfEntriesToReplicate, wal);
      wal.rollWriter();
    }

    hbaseAdmin.enableReplicationPeer(PEER_ID2);
    // ReplicationSource should advance past the empty wal, or else the test will fail
    waitForLogAdvance(numRs);

    // Now we should expect numOfEntriesToReplicate entries
    // replicated from each region server. This makes sure we didn't loose data
    // from any previous batch when we encounter EOF exception for empty file.
    Assert.assertEquals("Replicated entries are not correct", numOfEntriesToReplicate * numRs * 2,
      replicatedEntries.size());

    // We expect two batch of replication to be shipped which will
    // for non empty WAL
    Assert.assertEquals("Replicated batches are not correct", 2, replicateCount.get());
    verifyNumberOfLogsInQueue(1, numRs);
    // we're now writing to the new wal
    // if everything works, the source should've stopped reading from the empty wal, and start
    // replicating from the new wal
    runSimplePutDeleteTest();
    rollWalsAndWaitForDeque(numRs);
  }

  // inject our empty wal into the replication queue, and then roll the original wal, which
  // enqueues a new wal behind our empty wal. We must roll the wal here as now we use the WAL to
  // determine if the file being replicated currently is still opened for write, so just inject a
  // new wal to the replication queue does not mean the previous file is closed.
  private void injectEmptyWAL(int numRs, List<Path> emptyWalPaths) throws IOException {
    for (int i = 0; i < numRs; i++) {
      HRegionServer hrs = UTIL1.getHBaseCluster().getRegionServer(i);
      Replication replicationService = (Replication) hrs.getReplicationSourceService();
      replicationService.getReplicationManager().preLogRoll(emptyWalPaths.get(i));
      replicationService.getReplicationManager().postLogRoll(emptyWalPaths.get(i));
      RegionInfo regionInfo =
        UTIL1.getHBaseCluster().getRegions(htable1.getName()).get(0).getRegionInfo();
      WAL wal = hrs.getWAL(regionInfo);
      wal.rollWriter(true);
    }
  }

  protected WALKeyImpl getWalKeyImpl() {
    return new WALKeyImpl(info.getEncodedNameAsBytes(), tableName, 0, mvcc, scopes);
  }

  // Roll the WAL and wait for it to get deque from the log queue
  private void rollWalsAndWaitForDeque(int numRs) throws IOException {
    RegionInfo regionInfo =
      UTIL1.getHBaseCluster().getRegions(tableName.getName()).get(0).getRegionInfo();
    for (int i = 0; i < numRs; i++) {
      WAL wal = UTIL1.getHBaseCluster().getRegionServer(i).getWAL(regionInfo);
      wal.rollWriter();
    }
    waitForLogAdvance(numRs);
  }

  private void appendEntriesToWal(int numEntries, WAL wal) throws IOException {
    long txId = -1;
    for (int i = 0; i < numEntries; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b, famName, b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      txId = wal.appendData(info, getWalKeyImpl(), edit);
    }
    wal.sync(txId);
  }
}
