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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationEmptyWALRecovery extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationEmptyWALRecovery.class);

  @Before
  public void setUp() throws IOException, InterruptedException {
    cleanUp();
  }

  /**
   * Waits until there is only one log(the current writing one) in the replication queue
   * @param numRs number of regionservers
   */
  private void waitForLogAdvance(int numRs) throws Exception {
    Waiter.waitFor(conf1, 10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        for (int i = 0; i < numRs; i++) {
          HRegionServer hrs = utility1.getHBaseCluster().getRegionServer(i);
          RegionInfo regionInfo =
              utility1.getHBaseCluster().getRegions(htable1.getName()).get(0).getRegionInfo();
          WAL wal = hrs.getWAL(regionInfo);
          Path currentFile = ((AbstractFSWAL<?>) wal).getCurrentFileName();
          Replication replicationService = (Replication) utility1.getHBaseCluster()
              .getRegionServer(i).getReplicationSourceService();
          for (ReplicationSourceInterface rsi : replicationService.getReplicationManager()
              .getSources()) {
            ReplicationSource source = (ReplicationSource) rsi;
            if (!currentFile.equals(source.getCurrentPath())) {
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
    final int numRs = utility1.getHBaseCluster().getRegionServerThreads().size();

    // for each RS, create an empty wal with same walGroupId
    final List<Path> emptyWalPaths = new ArrayList<>();
    long ts = System.currentTimeMillis();
    for (int i = 0; i < numRs; i++) {
      RegionInfo regionInfo =
          utility1.getHBaseCluster().getRegions(htable1.getName()).get(0).getRegionInfo();
      WAL wal = utility1.getHBaseCluster().getRegionServer(i).getWAL(regionInfo);
      Path currentWalPath = AbstractFSWALProvider.getCurrentFileName(wal);
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(currentWalPath.getName());
      Path emptyWalPath = new Path(utility1.getDataTestDir(), walGroupId + "." + ts);
      utility1.getTestFileSystem().create(emptyWalPath).close();
      emptyWalPaths.add(emptyWalPath);
    }

    // inject our empty wal into the replication queue, and then roll the original wal, which
    // enqueues a new wal behind our empty wal. We must roll the wal here as now we use the WAL to
    // determine if the file being replicated currently is still opened for write, so just inject a
    // new wal to the replication queue does not mean the previous file is closed.
    for (int i = 0; i < numRs; i++) {
      HRegionServer hrs = utility1.getHBaseCluster().getRegionServer(i);
      Replication replicationService = (Replication) hrs.getReplicationSourceService();
      replicationService.getReplicationManager().preLogRoll(emptyWalPaths.get(i));
      replicationService.getReplicationManager().postLogRoll(emptyWalPaths.get(i));
      RegionInfo regionInfo =
        utility1.getHBaseCluster().getRegions(htable1.getName()).get(0).getRegionInfo();
      WAL wal = hrs.getWAL(regionInfo);
      wal.rollWriter(true);
    }

    // ReplicationSource should advance past the empty wal, or else the test will fail
    waitForLogAdvance(numRs);

    // we're now writing to the new wal
    // if everything works, the source should've stopped reading from the empty wal, and start
    // replicating from the new wal
    runSimplePutDeleteTest();
  }
}
