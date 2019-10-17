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

import static org.apache.hadoop.hbase.HBaseTestingUtility.countRows;
import static org.apache.hadoop.hbase.replication.TestReplicationBase.NB_RETRIES;
import static org.apache.hadoop.hbase.replication.TestReplicationBase.SLEEP_TIME;
import static org.apache.hadoop.hbase.replication.TestReplicationBase.row;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.replication.regionserver.TestSourceFSConfigurationProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationSyncUpToolWithBulkLoadedData extends TestReplicationSyncUpToolBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationSyncUpToolWithBulkLoadedData.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReplicationSyncUpToolWithBulkLoadedData.class);

  @Override
  protected void customizeClusterConf(Configuration conf) {
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf.set(HConstants.REPLICATION_CLUSTER_ID, "12345");
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    conf.set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
  }

  @Test
  public void testSyncUpTool() throws Exception {
    /**
     * Set up Replication: on Master and one Slave Table: t1_syncup and t2_syncup columnfamily:
     * 'cf1' : replicated 'norep': not replicated
     */
    setupReplication();

    /**
     * Prepare 24 random hfile ranges required for creating hfiles
     */
    Iterator<String> randomHFileRangeListIterator = null;
    Set<String> randomHFileRanges = new HashSet<>(24);
    for (int i = 0; i < 24; i++) {
      randomHFileRanges.add(UTIL1.getRandomUUID().toString());
    }
    List<String> randomHFileRangeList = new ArrayList<>(randomHFileRanges);
    Collections.sort(randomHFileRangeList);
    randomHFileRangeListIterator = randomHFileRangeList.iterator();

    /**
     * at Master: t1_syncup: Load 50 rows into cf1, and 50 rows from other hdfs into cf1, and 3
     * rows into norep t2_syncup: Load 100 rows into cf1, and 100 rows from other hdfs into cf1,
     * and 3 rows into norep verify correctly replicated to slave
     */
    loadAndReplicateHFiles(true, randomHFileRangeListIterator);

    /**
     * Verify hfile load works step 1: stop hbase on Slave step 2: at Master: t1_syncup: Load
     * another 100 rows into cf1 and 3 rows into norep t2_syncup: Load another 200 rows into cf1 and
     * 3 rows into norep step 3: stop hbase on master, restart hbase on Slave step 4: verify Slave
     * still has the rows before load t1_syncup: 100 rows from cf1 t2_syncup: 200 rows from cf1 step
     * 5: run syncup tool on Master step 6: verify that hfiles show up on Slave and 'norep' does not
     * t1_syncup: 200 rows from cf1 t2_syncup: 400 rows from cf1 verify correctly replicated to
     * Slave
     */
    mimicSyncUpAfterBulkLoad(randomHFileRangeListIterator);

  }

  private void mimicSyncUpAfterBulkLoad(Iterator<String> randomHFileRangeListIterator)
      throws Exception {
    LOG.debug("mimicSyncUpAfterBulkLoad");
    UTIL2.shutdownMiniHBaseCluster();

    loadAndReplicateHFiles(false, randomHFileRangeListIterator);

    int rowCount_ht1Source = countRows(ht1Source);
    assertEquals("t1_syncup has 206 rows on source, after bulk load of another 103 hfiles", 206,
      rowCount_ht1Source);

    int rowCount_ht2Source = countRows(ht2Source);
    assertEquals("t2_syncup has 406 rows on source, after bulk load of another 203 hfiles", 406,
      rowCount_ht2Source);

    UTIL1.shutdownMiniHBaseCluster();
    UTIL2.restartHBaseCluster(1);

    Thread.sleep(SLEEP_TIME);

    // Before sync up
    int rowCountHt1TargetAtPeer1 = countRows(ht1TargetAtPeer1);
    int rowCountHt2TargetAtPeer1 = countRows(ht2TargetAtPeer1);
    assertEquals("@Peer1 t1_syncup should still have 100 rows", 100, rowCountHt1TargetAtPeer1);
    assertEquals("@Peer1 t2_syncup should still have 200 rows", 200, rowCountHt2TargetAtPeer1);

    // Run sync up tool
    syncUp(UTIL1);

    // After syun up
    for (int i = 0; i < NB_RETRIES; i++) {
      syncUp(UTIL1);
      rowCountHt1TargetAtPeer1 = countRows(ht1TargetAtPeer1);
      rowCountHt2TargetAtPeer1 = countRows(ht2TargetAtPeer1);
      if (i == NB_RETRIES - 1) {
        if (rowCountHt1TargetAtPeer1 != 200 || rowCountHt2TargetAtPeer1 != 400) {
          // syncUP still failed. Let's look at the source in case anything wrong there
          UTIL1.restartHBaseCluster(1);
          rowCount_ht1Source = countRows(ht1Source);
          LOG.debug("t1_syncup should have 206 rows at source, and it is " + rowCount_ht1Source);
          rowCount_ht2Source = countRows(ht2Source);
          LOG.debug("t2_syncup should have 406 rows at source, and it is " + rowCount_ht2Source);
        }
        assertEquals("@Peer1 t1_syncup should be sync up and have 200 rows", 200,
          rowCountHt1TargetAtPeer1);
        assertEquals("@Peer1 t2_syncup should be sync up and have 400 rows", 400,
          rowCountHt2TargetAtPeer1);
      }
      if (rowCountHt1TargetAtPeer1 == 200 && rowCountHt2TargetAtPeer1 == 400) {
        LOG.info("SyncUpAfterBulkLoad succeeded at retry = " + i);
        break;
      } else {
        LOG.debug("SyncUpAfterBulkLoad failed at retry = " + i +
          ", with rowCount_ht1TargetPeer1 =" + rowCountHt1TargetAtPeer1 +
          " and rowCount_ht2TargetAtPeer1 =" + rowCountHt2TargetAtPeer1);
      }
      Thread.sleep(SLEEP_TIME);
    }
  }

  private void loadAndReplicateHFiles(boolean verifyReplicationOnSlave,
      Iterator<String> randomHFileRangeListIterator) throws Exception {
    LOG.debug("loadAndReplicateHFiles");

    // Load 50 + 50 + 3 hfiles to t1_syncup.
    byte[][][] hfileRanges =
      new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
        Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, FAMILY, ht1Source, hfileRanges, 50);

    hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadFromOtherHDFSAndValidateHFileReplication("HFileReplication_1", row, FAMILY, ht1Source,
        hfileRanges, 50);

    hfileRanges =
      new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
        Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, NO_REP_FAMILY, ht1Source,
      hfileRanges, 3);

    // Load 100 + 100 + 3 hfiles to t2_syncup.
    hfileRanges =
      new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
        Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, FAMILY, ht2Source, hfileRanges, 100);

    hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadFromOtherHDFSAndValidateHFileReplication("HFileReplication_1", row, FAMILY, ht2Source,
        hfileRanges, 100);

    hfileRanges =
      new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
        Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, NO_REP_FAMILY, ht2Source,
      hfileRanges, 3);

    if (verifyReplicationOnSlave) {
      // ensure replication completed
      wait(ht1TargetAtPeer1, countRows(ht1Source) - 3,
        "t1_syncup has 103 rows on source, and 100 on slave1");

      wait(ht2TargetAtPeer1, countRows(ht2Source) - 3,
        "t2_syncup has 203 rows on source, and 200 on slave1");
    }
  }

  private void loadAndValidateHFileReplication(String testName, byte[] row, byte[] fam,
      Table source, byte[][][] hfileRanges, int numOfRows) throws Exception {
    Path dir = UTIL1.getDataTestDirOnTestFS(testName);
    FileSystem fs = UTIL1.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(fam));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(UTIL1.getConfiguration(), fs,
        new Path(familyDir, "hfile_" + hfileIdx++), fam, row, from, to, numOfRows);
    }

    final TableName tableName = source.getName();
    BulkLoadHFiles loader = BulkLoadHFiles.create(UTIL1.getConfiguration());
    loader.bulkLoad(tableName, dir);
  }

  private void loadFromOtherHDFSAndValidateHFileReplication(String testName, byte[] row, byte[] fam,
      Table source, byte[][][] hfileRanges, int numOfRows) throws Exception {
    Path dir = UTIL2.getDataTestDirOnTestFS(testName);
    FileSystem fs = UTIL2.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(fam));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(UTIL2.getConfiguration(), fs,
          new Path(familyDir, "hfile_" + hfileIdx++), fam, row, from, to, numOfRows);
    }

    final TableName tableName = source.getName();
    BulkLoadHFiles loader = BulkLoadHFiles.create(UTIL1.getConfiguration());
    loader.bulkLoad(tableName, dir);
  }

  private void wait(Table target, int expectedCount, String msg)
      throws IOException, InterruptedException {
    for (int i = 0; i < NB_RETRIES; i++) {
      int rowCountHt2TargetAtPeer1 = countRows(target);
      if (i == NB_RETRIES - 1) {
        assertEquals(msg, expectedCount, rowCountHt2TargetAtPeer1);
      }
      if (expectedCount == rowCountHt2TargetAtPeer1) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }
  }
}
