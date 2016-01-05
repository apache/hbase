/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.replication.regionserver.TestSourceFSConfigurationProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationSyncUpToolWithBulkLoadedData extends TestReplicationSyncUpTool {

  private static final Log LOG = LogFactory
      .getLog(TestReplicationSyncUpToolWithBulkLoadedData.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf1.set(HConstants.REPLICATION_CLUSTER_ID, "12345");
    conf1.set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
    String classes = conf1.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    if (!classes.contains("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint")) {
      classes = classes + ",org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint";
      conf1.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, classes);
    }

    TestReplicationBase.setUpBeforeClass();
  }

  @Override
  public void testSyncUpTool() throws Exception {
    /**
     * Set up Replication: on Master and one Slave Table: t1_syncup and t2_syncup columnfamily:
     * 'cf1' : replicated 'norep': not replicated
     */
    setupReplication();

    /**
     * Prepare 16 random hfile ranges required for creating hfiles
     */
    Iterator<String> randomHFileRangeListIterator = null;
    Set<String> randomHFileRanges = new HashSet<String>(16);
    for (int i = 0; i < 16; i++) {
      randomHFileRanges.add(UUID.randomUUID().toString());
    }
    List<String> randomHFileRangeList = new ArrayList<>(randomHFileRanges);
    Collections.sort(randomHFileRangeList);
    randomHFileRangeListIterator = randomHFileRangeList.iterator();

    /**
     * at Master: t1_syncup: Load 100 rows into cf1, and 3 rows into norep t2_syncup: Load 200 rows
     * into cf1, and 3 rows into norep verify correctly replicated to slave
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
    utility2.shutdownMiniHBaseCluster();

    loadAndReplicateHFiles(false, randomHFileRangeListIterator);

    int rowCount_ht1Source = utility1.countRows(ht1Source);
    assertEquals("t1_syncup has 206 rows on source, after bulk load of another 103 hfiles", 206,
      rowCount_ht1Source);

    int rowCount_ht2Source = utility1.countRows(ht2Source);
    assertEquals("t2_syncup has 406 rows on source, after bulk load of another 203 hfiles", 406,
      rowCount_ht2Source);

    utility1.shutdownMiniHBaseCluster();
    utility2.restartHBaseCluster(1);

    Thread.sleep(SLEEP_TIME);

    // Before sync up
    int rowCount_ht1TargetAtPeer1 = utility2.countRows(ht1TargetAtPeer1);
    int rowCount_ht2TargetAtPeer1 = utility2.countRows(ht2TargetAtPeer1);
    assertEquals("@Peer1 t1_syncup should still have 100 rows", 100, rowCount_ht1TargetAtPeer1);
    assertEquals("@Peer1 t2_syncup should still have 200 rows", 200, rowCount_ht2TargetAtPeer1);

    // Run sync up tool
    syncUp(utility1);

    // After syun up
    for (int i = 0; i < NB_RETRIES; i++) {
      syncUp(utility1);
      rowCount_ht1TargetAtPeer1 = utility2.countRows(ht1TargetAtPeer1);
      rowCount_ht2TargetAtPeer1 = utility2.countRows(ht2TargetAtPeer1);
      if (i == NB_RETRIES - 1) {
        if (rowCount_ht1TargetAtPeer1 != 200 || rowCount_ht2TargetAtPeer1 != 400) {
          // syncUP still failed. Let's look at the source in case anything wrong there
          utility1.restartHBaseCluster(1);
          rowCount_ht1Source = utility1.countRows(ht1Source);
          LOG.debug("t1_syncup should have 206 rows at source, and it is " + rowCount_ht1Source);
          rowCount_ht2Source = utility1.countRows(ht2Source);
          LOG.debug("t2_syncup should have 406 rows at source, and it is " + rowCount_ht2Source);
        }
        assertEquals("@Peer1 t1_syncup should be sync up and have 200 rows", 200,
          rowCount_ht1TargetAtPeer1);
        assertEquals("@Peer1 t2_syncup should be sync up and have 400 rows", 400,
          rowCount_ht2TargetAtPeer1);
      }
      if (rowCount_ht1TargetAtPeer1 == 200 && rowCount_ht2TargetAtPeer1 == 400) {
        LOG.info("SyncUpAfterBulkLoad succeeded at retry = " + i);
        break;
      } else {
        LOG.debug("SyncUpAfterBulkLoad failed at retry = " + i + ", with rowCount_ht1TargetPeer1 ="
            + rowCount_ht1TargetAtPeer1 + " and rowCount_ht2TargetAtPeer1 ="
            + rowCount_ht2TargetAtPeer1);
      }
      Thread.sleep(SLEEP_TIME);
    }
  }

  private void loadAndReplicateHFiles(boolean verifyReplicationOnSlave,
      Iterator<String> randomHFileRangeListIterator) throws Exception {
    LOG.debug("loadAndReplicateHFiles");

    // Load 100 + 3 hfiles to t1_syncup.
    byte[][][] hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, famName, ht1Source, hfileRanges,
      100);

    hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, noRepfamName, ht1Source,
      hfileRanges, 3);

    // Load 200 + 3 hfiles to t2_syncup.
    hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, famName, ht2Source, hfileRanges,
      200);

    hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1", row, noRepfamName, ht2Source,
      hfileRanges, 3);

    if (verifyReplicationOnSlave) {
      // ensure replication completed
      wait(ht1TargetAtPeer1, utility1.countRows(ht1Source) - 3,
        "t1_syncup has 103 rows on source, and 100 on slave1");

      wait(ht2TargetAtPeer1, utility1.countRows(ht2Source) - 3,
        "t2_syncup has 203 rows on source, and 200 on slave1");
    }
  }

  private void loadAndValidateHFileReplication(String testName, byte[] row, byte[] fam,
      Table source, byte[][][] hfileRanges, int numOfRows) throws Exception {
    Path dir = utility1.getDataTestDirOnTestFS(testName);
    FileSystem fs = utility1.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(fam));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(utility1.getConfiguration(), fs, new Path(familyDir, "hfile_"
          + hfileIdx++), fam, row, from, to, numOfRows);
    }

    final TableName tableName = source.getName();
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(utility1.getConfiguration());
    String[] args = { dir.toString(), tableName.toString() };
    loader.run(args);
  }

  private void wait(Table target, int expectedCount, String msg) throws IOException,
      InterruptedException {
    for (int i = 0; i < NB_RETRIES; i++) {
      int rowCount_ht2TargetAtPeer1 = utility2.countRows(target);
      if (i == NB_RETRIES - 1) {
        assertEquals(msg, expectedCount, rowCount_ht2TargetAtPeer1);
      }
      if (expectedCount == rowCount_ht2TargetAtPeer1) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }
  }
}
