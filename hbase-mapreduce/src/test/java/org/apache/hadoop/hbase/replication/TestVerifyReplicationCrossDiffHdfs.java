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
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestVerifyReplicationCrossDiffHdfs {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVerifyReplicationCrossDiffHdfs.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestVerifyReplicationCrossDiffHdfs.class);

  private static HBaseTestingUtility util1;
  private static HBaseTestingUtility util2;
  private static HBaseTestingUtility mapReduceUtil = new HBaseTestingUtility();

  private static Configuration conf1 = HBaseConfiguration.create();
  private static Configuration conf2;

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final String PEER_ID = "1";
  private static final TableName TABLE_NAME = TableName.valueOf("testVerifyRepCrossDiffHDFS");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    util1 = new HBaseTestingUtility(conf1);
    util1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = util1.getZkCluster();
    conf1 = util1.getConfiguration();

    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    util2 = new HBaseTestingUtility(conf2);
    util2.setZkCluster(miniZK);

    util1.startMiniCluster();
    util2.startMiniCluster();

    createTestingTable(util1.getAdmin());
    createTestingTable(util2.getAdmin());
    addTestingPeer();

    LOG.info("Start to load some data to source cluster.");
    loadSomeData();

    LOG.info("Start mini MapReduce cluster.");
    mapReduceUtil.setZkCluster(miniZK);
    mapReduceUtil.startMiniMapReduceCluster();
  }

  private static void createTestingTable(Admin admin) throws IOException {
    TableDescriptor table = TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(100)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .build();
    admin.createTable(table);
  }

  private static void addTestingPeer() throws IOException {
    ReplicationPeerConfig rpc = ReplicationPeerConfig.newBuilder()
        .setClusterKey(util2.getClusterKey()).setReplicateAllUserTables(false)
        .setTableCFsMap(ImmutableMap.of(TABLE_NAME, ImmutableList.of())).build();
    util1.getAdmin().addReplicationPeer(PEER_ID, rpc);
  }

  private static void loadSomeData() throws IOException, InterruptedException {
    int numOfRows = 10;
    try (Table table = util1.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < numOfRows; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i)));
      }
    }
    // Wait some time until the peer received those rows.
    Result[] results = null;
    try (Table table = util2.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < 100; i++) {
        try (ResultScanner rs = table.getScanner(new Scan())) {
          results = rs.next(numOfRows);
          if (results == null || results.length < numOfRows) {
            LOG.info("Retrying, wait until the peer received all the rows, currentRows:"
                + (results == null ? 0 : results.length));
            Thread.sleep(100);
          }
        }
      }
    }
    Assert.assertNotNull(results);
    Assert.assertEquals(10, results.length);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (mapReduceUtil != null) {
      mapReduceUtil.shutdownMiniCluster();
    }
    if (util2 != null) {
      util2.shutdownMiniCluster();
    }
    if (util1 != null) {
      util1.shutdownMiniCluster();
    }
  }

  @Test
  public void testVerifyRepBySnapshot() throws Exception {
    Path rootDir = CommonFSUtils.getRootDir(conf1);
    FileSystem fs = rootDir.getFileSystem(conf1);
    String sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(util1.getAdmin(), TABLE_NAME, new String(FAMILY),
      sourceSnapshotName, rootDir, fs, true);

    // Take target snapshot
    Path peerRootDir = CommonFSUtils.getRootDir(conf2);
    FileSystem peerFs = peerRootDir.getFileSystem(conf2);
    String peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(util2.getAdmin(), TABLE_NAME, new String(FAMILY),
      peerSnapshotName, peerRootDir, peerFs, true);

    String peerFSAddress = peerFs.getUri().toString();
    String temPath1 = new Path(fs.getUri().toString(), "/tmp1").toString();
    String temPath2 = "/tmp2";

    String[] args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
      "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
      "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
      "--peerHBaseRootAddress=" + CommonFSUtils.getRootDir(conf2), PEER_ID, TABLE_NAME.toString() };

    // Use the yarn's config override the source cluster's config.
    Configuration newConf = HBaseConfiguration.create(conf1);
    HBaseConfiguration.merge(newConf, mapReduceUtil.getConfiguration());
    newConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    CommonFSUtils.setRootDir(newConf, CommonFSUtils.getRootDir(conf1));
    Job job = new VerifyReplication().createSubmittableJob(newConf, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(10,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
  }
}
