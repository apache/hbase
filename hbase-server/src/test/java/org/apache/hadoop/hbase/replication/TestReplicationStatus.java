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

import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationStatus extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationStatus.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationStatus.class);
  private static final String PEER_ID = "2";

  /**
   * Test for HBASE-9531
   * put a few rows into htable1, which should be replicated to htable2
   * create a ClusterStatus instance 'status' from HBaseAdmin
   * test : status.getLoad(server).getReplicationLoadSourceList()
   * test : status.getLoad(server).getReplicationLoadSink()
   * * @throws Exception
   */
  @Test
  public void testReplicationStatus() throws Exception {
    LOG.info("testReplicationStatus");
    utility2.shutdownMiniHBaseCluster();
    utility2.startMiniHBaseCluster(1,4);
    try (Admin hbaseAdmin = utility1.getConnection().getAdmin()) {
      // disable peer
      admin.disablePeer(PEER_ID);

      final byte[] qualName = Bytes.toBytes("q");
      Put p;

      for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
        p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        htable1.put(p);
      }

      ClusterStatus status = new ClusterStatus(hbaseAdmin.getClusterMetrics(
        EnumSet.of(Option.LIVE_SERVERS)));

      for (JVMClusterUtil.RegionServerThread thread : utility1.getHBaseCluster()
          .getRegionServerThreads()) {
        ServerName server = thread.getRegionServer().getServerName();
        ServerLoad sl = status.getLoad(server);
        List<ReplicationLoadSource> rLoadSourceList = sl.getReplicationLoadSourceList();
        ReplicationLoadSink rLoadSink = sl.getReplicationLoadSink();

        // check SourceList only has one entry, beacuse only has one peer
        assertTrue("failed to get ReplicationLoadSourceList", (rLoadSourceList.size() == 1));
        assertEquals(PEER_ID, rLoadSourceList.get(0).getPeerID());

        // check Sink exist only as it is difficult to verify the value on the fly
        assertTrue("failed to get ReplicationLoadSink.AgeOfLastShippedOp ",
          (rLoadSink.getAgeOfLastAppliedOp() >= 0));
        assertTrue("failed to get ReplicationLoadSink.TimeStampsOfLastAppliedOp ",
          (rLoadSink.getTimestampsOfLastAppliedOp() >= 0));
      }

      // Stop rs1, then the queue of rs1 will be transfered to rs0
      utility1.getHBaseCluster().getRegionServer(1).stop("Stop RegionServer");
      Thread.sleep(10000);
      status = new ClusterStatus(hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
      ServerName server = utility1.getHBaseCluster().getRegionServer(0).getServerName();
      ServerLoad sl = status.getLoad(server);
      List<ReplicationLoadSource> rLoadSourceList = sl.getReplicationLoadSourceList();
      // check SourceList still only has one entry
      assertTrue("failed to get ReplicationLoadSourceList", (rLoadSourceList.size() == 2));
      assertEquals(PEER_ID, rLoadSourceList.get(0).getPeerID());
    } finally {
      admin.enablePeer(PEER_ID);
      utility1.getHBaseCluster().getRegionServer(1).start();
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    //we need to perform initialisations from TestReplicationBase.setUpBeforeClass() on each
    //test here, so we override BeforeClass to do nothing and call
    // TestReplicationBase.setUpBeforeClass() from setup method
    TestReplicationBase.configureClusters();
  }

  @Before
  @Override
  public void setUpBase() throws Exception {
    TestReplicationBase.startClusters();
    super.setUpBase();
  }

  @After
  @Override
  public void tearDownBase() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass(){
    //We need to override it here to avoid issues when trying to execute super class teardown
  }

  @Test
  public void testReplicationStatusSourceStartedTargetStoppedNoOps() throws Exception {
    utility2.shutdownMiniHBaseCluster();
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    Admin hbaseAdmin = utility1.getConnection().getAdmin();
    ServerName serverName = utility1.getHBaseCluster().
        getRegionServer(0).getServerName();
    Thread.sleep(10000);
    ClusterStatus status = new ClusterStatus(hbaseAdmin.
        getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
    List<ReplicationLoadSource> loadSources = status.getLiveServerMetrics().
        get(serverName).getReplicationLoadSourceList();
    assertEquals(1, loadSources.size());
    ReplicationLoadSource loadSource = loadSources.get(0);
    assertFalse(loadSource.hasEditsSinceRestart());
    assertEquals(0, loadSource.getTimestampOfLastShippedOp());
    assertEquals(0, loadSource.getReplicationLag());
    assertFalse(loadSource.isRecovered());
  }

  @Test
  public void testReplicationStatusSourceStartedTargetStoppedNewOp() throws Exception {
    utility2.shutdownMiniHBaseCluster();
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    Admin hbaseAdmin = utility1.getConnection().getAdmin();
    //add some values to source cluster
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, Bytes.toBytes("col1"), Bytes.toBytes("val" + i));
      htable1.put(p);
    }
    Thread.sleep(10000);
    ServerName serverName = utility1.getHBaseCluster().
        getRegionServer(0).getServerName();
    ClusterStatus status = new ClusterStatus(hbaseAdmin.
        getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
    List<ReplicationLoadSource> loadSources = status.getLiveServerMetrics().
        get(serverName).getReplicationLoadSourceList();
    assertEquals(1, loadSources.size());
    ReplicationLoadSource loadSource = loadSources.get(0);
    assertTrue(loadSource.hasEditsSinceRestart());
    assertEquals(0, loadSource.getTimestampOfLastShippedOp());
    assertTrue(loadSource.getReplicationLag()>0);
    assertFalse(loadSource.isRecovered());
  }

  @Test
  public void testReplicationStatusSourceStartedTargetStoppedWithRecovery() throws Exception {
    utility2.shutdownMiniHBaseCluster();
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    //add some values to cluster 1
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, Bytes.toBytes("col1"), Bytes.toBytes("val" + i));
      htable1.put(p);
    }
    Thread.sleep(10000);
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    Admin hbaseAdmin = utility1.getConnection().getAdmin();
    ServerName serverName = utility1.getHBaseCluster().
        getRegionServer(0).getServerName();
    Thread.sleep(10000);
    ClusterStatus status = new ClusterStatus(hbaseAdmin.
        getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
    List<ReplicationLoadSource> loadSources = status.getLiveServerMetrics().
        get(serverName).getReplicationLoadSourceList();
    assertEquals(2, loadSources.size());
    boolean foundRecovery = false;
    boolean foundNormal = false;
    for(ReplicationLoadSource loadSource : loadSources){
      if (loadSource.isRecovered()){
        foundRecovery = true;
        assertTrue(loadSource.hasEditsSinceRestart());
        assertEquals(0, loadSource.getTimestampOfLastShippedOp());
        assertTrue(loadSource.getReplicationLag()>0);
      } else {
        foundNormal = true;
        assertFalse(loadSource.hasEditsSinceRestart());
        assertEquals(0, loadSource.getTimestampOfLastShippedOp());
        assertEquals(0, loadSource.getReplicationLag());
      }
    }
    assertTrue("No normal queue found.", foundNormal);
    assertTrue("No recovery queue found.", foundRecovery);
  }

  @Test
  public void testReplicationStatusBothNormalAndRecoveryLagging() throws Exception {
    utility2.shutdownMiniHBaseCluster();
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    //add some values to cluster 1
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, Bytes.toBytes("col1"), Bytes.toBytes("val" + i));
      htable1.put(p);
    }
    Thread.sleep(10000);
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    Admin hbaseAdmin = utility1.getConnection().getAdmin();
    ServerName serverName = utility1.getHBaseCluster().
        getRegionServer(0).getServerName();
    Thread.sleep(10000);
    //add more values to cluster 1, these should cause normal queue to lag
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, Bytes.toBytes("col1"), Bytes.toBytes("val" + i));
      htable1.put(p);
    }
    Thread.sleep(10000);
    ClusterStatus status = new ClusterStatus(hbaseAdmin.
        getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
    List<ReplicationLoadSource> loadSources = status.getLiveServerMetrics().
        get(serverName).getReplicationLoadSourceList();
    assertEquals(2, loadSources.size());
    boolean foundRecovery = false;
    boolean foundNormal = false;
    for(ReplicationLoadSource loadSource : loadSources){
      if (loadSource.isRecovered()){
        foundRecovery = true;
      } else {
        foundNormal = true;
      }
      assertTrue(loadSource.hasEditsSinceRestart());
      assertEquals(0, loadSource.getTimestampOfLastShippedOp());
      assertTrue(loadSource.getReplicationLag()>0);
    }
    assertTrue("No normal queue found.", foundNormal);
    assertTrue("No recovery queue found.", foundRecovery);
  }

  @Test
  public void testReplicationStatusAfterLagging() throws Exception {
    utility2.shutdownMiniHBaseCluster();
    utility1.shutdownMiniHBaseCluster();
    utility1.startMiniHBaseCluster();
    //add some values to cluster 1
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(famName, Bytes.toBytes("col1"), Bytes.toBytes("val" + i));
      htable1.put(p);
    }
    utility2.startMiniHBaseCluster();
    Thread.sleep(10000);
    try(Admin hbaseAdmin = utility1.getConnection().getAdmin()) {
      ServerName serverName = utility1.getHBaseCluster().getRegionServer(0).
          getServerName();
      ClusterStatus status =
          new ClusterStatus(hbaseAdmin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
      List<ReplicationLoadSource> loadSources = status.getLiveServerMetrics().get(serverName).
          getReplicationLoadSourceList();
      assertEquals(1, loadSources.size());
      ReplicationLoadSource loadSource = loadSources.get(0);
      assertTrue(loadSource.hasEditsSinceRestart());
      assertTrue(loadSource.getTimestampOfLastShippedOp() > 0);
      assertEquals(0, loadSource.getReplicationLag());
    }finally{
      utility2.shutdownMiniHBaseCluster();
    }
  }
}
