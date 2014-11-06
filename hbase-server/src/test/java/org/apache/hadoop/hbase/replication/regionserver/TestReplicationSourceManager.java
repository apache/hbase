/*
 *
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager.NodeFailoverWorker;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationSourceManager {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSourceManager.class);

  private static Configuration conf;

  private static HBaseTestingUtility utility;

  private static Replication replication;

  private static ReplicationSourceManager manager;

  private static ZooKeeperWatcher zkw;

  private static HTableDescriptor htd;

  private static HRegionInfo hri;

  private static final byte[] r1 = Bytes.toBytes("r1");

  private static final byte[] r2 = Bytes.toBytes("r2");

  private static final byte[] f1 = Bytes.toBytes("f1");

  private static final TableName test =
      TableName.valueOf("test");

  private static final String slaveId = "1";

  private static FileSystem fs;

  private static String logName;

  private static Path oldLogDir;

  private static Path logDir;
  
  private static CountDownLatch latch;

  private static List<String> files = new ArrayList<String>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
        ReplicationSourceDummy.class.getCanonicalName());
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT);
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniZKCluster();

    zkw = new ZooKeeperWatcher(conf, "test", null);
    ZKUtil.createWithParents(zkw, "/hbase/replication");
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1",
        Bytes.toBytes(conf.get(HConstants.ZOOKEEPER_QUORUM) + ":"
            + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT) + ":/1"));
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1/peer-state");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1/peer-state",
      ReplicationStateZKBase.ENABLED_ZNODE_BYTES);
    ZKUtil.createWithParents(zkw, "/hbase/replication/state");
    ZKUtil.setData(zkw, "/hbase/replication/state", ReplicationStateZKBase.ENABLED_ZNODE_BYTES);

    ZKClusterId.setClusterId(zkw, new ClusterId());
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getDataTestDir(),
        HConstants.HREGION_LOGDIR_NAME);
    replication = new Replication(new DummyServer(), fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();
    
    logName = HConstants.HREGION_LOGDIR_NAME;

    manager.addSource(slaveId);

    htd = new HTableDescriptor(test);
    HColumnDescriptor col = new HColumnDescriptor("f1");
    col.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    htd.addFamily(col);
    col = new HColumnDescriptor("f2");
    col.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    htd.addFamily(col);

    hri = new HRegionInfo(htd.getTableName(), r1, r2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    manager.join();
    utility.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
  }

  @After
  public void tearDown() throws Exception {
    setUp();
  }

  @Test
  public void testLogRoll() throws Exception {
    long seq = 0;
    long baseline = 1000;
    long time = baseline;
    KeyValue kv = new KeyValue(r1, f1, r1);
    WALEdit edit = new WALEdit();
    edit.add(kv);

    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    listeners.add(replication);
    HLog hlog = HLogFactory.createHLog(fs, utility.getDataTestDir(), logName,
        conf, listeners, URLEncoder.encode("regionserver:60020", "UTF8"));
    final AtomicLong sequenceId = new AtomicLong(1);
    manager.init();
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor(f1));
    // Testing normal log rolling every 20
    for(long i = 1; i < 101; i++) {
      if(i > 1 && i % 20 == 0) {
        hlog.rollWriter();
      }
      LOG.info(i);
      HLogKey key = new HLogKey(hri.getRegionName(), test, seq++,
          System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
      hlog.append(hri, test, edit, System.currentTimeMillis(), htd, sequenceId);
    }

    // Simulate a rapid insert that's followed
    // by a report that's still not totally complete (missing last one)
    LOG.info(baseline + " and " + time);
    baseline += 101;
    time = baseline;
    LOG.info(baseline + " and " + time);

    for (int i = 0; i < 3; i++) {
      hlog.append(hri, test, edit, System.currentTimeMillis(), htd, sequenceId);
    }

    assertEquals(6, manager.getHLogs().get(slaveId).size());

    hlog.rollWriter();

    manager.logPositionAndCleanOldLogs(manager.getSources().get(0).getCurrentPath(),
        "1", 0, false, false);

    hlog.append(hri, test, edit, System.currentTimeMillis(), htd, sequenceId);

    assertEquals(1, manager.getHLogs().size());


    // TODO Need a case with only 2 HLogs and we only want to delete the first one
  }
  
  @Test
  public void testClaimQueues() throws Exception {
    LOG.debug("testNodeFailoverWorkerCopyQueuesFromRSUsingMulti");
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    final Server server = new DummyServer("hostname0.example.org");
    ReplicationQueues rq =
        ReplicationFactory.getReplicationQueues(server.getZooKeeper(), server.getConfiguration(),
          server);
    rq.init(server.getServerName().toString());
    // populate some znodes in the peer znode
    files.add("log1");
    files.add("log2");
    for (String file : files) {
      rq.addLog("1", file);
    }
    // create 3 DummyServers
    Server s1 = new DummyServer("dummyserver1.example.org");
    Server s2 = new DummyServer("dummyserver2.example.org");
    Server s3 = new DummyServer("dummyserver3.example.org");

    // create 3 DummyNodeFailoverWorkers
    DummyNodeFailoverWorker w1 = new DummyNodeFailoverWorker(
        server.getServerName().getServerName(), s1);
    DummyNodeFailoverWorker w2 = new DummyNodeFailoverWorker(
        server.getServerName().getServerName(), s2);
    DummyNodeFailoverWorker w3 = new DummyNodeFailoverWorker(
        server.getServerName().getServerName(), s3);

    latch = new CountDownLatch(3);
    // start the threads
    w1.start();
    w2.start();
    w3.start();
    // make sure only one is successful
    int populatedMap = 0;
    // wait for result now... till all the workers are done.
    latch.await();
    populatedMap += w1.isLogZnodesMapPopulated() + w2.isLogZnodesMapPopulated()
        + w3.isLogZnodesMapPopulated();
    assertEquals(1, populatedMap);
    server.abort("", null);
  }
  
  @Test
  public void testCleanupFailoverQueues() throws Exception {
    final Server server = new DummyServer("hostname1.example.org");
    ReplicationQueues rq =
        ReplicationFactory.getReplicationQueues(server.getZooKeeper(), server.getConfiguration(),
          server);
    rq.init(server.getServerName().toString());
    // populate some znodes in the peer znode
    SortedSet<String> files = new TreeSet<String>();
    files.add("log1");
    files.add("log2");
    for (String file : files) {
      rq.addLog("1", file);
    }
    Server s1 = new DummyServer("dummyserver1.example.org");
    ReplicationQueues rq1 =
        ReplicationFactory.getReplicationQueues(s1.getZooKeeper(), s1.getConfiguration(), s1);
    rq1.init(s1.getServerName().toString());
    ReplicationPeers rp1 =
        ReplicationFactory.getReplicationPeers(s1.getZooKeeper(), s1.getConfiguration(), s1);
    rp1.init();
    NodeFailoverWorker w1 =
        manager.new NodeFailoverWorker(server.getServerName().getServerName(), rq1, rp1, new UUID(
            new Long(1), new Long(2)));
    w1.start();
    w1.join(5000);
    assertEquals(1, manager.getHlogsByIdRecoveredQueues().size());
    String id = "1-" + server.getServerName().getServerName();
    assertEquals(files, manager.getHlogsByIdRecoveredQueues().get(id));
    manager.cleanOldLogs("log2", id, true);
    // log1 should be deleted
    assertEquals(Sets.newHashSet("log2"), manager.getHlogsByIdRecoveredQueues().get(id));
  }

  @Test
  public void testNodeFailoverDeadServerParsing() throws Exception {
    LOG.debug("testNodeFailoverDeadServerParsing");
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    final Server server = new DummyServer("ec2-54-234-230-108.compute-1.amazonaws.com");
    ReplicationQueues repQueues =
        ReplicationFactory.getReplicationQueues(server.getZooKeeper(), conf, server);
    repQueues.init(server.getServerName().toString());
    // populate some znodes in the peer znode
    files.add("log1");
    files.add("log2");
    for (String file : files) {
      repQueues.addLog("1", file);
    }

    // create 3 DummyServers
    Server s1 = new DummyServer("ip-10-8-101-114.ec2.internal");
    Server s2 = new DummyServer("ec2-107-20-52-47.compute-1.amazonaws.com");
    Server s3 = new DummyServer("ec2-23-20-187-167.compute-1.amazonaws.com");

    // simulate three servers fail sequentially
    ReplicationQueues rq1 =
        ReplicationFactory.getReplicationQueues(s1.getZooKeeper(), s1.getConfiguration(), s1);
    rq1.init(s1.getServerName().toString());
    SortedMap<String, SortedSet<String>> testMap =
        rq1.claimQueues(server.getServerName().getServerName());
    ReplicationQueues rq2 =
        ReplicationFactory.getReplicationQueues(s2.getZooKeeper(), s2.getConfiguration(), s2);
    rq2.init(s2.getServerName().toString());
    testMap = rq2.claimQueues(s1.getServerName().getServerName());
    ReplicationQueues rq3 =
        ReplicationFactory.getReplicationQueues(s3.getZooKeeper(), s3.getConfiguration(), s3);
    rq3.init(s3.getServerName().toString());
    testMap = rq3.claimQueues(s2.getServerName().getServerName());

    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(testMap.firstKey());
    List<String> result = replicationQueueInfo.getDeadRegionServers();

    // verify
    assertTrue(result.contains(server.getServerName().getServerName()));
    assertTrue(result.contains(s1.getServerName().getServerName()));
    assertTrue(result.contains(s2.getServerName().getServerName()));

    server.abort("", null);
  }
  
  
  static class DummyNodeFailoverWorker extends Thread {
    private SortedMap<String, SortedSet<String>> logZnodesMap;
    Server server;
    private String deadRsZnode;
    ReplicationQueues rq;

    public DummyNodeFailoverWorker(String znode, Server s) throws Exception {
      this.deadRsZnode = znode;
      this.server = s;
      this.rq =
          ReplicationFactory.getReplicationQueues(server.getZooKeeper(), server.getConfiguration(),
            server);
      this.rq.init(this.server.getServerName().toString());
    }

    @Override
    public void run() {
      try {
        logZnodesMap = rq.claimQueues(deadRsZnode);
        server.abort("Done with testing", null);
      } catch (Exception e) {
        LOG.error("Got exception while running NodeFailoverWorker", e);
      } finally {
        latch.countDown();
      }
    }

    /**
     * @return 1 when the map is not empty.
     */
    private int isLogZnodesMapPopulated() {
      Collection<SortedSet<String>> sets = logZnodesMap.values();
      if (sets.size() > 1) {
        throw new RuntimeException("unexpected size of logZnodesMap: " + sets.size());
      }
      if (sets.size() == 1) {
        SortedSet<String> s = sets.iterator().next();
        for (String file : files) {
          // at least one file was missing
          if (!s.contains(file)) {
            return 0;
          }
        }
        return 1; // we found all the files
      }
      return 0;
    }
  }
  
  static class DummyServer implements Server {
    String hostname;

    DummyServer() {
      hostname = "hostname.example.org";
    }

    DummyServer(String hostname) {
      this.hostname = hostname;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }
    @Override
    public HConnection getShortCircuitConnection() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(hostname, 1234, 1L);
    }

    @Override
    public void abort(String why, Throwable e) {
      // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
      // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isStopped() {
      return false; // To change body of implemented methods use File | Settings | File Templates.
    }
  }

}

