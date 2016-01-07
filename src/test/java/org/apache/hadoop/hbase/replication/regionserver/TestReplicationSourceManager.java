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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
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

  private static final byte[] test = Bytes.toBytes("test");

  private static final String slaveId = "1";

  private static FileSystem fs;

  private static Path oldLogDir;

  private static Path logDir;
  
  private static CountDownLatch latch;

  private static List<String> files = new ArrayList<String>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
        ReplicationSourceDummy.class.getCanonicalName());
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
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
        Bytes.toBytes(ReplicationZookeeper.PeerState.ENABLED.name()));
    ZKUtil.createWithParents(zkw, "/hbase/replication/state");
    ZKUtil.setData(zkw, "/hbase/replication/state", Bytes.toBytes("true"));

    replication = new Replication(new DummyServer(), fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getDataTestDir(),
        HConstants.HREGION_LOGDIR_NAME);

    manager.addSource(slaveId);

    htd = new HTableDescriptor(test);
    HColumnDescriptor col = new HColumnDescriptor("f1");
    col.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    htd.addFamily(col);
    col = new HColumnDescriptor("f2");
    col.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    htd.addFamily(col);

    hri = new HRegionInfo(htd.getName(), r1, r2);


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
    HLog hlog = new HLog(fs, logDir, oldLogDir, conf, listeners,
      URLEncoder.encode("regionserver:60020", "UTF8"));

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
      hlog.append(hri, key, edit, htd, true);
    }

    // Simulate a rapid insert that's followed
    // by a report that's still not totally complete (missing last one)
    LOG.info(baseline + " and " + time);
    baseline += 101;
    time = baseline;
    LOG.info(baseline + " and " + time);

    for (int i = 0; i < 3; i++) {
      HLogKey key = new HLogKey(hri.getRegionName(), test, seq++,
          System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
      hlog.append(hri, key, edit, htd, true);
    }

    assertEquals(6, manager.getHLogs().get(slaveId).size());

    hlog.rollWriter();

    manager.logPositionAndCleanOldLogs(manager.getSources().get(0).getCurrentPath(),
        "1", 0, false, false);

    HLogKey key = new HLogKey(hri.getRegionName(), test, seq++,
        System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
    hlog.append(hri, key, edit, htd, true);

    assertEquals(1, manager.getHLogs().size());


    // TODO Need a case with only 2 HLogs and we only want to delete the first one
  }
  
  @Test
  public void testNodeFailoverWorkerCopyQueuesFromRSUsingMulti() throws Exception {
    LOG.debug("testNodeFailoverWorkerCopyQueuesFromRSUsingMulti");
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    final Server server = new DummyServer("hostname0.example.org");
    AtomicBoolean replicating = new AtomicBoolean(true);
    ReplicationZookeeper rz = new ReplicationZookeeper(server, replicating);
    // populate some znodes in the peer znode
    files.add("log1");
    files.add("log2");
    for (String file : files) {
      rz.addLogToList(file, "1");
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
    // close out the resources.
    server.abort("", null);
  }

  @Test
  public void testNodeFailoverDeadServerParsing() throws Exception {
    LOG.debug("testNodeFailoverDeadServerParsing");
    conf.setBoolean(HConstants.ZOOKEEPER_USEMULTI, true);
    final Server server = new DummyServer("ec2-54-234-230-108.compute-1.amazonaws.com");
    AtomicBoolean replicating = new AtomicBoolean(true);
    ReplicationZookeeper rz = new ReplicationZookeeper(server, replicating);
    // populate some znodes in the peer znode
    files.add("log1");
    files.add("log2");
    for (String file : files) {
      rz.addLogToList(file, "1");
    }
    // create 3 DummyServers
    Server s1 = new DummyServer("ip-10-8-101-114.ec2.internal");
    Server s2 = new DummyServer("ec2-107-20-52-47.compute-1.amazonaws.com");
    Server s3 = new DummyServer("ec2-23-20-187-167.compute-1.amazonaws.com");

    // simulate three server fail sequentially
    ReplicationZookeeper rz1 = new ReplicationZookeeper(s1, new AtomicBoolean(true));
    SortedMap<String, SortedSet<String>> testMap =
        rz1.copyQueuesFromRSUsingMulti(server.getServerName().getServerName());
    ReplicationZookeeper rz2 = new ReplicationZookeeper(s2, new AtomicBoolean(true));
    testMap = rz2.copyQueuesFromRSUsingMulti(s1.getServerName().getServerName());
    ReplicationZookeeper rz3 = new ReplicationZookeeper(s3, new AtomicBoolean(true));
    testMap = rz3.copyQueuesFromRSUsingMulti(s2.getServerName().getServerName());

    ReplicationSource s = new ReplicationSource();
    s.checkIfQueueRecovered(testMap.firstKey());
    List<String> result = s.getDeadRegionServers();

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
    ReplicationZookeeper rz;

    public DummyNodeFailoverWorker(String znode, Server s) throws Exception {
      this.deadRsZnode = znode;
      this.server = s;
      rz = new ReplicationZookeeper(server, new AtomicBoolean(true));
    }

    @Override
    public void run() {
      try {
        logZnodesMap = rz.copyQueuesFromRSUsingMulti(deadRsZnode);
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
    public CatalogTracker getCatalogTracker() {
      return null; // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ServerName getServerName() {
      return new ServerName(hostname, 1234, 1L);
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
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isStopped() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }


  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

