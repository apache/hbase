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

import static org.apache.hadoop.hbase.replication.ReplicationSourceDummy.fakeExceptionMessage;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummyWithNoTermination;
import org.apache.hadoop.hbase.replication.ReplicationSourceWithoutPeerException;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.replication.regionserver.helper.DummyServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestReplicationSourceWithoutReplicationZnodes {

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

  private static final byte[] f2 = Bytes.toBytes("f2");

  private static final TableName test =
    TableName.valueOf("test");

  private static final String slaveId = "1";

  private static FileSystem fs;

  private static Path oldLogDir;

  private static Path logDir;

  private static DummyServer server;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
      ReplicationSourceDummyWithNoTermination.class.getCanonicalName());
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
    FSUtils.setRootDir(utility.getConfiguration(), utility.getDataTestDir());
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getDataTestDir(),
      HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getDataTestDir(),
      HConstants.HREGION_LOGDIR_NAME);
    server = new DummyServer(conf, "example.hostname.com", zkw);
    replication = new Replication(server, fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();

    manager.addSource(slaveId);

    htd = new HTableDescriptor(test);
    HColumnDescriptor col = new HColumnDescriptor(f1);
    col.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    htd.addFamily(col);
    col = new HColumnDescriptor(f2);
    col.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    htd.addFamily(col);

    hri = new HRegionInfo(htd.getTableName(), r1, r2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      manager.join();
    } catch (RuntimeException re) {
      if (re.getMessage().equals(fakeExceptionMessage)) {
        LOG.info("It is fine");
      }
    }
    utility.shutdownMiniCluster();
  }

  @Rule
  public TestName testName = new TestName();

  private void cleanLogDir() throws IOException {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
  }

  @Before
  public void setUp() throws Exception {
    LOG.info("Start " + testName.getMethodName());
    cleanLogDir();
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("End " + testName.getMethodName());
    cleanLogDir();
  }

  /**
   * When the peer is removed, hbase remove the peer znodes and there is zk watcher
   * which terminates the replication sources. In case of zk watcher not getting invoked
   * or a race condition between source deleting the log znode and zk watcher
   * terminating the source, we might get the NoNode exception. In that case, the right
   * thing is to terminate the replication source.
   * @throws Exception
   */
  @Test
  public void testReplicationSourceRunningWithoutPeerZnodes() throws Exception {
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    KeyValue kv = new KeyValue(r1, f1, r1);
    WALEdit edit = new WALEdit();
    edit.add(kv);

    List<WALActionsListener> listeners = new ArrayList<>();
    listeners.add(replication);
    final WALFactory wals = new WALFactory(utility.getConfiguration(), listeners,
      URLEncoder.encode("regionserver:60020", "UTF8"));
    final WAL wal = wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
    manager.init();

    final long txid = wal.append(htd, hri,
      new WALKey(hri.getEncodedNameAsBytes(), test, System.currentTimeMillis(), mvcc),
      edit, true);
    wal.sync(txid);


    wal.rollWriter();
    ZKUtil.deleteNodeRecursively(zkw, "/hbase/replication/peers/1");
    ZKUtil.deleteNodeRecursively(zkw, "/hbase/replication/rs/"+ server.getServerName() + "/1");

    ReplicationException exceptionThrown = null;
    try {
      manager.logPositionAndCleanOldLogs(manager.getSources().get(0).getCurrentPath(), "1", 0, false,
        false);
    } catch (ReplicationException e) {
      exceptionThrown = e;

    }

    Assert.assertTrue(exceptionThrown instanceof ReplicationSourceWithoutPeerException);
  }
}
