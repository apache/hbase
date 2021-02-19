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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummyWithNoTermination;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.replication.regionserver.helper.DummyServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class TestReplicationSourceBase {

  private static final Log LOG =
    LogFactory.getLog(TestReplicationSourceBase.class);

  protected static Configuration conf;
  protected static HBaseTestingUtility utility;
  protected static Replication replication;
  protected static ReplicationSourceManager manager;
  protected static ZooKeeperWatcher zkw;
  protected static HTableDescriptor htd;
  protected static HRegionInfo hri;

  protected static final byte[] r1 = Bytes.toBytes("r1");
  protected static final byte[] r2 = Bytes.toBytes("r2");
  protected static final byte[] f1 = Bytes.toBytes("f1");
  protected static final byte[] f2 = Bytes.toBytes("f2");
  protected static final TableName test = TableName.valueOf("test");
  protected static final String slaveId = "1";
  protected static FileSystem fs;
  protected static Path oldLogDir;
  protected static Path logDir;
  protected static DummyServer server;

  @BeforeClass public static void setUpBeforeClass() throws Exception {

    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
      ReplicationSourceDummyWithNoTermination.class.getCanonicalName());
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniZKCluster();

    zkw = new ZooKeeperWatcher(conf, "test", null);
    ZKUtil.createWithParents(zkw, "/hbase/replication");
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1", Bytes.toBytes(
      conf.get(HConstants.ZOOKEEPER_QUORUM) + ":" + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)
        + ":/1"));
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1/peer-state");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1/peer-state",
      ReplicationStateZKBase.ENABLED_ZNODE_BYTES);
    ZKUtil.createWithParents(zkw, "/hbase/replication/state");
    ZKUtil.setData(zkw, "/hbase/replication/state", ReplicationStateZKBase.ENABLED_ZNODE_BYTES);

    ZKClusterId.setClusterId(zkw, new ClusterId());
    FSUtils.setRootDir(utility.getConfiguration(), utility.getDataTestDir());
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getDataTestDir(), HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getDataTestDir(), HConstants.HREGION_LOGDIR_NAME);
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

  @AfterClass public static void tearDownAfterClass() throws Exception {
    try {
      manager.join();
    } catch (RuntimeException re) {
      if (re.getMessage().equals(fakeExceptionMessage)) {
        LOG.info("It is fine");
      }
    }
    utility.shutdownMiniCluster();
  }

  @Rule public TestName testName = new TestName();

  private void cleanLogDir() throws IOException {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
  }

  @Before public void setUp() throws Exception {
    LOG.info("Start " + testName.getMethodName());
    cleanLogDir();
  }

  @After public void tearDown() throws Exception {
    LOG.info("End " + testName.getMethodName());
    cleanLogDir();
  }
}
