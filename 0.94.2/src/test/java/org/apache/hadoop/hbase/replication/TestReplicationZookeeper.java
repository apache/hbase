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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestReplicationZookeeper {

  private static Configuration conf;

  private static HBaseTestingUtility utility;

  private static ZooKeeperWatcher zkw;

  private static ReplicationZookeeper repZk;

  private static String slaveClusterKey;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniZKCluster();
    conf = utility.getConfiguration();
    zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
    DummyServer server = new DummyServer();
    repZk = new ReplicationZookeeper(server, new AtomicBoolean());
    slaveClusterKey = conf.get(HConstants.ZOOKEEPER_QUORUM) + ":" +
      conf.get("hbase.zookeeper.property.clientPort") + ":/1";
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility.shutdownMiniZKCluster();
  }

  @Test
  public void testGetAddressesMissingSlave()
    throws IOException, KeeperException {
    repZk.addPeer("1", slaveClusterKey);
    // HBASE-5586 used to get an NPE
    assertEquals(0, repZk.getSlavesAddresses("1").size());
  }

  static class DummyServer implements Server {

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
      return null;
    }

    @Override
    public ServerName getServerName() {
      return new ServerName("hostname.example.org", 1234, -1L);
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }
}
