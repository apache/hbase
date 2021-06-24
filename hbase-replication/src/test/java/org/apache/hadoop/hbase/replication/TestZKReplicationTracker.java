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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestZKReplicationTracker extends ReplicationTrackerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZKReplicationTracker.class);

  private static Configuration CONF;

  private static HBaseZKTestingUtility UTIL;

  private static ZKWatcher ZKW;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL = new HBaseZKTestingUtility();
    UTIL.startMiniZKCluster();
    CONF = UTIL.getConfiguration();
    CONF.setClass(ReplicationFactory.REPLICATION_TRACKER_IMPL, ZKReplicationTracker.class,
      ReplicationTracker.class);
    ZKWatcher zk = HBaseZKTestingUtility.getZooKeeperWatcher(UTIL);
    ZKUtil.createWithParents(zk, zk.getZNodePaths().rsZNode);
    ZKW = HBaseZKTestingUtility.getZooKeeperWatcher(UTIL);
  }

  @Override
  protected ReplicationTrackerParams createParams() {
    return ReplicationTrackerParams.create(CONF, new WarnOnlyStoppable()).zookeeper(ZKW);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ZKW, true);
    UTIL.shutdownMiniZKCluster();
  }

  @Override
  protected void addServer(ServerName sn) throws Exception {
    ZKUtil.createAndWatch(ZKW, ZNodePaths.joinZNode(ZKW.getZNodePaths().rsZNode, sn.toString()),
      HConstants.EMPTY_BYTE_ARRAY);
  }

  @Override
  protected void removeServer(ServerName sn) throws Exception {
    ZKUtil.deleteNode(ZKW, ZNodePaths.joinZNode(ZKW.getZNodePaths().rsZNode, sn.toString()));
  }
}
