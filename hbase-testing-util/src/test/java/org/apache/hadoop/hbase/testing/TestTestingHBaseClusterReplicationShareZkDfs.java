/*
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
package org.apache.hadoop.hbase.testing;

import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, LargeTests.class })
public class TestTestingHBaseClusterReplicationShareZkDfs
  extends TestingHBaseClusterReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTestingHBaseClusterReplicationShareZkDfs.class);

  private HBaseTestingUtil util = new HBaseTestingUtil();

  @Override
  protected void startClusters() throws Exception {
    util.startMiniZKCluster();
    util.startMiniDFSCluster(1);
    String zkConnectString = util.getZkCluster().getAddress().toString();
    String dfsUri = util.getDFSCluster().getFileSystem().getUri().toString();
    sourceCluster = TestingHBaseCluster.create(TestingHBaseClusterOption.builder()
      .useExternalZooKeeper(zkConnectString).useExternalDfs(dfsUri).build());
    sourceCluster.start();
    peerCluster = TestingHBaseCluster.create(TestingHBaseClusterOption.builder()
      .useExternalZooKeeper(zkConnectString).useExternalDfs(dfsUri).build());
    peerCluster.start();
    assertNotEquals(sourceCluster.getConf().get(HConstants.ZOOKEEPER_ZNODE_PARENT),
      peerCluster.getConf().get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    assertNotEquals(sourceCluster.getConf().get(HConstants.HBASE_DIR),
      peerCluster.getConf().get(HConstants.HBASE_DIR));
  }

  @Override
  protected void stopClusters() throws Exception {
    util.shutdownMiniDFSCluster();
    util.shutdownMiniZKCluster();
  }
}
