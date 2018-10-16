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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningRequest;

@Category({ MasterTests.class, MediumTests.class })
public class TestHMasterRPCException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHMasterRPCException.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHMasterRPCException.class);

  private final HBaseTestingUtility testUtil = HBaseTestingUtility.createLocalHTU();

  private HMaster master;

  private RpcClient rpcClient;

  @Before
  public void setUp() throws Exception {
    Configuration conf = testUtil.getConfiguration();
    conf.set(HConstants.MASTER_PORT, "0");
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 2000);
    testUtil.startMiniZKCluster();

    ZKWatcher watcher = testUtil.getZooKeeperWatcher();
    ZKUtil.createWithParents(watcher, watcher.getZNodePaths().masterAddressZNode,
            Bytes.toBytes("fake:123"));
    master = new HMaster(conf);
    rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
  }

  @After
  public void tearDown() throws IOException {
    if (rpcClient != null) {
      rpcClient.close();
    }
    if (master != null) {
      master.stopMaster();
    }
    testUtil.shutdownMiniZKCluster();
  }

  @Test
  public void testRPCException() throws IOException, InterruptedException, KeeperException {
    ServerName sm = master.getServerName();
    boolean fakeZNodeDelete = false;
    for (int i = 0; i < 20; i++) {
      try {
        BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sm, User.getCurrent(), 0);
        MasterProtos.MasterService.BlockingInterface stub =
            MasterProtos.MasterService.newBlockingStub(channel);
        assertTrue(stub.isMasterRunning(null, IsMasterRunningRequest.getDefaultInstance())
            .getIsMasterRunning());
        return;
      } catch (ServiceException ex) {
        IOException ie = ProtobufUtil.handleRemoteException(ex);
        // No SocketTimeoutException here. RpcServer is already started after the construction of
        // HMaster.
        assertTrue(ie.getMessage().startsWith(
          "org.apache.hadoop.hbase.ipc.ServerNotRunningYetException: Server is not running yet"));
        LOG.info("Expected exception: ", ie);
        if (!fakeZNodeDelete) {
          testUtil.getZooKeeperWatcher().getRecoverableZooKeeper()
              .delete(testUtil.getZooKeeperWatcher().getZNodePaths().masterAddressZNode, -1);
          fakeZNodeDelete = true;
        }
      }
      Thread.sleep(1000);
    }
  }
}
