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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;

/**
 * All the modification method will fail once in the test and should finally succeed.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationProcedureRetry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationProcedureRetry.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, MockHMaster.class, HMaster.class);
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDownAfterTest() throws IOException {
    for (ReplicationPeerDescription desc : UTIL.getAdmin().listReplicationPeers()) {
      UTIL.getAdmin().removeReplicationPeer(desc.getPeerId());
    }
  }

  private void doTest() throws IOException {
    Admin admin = UTIL.getAdmin();
    String peerId = "1";
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
        .setClusterKey(UTIL.getZkCluster().getAddress().toString() + ":/hbase2").build();
    admin.addReplicationPeer(peerId, peerConfig, true);

    assertEquals(peerConfig.getClusterKey(),
      admin.getReplicationPeerConfig(peerId).getClusterKey());
    ReplicationPeerConfig newPeerConfig =
        ReplicationPeerConfig.newBuilder(peerConfig).setBandwidth(123456).build();
    admin.updateReplicationPeerConfig(peerId, newPeerConfig);
    assertEquals(newPeerConfig.getBandwidth(),
      admin.getReplicationPeerConfig(peerId).getBandwidth());

    admin.disableReplicationPeer(peerId);
    assertFalse(admin.listReplicationPeers().get(0).isEnabled());

    admin.enableReplicationPeer(peerId);
    assertTrue(admin.listReplicationPeers().get(0).isEnabled());

    admin.removeReplicationPeer(peerId);
    assertTrue(admin.listReplicationPeers().isEmpty());

    // make sure that we have run into the mocked method
    MockHMaster master = (MockHMaster) UTIL.getHBaseCluster().getMaster();
    assertTrue(master.addPeerCalled);
    assertTrue(master.removePeerCalled);
    assertTrue(master.updatePeerConfigCalled);
    assertTrue(master.enablePeerCalled);
    assertTrue(master.disablePeerCalled);
  }

  @Test
  public void testErrorBeforeUpdate() throws IOException, ReplicationException {
    ((MockHMaster) UTIL.getHBaseCluster().getMaster()).reset(true);
    doTest();
  }

  @Test
  public void testErrorAfterUpdate() throws IOException, ReplicationException {
    ((MockHMaster) UTIL.getHBaseCluster().getMaster()).reset(false);
    doTest();
  }

  public static final class MockHMaster extends HMaster {

    volatile boolean addPeerCalled;

    volatile boolean removePeerCalled;

    volatile boolean updatePeerConfigCalled;

    volatile boolean enablePeerCalled;

    volatile boolean disablePeerCalled;

    private ReplicationPeerManager manager;

    public MockHMaster(Configuration conf) throws IOException {
      super(conf);
    }

    private Object invokeWithError(InvocationOnMock invocation, boolean errorBeforeUpdate)
        throws Throwable {
      if (errorBeforeUpdate) {
        throw new ReplicationException("mock error before update");
      }
      invocation.callRealMethod();
      throw new ReplicationException("mock error after update");
    }

    public void reset(boolean errorBeforeUpdate) throws ReplicationException {
      addPeerCalled = false;
      removePeerCalled = false;
      updatePeerConfigCalled = false;
      enablePeerCalled = false;
      disablePeerCalled = false;
      ReplicationPeerManager m = super.getReplicationPeerManager();
      manager = spy(m);
      doAnswer(invocation -> {
        if (!addPeerCalled) {
          addPeerCalled = true;
          return invokeWithError(invocation, errorBeforeUpdate);
        } else {
          return invocation.callRealMethod();
        }
      }).when(manager).addPeer(anyString(), any(ReplicationPeerConfig.class), anyBoolean());
      doAnswer(invocation -> {
        if (!removePeerCalled) {
          removePeerCalled = true;
          return invokeWithError(invocation, errorBeforeUpdate);
        } else {
          return invocation.callRealMethod();
        }
      }).when(manager).removePeer(anyString());
      doAnswer(invocation -> {
        if (!updatePeerConfigCalled) {
          updatePeerConfigCalled = true;
          return invokeWithError(invocation, errorBeforeUpdate);
        } else {
          return invocation.callRealMethod();
        }
      }).when(manager).updatePeerConfig(anyString(), any(ReplicationPeerConfig.class));
      doAnswer(invocation -> {
        if (!enablePeerCalled) {
          enablePeerCalled = true;
          return invokeWithError(invocation, errorBeforeUpdate);
        } else {
          return invocation.callRealMethod();
        }
      }).when(manager).enablePeer(anyString());
      doAnswer(invocation -> {
        if (!disablePeerCalled) {
          disablePeerCalled = true;
          return invokeWithError(invocation, errorBeforeUpdate);
        } else {
          return invocation.callRealMethod();
        }
      }).when(manager).disablePeer(anyString());
    }

    @Override
    public ReplicationPeerManager getReplicationPeerManager() {
      return manager;
    }
  }
}
