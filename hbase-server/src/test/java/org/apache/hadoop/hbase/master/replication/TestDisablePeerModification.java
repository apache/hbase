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
package org.apache.hadoop.hbase.master.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.replication.DummyReplicationEndpoint;
import org.apache.hadoop.hbase.replication.FSReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MasterTests.class, LargeTests.class })
public class TestDisablePeerModification {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDisablePeerModification.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static volatile CountDownLatch ARRIVE;

  private static volatile CountDownLatch RESUME;

  public static final class MockPeerStorage extends FSReplicationPeerStorage {

    public MockPeerStorage(FileSystem fs, Configuration conf) throws IOException {
      super(fs, conf);
    }

    @Override
    public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled,
      SyncReplicationState syncReplicationState) throws ReplicationException {
      ARRIVE.countDown();
      try {
        RESUME.await();
      } catch (InterruptedException e) {
        throw new ReplicationException(e);
      }
      super.addPeer(peerId, peerConfig, enabled, syncReplicationState);
    }
  }

  @Parameter
  public boolean async;

  @Parameters(name = "{index}: async={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
      MockPeerStorage.class, ReplicationPeerStorage.class);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest() throws IOException {
    UTIL.getAdmin().replicationPeerModificationSwitch(true, true);
  }

  @Test
  public void testDrainProcs() throws Exception {
    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    AsyncAdmin admin = UTIL.getAsyncConnection().getAdmin();
    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL.getClusterKey() + "-test")
        .setReplicationEndpointImpl(DummyReplicationEndpoint.class.getName()).build();
    CompletableFuture<Void> addFuture = admin.addReplicationPeer("test_peer_" + async, rpc);
    ARRIVE.await();

    // we have a pending add peer procedure which has already passed the first state, let's issue a
    // peer modification switch request to disable peer modification and set drainProcs to true
    CompletableFuture<Boolean> switchFuture;
    if (async) {
      switchFuture = admin.replicationPeerModificationSwitch(false, true);
    } else {
      switchFuture = new CompletableFuture<>();
      ForkJoinPool.commonPool().submit(() -> {
        try {
          switchFuture.complete(UTIL.getAdmin().replicationPeerModificationSwitch(false, true));
        } catch (IOException e) {
          switchFuture.completeExceptionally(e);
        }
      });
    }

    // sleep a while, the switchFuture should not finish yet
    // the sleep is necessary as we can not join on the switchFuture, so there is no stable way to
    // make sure we have already changed the flag at master side, sleep a while is the most suitable
    // way here
    Thread.sleep(5000);
    assertFalse(switchFuture.isDone());

    // also verify that we can not schedule a new peer modification procedure
    AddPeerProcedure proc = new AddPeerProcedure("failure", rpc, true);
    UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().submitProcedure(proc);
    UTIL.waitFor(15000, () -> proc.isFinished());
    // make sure the procedure is failed because of peer modification disabled
    assertTrue(proc.isFailed());
    assertThat(proc.getException().getCause().getMessage(),
      containsString("Replication peer modification disabled"));

    // sleep a while and check again, make sure the switchFuture is still not done
    Thread.sleep(5000);
    assertFalse(switchFuture.isDone());

    // resume the add peer procedure and wait it done
    RESUME.countDown();
    addFuture.get();

    // this time the switchFuture should be able to finish
    assertTrue(switchFuture.get());
  }
}
