package org.apache.hadoop.hbase.consensus.fsm;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.common.util.concurrent.SettableFuture;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.consensus.RaftTestUtil;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.raft.states.RaftStateType;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

public class TestAsyncStatesInRaftStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(
    TestAsyncStatesInRaftStateMachine.class);

  private static final int QUORUM_SIZE = 5;
  private static final int QUORUM_MAJORITY = 3;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private Configuration conf;
  private QuorumClient client;
  private ExecutorService executorService;

  class DummyExecutorService extends AbstractExecutorService {
    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
      return null;
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return false;
    }

    @Override
    public Future<?> submit(Runnable task) {
      return SettableFuture.create();
    }

    @Override
    public void execute(Runnable command) {
    }
  }

  public void setUpExecutors(boolean useDummyWriteOpsPool,
                             boolean useDummyReadOpsPool) {
    FSMLargeOpsExecutorService
      .initializeForTesting(
        (useDummyWriteOpsPool ?
          new DummyExecutorService() :
          FSMLargeOpsExecutorService.createWriteOpsExecutorService(conf)),
        (useDummyReadOpsPool ?
          new DummyExecutorService() :
          FSMLargeOpsExecutorService.createReadOpsExecutorService(conf)));
  }

  public void setUp(boolean useDummyWriteOpsPool,
                    boolean useDummyReadOpsPool) throws Exception {
    conf = RAFT_TEST_UTIL.getConf();
    setUpExecutors(useDummyWriteOpsPool, useDummyReadOpsPool);
    RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
    RAFT_TEST_UTIL.assertAllServersRunning();
    RAFT_TEST_UTIL.setUsePeristentLog(true);
    quorumInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(quorumInfo, null);
    RAFT_TEST_UTIL.startQuorum(quorumInfo);
    client = RAFT_TEST_UTIL.getQuorumClient(quorumInfo);
  }

  @After
  public void tearDown() throws Exception {
    if (executorService != null) {
      while (!executorService.isTerminated()) {
        executorService.shutdownNow();
        Threads.sleep(1000);
      }
    }
    LOG.info("Shutting down the FSM");
    RAFT_TEST_UTIL.setRaftQuorumContextClass(RaftQuorumContext.class);
    RAFT_TEST_UTIL.shutdown();
  }

  @Test(expected = TimeoutException.class)
  public void ensureNoProgressIfSendAppendRequestIsNotComplete()
    throws Exception {
    setUp(true, false);
    try {
      testReplicatingCommitsAsync(1).get(3000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // the peer with the lowest timeout will try to write the
      // votedFor and should get stuck.
      RaftQuorumContext r = RAFT_TEST_UTIL.getRaftQuorumContextByRank(
        quorumInfo, 5);
      assertEquals(RaftStateType.SEND_VOTE_REQUEST,
        r.getCurrentRaftState().getStateType());
      throw e;
    }
  }

  public PeerServer killAndRevivePeerWhileReplicatingEdits(
    boolean blockOnHandleAppendResponse) throws Exception {
    setUp(false, blockOnHandleAppendResponse);
    // Make one single commit.
    testReplicatingCommits(1);

    RaftQuorumContext leader =
      RAFT_TEST_UTIL.getLeaderQuorumContext(quorumInfo);

    // Stop the peer with rank = 1.
    RaftQuorumContext peer =
      RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 1);

    PeerServer peerServer = leader.getPeerServers().get(peer.getMyAddress());
    LocalConsensusServer peerConsensusServer =
      RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 1);

    // Replicate some other commits, the dead server will miss out.
    testReplicatingCommits(10);

    // Restart that dead server.
    RAFT_TEST_UTIL.restartLocalConsensusServer(peerConsensusServer,
      quorumInfo, peer.getMyAddress());

    // Wait for dead server to come back
    long start = System.currentTimeMillis();
    while (!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE, true) &&
      !blockOnHandleAppendResponse) {
      Thread.sleep(1000);
      // stop if we waited for more than 10 seconds
      if (System.currentTimeMillis() - start > 100000) {
        Assert.fail("Timed out while waiting for dead server to come back");
      }
    }

    return peerServer;
  }

  // TODO
  // The two tests below are unstable, and can be flaky dependent on the killing
  // of the server, and restarting it. I could fix it by adding sleep, but
  // that is no guarantee that the test won't break in the future.
  /**
  @Test
  public void ensureNoProgressIfPeerHandleAppendResponseIsNotComplete()
    throws Exception {
    PeerServer s = killAndRevivePeerWhileReplicatingEdits(true);
    assertTrue(
      ((AbstractPeer) s).getStateMachineService().getCurrentState().getStateType().equals(
        PeerServerStateType.HANDLE_APPEND_RESPONSE));
  }

  @Test
  public void ensureProgressIfPeerHandleAppendResponseIsComplete()
    throws Exception {
    PeerServer s = killAndRevivePeerWhileReplicatingEdits(false);
    assertTrue(
      ((AbstractPeer) s).getStateMachineService().getCurrentState().getStateType().equals(
        PeerServerStateType.PEER_FOLLOWER));
  }
   */

  @Test
  public void ensureProgressWhenSendAppendRequestCompletes() throws Exception {
    setUp(false, false);
    testReplicatingCommitsAsync(1).get(3000, TimeUnit.MILLISECONDS);
  }

  private Future testReplicatingCommitsAsync(final int numCommits)
    throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        testReplicatingCommits(numCommits);
      }
    };
    executorService = Executors.newSingleThreadExecutor();
    return executorService.submit(r);
  }

  private void testReplicatingCommits(int numCommits) {
    try {
      RAFT_TEST_UTIL.waitForLeader(quorumInfo);
      RaftQuorumContext leader =
        RAFT_TEST_UTIL.getLeaderQuorumContext(quorumInfo);
      Assert.assertNotNull(leader);

      RAFT_TEST_UTIL.dumpStates(quorumInfo);
      for (int i = 0; i < numCommits; i++) {
        client.replicateCommits(Arrays.asList(generateTestingWALEdit()));

      }
      RAFT_TEST_UTIL.dumpStates(quorumInfo);
    } catch (Exception e) {
      LOG.error("Errors: ", e);
      fail("Unexpected exception: " + e);
    }
  }

  private static WALEdit generateTestingWALEdit() {
    KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("TestQuorum"));
    return new WALEdit(Arrays.asList(kv));
  }
}
