package org.apache.hadoop.hbase.consensus;

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


import junit.framework.Assert;
 import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.NoLeaderForRegionException;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
 import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.runner.RunWith;
 import org.junit.runners.Parameterized;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 import org.apache.hadoop.hbase.HConstants;
 import org.apache.hadoop.hbase.consensus.client.QuorumClient;
 import org.junit.After;
 import org.junit.Before;
 import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

@RunWith(value = Parameterized.class)
 public class TestBasicQuorumMembershipChange {
   private static final Logger LOG = LoggerFactory.getLogger(
     TestBasicPeerFailure.class);
   private static int QUORUM_SIZE = 5;
   private static int QUORUM_MAJORITY = 3;
   private static QuorumInfo quorumInfo;
   private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
   private static QuorumClient client;
   private static volatile int transactionNums = 0;
   private static ThreadPoolExecutor loadGeneratorExecutor;
   private static volatile boolean stop = false;
   private static volatile long clientTrafficFrequency = 10;
   private final int numPeersToChange;
   private ReplicationLoadForUnitTest loader;
   private boolean replaceLeader = false;

   @Before
   public void setUp() throws Exception {
     RAFT_TEST_UTIL.disableVerboseLogging();
     RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
     RAFT_TEST_UTIL.setUsePeristentLog(true);
     RAFT_TEST_UTIL.assertAllServersRunning();
     quorumInfo = RAFT_TEST_UTIL.initializePeers();
     RAFT_TEST_UTIL.addQuorum(quorumInfo, null);
     RAFT_TEST_UTIL.startQuorum(quorumInfo);
     client = RAFT_TEST_UTIL.getQuorumClient(quorumInfo);

     loader = new ReplicationLoadForUnitTest(quorumInfo, client, RAFT_TEST_UTIL,
       QUORUM_SIZE, QUORUM_MAJORITY);

     transactionNums = 0;
     stop = false;
   }

   @Parameterized.Parameters
   public static Collection<Object[]> data() {
     return Arrays.asList(new Object[][] {
       {1, false},
       {1, true},
       {2, false},
       {2, true},
       {3, true}
     });
   }

   @After
   public void tearDown() throws Exception {
     RAFT_TEST_UTIL.shutdown();
   }

   public TestBasicQuorumMembershipChange(int numPeersToChange, boolean replaceLeader) {
     this.replaceLeader = replaceLeader;
     this.numPeersToChange = numPeersToChange;
   }

   @Test(timeout=600000)
   public void testSingleMemberChange()
           throws InterruptedException, ExecutionException, IOException {
     final long sleepTime = HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS;

     Thread.sleep(sleepTime);

     // Start the client load
     loader.startReplicationLoad(100);

     // Let the traffic fly for a while
     loader.makeProgress(sleepTime, transactionNums);

     int currPort = RAFT_TEST_UTIL.getNextPortNumber();

     int[] newPorts = new int[numPeersToChange];
     for (int i = 0; i < numPeersToChange; i++) {
       newPorts[i] = currPort++;
     }

     RAFT_TEST_UTIL.setNextPortNumber(++currPort);

     // Get the new config
     QuorumInfo newConfig = this.createNewQuorumInfo(newPorts);

     LOG.debug("Old Config " + quorumInfo.getPeersWithRank());

     LOG.debug("New Config " + newConfig.getPeersWithRank());

     // Add servers with new config
     addServers(newPorts, newConfig);

     // Send down the request
     // We are not allowed to change majority of more number of peers at the
     // same time.
     if (numPeersToChange >= this.QUORUM_MAJORITY) {
      Assert.assertFalse(client.changeQuorum(newConfig));
      newConfig = quorumInfo;
     } else {
       Assert.assertTrue(client.changeQuorum(newConfig));
       // Tell the quorum client about the new config
       client.refreshConfig(newConfig);
     }

     // Simulate all the events
     loader.makeProgress(sleepTime, transactionNums);

     // Slow down the client traffic;
     clientTrafficFrequency = clientTrafficFrequency * 5;

     // Let the traffic fly for a while
     loader.makeProgress(sleepTime, transactionNums);

     // Verify logs are identical across all the quorum members
     while (!RAFT_TEST_UTIL.verifyLogs(newConfig, QUORUM_SIZE)) {
       Thread.sleep(5 * 1000);
       clientTrafficFrequency = clientTrafficFrequency * 10;
       System.out.println("Verifying logs ....");
     }

     // Stop the replication load
     loader.stopReplicationLoad();

     System.out.println(transactionNums + " transactions have been successfully replicated");
   }

   private void addServers(int[] ports, final QuorumInfo info)
           throws IOException {
     for (int port : ports) {
       // Spin up a new server
       final LocalConsensusServer server = RAFT_TEST_UTIL.addServer(port);
       server.startService();

       // Add the new config to the new server map
       RAFT_TEST_UTIL.addQuorumForServer(server, info, null);

       // Start the raft protocol on the server
       server.getHandler().getRaftQuorumContext(
         quorumInfo.getQuorumName()).initializeAll(
         HConstants.UNDEFINED_TERM_INDEX);
     }
   }

   private QuorumInfo createNewQuorumInfo(int[] ports)
     throws NoLeaderForRegionException {

     // Make a copy

     QuorumInfo info = new QuorumInfo(quorumInfo);
     boolean leaderReplaced = false;
     List<HServerAddress> peerToReplaceAddr;
     peerToReplaceAddr = new ArrayList<>();
     List<Pair<HServerAddress, Integer>> newServers = new ArrayList<>();

     Map<HServerAddress, Integer> currentPeers =
       info.getPeers().get(QuorumInfo.LOCAL_DC_KEY);

     HServerAddress oldPeer, newPeer;
     for (int newServerPort : ports) {

       newPeer = new HServerAddress(RAFT_TEST_UTIL.LOCAL_HOST,
         newServerPort - HConstants.CONSENSUS_SERVER_PORT_JUMP);

       LOG.debug("Adding new server with address " + newPeer);
       if (replaceLeader && !leaderReplaced) {
         oldPeer = RaftUtil.getHRegionServerAddress(
           new HServerAddress(client.getLeader().getServerAddress()));
         leaderReplaced = true;
         System.out.println(
           "Replacing leader " + oldPeer + " with port " + newPeer);

       } else {
         oldPeer = currentPeers.keySet().iterator().next();
         System.out.println("Replacing non-leader " + oldPeer + " with port " +
           newPeer);
       }

       peerToReplaceAddr.add(oldPeer);
       int rank = currentPeers.remove(oldPeer);
       newServers.add(new Pair<HServerAddress, Integer>(newPeer, rank));
     }

     // Make sure we actually removed the required number of peers
     Assert.assertTrue(info.getPeers().get(QuorumInfo.LOCAL_DC_KEY).size() ==
       QUORUM_SIZE - ports.length);

     for (Pair<HServerAddress, Integer> server : newServers) {
       // Update the config
       info.getPeers().get(QuorumInfo.LOCAL_DC_KEY).put(server.getFirst(),
               server.getSecond());
     }
     info.refresh();

     Assert.assertTrue(info.getPeersWithRank().size() == QUORUM_SIZE);

     return info;
   }
 }
