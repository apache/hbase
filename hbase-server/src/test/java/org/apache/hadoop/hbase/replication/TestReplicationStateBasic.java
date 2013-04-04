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

import static org.junit.Assert.*;

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.hadoop.hbase.ServerName;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

/**
 * White box testing for replication state interfaces. Implementations should extend this class, and
 * initialize the interfaces properly.
 */
public abstract class TestReplicationStateBasic {

  protected ReplicationQueues rq1;
  protected ReplicationQueues rq2;
  protected ReplicationQueues rq3;
  protected ReplicationQueuesClient rqc;
  protected String server1 = new ServerName("hostname1.example.org", 1234, -1L).toString();
  protected String server2 = new ServerName("hostname2.example.org", 1234, -1L).toString();
  protected String server3 = new ServerName("hostname3.example.org", 1234, -1L).toString();

  @Test
  public void testReplicationQueuesClient() throws KeeperException {
    // Test methods with empty state
    assertEquals(0, rqc.getListOfReplicators().size());
    assertNull(rqc.getLogsInQueue(server1, "qId1"));
    assertNull(rqc.getAllQueues(server1));

    /*
     * Set up data Two replicators: -- server1: three queues with 0, 1 and 2 log files each --
     * server2: zero queues
     */
    rq1.init(server1);
    rq2.init(server2);
    rq1.addLog("qId1", "trash");
    rq1.removeLog("qId1", "trash");
    rq1.addLog("qId2", "filename1");
    rq1.addLog("qId3", "filename2");
    rq1.addLog("qId3", "filename3");
    rq2.addLog("trash", "trash");
    rq2.removeQueue("trash");

    List<String> reps = rqc.getListOfReplicators();
    assertEquals(2, reps.size());
    assertTrue(server1, reps.contains(server1));
    assertTrue(server2, reps.contains(server2));

    assertNull(rqc.getLogsInQueue("bogus", "bogus"));
    assertNull(rqc.getLogsInQueue(server1, "bogus"));
    assertEquals(0, rqc.getLogsInQueue(server1, "qId1").size());
    assertEquals(1, rqc.getLogsInQueue(server1, "qId2").size());
    assertEquals("filename1", rqc.getLogsInQueue(server1, "qId2").get(0));

    assertNull(rqc.getAllQueues("bogus"));
    assertEquals(0, rqc.getAllQueues(server2).size());
    List<String> list = rqc.getAllQueues(server1);
    assertEquals(3, list.size());
    assertTrue(list.contains("qId2"));
    assertTrue(list.contains("qId3"));
  }

  @Test
  public void testReplicationQueues() throws KeeperException {
    rq1.init(server1);
    rq2.init(server2);
    rq3.init(server3);

    // Zero queues or replicators exist
    assertEquals(0, rq1.getListOfReplicators().size());
    rq1.removeQueue("bogus");
    rq1.removeLog("bogus", "bogus");
    rq1.removeAllQueues();
    assertNull(rq1.getAllQueues());
    // TODO fix NPE if getting a log position on a file that does not exist
    // assertEquals(0, rq1.getLogPosition("bogus", "bogus"));
    assertNull(rq1.getLogsInQueue("bogus"));
    assertEquals(0, rq1.claimQueues(new ServerName("bogus", 1234, -1L).toString()).size());

    // TODO test setting a log position on a bogus file
    // rq1.setLogPosition("bogus", "bogus", 5L);

    populateQueues();

    assertEquals(3, rq1.getListOfReplicators().size());
    assertEquals(0, rq2.getLogsInQueue("qId1").size());
    assertEquals(5, rq3.getLogsInQueue("qId5").size());
    assertEquals(0, rq3.getLogPosition("qId1", "filename0"));
    rq3.setLogPosition("qId5", "filename4", 354L);
    assertEquals(354L, rq3.getLogPosition("qId5", "filename4"));

    assertEquals(5, rq3.getLogsInQueue("qId5").size());
    assertEquals(0, rq2.getLogsInQueue("qId1").size());
    assertEquals(0, rq1.getAllQueues().size());
    assertEquals(1, rq2.getAllQueues().size());
    assertEquals(5, rq3.getAllQueues().size());

    assertEquals(0, rq3.claimQueues(server1).size());
    assertEquals(2, rq3.getListOfReplicators().size());

    SortedMap<String, SortedSet<String>> queues = rq2.claimQueues(server3);
    assertEquals(5, queues.size());
    assertEquals(1, rq2.getListOfReplicators().size());

    // TODO test claimQueues on yourself
    // rq2.claimQueues(server2);

    assertEquals(6, rq2.getAllQueues().size());

    rq2.removeAllQueues();

    assertEquals(0, rq2.getListOfReplicators().size());
  }

  /*
   * three replicators: rq1 has 0 queues, rq2 has 1 queue with no logs, rq3 has 5 queues with 1, 2,
   * 3, 4, 5 log files respectively
   */
  protected void populateQueues() throws KeeperException {
    rq1.addLog("trash", "trash");
    rq1.removeQueue("trash");

    rq2.addLog("qId1", "trash");
    rq2.removeLog("qId1", "trash");

    for (int i = 1; i < 6; i++) {
      for (int j = 0; j < i; j++) {
        rq3.addLog("qId" + i, "filename" + j);
      }
    }
  }
}

