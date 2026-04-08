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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG) // Plays with the ManualEnvironmentEdge
public class TestClusterStatusPublisher {

  private ManualEnvironmentEdge mee = new ManualEnvironmentEdge();

  @BeforeEach
  public void before() {
    mee.setValue(0);
    EnvironmentEdgeManager.injectEdge(mee);
  }

  @Test
  public void testEmpty() {
    ClusterStatusPublisher csp = new ClusterStatusPublisher() {
      @Override
      protected List<Pair<ServerName, Long>> getDeadServers(long since) {
        return new ArrayList<>();
      }
    };

    assertTrue(csp.generateDeadServersListToSend().isEmpty());
  }

  @Test
  public void testMaxSend() {
    ClusterStatusPublisher csp = new ClusterStatusPublisher() {
      @SuppressWarnings("MissingDefault")
      @Override
      protected List<Pair<ServerName, Long>> getDeadServers(long since) {
        List<Pair<ServerName, Long>> res = new ArrayList<>();
        switch ((int) EnvironmentEdgeManager.currentTime()) {
          case 2:
            res.add(new Pair<>(ServerName.valueOf("hn", 10, 10), 1L));
            break;
          case 1000:
            break;
        }

        return res;
      }
    };

    mee.setValue(2);
    for (int i = 0; i < ClusterStatusPublisher.NB_SEND; i++) {
      assertEquals(1, csp.generateDeadServersListToSend().size(), "i=" + i);
    }
    mee.setValue(1000);
    assertTrue(csp.generateDeadServersListToSend().isEmpty());
  }

  @Test
  public void testOrder() {
    ClusterStatusPublisher csp = new ClusterStatusPublisher() {
      @Override
      protected List<Pair<ServerName, Long>> getDeadServers(long since) {
        List<Pair<ServerName, Long>> res = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
          res.add(new Pair<>(ServerName.valueOf("hn" + i, 10, 10), 20L));
        }

        return res;
      }
    };

    mee.setValue(3);
    List<ServerName> allSNS = csp.generateDeadServersListToSend();

    assertEquals(10, ClusterStatusPublisher.MAX_SERVER_PER_MESSAGE);
    assertEquals(10, allSNS.size());

    List<ServerName> nextMes = csp.generateDeadServersListToSend();
    assertEquals(10, nextMes.size());
    for (ServerName sn : nextMes) {
      if (!allSNS.contains(sn)) {
        allSNS.add(sn);
      }
    }
    assertEquals(20, allSNS.size());

    nextMes = csp.generateDeadServersListToSend();
    assertEquals(10, nextMes.size());
    for (ServerName sn : nextMes) {
      if (!allSNS.contains(sn)) {
        allSNS.add(sn);
      }
    }
    assertEquals(25, allSNS.size());

    nextMes = csp.generateDeadServersListToSend();
    assertEquals(10, nextMes.size());
    for (ServerName sn : nextMes) {
      if (!allSNS.contains(sn)) {
        allSNS.add(sn);
      }
    }
    assertEquals(25, allSNS.size());
  }
}
