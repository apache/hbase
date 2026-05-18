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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestDeadServer {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  final ServerName hostname123 = ServerName.valueOf("127.0.0.1", 123, 3L);
  final ServerName hostname123_2 = ServerName.valueOf("127.0.0.1", 123, 4L);
  final ServerName hostname1234 = ServerName.valueOf("127.0.0.2", 1234, 4L);
  final ServerName hostname12345 = ServerName.valueOf("127.0.0.2", 12345, 4L);

  @BeforeAll
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testIsDead() {
    DeadServer ds = new DeadServer();
    ds.putIfAbsent(hostname123);

    ds.putIfAbsent(hostname1234);

    ds.putIfAbsent(hostname12345);

    // Already dead = 127.0.0.1,9090,112321
    // Coming back alive = 127.0.0.1,9090,223341

    final ServerName deadServer = ServerName.valueOf("127.0.0.1", 9090, 112321L);
    assertFalse(ds.cleanPreviousInstance(deadServer));
    ds.putIfAbsent(deadServer);
    assertTrue(ds.isDeadServer(deadServer));
    Set<ServerName> deadServerNames = ds.copyServerNames();
    for (ServerName eachDeadServer : deadServerNames) {
      assertNotEquals(0, ds.getDeathTimestamp(eachDeadServer));
    }
    final ServerName deadServerHostComingAlive = ServerName.valueOf("127.0.0.1", 9090, 223341L);
    assertTrue(ds.cleanPreviousInstance(deadServerHostComingAlive));
    assertFalse(ds.isDeadServer(deadServer));
    assertFalse(ds.cleanPreviousInstance(deadServerHostComingAlive));
  }

  @Test
  public void testCrashProcedureReplay() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    final ProcedureExecutor<MasterProcedureEnv> pExecutor = master.getMasterProcedureExecutor();
    ServerCrashProcedure proc =
      new ServerCrashProcedure(pExecutor.getEnvironment(), hostname123, false, false);

    pExecutor.stop();
    ProcedureTestingUtility.submitAndWait(pExecutor, proc);
    assertTrue(master.getServerManager().areDeadServersInProgress());

    ProcedureTestingUtility.restart(pExecutor);
    ProcedureTestingUtility.waitProcedure(pExecutor, proc);
    assertFalse(master.getServerManager().areDeadServersInProgress());
  }

  @Test
  public void testSortExtract() {
    ManualEnvironmentEdge mee = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(mee);
    mee.setValue(1);

    DeadServer d = new DeadServer();

    d.putIfAbsent(hostname123);
    mee.incValue(1);
    d.putIfAbsent(hostname1234);
    mee.incValue(1);
    d.putIfAbsent(hostname12345);

    List<Pair<ServerName, Long>> copy = d.copyDeadServersSince(2L);
    assertEquals(2, copy.size());

    assertEquals(hostname1234, copy.get(0).getFirst());
    assertEquals(Long.valueOf(2L), copy.get(0).getSecond());

    assertEquals(hostname12345, copy.get(1).getFirst());
    assertEquals(Long.valueOf(3L), copy.get(1).getSecond());

    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testClean() {
    DeadServer d = new DeadServer();
    d.putIfAbsent(hostname123);

    d.cleanPreviousInstance(hostname12345);
    assertFalse(d.isEmpty());

    d.cleanPreviousInstance(hostname1234);
    assertFalse(d.isEmpty());

    d.cleanPreviousInstance(hostname123_2);
    assertTrue(d.isEmpty());
  }

  @Test
  public void testClearDeadServer() {
    DeadServer d = new DeadServer();
    d.putIfAbsent(hostname123);
    d.putIfAbsent(hostname1234);
    assertEquals(2, d.size());

    d.removeDeadServer(hostname123);
    assertEquals(1, d.size());
    d.removeDeadServer(hostname1234);
    assertTrue(d.isEmpty());

    d.putIfAbsent(hostname1234);
    assertFalse(d.removeDeadServer(hostname123_2));
    assertEquals(1, d.size());
    assertTrue(d.removeDeadServer(hostname1234));
    assertTrue(d.isEmpty());
  }
}
