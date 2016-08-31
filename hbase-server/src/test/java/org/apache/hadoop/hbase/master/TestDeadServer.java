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

import org.apache.hadoop.hbase.HBaseTestingUtility;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestDeadServer {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  final ServerName hostname123 = ServerName.valueOf("127.0.0.1", 123, 3L);
  final ServerName hostname123_2 = ServerName.valueOf("127.0.0.1", 123, 4L);
  final ServerName hostname1234 = ServerName.valueOf("127.0.0.2", 1234, 4L);
  final ServerName hostname12345 = ServerName.valueOf("127.0.0.2", 12345, 4L);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test public void testIsDead() {
    DeadServer ds = new DeadServer();
    ds.add(hostname123);
    ds.notifyServer(hostname123);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname123);
    assertFalse(ds.areDeadServersInProgress());

    ds.add(hostname1234);
    ds.notifyServer(hostname1234);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname1234);
    assertFalse(ds.areDeadServersInProgress());

    ds.add(hostname12345);
    ds.notifyServer(hostname12345);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname12345);
    assertFalse(ds.areDeadServersInProgress());

    // Already dead =       127.0.0.1,9090,112321
    // Coming back alive =  127.0.0.1,9090,223341

    final ServerName deadServer = ServerName.valueOf("127.0.0.1", 9090, 112321L);
    assertFalse(ds.cleanPreviousInstance(deadServer));
    ds.add(deadServer);
    assertTrue(ds.isDeadServer(deadServer));
    Set<ServerName> deadServerNames = ds.copyServerNames();
    for (ServerName eachDeadServer : deadServerNames) {
      Assert.assertNotNull(ds.getTimeOfDeath(eachDeadServer));
    }
    final ServerName deadServerHostComingAlive =
        ServerName.valueOf("127.0.0.1", 9090, 223341L);
    assertTrue(ds.cleanPreviousInstance(deadServerHostComingAlive));
    assertFalse(ds.isDeadServer(deadServer));
    assertFalse(ds.cleanPreviousInstance(deadServerHostComingAlive));
  }

  @Test(timeout = 15000)
  public void testCrashProcedureReplay() {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    final ProcedureExecutor<MasterProcedureEnv> pExecutor = master.getMasterProcedureExecutor();
    ServerCrashProcedure proc = new ServerCrashProcedure(
      pExecutor.getEnvironment(), hostname123, false, false);

    ProcedureTestingUtility.submitAndWait(pExecutor, proc);
    
    assertFalse(master.getServerManager().getDeadServers().areDeadServersInProgress());
  }

  @Test
  public void testSortExtract(){
    ManualEnvironmentEdge mee = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(mee);
    mee.setValue(1);

    DeadServer d = new DeadServer();


    d.add(hostname123);
    mee.incValue(1);
    d.add(hostname1234);
    mee.incValue(1);
    d.add(hostname12345);

    List<Pair<ServerName, Long>> copy = d.copyDeadServersSince(2L);
    Assert.assertEquals(2, copy.size());

    Assert.assertEquals(hostname1234, copy.get(0).getFirst());
    Assert.assertEquals(new Long(2L), copy.get(0).getSecond());

    Assert.assertEquals(hostname12345, copy.get(1).getFirst());
    Assert.assertEquals(new Long(3L), copy.get(1).getSecond());

    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testClean(){
    DeadServer d = new DeadServer();
    d.add(hostname123);

    d.cleanPreviousInstance(hostname12345);
    Assert.assertFalse(d.isEmpty());

    d.cleanPreviousInstance(hostname1234);
    Assert.assertFalse(d.isEmpty());

    d.cleanPreviousInstance(hostname123_2);
    Assert.assertTrue(d.isEmpty());
  }

}

