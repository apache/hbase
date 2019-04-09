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
package org.apache.hadoop.hbase.master.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({ MasterTests.class, SmallTests.class })
public class TestSyncReplicationReplayWALManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSyncReplicationReplayWALManager.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private SyncReplicationReplayWALManager manager;

  private MasterProcedureScheduler scheduler;

  private Set<ServerName> onlineServers;

  private List<ServerListener> listeners;

  private Queue<Procedure<?>> wokenProcedures;

  @Before
  public void setUp() throws IOException, ReplicationException {
    wokenProcedures = new ArrayDeque<>();
    onlineServers = new HashSet<>();
    listeners = new ArrayList<>();
    ServerManager serverManager = mock(ServerManager.class);
    doAnswer(inv -> listeners.add(inv.getArgument(0))).when(serverManager)
      .registerListener(any(ServerListener.class));
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    doAnswer(inv -> onlineServers.stream()
      .collect(Collectors.toMap(Function.identity(), k -> serverMetrics))).when(serverManager)
        .getOnlineServers();

    MasterFileSystem mfs = mock(MasterFileSystem.class);
    when(mfs.getFileSystem()).thenReturn(UTIL.getTestFileSystem());
    when(mfs.getWALRootDir()).thenReturn(new Path("/"));

    scheduler = mock(MasterProcedureScheduler.class);
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ProcedureEvent<?> event = ((ProcedureEvent<?>[]) invocation.getArgument(0))[0];
        event.wakeInternal(new MasterProcedureScheduler(pid -> null) {

          @Override
          public void addFront(Iterator<Procedure> procedureIterator) {
            procedureIterator.forEachRemaining(wokenProcedures::add);
          }
        });
        return null;
      }
    }).when(scheduler).wakeEvents(any(ProcedureEvent[].class));
    MasterProcedureEnv env = mock(MasterProcedureEnv.class);
    when(env.getProcedureScheduler()).thenReturn(scheduler);
    ProcedureExecutor<MasterProcedureEnv> procExec = mock(ProcedureExecutor.class);
    when(procExec.getEnvironment()).thenReturn(env);

    MasterServices services = mock(MasterServices.class);
    when(services.getServerManager()).thenReturn(serverManager);
    when(services.getMasterFileSystem()).thenReturn(mfs);
    when(services.getMasterProcedureExecutor()).thenReturn(procExec);
    manager = new SyncReplicationReplayWALManager(services);
    assertEquals(1, listeners.size());
  }

  @Test
  public void testUsedWorkers() throws ProcedureSuspendedException {
    String peerId1 = "1";
    String peerId2 = "2";
    ServerName sn1 = ServerName.valueOf("host1", 123, 12345);
    ServerName sn2 = ServerName.valueOf("host2", 234, 23456);
    ServerName sn3 = ServerName.valueOf("host3", 345, 34567);
    onlineServers.add(sn1);
    manager.registerPeer(peerId1);
    manager.registerPeer(peerId2);
    // confirm that different peer ids does not affect each other
    assertEquals(sn1, manager.acquirePeerWorker(peerId1, new NoopProcedure<>()));
    assertEquals(sn1, manager.acquirePeerWorker(peerId2, new NoopProcedure<>()));
    onlineServers.add(sn2);
    assertEquals(sn2, manager.acquirePeerWorker(peerId1, new NoopProcedure<>()));
    assertEquals(sn2, manager.acquirePeerWorker(peerId2, new NoopProcedure<>()));

    NoopProcedure<?> proc = new NoopProcedure<>();
    try {
      manager.acquirePeerWorker(peerId1, proc);
      fail("Should suspend");
    } catch (ProcedureSuspendedException e) {
      // expected
    }
    manager.releasePeerWorker(peerId1, sn1, scheduler);
    assertEquals(1, wokenProcedures.size());
    assertSame(proc, wokenProcedures.poll());

    assertEquals(sn1, manager.acquirePeerWorker(peerId1, new NoopProcedure<>()));

    NoopProcedure<?> proc1 = new NoopProcedure<>();
    NoopProcedure<?> proc2 = new NoopProcedure<>();
    try {
      manager.acquirePeerWorker(peerId1, proc1);
      fail("Should suspend");
    } catch (ProcedureSuspendedException e) {
      // expected
    }
    try {
      manager.acquirePeerWorker(peerId1, proc2);
      fail("Should suspend");
    } catch (ProcedureSuspendedException e) {
      // expected
    }

    listeners.get(0).serverAdded(sn3);
    assertEquals(2, wokenProcedures.size());
    assertSame(proc2, wokenProcedures.poll());
    assertSame(proc1, wokenProcedures.poll());
  }
}
