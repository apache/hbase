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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RegionServerTests.class, SmallTests.class})
public class TestAssignProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignProcedure.class);
  @Rule public TestName name = new TestName();

  /**
   * An override that opens up the updateTransition method inside in AssignProcedure so can call it
   * below directly in test and mess with targetServer. Used by test
   * {@link #testTargetServerBeingNulledOnUs()}.
   */
  public static class TargetServerBeingNulledOnUsAssignProcedure extends AssignProcedure {
    public final AtomicBoolean addToRemoteDispatcherWasCalled = new AtomicBoolean(false);
    public final AtomicBoolean remoteCallFailedWasCalled = new AtomicBoolean(false);
    private final RegionStates.RegionStateNode rsn;

    public TargetServerBeingNulledOnUsAssignProcedure(RegionInfo regionInfo,
        RegionStates.RegionStateNode rsn) {
      super(regionInfo);
      this.rsn = rsn;
    }

    /**
     * Override so can change access from protected to public.
     */
    @Override
    public boolean updateTransition(MasterProcedureEnv env, RegionStates.RegionStateNode regionNode)
        throws IOException, ProcedureSuspendedException {
      return super.updateTransition(env, regionNode);
    }

    @Override
    protected boolean addToRemoteDispatcher(MasterProcedureEnv env, ServerName targetServer) {
      // So, mock the ServerCrashProcedure nulling out the targetServer AFTER updateTransition
      // has been called and BEFORE updateTransition gets to here.
      // We used to throw a NullPointerException. Now we just say the assign failed so it will
      // be rescheduled.
      boolean b = super.addToRemoteDispatcher(env, null);
      assertFalse(b);
      // Assert we were actually called.
      this.addToRemoteDispatcherWasCalled.set(true);
      return b;
    }

    @Override
    public RegionStates.RegionStateNode getRegionState(MasterProcedureEnv env) {
      // Do this so we don't have to mock a bunch of stuff.
      return this.rsn;
    }

    @Override
    public boolean remoteCallFailed(final MasterProcedureEnv env,
        final ServerName serverName, final IOException exception) {
      // Just skip this remoteCallFailed. Its too hard to mock. Assert it is called though.
      // Happens after the code we are testing has been called.
      this.remoteCallFailedWasCalled.set(true);
      return true;
    }
  };

  /**
   * Test that we deal with ServerCrashProcedure zero'ing out the targetServer in the
   * RegionStateNode in the midst of our doing an assign. The trickery is done above in
   * TargetServerBeingNulledOnUsAssignProcedure. We skip a bunch of logic to get at the guts
   * where the problem happens (We also skip-out the failure handling because it'd take a bunch
   * of mocking to get it to run). Fix is inside in RemoteProcedureDispatch#addOperationToNode.
   * It now notices empty targetServer and just returns false so we fall into failure processing
   * and we'll reassign elsewhere instead of NPE'ing. The fake of ServerCrashProcedure nulling out
   * the targetServer happens inside in updateTransition just after it was called but before it
   * gets to the near the end when addToRemoteDispatcher is called. See the
   * TargetServerBeingNulledOnUsAssignProcedure class above. See HBASE-19218.
   * Before fix, this test would fail w/ a NullPointerException.
   */
  @Test
  public void testTargetServerBeingNulledOnUs() throws ProcedureSuspendedException, IOException {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    RegionInfo ri = RegionInfoBuilder.newBuilder(tn).build();
    // Create an RSN with location/target server. Will be cleared above in addToRemoteDispatcher to
    // simulate issue in HBASE-19218
    RegionStates.RegionStateNode rsn = new RegionStates.RegionStateNode(ri);
    rsn.setRegionLocation(ServerName.valueOf("server.example.org", 0, 0));
    MasterProcedureEnv env = Mockito.mock(MasterProcedureEnv.class);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    ServerManager sm = Mockito.mock(ServerManager.class);
    Mockito.when(sm.isServerOnline(Mockito.any())).thenReturn(true);
    MasterServices ms = Mockito.mock(MasterServices.class);
    Mockito.when(ms.getServerManager()).thenReturn(sm);
    Configuration configuration = HBaseConfiguration.create();
    Mockito.when(ms.getConfiguration()).thenReturn(configuration);
    Mockito.when(env.getAssignmentManager()).thenReturn(am);
    Mockito.when(env.getMasterServices()).thenReturn(ms);
    RSProcedureDispatcher rsd = new RSProcedureDispatcher(ms);
    Mockito.when(env.getRemoteDispatcher()).thenReturn(rsd);

    TargetServerBeingNulledOnUsAssignProcedure assignProcedure =
        new TargetServerBeingNulledOnUsAssignProcedure(ri, rsn);
    assignProcedure.updateTransition(env, rsn);
    assertTrue(assignProcedure.remoteCallFailedWasCalled.get());
    assertTrue(assignProcedure.addToRemoteDispatcherWasCalled.get());
  }

  @Test
  public void testSimpleComparator() {
    List<AssignProcedure> procedures = new ArrayList<AssignProcedure>();
    RegionInfo user1 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space1")).build();
    procedures.add(new AssignProcedure(user1));
    RegionInfo user2 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space2")).build();
    procedures.add(new AssignProcedure(RegionInfoBuilder.FIRST_META_REGIONINFO));
    procedures.add(new AssignProcedure(user2));
    RegionInfo system = RegionInfoBuilder.newBuilder(TableName.NAMESPACE_TABLE_NAME).build();
    procedures.add(new AssignProcedure(system));
    procedures.sort(AssignProcedure.COMPARATOR);
    assertTrue(procedures.get(0).isMeta());
    assertTrue(procedures.get(1).getRegionInfo().getTable().equals(TableName.NAMESPACE_TABLE_NAME));
  }

  @Test
  public void testComparatorWithMetas() {
    List<AssignProcedure> procedures = new ArrayList<AssignProcedure>();
    RegionInfo user3 = RegionInfoBuilder.newBuilder(TableName.valueOf("user3")).build();
    procedures.add(new AssignProcedure(user3));
    RegionInfo system = RegionInfoBuilder.newBuilder(TableName.NAMESPACE_TABLE_NAME).build();
    procedures.add(new AssignProcedure(system));
    RegionInfo user1 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space1")).build();
    RegionInfo user2 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space2")).build();
    procedures.add(new AssignProcedure(user1));
    RegionInfo meta2 = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(Bytes.toBytes("002")).build();
    procedures.add(new AssignProcedure(meta2));
    procedures.add(new AssignProcedure(user2));
    RegionInfo meta1 = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(Bytes.toBytes("001")).build();
    procedures.add(new AssignProcedure(meta1));
    procedures.add(new AssignProcedure(RegionInfoBuilder.FIRST_META_REGIONINFO));
    RegionInfo meta0 = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(Bytes.toBytes("000")).build();
    procedures.add(new AssignProcedure(meta0));
    for (int i = 0; i < 10; i++) {
      Collections.shuffle(procedures);
      procedures.sort(AssignProcedure.COMPARATOR);
      try {
        assertTrue(procedures.get(0).getRegionInfo().equals(RegionInfoBuilder.FIRST_META_REGIONINFO));
        assertTrue(procedures.get(1).getRegionInfo().equals(meta0));
        assertTrue(procedures.get(2).getRegionInfo().equals(meta1));
        assertTrue(procedures.get(3).getRegionInfo().equals(meta2));
        assertTrue(procedures.get(4).getRegionInfo().getTable().equals(TableName.NAMESPACE_TABLE_NAME));
        assertTrue(procedures.get(5).getRegionInfo().equals(user1));
        assertTrue(procedures.get(6).getRegionInfo().equals(user2));
        assertTrue(procedures.get(7).getRegionInfo().equals(user3));
      } catch (Throwable t) {
        for (AssignProcedure proc : procedures) {
          LOG.debug(Objects.toString(proc));
        }
        throw t;
      }
    }
  }
}
