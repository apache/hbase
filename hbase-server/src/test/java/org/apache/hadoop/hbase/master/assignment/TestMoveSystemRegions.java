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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestMoveSystemRegions {

  private static final ServerName OLD_SERVER = ServerName.valueOf("old", 16020, 1);
  private static final ServerName OTHER_OLD_SERVER = ServerName.valueOf("other-old", 16020, 1);
  private static final ServerName NEW_SERVER = ServerName.valueOf("new", 16020, 1);
  private static final RegionInfo META = RegionInfoBuilder.FIRST_META_REGIONINFO;
  private static final RegionInfo SYSTEM_REGION =
    RegionInfoBuilder.newBuilder(TableName.valueOf("hbase:test")).build();

  private AssignmentManager am;
  private ServerManager serverManager;
  private final List<RegionInfo> movedRegions = new ArrayList<>();

  @BeforeEach
  public void setUp() throws Exception {
    MasterServices master = mock(MasterServices.class);
    when(master.getConfiguration()).thenReturn(new Configuration(false));
    serverManager = mock(ServerManager.class);
    when(master.getServerManager()).thenReturn(serverManager);
    when(serverManager.countOfRegionServers()).thenReturn(3);
    when(serverManager.getOnlineServersList())
      .thenReturn(List.of(OLD_SERVER, OTHER_OLD_SERVER, NEW_SERVER));
    when(serverManager.isServerOnline(any())).thenReturn(true);
    when(master.getRegionServerVersion(OLD_SERVER)).thenReturn("2.6.4");
    when(master.getRegionServerVersion(OTHER_OLD_SERVER)).thenReturn("2.6.4");
    when(master.getRegionServerVersion(NEW_SERVER)).thenReturn("4.0.0");
    am = spy(new AssignmentManager(master, null));
    doAnswer(invocation -> {
      RegionPlan plan = invocation.getArgument(0);
      movedRegions.add(plan.getRegionInfo());
      return CompletableFuture.completedFuture(null);
    }).when(am).moveAsync(any());
  }

  private RegionStateNode addRegion(RegionInfo region, ServerName server) {
    RegionStateNode node = am.getRegionStates().getOrCreateRegionStateNode(region);
    node.setState(State.OPEN);
    node.setRegionLocation(server);
    am.getRegionStates().createServer(server);
    am.getRegionStates().addRegionToServer(node);
    return node;
  }

  private void checkSystemRegions() throws Exception {
    CompletableFuture<Thread> checker = new CompletableFuture<>();
    doAnswer(invocation -> {
      checker.complete(Thread.currentThread());
      return invocation.callRealMethod();
    }).when(am).getExcludedServersForSystemTable();
    am.checkIfShouldMoveSystemRegionAsync();
    Thread thread = checker.get(10, TimeUnit.SECONDS);
    thread.join(TimeUnit.SECONDS.toMillis(10));
    assertFalse(thread.isAlive(), "System region check did not finish");
  }

  @Test
  public void testSkipMetaInTransition() throws Exception {
    RegionStateNode meta = addRegion(META, OLD_SERVER);
    meta.setState(State.OPENING);
    TransitRegionStateProcedure recovery = mock(TransitRegionStateProcedure.class);
    meta.setProcedure(recovery);
    addRegion(SYSTEM_REGION, OLD_SERVER);
    // Exercise the real preTransitCheck if the compatibility check tries to move meta.
    doCallRealMethod().when(am).moveAsync(argThat(p -> p.getRegionInfo().isMetaRegion()));

    checkSystemRegions();

    assertEquals(List.of(SYSTEM_REGION), movedRegions);
    assertSame(recovery, meta.getProcedure());
    assertEquals(State.OPENING, meta.getState());
    verify(am, never()).moveAsync(argThat(p -> p.getRegionInfo().isMetaRegion()));
  }

  @Test
  public void testSubmitEachPlanOnceAndMetaFirst() throws Exception {
    addRegion(SYSTEM_REGION, OLD_SERVER);
    addRegion(META, OTHER_OLD_SERVER);

    checkSystemRegions();

    assertEquals(List.of(META, SYSTEM_REGION), movedRegions);
    verify(am, times(2)).moveAsync(any());
  }

  @Test
  public void testSkipOfflineSource() throws Exception {
    addRegion(META, OLD_SERVER);
    addRegion(SYSTEM_REGION, OTHER_OLD_SERVER);
    // The online-server snapshot can become stale before we examine its regions.
    when(serverManager.isServerOnline(OLD_SERVER)).thenReturn(false);

    checkSystemRegions();

    assertEquals(List.of(SYSTEM_REGION), movedRegions);
  }

  @Test
  public void testSkipChangedRegionLocation() throws Exception {
    RegionStateNode meta = addRegion(META, OLD_SERVER);
    meta.setRegionLocation(NEW_SERVER);
    addRegion(SYSTEM_REGION, OTHER_OLD_SERVER);

    checkSystemRegions();

    assertEquals(List.of(SYSTEM_REGION), movedRegions);
  }

  @Test
  public void testContinueAfterMoveFailure() throws Exception {
    addRegion(META, OLD_SERVER);
    addRegion(SYSTEM_REGION, OLD_SERVER);
    doThrow(new HBaseIOException("Region entered transition after the check")).when(am)
      .moveAsync(argThat(p -> p.getRegionInfo().isMetaRegion()));

    checkSystemRegions();

    assertEquals(List.of(SYSTEM_REGION), movedRegions);
  }
}
