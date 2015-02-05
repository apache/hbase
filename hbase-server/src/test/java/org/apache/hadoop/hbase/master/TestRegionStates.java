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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import static org.junit.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionStates {
  @Test (timeout=10000)
  public void testCanMakeProgressThoughMetaIsDown()
  throws IOException, InterruptedException, BrokenBarrierException {
    Server server = mock(Server.class);
    when(server.getServerName()).thenReturn(ServerName.valueOf("master,1,1"));
    Connection connection = mock(ClusterConnection.class);
    // Set up a table that gets 'stuck' when we try to fetch a row from the meta table.
    // It is stuck on a CyclicBarrier latch. We use CyclicBarrier because it will tell us when
    // thread is waiting on latch.
    Table metaTable = Mockito.mock(Table.class);
    final CyclicBarrier latch = new CyclicBarrier(2);
    when(metaTable.get((Get)Mockito.any())).thenAnswer(new Answer<Result>() {
      @Override
      public Result answer(InvocationOnMock invocation) throws Throwable {
        latch.await();
        throw new java.net.ConnectException("Connection refused");
      }
    });
    when(connection.getTable(TableName.META_TABLE_NAME)).thenReturn(metaTable);
    when(server.getConnection()).thenReturn((ClusterConnection)connection);
    Configuration configuration = mock(Configuration.class);
    when(server.getConfiguration()).thenReturn(configuration);
    TableStateManager tsm = mock(TableStateManager.class);
    ServerManager sm = mock(ServerManager.class);
    when(sm.isServerOnline(isA(ServerName.class))).thenReturn(true);

    RegionStateStore rss = mock(RegionStateStore.class);
    final RegionStates regionStates = new RegionStates(server, tsm, sm, rss);
    final ServerName sn = mockServer("one", 1);
    regionStates.updateRegionState(HRegionInfo.FIRST_META_REGIONINFO, State.SPLITTING_NEW, sn);
    Thread backgroundThread = new Thread("Get stuck setting server offline") {
      @Override
      public void run() {
        regionStates.serverOffline(sn);
      }
    };
    assertTrue(latch.getNumberWaiting() == 0);
    backgroundThread.start();
    while (latch.getNumberWaiting() == 0);
    // Verify I can do stuff with synchronized RegionStates methods, that I am not locked out.
    // Below is a call that is synchronized.  Can I do it and not block?
    regionStates.getRegionServerOfRegion(HRegionInfo.FIRST_META_REGIONINFO);
    // Done. Trip the barrier on the background thread.
    latch.await();
  }

  @Test
  public void testWeDontReturnDrainingServersForOurBalancePlans() throws Exception {
    Server server = mock(Server.class);
    when(server.getServerName()).thenReturn(ServerName.valueOf("master,1,1"));
    Configuration configuration = mock(Configuration.class);
    when(server.getConfiguration()).thenReturn(configuration);
    TableStateManager tsm = mock(TableStateManager.class);
    ServerManager sm = mock(ServerManager.class);
    when(sm.isServerOnline(isA(ServerName.class))).thenReturn(true);

    RegionStateStore rss = mock(RegionStateStore.class);
    RegionStates regionStates = new RegionStates(server, tsm, sm, rss);

    ServerName one = mockServer("one", 1);
    ServerName two = mockServer("two", 1);
    ServerName three = mockServer("three", 1);

    when(sm.getDrainingServersList()).thenReturn(Arrays.asList(three));

    regionStates.regionOnline(createFakeRegion(), one);
    regionStates.regionOnline(createFakeRegion(), two);
    regionStates.regionOnline(createFakeRegion(), three);


    Map<TableName, Map<ServerName, List<HRegionInfo>>> result =
        regionStates.getAssignmentsByTable();
    for (Map<ServerName, List<HRegionInfo>> map : result.values()) {
      assertFalse(map.keySet().contains(three));
    }
  }

  private HRegionInfo createFakeRegion() {
    HRegionInfo info = mock(HRegionInfo.class);
    when(info.getEncodedName()).thenReturn(UUID.randomUUID().toString());
    return info;
  }

  private ServerName mockServer(String fakeHost, int fakePort) {
    ServerName serverName = mock(ServerName.class);
    when(serverName.getHostname()).thenReturn(fakeHost);
    when(serverName.getPort()).thenReturn(fakePort);
    return serverName;
  }
}
