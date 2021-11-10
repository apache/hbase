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
package org.apache.hadoop.hbase.regionserver;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestBootstrapNodeManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBootstrapNodeManager.class);

  private Configuration conf;

  private AsyncClusterConnection conn;

  private MasterAddressTracker tracker;

  private BootstrapNodeManager manager;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    conf.setLong(BootstrapNodeManager.REQUEST_MASTER_INTERVAL_SECS, 5);
    conf.setLong(BootstrapNodeManager.REQUEST_MASTER_MIN_INTERVAL_SECS, 1);
    conf.setLong(BootstrapNodeManager.REQUEST_REGIONSERVER_INTERVAL_SECS, 1);
    conf.setInt(HBaseRpcServicesBase.CLIENT_BOOTSTRAP_NODE_LIMIT, 2);
    conn = mock(AsyncClusterConnection.class);
    when(conn.getConfiguration()).thenReturn(conf);
    tracker = mock(MasterAddressTracker.class);
  }

  @After
  public void tearDown() {
    if (manager != null) {
      manager.stop();
    }
  }

  private void assertListEquals(List<ServerName> expected, List<ServerName> actual) {
    assertEquals(expected.size(), expected.size());
    assertThat(actual, hasItems(expected.toArray(new ServerName[0])));
  }

  @Test
  public void testNormal() throws Exception {
    List<ServerName> regionServers =
      Arrays.asList(ServerName.valueOf("server1", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server2", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server3", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server4", 12345, EnvironmentEdgeManager.currentTime()));
    when(conn.getLiveRegionServers(any(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(regionServers));
    when(conn.getAllBootstrapNodes(any()))
      .thenReturn(CompletableFuture.completedFuture(regionServers));
    manager = new BootstrapNodeManager(conn, tracker);
    Thread.sleep(3000);
    verify(conn, times(1)).getLiveRegionServers(any(), anyInt());
    verify(conn, atLeastOnce()).getAllBootstrapNodes(any());
    assertListEquals(regionServers, manager.getBootstrapNodes());
  }

  // if we do not return enough region servers, we will always get from master
  @Test
  public void testOnlyMaster() throws Exception {
    List<ServerName> regionServers =
      Arrays.asList(ServerName.valueOf("server1", 12345, EnvironmentEdgeManager.currentTime()));
    when(conn.getLiveRegionServers(any(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(regionServers));
    when(conn.getAllBootstrapNodes(any()))
      .thenReturn(CompletableFuture.completedFuture(regionServers));
    manager = new BootstrapNodeManager(conn, tracker);
    Thread.sleep(3000);
    verify(conn, atLeast(2)).getLiveRegionServers(any(), anyInt());
    verify(conn, never()).getAllBootstrapNodes(any());
    assertListEquals(regionServers, manager.getBootstrapNodes());
  }

  @Test
  public void testRegionServerError() throws Exception {
    List<ServerName> regionServers =
      Arrays.asList(ServerName.valueOf("server1", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server2", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server3", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server4", 12345, EnvironmentEdgeManager.currentTime()));
    List<ServerName> newRegionServers =
      Arrays.asList(ServerName.valueOf("server5", 12345, EnvironmentEdgeManager.currentTime()),
        ServerName.valueOf("server6", 12345, EnvironmentEdgeManager.currentTime()));
    when(conn.getLiveRegionServers(any(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(regionServers));
    when(conn.getAllBootstrapNodes(any())).thenAnswer(invocation -> {
      if (invocation.getArgument(0, ServerName.class).getHostname().equals("server4")) {
        return FutureUtils.failedFuture(new IOException("Inject error"));
      } else {
        return CompletableFuture.completedFuture(regionServers.subList(0, 3));
      }
    });
    manager = new BootstrapNodeManager(conn, tracker);
    // we should remove server4 from the list
    Waiter.waitFor(conf, 30000, () -> manager.getBootstrapNodes().size() == 3);
    assertListEquals(regionServers.subList(0, 3), manager.getBootstrapNodes());
    when(conn.getLiveRegionServers(any(), anyInt()))
      .thenReturn(CompletableFuture.completedFuture(newRegionServers));
    doAnswer(invocation -> {
      String hostname = invocation.getArgument(0, ServerName.class).getHostname();
      switch (hostname) {
        case "server1":
          return CompletableFuture.completedFuture(regionServers.subList(0, 1));
        case "server2":
        case "server3":
          return FutureUtils.failedFuture(new IOException("Inject error"));
        default:
          return CompletableFuture.completedFuture(newRegionServers);
      }
    }).when(conn).getAllBootstrapNodes(any());
    // we should remove server2, server3 from the list and then get the new list from master again
    Waiter.waitFor(conf, 30000, () -> {
      List<ServerName> bootstrapNodes = manager.getBootstrapNodes();
      if (bootstrapNodes.size() != 2) {
        return false;
      }
      String hostname = bootstrapNodes.get(0).getHostname();
      return hostname.equals("server5") || hostname.equals("server6");
    });
    assertListEquals(newRegionServers, manager.getBootstrapNodes());
  }
}
