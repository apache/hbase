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
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static junit.framework.Assert.assertFalse;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestRegionStates {


  @Test
  public void testWeDontReturnDrainingServersForOurBalancePlans() throws Exception {
    MasterServices server = mock(MasterServices.class);
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
