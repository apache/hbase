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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestMasterReplicationTracker extends ReplicationTrackerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterReplicationTracker.class);

  private static Configuration CONF;

  private static Connection CONN;

  private static ChoreService CHORE_SERVICE;

  private static final class RegionServerListSnapshot {
    final List<ServerName> servers;

    final long hashCode;

    RegionServerListSnapshot(List<ServerName> servers) {
      this.servers = servers;
      this.hashCode = servers.hashCode();
    }

    RegionServerListSnapshot add(ServerName sn) {
      List<ServerName> newServers = new ArrayList<>(servers);
      newServers.add(sn);
      return new RegionServerListSnapshot(newServers);
    }

    RegionServerListSnapshot remove(ServerName sn) {
      return new RegionServerListSnapshot(
        servers.stream().filter(s -> !s.equals(sn)).collect(Collectors.toList()));
    }

    Pair<List<ServerName>, Long> toPair() {
      return new Pair<>(servers, hashCode);
    }
  }

  private static RegionServerListSnapshot SNAPSHOT =
    new RegionServerListSnapshot(Collections.emptyList());

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    CONF = HBaseConfiguration.create();
    CONF.setClass(ReplicationFactory.REPLICATION_TRACKER_IMPL, MasterReplicationTracker.class,
      ReplicationTracker.class);
    CONF.setInt(MasterReplicationTracker.REFRESH_INTERVAL_SECONDS, 1);
    Admin admin = mock(Admin.class);
    when(admin.syncRegionServers()).thenAnswer(i -> SNAPSHOT.toPair());
    when(admin.syncRegionServers(anyLong())).thenAnswer(i -> {
      long previousHashCode = i.getArgument(0, Long.class).longValue();
      RegionServerListSnapshot snapshot = SNAPSHOT;
      if (previousHashCode == snapshot.hashCode) {
        return Optional.empty();
      } else {
        return Optional.of(snapshot.toPair());
      }
    });
    CONN = mock(Connection.class);
    when(CONN.getAdmin()).thenReturn(admin);
    CHORE_SERVICE = new ChoreService("TestMasterReplicationTracker");
  }

  @AfterClass
  public static void tearDownAfterClass() {
    CHORE_SERVICE.shutdown();
  }

  @Override
  protected ReplicationTrackerParams createParams() {
    return ReplicationTrackerParams.create(CONF, new WarnOnlyStoppable())
      .abortable(new WarnOnlyAbortable()).connection(CONN).choreService(CHORE_SERVICE);
  }

  @Override
  protected void addServer(ServerName sn) throws Exception {
    SNAPSHOT = SNAPSHOT.add(sn);
  }

  @Override
  protected void removeServer(ServerName sn) throws Exception {
    SNAPSHOT = SNAPSHOT.remove(sn);
  }
}
