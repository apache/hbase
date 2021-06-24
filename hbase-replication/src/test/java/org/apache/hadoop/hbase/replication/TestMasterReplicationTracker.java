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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
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

  private static List<ServerName> SERVERS = new CopyOnWriteArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    CONF = HBaseConfiguration.create();
    CONF.setClass(ReplicationFactory.REPLICATION_TRACKER_IMPL, MasterReplicationTracker.class,
      ReplicationTracker.class);
    Admin admin = mock(Admin.class);
    when(admin.getRegionServers()).thenReturn(SERVERS);
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
    return ReplicationTrackerParams.create(CONF, new WarnOnlyStoppable()).connection(CONN)
      .choreService(CHORE_SERVICE);
  }

  @Override
  protected void addServer(ServerName sn) throws Exception {
    SERVERS.add(sn);
  }

  @Override
  protected void removeServer(ServerName sn) throws Exception {
    SERVERS.remove(sn);
  }
}
