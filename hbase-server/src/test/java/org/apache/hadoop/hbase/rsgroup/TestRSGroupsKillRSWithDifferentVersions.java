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
package org.apache.hadoop.hbase.rsgroup;

import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.MetaRWQueueRpcExecutor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(RSGroupTests.TAG)
@Tag(LargeTests.TAG)
public class TestRSGroupsKillRSWithDifferentVersions extends TestRSGroupsBase {

  private static final String LOWER_VERSION = "0.0.0";

  public static final class HMasterForTest extends HMaster {

    private volatile Address serverWithLowerVersion;

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    void setServerWithLowerVersion(Address server) {
      serverWithLowerVersion = server;
    }

    @Override
    public String getRegionServerVersion(ServerName serverName) {
      return serverName.getAddress().equals(serverWithLowerVersion)
        ? LOWER_VERSION
        : super.getRegionServerVersion(serverName);
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class,
      HMaster.class);
    // avoid all the handlers blocked when meta is offline, and regionServerReport can not be
    // processed which causes dead lock.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    TEST_UTIL.getConfiguration()
      .setFloat(MetaRWQueueRpcExecutor.META_CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
    setUpTestBeforeClass();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    try {
      tearDownAfterClass();
    } finally {
      TEST_UTIL.getConfiguration().unset(HConstants.MASTER_IMPL);
    }
  }

  @BeforeEach
  public void beforeMethod(TestInfo testInfo) throws Exception {
    setUpBeforeMethod(testInfo);
  }

  @AfterEach
  public void afterMethod() throws Exception {
    ((HMasterForTest) MASTER).setServerWithLowerVersion(null);
    tearDownAfterMethod();
  }

  @Test
  public void testLowerMetaGroupVersion() throws Exception {
    String groupName = "meta_group";
    addGroup(groupName, 1);

    Set<TableName> toAddTables = new HashSet<>();
    toAddTables.add(TableName.META_TABLE_NAME);
    ADMIN.setRSGroup(toAddTables, groupName);
    assertTrue(ADMIN.getConfiguredNamespacesAndTablesInRSGroup(groupName).getSecond()
      .contains(TableName.META_TABLE_NAME));

    Address address = ADMIN.getRSGroup(groupName).getServers().iterator().next();
    ServerName serverName = getServerName(address);
    String originVersion = MASTER.getRegionServerVersion(serverName);
    TEST_UTIL.getMiniHBaseCluster().stopRegionServer(serverName);

    // better wait for a while for region reassign
    sleep(10000);
    assertEquals(NUM_SLAVES_BASE - 1,
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    ((HMasterForTest) MASTER).setServerWithLowerVersion(address);
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(address.getHostName(), address.getPort());
    assertEquals(NUM_SLAVES_BASE,
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    assertTrue(VersionInfo.compareVersion(originVersion,
      MASTER.getRegionServerVersion(getServerName(address))) > 0);
    LOG.debug("wait for META assigned...");
    // SCP finished, which means all regions assigned too.
    TEST_UTIL.waitFor(60000, () -> !TEST_UTIL.getHBaseCluster().getMaster().getProcedures().stream()
      .anyMatch(p -> p instanceof ServerCrashProcedure));
  }
}
