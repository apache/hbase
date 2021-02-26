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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl.RSGroupMappingScript;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link RSGroupInfoManager#determineRSGroupInfoForTable(TableName)}
 */
@Category({ MediumTests.class })
public class TestDetermineRSGroupInfoForTable {

  private static final Logger LOG = LoggerFactory.getLogger(TestDetermineRSGroupInfoForTable.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDetermineRSGroupInfoForTable.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static HMaster master;

  private static Admin admin;

  private static RSGroupInfoManager rsGroupInfoManager;

  private static RSGroupAdminClient rsGroupAdminClient;

  private static final String GROUP_NAME = "rsg";

  private static final String NAMESPACE_NAME = "ns";
  private static final String OTHER_NAMESPACE_NAME = "other";

  private static final TableName TABLE_NAME = TableName.valueOf(NAMESPACE_NAME, "tb");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().set(
      HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());
    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName());
    UTIL.startMiniCluster(5);
    master = UTIL.getMiniHBaseCluster().getMaster();
    admin = UTIL.getAdmin();
    rsGroupAdminClient = new RSGroupAdminClient(UTIL.getConnection());

    UTIL.waitFor(60000, (Predicate<Exception>) () ->
        master.isInitialized() && ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline());

    List<RSGroupAdminEndpoint> cps =
        master.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class);
    assertTrue(cps.size() > 0);
    rsGroupInfoManager = cps.get(0).getGroupInfoManager();

    HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
    rsGroupAdminClient.addRSGroup(GROUP_NAME);
    rsGroupAdminClient.moveServers(
      Collections.singleton(rs.getServerName().getAddress()), GROUP_NAME);
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE_NAME)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, GROUP_NAME)
      .build());
    admin.createNamespace(NamespaceDescriptor.create(OTHER_NAMESPACE_NAME).build());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    admin.deleteNamespace(NAMESPACE_NAME);

    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testByDefault() throws IOException {
    RSGroupInfo group =
      rsGroupInfoManager.determineRSGroupInfoForTable(TableName.valueOf("tb"));
    assertEquals(group.getName(), RSGroupInfo.DEFAULT_GROUP);
  }

  @Test
  public void testDetermineByNamespaceConfig() throws IOException {
    RSGroupInfo group = rsGroupInfoManager.determineRSGroupInfoForTable(TABLE_NAME);
    assertEquals(group.getName(), GROUP_NAME);

    group = rsGroupInfoManager.determineRSGroupInfoForTable(
      TableName.valueOf(OTHER_NAMESPACE_NAME, "tb"));
    assertEquals(group.getName(), RSGroupInfo.DEFAULT_GROUP);
  }

  /**
   * determine by script
   */
  @Test
  public void testDetermineByScript() throws IOException {
    RSGroupMappingScript script = mock(RSGroupMappingScript.class);
    when(script.getRSGroup(anyString(), anyString())).thenReturn(GROUP_NAME);
    RSGroupMappingScript oldScript = ((RSGroupInfoManagerImpl) rsGroupInfoManager).script;
    ((RSGroupInfoManagerImpl) rsGroupInfoManager).script = script;

    RSGroupInfo group = rsGroupInfoManager.determineRSGroupInfoForTable(TABLE_NAME);
    assertEquals(group.getName(), GROUP_NAME);
    // reset script to avoid affecting other tests
    ((RSGroupInfoManagerImpl) rsGroupInfoManager).script = oldScript;
  }

}
