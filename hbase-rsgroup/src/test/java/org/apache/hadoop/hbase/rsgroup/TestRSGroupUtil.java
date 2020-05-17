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

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRSGroupUtil {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static Admin admin;

  private static HMaster master;

  private static RSGroupAdminClient rsGroupAdminClient;

  private static final String GROUP_NAME = "rsg";

  private static final String NAMESPACE_NAME = "ns";

  private static RSGroupInfoManager rsGroupInfoManager;

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

    HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
    rsGroupAdminClient.addRSGroup(GROUP_NAME);
    rsGroupAdminClient.moveServers(Collections.singleton(rs.getServerName().getAddress()), GROUP_NAME);
    admin.createNamespace(NamespaceDescriptor.create(NAMESPACE_NAME)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, GROUP_NAME)
      .build());

    rsGroupInfoManager = RSGroupInfoManagerImpl.getInstance(master);
    rsGroupInfoManager.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    admin.deleteNamespace(NAMESPACE_NAME);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void rsGroupHasOnlineServer() throws IOException {
    rsGroupInfoManager.refresh();
    RSGroupInfo defaultGroup = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    Assert.assertTrue(RSGroupUtil.rsGroupHasOnlineServer(master, defaultGroup));

    RSGroupInfo rsGroup = rsGroupInfoManager.getRSGroup(GROUP_NAME);
    Assert.assertTrue(RSGroupUtil.rsGroupHasOnlineServer(master, rsGroup));

    rsGroupAdminClient.addRSGroup("empty");
    rsGroupInfoManager.refresh();
    RSGroupInfo emptyGroup = rsGroupInfoManager.getRSGroup("empty");
    Assert.assertFalse(RSGroupUtil.rsGroupHasOnlineServer(master, emptyGroup));
  }
}
