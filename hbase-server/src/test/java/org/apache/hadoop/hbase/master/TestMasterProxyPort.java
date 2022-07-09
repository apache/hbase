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

package org.apache.hadoop.hbase.master;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterProxyPort {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterProxyPort.class);

  private static HBaseTestingUtil TEST_UTIL1;
  private static HBaseTestingUtil TEST_UTIL2;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL1 = new HBaseTestingUtil();
    TEST_UTIL2 = new HBaseTestingUtil();
  }

  @Test
  public void testProxyPortPublishByMaster() throws Exception {
    TEST_UTIL1.startMiniCluster();
    Optional<ServerName> activeMaster1 =
      TEST_UTIL1.getMiniHBaseCluster().getRegionServer(0).getActiveMaster();
    Configuration conf = TEST_UTIL2.getConfiguration();
    conf.setInt(HMaster.MASTER_PROXY_PORT_EXPOSE, activeMaster1.get().getPort());
    TEST_UTIL2.startMiniCluster();
    Optional<ServerName> activeMaster2 =
      TEST_UTIL2.getMiniHBaseCluster().getRegionServer(0).getActiveMaster();
    Assert.assertNotEquals(activeMaster1.get().getPort(), activeMaster2.get().getPort());
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL2.shutdownMiniCluster();
  }

  @Test
  public void testProxyPortConsume() throws Exception {
    TEST_UTIL1.startMiniCluster();
    Optional<ServerName> activeMaster1 =
      TEST_UTIL1.getMiniHBaseCluster().getRegionServer(0).getActiveMaster();
    Configuration conf = TEST_UTIL2.getConfiguration();
    conf.setBoolean(HConstants.CONSUME_MASTER_PROXY_PORT, true);
    TEST_UTIL2.startMiniCluster();
    Optional<ServerName> activeMaster2 =
      TEST_UTIL2.getMiniHBaseCluster().getRegionServer(0).getActiveMaster();
    Assert.assertNotEquals(activeMaster1.get().getPort(), activeMaster2.get().getPort());
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL2.shutdownMiniCluster();
  }

  @Test
  public void testProxyPortPublishAndConsume() throws Exception {
    TEST_UTIL1.startMiniCluster();
    Optional<ServerName> activeMaster1 =
      TEST_UTIL1.getMiniHBaseCluster().getRegionServer(0).getActiveMaster();
    Configuration conf = TEST_UTIL2.getConfiguration();
    conf.setInt(HMaster.MASTER_PROXY_PORT_EXPOSE, activeMaster1.get().getPort());
    conf.setBoolean(HConstants.CONSUME_MASTER_PROXY_PORT, true);
    ExecutorService executorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("testProxyPortPublishAndConsume-%d").setDaemon(true)
        .build());
    executorService.submit(() -> TEST_UTIL2.startMiniCluster());
    for (int i = 0; i < 25; i++) {
      Thread.sleep(1000);
      // Cluster2 is not going to come up because Cluster2 RS is trying to connect to
      // Cluster1 master port.
      Assert.assertNull(TEST_UTIL2.getMiniHBaseCluster());
    }
    TEST_UTIL2.shutdownMiniCluster();
    executorService.shutdown();
  }

}
