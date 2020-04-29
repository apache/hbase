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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test metrics incremented on region server operations.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestClusterId {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClusterId.class);

  private final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private JVMClusterUtil.RegionServerThread rst;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ShutdownHook.RUN_SHUTDOWN_HOOK, false);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if(rst != null && rst.getRegionServer() != null) {
      rst.getRegionServer().stop("end of test");
      rst.join();
    }
  }

  @Test
  public void testClusterId() throws Exception  {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    //start region server, needs to be separate
    //so we get an unset clusterId
    rst = JVMClusterUtil.createRegionServerThread(conf, HRegionServer.class, 0);
    rst.start();
    //Make sure RS is in blocking state
    Thread.sleep(10000);

    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(1).numRegionServers(0).build();
    TEST_UTIL.startMiniHBaseCluster(option);

    rst.waitForServerOnline();

    String clusterId = ZKClusterId.readClusterIdZNode(TEST_UTIL.getZooKeeperWatcher());
    assertNotNull(clusterId);
    assertEquals(clusterId, rst.getRegionServer().getClusterId());
  }

  @Test
  public void testRewritingClusterIdToPB() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    TEST_UTIL.createRootDir();
    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(TEST_UTIL.getConfiguration());
    Path filePath = new Path(rootDir, HConstants.CLUSTER_ID_FILE_NAME);
    FSDataOutputStream s = null;
    try {
      s = fs.create(filePath);
      s.writeUTF(HBaseCommonTestingUtility.getRandomUUID().toString());
    } finally {
      if (s != null) {
        s.close();
      }
    }
    TEST_UTIL.startMiniHBaseCluster();
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    int expected = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration())? 2: 1;
    assertEquals(expected, master.getServerManager().getOnlineServersList().size());
  }

}

