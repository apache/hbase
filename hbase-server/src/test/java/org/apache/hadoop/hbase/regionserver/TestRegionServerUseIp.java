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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionServerUseIp {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerUseIp.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionServerUseIp.class);

  private HBaseTestingUtil TEST_UTIL;
  private SingleProcessHBaseCluster CLUSTER;

  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 1;

  @Before
  public void setup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_SERVER_USEIP_ENABLED_KEY, true);
    TEST_UTIL = new HBaseTestingUtil(conf);
    StartTestingClusterOption option = StartTestingClusterOption.builder().numMasters(NUM_MASTERS)
      .numRegionServers(NUM_RS).numDataNodes(NUM_RS).build();
    CLUSTER = TEST_UTIL.startMiniCluster(option);
  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRegionServerUseIp() throws Exception {
    String hostname = CLUSTER.getRegionServer(0).getServerName().getHostname();
    String ip = InetAddress.getByName(hostname).getHostAddress();
    LOG.info("hostname= " + hostname + " ,ip=" + ip);
    assertEquals(ip, hostname);
  }
}
