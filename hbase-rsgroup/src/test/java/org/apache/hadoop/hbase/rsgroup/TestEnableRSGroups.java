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

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test enable RSGroup
 */
@Category({ MediumTests.class })
public class TestEnableRSGroups {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEnableRSGroups.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestEnableRSGroups.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(5);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    LOG.info("to stop miniCluster");
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testEnableRSGroup() throws IOException, InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().stopMaster(0);
    TEST_UTIL.getMiniHBaseCluster().waitOnMaster(0);

    LOG.info("stopped master...");
    final Configuration conf = TEST_UTIL.getMiniHBaseCluster().getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());

    TEST_UTIL.getMiniHBaseCluster().startMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000);
    LOG.info("started master...");

    // check if master started successfully
    assertTrue(TEST_UTIL.getMiniHBaseCluster().getMaster() != null);

    // wait RSGroupBasedLoadBalancer online
    RSGroupBasedLoadBalancer loadBalancer =
        (RSGroupBasedLoadBalancer) TEST_UTIL.getMiniHBaseCluster().getMaster().getLoadBalancer();
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start <= 60000 && !loadBalancer.isOnline()) {
      LOG.info("waiting for rsgroup load balancer onLine...");
      sleep(200);
    }

    assertTrue(loadBalancer.isOnline());
  }

}
