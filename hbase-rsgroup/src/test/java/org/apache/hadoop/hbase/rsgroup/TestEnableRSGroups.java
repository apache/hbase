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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test enable RSGroup
 */
@Category({ MediumTests.class })
public class TestEnableRSGroups {

  protected static final Logger LOG = LoggerFactory.getLogger(TestEnableRSGroups.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testEnableRSGroups() throws IOException, InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().stopMaster(0);
    LOG.info("stopped master...");
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getMiniHBaseCluster().setConf(conf);

    TEST_UTIL.getMiniHBaseCluster().startMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000);
    LOG.info("started master...");

    // check if master started successfully
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 60000, new ExplainingPredicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return TEST_UTIL.getMiniHBaseCluster().getMaster() != null;
      }

      @Override
      public String explainFailure() throws IOException {
        return "Master failed to start up";
      }
    });

    // wait RSGroupBasedLoadBalancer online
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 60000, new ExplainingPredicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        RSGroupBasedLoadBalancer loadBalancer =
            (RSGroupBasedLoadBalancer) TEST_UTIL.getMiniHBaseCluster().getMaster().getLoadBalancer();
        return loadBalancer != null && loadBalancer.isOnline();
      }

      @Override
      public String explainFailure() throws IOException {
        return "RSGroupBasedLoadBalancer failed to come online";
      }
    });
  }

}
