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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class })
public class TestBalancerStatusTagInJMXMetrics extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBalancerStatusTagInJMXMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBalancerStatusTagInJMXMetrics.class);
  private static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static int connectorPort = 61120;
  private static HMaster master;
  private static SingleProcessHBaseCluster cluster;
  private static Configuration conf = null;

  /**
   * Setup the environment for the test.
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 10; i++) {
      do {
        int sign = i % 2 == 0 ? 1 : -1;
        connectorPort += sign * rand.nextInt(100);
      } while (!HBaseTestingUtil.available(connectorPort));
      try {
        conf.setInt("regionserver.rmi.registry.port", connectorPort);
        cluster = UTIL.startMiniCluster();
        LOG.info("Waiting for active/ready master");
        cluster.waitForActiveAndReadyMaster();
        master = cluster.getMaster();
        break;
      } catch (Exception e) {
        LOG.debug("Encountered exception when starting mini cluster. Trying port " + connectorPort,
          e);
        try {
          // this is to avoid "IllegalStateException: A mini-cluster is already running"
          UTIL.shutdownMiniCluster();
        } catch (Exception ex) {
          LOG.debug("Encountered exception shutting down cluster", ex);
        }
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Tests the status change using the Default Metrics System
   */
  @Test
  public void testJmxMetrics() throws Exception {

    assertEquals(getStatus(), "true");
    master.getLoadBalancer().updateBalancerStatus(false);
    assertEquals(getStatus(), "false");

  }

  /**
   * Gets the balancer status tag from the Metrics registry
   */
  public String getStatus() throws Exception {
    MetricsSource source =
        DefaultMetricsSystem.instance().getSource(MetricsBalancerSource.METRICS_JMX_CONTEXT);
    if (source instanceof MetricsBalancerSourceImpl) {
      MetricsTag status = ((MetricsBalancerSourceImpl) source).getMetricsRegistry()
          .getTag(MetricsBalancerSource.BALANCER_STATUS);
      return status.value();
    } else {
      LOG.warn("Balancer JMX Metrics not registered");
      throw new Exception("MetricsBalancer JMX Context not found");
    }
  }

}
