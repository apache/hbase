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

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.balancer.BalancerTestBase;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

@Category({ MiscTests.class, MediumTests.class })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestStochasticBalancerJmxMetrics extends BalancerTestBase {
  private static final Log LOG = LogFactory.getLog(TestStochasticBalancerJmxMetrics.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static int connectorPort = 61120;
  private static StochasticLoadBalancer loadBalancer;
  /**
   * a simple cluster for testing JMX.
   */
  private static int[] mockCluster_ensemble = new int[] { 0, 1, 2, 3 };
  private static int[] mockCluster_pertable_1 = new int[] { 0, 1, 2 };
  private static int[] mockCluster_pertable_2 = new int[] { 3, 1, 1 };
  private static int[] mockCluster_pertable_namespace = new int[] { 1, 3, 1 };

  private static final String TABLE_NAME_1 = "Table1";
  private static final String TABLE_NAME_2 = "Table2";
  private static final String TABLE_NAME_NAMESPACE = "hbase:namespace";

  private static Configuration conf = null;

  /**
   * Setup the environment for the test.
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf = UTIL.getConfiguration();

    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 0.75f);
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      do {
        int sign = i % 2 == 0 ? 1 : -1;
        connectorPort += sign * rand.nextInt(100);
      } while (!HBaseTestingUtility.available(connectorPort));
      try {
        conf.setInt("regionserver.rmi.registry.port", connectorPort);

        UTIL.startMiniCluster();
        break;
      } catch (Exception e) {
        LOG.debug("Encountered exception when starting cluster. Trying port " + connectorPort, e);
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
   * In Ensemble mode, there should be only one ensemble table
   */
  @Test
  public void testJmxMetrics_EnsembleMode() throws Exception {
    loadBalancer = new StochasticLoadBalancer();

    conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, false);
    loadBalancer.setConf(conf);

    TableName tableName = TableName.valueOf(HConstants.ENSEMBLE_TABLE_NAME);
    Map<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(mockCluster_ensemble);
    loadBalancer.balanceCluster(tableName, clusterState);

    String[] tableNames = new String[] { tableName.getNameAsString() };
    String[] functionNames = loadBalancer.getCostFunctionNames();
    Set<String> jmxMetrics = readJmxMetrics();
    Set<String> expectedMetrics = getExpectedJmxMetrics(tableNames, functionNames);

    // printMetrics(jmxMetrics, "existing metrics in ensemble mode");
    // printMetrics(expectedMetrics, "expected metrics in ensemble mode");

    // assert that every expected is in the JMX
    for (String expected : expectedMetrics) {
      assertTrue("Metric " + expected + " can not be found in JMX in ensemble mode.",
        jmxMetrics.contains(expected));
    }
  }

  /**
   * In per-table mode, each table has a set of metrics
   */
  @Test
  public void testJmxMetrics_PerTableMode() throws Exception {
    loadBalancer = new StochasticLoadBalancer();

    conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, true);
    loadBalancer.setConf(conf);

    // NOTE the size is normally set in setClusterStatus, for test purpose, we set it manually
    // Tables: hbase:namespace, table1, table2
    // Functions: costFunctions, overall
    String[] functionNames = loadBalancer.getCostFunctionNames();
    loadBalancer.updateMetricsSize(3 * (functionNames.length + 1));

    // table 1
    TableName tableName = TableName.valueOf(TABLE_NAME_1);
    Map<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(mockCluster_pertable_1);
    loadBalancer.balanceCluster(tableName, clusterState);

    // table 2
    tableName = TableName.valueOf(TABLE_NAME_2);
    clusterState = mockClusterServers(mockCluster_pertable_2);
    loadBalancer.balanceCluster(tableName, clusterState);

    // table hbase:namespace
    tableName = TableName.valueOf(TABLE_NAME_NAMESPACE);
    clusterState = mockClusterServers(mockCluster_pertable_namespace);
    loadBalancer.balanceCluster(tableName, clusterState);

    String[] tableNames = new String[] { TABLE_NAME_1, TABLE_NAME_2, TABLE_NAME_NAMESPACE };
    Set<String> jmxMetrics = readJmxMetrics();
    Set<String> expectedMetrics = getExpectedJmxMetrics(tableNames, functionNames);

    // printMetrics(jmxMetrics, "existing metrics in per-table mode");
    // printMetrics(expectedMetrics, "expected metrics in per-table mode");

    // assert that every expected is in the JMX
    for (String expected : expectedMetrics) {
      assertTrue("Metric " + expected + " can not be found in JMX in per-table mode.",
        jmxMetrics.contains(expected));
    }
  }

  /**
   * Read the attributes from Hadoop->HBase->Master->Balancer in JMX
   */
  private Set<String> readJmxMetrics() {
    JMXConnector connector = null;
    try {
      connector =
          JMXConnectorFactory.connect(JMXListener.buildJMXServiceURL(connectorPort, connectorPort));
      MBeanServerConnection mb = connector.getMBeanServerConnection();

      Hashtable<String, String> pairs = new Hashtable<>();
      pairs.put("service", "HBase");
      pairs.put("name", "Master");
      pairs.put("sub", "Balancer");
      ObjectName target = new ObjectName("Hadoop", pairs);
      MBeanInfo beanInfo = mb.getMBeanInfo(target);

      Set<String> existingAttrs = new HashSet<String>();
      for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
        existingAttrs.add(attrInfo.getName());
      }
      return existingAttrs;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connector != null) {
        try {
          connector.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  /**
   * Given the tables and functions, return metrics names that should exist in JMX
   */
  private Set<String> getExpectedJmxMetrics(String[] tableNames, String[] functionNames) {
    Set<String> ret = new HashSet<String>();

    for (String tableName : tableNames) {
      ret.add(StochasticLoadBalancer.composeAttributeName(tableName, "Overall"));
      for (String functionName : functionNames) {
        String metricsName = StochasticLoadBalancer.composeAttributeName(tableName, functionName);
        ret.add(metricsName);
      }
    }

    return ret;
  }

  private static void printMetrics(Set<String> metrics, String info) {
    if (null != info) LOG.info("++++ ------ " + info + " ------");

    LOG.info("++++ metrics count = " + metrics.size());
    for (String str : metrics) {
      LOG.info(" ++++ " + str);
    }
  }
}
