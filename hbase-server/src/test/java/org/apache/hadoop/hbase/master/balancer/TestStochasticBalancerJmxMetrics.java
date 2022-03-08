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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.JMXListener;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, MediumTests.class })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class TestStochasticBalancerJmxMetrics extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticBalancerJmxMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStochasticBalancerJmxMetrics.class);
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
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    Random rand = ThreadLocalRandom.current();
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

    TableName tableName = HConstants.ENSEMBLE_TABLE_NAME;
    Map<ServerName, List<RegionInfo>> clusterState = mockClusterServers(mockCluster_ensemble);
    loadBalancer.balanceTable(tableName, clusterState);

    String[] tableNames = new String[] { tableName.getNameAsString() };
    String[] functionNames = loadBalancer.getCostFunctionNames();
    Set<String> jmxMetrics = readJmxMetricsWithRetry();
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

    // NOTE the size is normally set in setClusterMetrics, for test purpose, we set it manually
    // Tables: hbase:namespace, table1, table2
    // Functions: costFunctions, overall
    String[] functionNames = loadBalancer.getCostFunctionNames();
    loadBalancer.updateMetricsSize(3 * (functionNames.length + 1));

    // table 1
    TableName tableName = TableName.valueOf(TABLE_NAME_1);
    Map<ServerName, List<RegionInfo>> clusterState = mockClusterServers(mockCluster_pertable_1);
    loadBalancer.balanceTable(tableName, clusterState);

    // table 2
    tableName = TableName.valueOf(TABLE_NAME_2);
    clusterState = mockClusterServers(mockCluster_pertable_2);
    loadBalancer.balanceTable(tableName, clusterState);

    // table hbase:namespace
    tableName = TableName.valueOf(TABLE_NAME_NAMESPACE);
    clusterState = mockClusterServers(mockCluster_pertable_namespace);
    loadBalancer.balanceTable(tableName, clusterState);

    String[] tableNames = new String[] { TABLE_NAME_1, TABLE_NAME_2, TABLE_NAME_NAMESPACE };
    Set<String> jmxMetrics = readJmxMetricsWithRetry();
    Set<String> expectedMetrics = getExpectedJmxMetrics(tableNames, functionNames);

    // printMetrics(jmxMetrics, "existing metrics in per-table mode");
    // printMetrics(expectedMetrics, "expected metrics in per-table mode");

    // assert that every expected is in the JMX
    for (String expected : expectedMetrics) {
      assertTrue("Metric " + expected + " can not be found in JMX in per-table mode.",
        jmxMetrics.contains(expected));
    }
  }

  private Set<String> readJmxMetricsWithRetry() throws IOException {
    final int count = 0;
    for (int i = 0; i < 10; i++) {
      Set<String> metrics = readJmxMetrics();
      if (metrics != null) {
        return metrics;
      }
      LOG.warn("Failed to get jmxmetrics... sleeping, retrying; " + i + " of " + count + " times");
      Threads.sleep(1000);
    }
    return null;
  }

  /**
   * Read the attributes from Hadoop->HBase->Master->Balancer in JMX
   */
  private Set<String> readJmxMetrics() throws IOException {
    JMXConnector connector = null;
    ObjectName target = null;
    MBeanServerConnection mb = null;
    try {
      connector =
          JMXConnectorFactory.connect(JMXListener.buildJMXServiceURL(connectorPort, connectorPort));
      mb = connector.getMBeanServerConnection();

      Hashtable<String, String> pairs = new Hashtable<>();
      pairs.put("service", "HBase");
      pairs.put("name", "Master");
      pairs.put("sub", "Balancer");
      target = new ObjectName("Hadoop", pairs);
      MBeanInfo beanInfo = mb.getMBeanInfo(target);

      Set<String> existingAttrs = new HashSet<>();
      for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
        existingAttrs.add(attrInfo.getName());
      }
      return existingAttrs;
    } catch (Exception e) {
      LOG.warn("Failed to get bean!!! " + target, e);
      if (mb != null) {
        Set<ObjectInstance> instances = mb.queryMBeans(null, null);
        Iterator<ObjectInstance> iterator = instances.iterator();
        System.out.println("MBean Found:");
        while (iterator.hasNext()) {
          ObjectInstance instance = iterator.next();
          System.out.println("Class Name: " + instance.getClassName());
          System.out.println("Object Name: " + instance.getObjectName());
        }
      }
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
    Set<String> ret = new HashSet<>();

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
    if (null != info) {
      LOG.info("++++ ------ " + info + " ------");
    }

    LOG.info("++++ metrics count = " + metrics.size());
    for (String str : metrics) {
      LOG.info(" ++++ " + str);
    }
  }
}
