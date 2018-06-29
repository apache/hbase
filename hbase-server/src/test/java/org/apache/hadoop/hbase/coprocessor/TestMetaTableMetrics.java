/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.JMXListener;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({ CoprocessorTests.class, MediumTests.class })
public class TestMetaTableMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableMetrics.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final TableName NAME1 = TableName.valueOf("TestExampleMetaTableMetricsOne");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int NUM_ROWS = 5;
  private static final String value = "foo";
  private static Configuration conf = null;
  private static int connectorPort = 61120;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf = UTIL.getConfiguration();
    // Set system coprocessor so it can be applied to meta regions
    UTIL.getConfiguration().set("hbase.coprocessor.region.classes",
      MetaTableMetrics.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      do {
        int sign = i % 2 == 0 ? 1 : -1;
        connectorPort += sign * rand.nextInt(100);
      } while (!HBaseTestingUtility.available(connectorPort));
      try {
        conf.setInt("regionserver.rmi.registry.port", connectorPort);
        UTIL.startMiniCluster(1);
        UTIL.createTable(NAME1, new byte[][]{FAMILY});
        LOG.error("util to string" + UTIL.toString());
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
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void writeData(Table t) throws IOException {
    List<Put> puts = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Put p = new Put(Bytes.toBytes(i + 1));
      p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(value));
      puts.add(p);
    }
    t.put(puts);
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
   * Read the attributes from Hadoop->HBase->RegionServer->MetaTableMetrics in JMX
   * @throws IOException when fails to retrieve jmx metrics.
   */
  // this method comes from this class: TestStochasticBalancerJmxMetrics with minor modifications.
  private Set<String> readJmxMetrics() throws IOException {
    JMXConnector connector = null;
    ObjectName target = null;
    MBeanServerConnection mb = null;
    try {
      connector =
          JMXConnectorFactory.connect(JMXListener.buildJMXServiceURL(connectorPort, connectorPort));
      mb = connector.getMBeanServerConnection();

      @SuppressWarnings("JdkObsolete")
      Hashtable<String, String> pairs = new Hashtable<>();
      pairs.put("service", "HBase");
      pairs.put("name", "RegionServer");
      pairs.put("sub",
        "Coprocessor.Region.CP_org.apache.hadoop.hbase.coprocessor"
            + ".MetaTableMetrics");
      target = new ObjectName("Hadoop", pairs);
      MBeanInfo beanInfo = mb.getMBeanInfo(target);

      Set<String> existingAttrs = new HashSet<>();
      for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
        existingAttrs.add(attrInfo.getName());
      }
      return existingAttrs;
    } catch (Exception e) {
      LOG.warn("Failed to get bean." + target, e);
      if (mb != null) {
        Set<ObjectInstance> instances = mb.queryMBeans(null, null);
        Iterator<ObjectInstance> iterator = instances.iterator();
        LOG.warn("MBean Found:");
        while (iterator.hasNext()) {
          ObjectInstance instance = iterator.next();
          LOG.warn("Class Name: " + instance.getClassName());
          LOG.warn("Object Name: " + instance.getObjectName());
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

  // verifies meta table metrics exist from jmx
  // for one table, there should be 5 MetaTable_table_<TableName> metrics.
  // such as:
  // [Time-limited test] example.TestMetaTableMetrics(204): ==
  //    MetaTable_table_TestExampleMetaTableMetricsOne_request_count
  // [Time-limited test] example.TestMetaTableMetrics(204): ==
  //    MetaTable_table_TestExampleMetaTableMetricsOne_request_mean_rate
  // [Time-limited test] example.TestMetaTableMetrics(204): ==
  //    MetaTable_table_TestExampleMetaTableMetricsOne_request_1min_rate
  // [Time-limited test] example.TestMetaTableMetrics(204): ==
  //    MetaTable_table_TestExampleMetaTableMetricsOne_request_5min_rate
  // [Time-limited test] example.TestMetaTableMetrics(204): ==
  // MetaTable_table_TestExampleMetaTableMetricsOne_request_15min_rate
  @Test
  public void test() throws IOException, InterruptedException {
    try (Table t = UTIL.getConnection().getTable(NAME1)) {
      writeData(t);
      // Flush the data
      UTIL.flush(NAME1);
      // Issue a compaction
      UTIL.compact(NAME1, true);
      Thread.sleep(2000);
    }
    Set<String> jmxMetrics = readJmxMetricsWithRetry();
    assertNotNull(jmxMetrics);

    long name1TableMetricsCount = 0;
    for(String metric : jmxMetrics) {
      if (metric.contains("MetaTable_table_" + NAME1)){
        name1TableMetricsCount++;
      }
    }
    assertEquals(5L, name1TableMetricsCount);

    String putWithClientMetricNameRegex = "MetaTable_client_.+_put_request.*";
    long putWithClientMetricsCount = 0;
    for(String metric : jmxMetrics) {
      if(metric.matches(putWithClientMetricNameRegex)) {
        putWithClientMetricsCount++;
      }
    }
    assertEquals(5L, putWithClientMetricsCount);
  }

}
