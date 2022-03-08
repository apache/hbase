/**
 *
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

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.hadoop.hbase.JMXListener;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.AllOf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ CoprocessorTests.class, LargeTests.class })
public class TestMetaTableMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaTableMetrics.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableMetrics.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final TableName NAME1 = TableName.valueOf("TestExampleMetaTableMetricsOne");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final ColumnFamilyDescriptor CFD =
      ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build();
  private static final int NUM_ROWS = 5;
  private static final String value = "foo";
  private static final String METRICS_ATTRIBUTE_NAME_PREFIX = "MetaTable_";
  private static final List<String> METRICS_ATTRIBUTE_NAME_POSTFIXES =
      Arrays.asList("_count", "_mean_rate", "_1min_rate", "_5min_rate", "_15min_rate");
  private static int connectorPort = 61120;

  private final byte[] cf = Bytes.toBytes("info");
  private final byte[] col = Bytes.toBytes("any");
  private byte[] tablename;
  private final int nthreads = 20;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // Set system coprocessor so it can be applied to meta regions
    UTIL.getConfiguration().set("hbase.coprocessor.region.classes",
      MetaTableMetrics.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 10; i++) {
      do {
        int sign = i % 2 == 0 ? 1 : -1;
        connectorPort += sign * rand.nextInt(100);
      } while (!HBaseTestingUtility.available(connectorPort));
      try {
        conf.setInt("regionserver.rmi.registry.port", connectorPort);
        UTIL.startMiniCluster(1);
        break;
      } catch (Exception e) {
        LOG.debug("Encountered exception when starting cluster. Trying port {}", connectorPort, e);
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

  // Verifies that meta table metrics exist in jmx. In case of one table (one region) with a single
  // client: 9 metrics
  // are generated and for each metrics, there should be 5 JMX attributes produced. e.g. for one
  // table, there should
  // be 5 MetaTable_table_<TableName>_request attributes, such as:
  // - MetaTable_table_TestExampleMetaTableMetricsOne_request_count
  // - MetaTable_table_TestExampleMetaTableMetricsOne_request_mean_rate
  // - MetaTable_table_TestExampleMetaTableMetricsOne_request_1min_rate
  // - MetaTable_table_TestExampleMetaTableMetricsOne_request_5min_rate
  // - MetaTable_table_TestExampleMetaTableMetricsOne_request_15min_rate
  @Test
  public void testMetaTableMetricsInJmx() throws Exception {
    UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(NAME1).setColumnFamily(CFD).build());
    assertTrue(UTIL.getAdmin().isTableEnabled(NAME1));
    readWriteData(NAME1);
    UTIL.deleteTable(NAME1);

    UTIL.waitFor(30000, 2000, true, () -> {
      Map<String, Double> jmxMetrics = readMetaTableJmxMetrics();
      boolean allMetricsFound = AllOf.allOf(
        containsPositiveJmxAttributesFor("MetaTable_get_request"),
        containsPositiveJmxAttributesFor("MetaTable_put_request"),
        containsPositiveJmxAttributesFor("MetaTable_delete_request"),
        containsPositiveJmxAttributesFor("MetaTable_region_.+_lossy_request"),
        containsPositiveJmxAttributesFor("MetaTable_table_" + NAME1 + "_request"),
        containsPositiveJmxAttributesFor("MetaTable_client_.+_put_request"),
        containsPositiveJmxAttributesFor("MetaTable_client_.+_get_request"),
        containsPositiveJmxAttributesFor("MetaTable_client_.+_delete_request"),
        containsPositiveJmxAttributesFor("MetaTable_client_.+_lossy_request")
      ).matches(jmxMetrics);

      if (allMetricsFound) {
        LOG.info("all the meta table metrics found with positive values: {}", jmxMetrics);
      } else {
        LOG.warn("couldn't find all the meta table metrics with positive values: {}", jmxMetrics);
      }
      return allMetricsFound;
    });
  }

  @Test
  public void testConcurrentAccess() {
    try {
      tablename = Bytes.toBytes("hbase:meta");
      int numRows = 3000;
      int numRowsInTableBefore = UTIL.countRows(TableName.valueOf(tablename));
      putData(numRows);
      Thread.sleep(2000);
      int numRowsInTableAfter = UTIL.countRows(TableName.valueOf(tablename));
      assertTrue(numRowsInTableAfter >= numRowsInTableBefore + numRows);
      getData(numRows);
    } catch (InterruptedException e) {
      LOG.info("Caught InterruptedException while testConcurrentAccess: {}", e.getMessage());
      fail();
    } catch (IOException e) {
      LOG.info("Caught IOException while testConcurrentAccess: {}", e.getMessage());
      fail();
    }
  }

  private void readWriteData(TableName tableName) throws IOException {
    try (Table t = UTIL.getConnection().getTable(tableName)) {
      List<Put> puts = new ArrayList<>(NUM_ROWS);
      for (int i = 0; i < NUM_ROWS; i++) {
        Put p = new Put(Bytes.toBytes(i + 1));
        p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(value));
        puts.add(p);
      }
      t.put(puts);
      for (int i = 0; i < NUM_ROWS; i++) {
        Get get = new Get(Bytes.toBytes(i + 1));
        assertArrayEquals(Bytes.toBytes(value), t.get(get).getValue(FAMILY, QUALIFIER));
      }
    }
  }

  private Matcher<Map<String, Double>> containsPositiveJmxAttributesFor(final String regexp) {
    return new CustomTypeSafeMatcher<Map<String, Double>>(
        "failed to find all the 5 positive JMX attributes for: " + regexp) {

      @Override
      protected boolean matchesSafely(final Map<String, Double> values) {
        for (String key : values.keySet()) {
          for (String metricsNamePostfix : METRICS_ATTRIBUTE_NAME_POSTFIXES) {
            if (key.matches(regexp + metricsNamePostfix) && values.get(key) > 0) {
              return true;
            }
          }
        }
        return false;
      }
    };
  }

  /**
   * Read the attributes from Hadoop->HBase->RegionServer->MetaTableMetrics in JMX
   * @throws IOException when fails to retrieve jmx metrics.
   */
  private Map<String, Double> readMetaTableJmxMetrics() throws IOException {
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
        "Coprocessor.Region.CP_org.apache.hadoop.hbase.coprocessor.MetaTableMetrics");
      target = new ObjectName("Hadoop", pairs);
      MBeanInfo beanInfo = mb.getMBeanInfo(target);

      Map<String, Double> existingAttrs = new HashMap<>();
      for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
        Object value = mb.getAttribute(target, attrInfo.getName());
        if (attrInfo.getName().startsWith(METRICS_ATTRIBUTE_NAME_PREFIX)
            && value instanceof Number) {
          existingAttrs.put(attrInfo.getName(), Double.parseDouble(value.toString()));
        }
      }
      LOG.info("MBean Found: {}", target);
      return existingAttrs;
    } catch (Exception e) {
      LOG.warn("Failed to get Meta Table Metrics bean (will retry later): {}", target, e);
      if (mb != null) {
        Set<ObjectInstance> instances = mb.queryMBeans(null, null);
        Iterator<ObjectInstance> iterator = instances.iterator();
        LOG.debug("All the MBeans we found:");
        while (iterator.hasNext()) {
          ObjectInstance instance = iterator.next();
          LOG.debug("Class and object name: {} [{}]", instance.getClassName(),
                    instance.getObjectName());
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
    return Collections.emptyMap();
  }

  private void putData(int nrows) throws InterruptedException {
    LOG.info("Putting {} rows in hbase:meta", nrows);
    Thread[] threads = new Thread[nthreads];
    for (int i = 1; i <= nthreads; i++) {
      threads[i - 1] = new PutThread(1, nrows);
    }
    startThreadsAndWaitToJoin(threads);
  }

  private void getData(int nrows) throws InterruptedException {
    LOG.info("Getting {} rows from hbase:meta", nrows);
    Thread[] threads = new Thread[nthreads];
    for (int i = 1; i <= nthreads; i++) {
      threads[i - 1] = new GetThread(1, nrows);
    }
    startThreadsAndWaitToJoin(threads);
  }

  private void startThreadsAndWaitToJoin(Thread[] threads) throws InterruptedException {
    for (int i = 1; i <= nthreads; i++) {
      threads[i - 1].start();
    }
    for (int i = 1; i <= nthreads; i++) {
      threads[i - 1].join();
    }
  }

  private class PutThread extends Thread {
    int start;
    int end;

    PutThread(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public void run() {
      try (Table table = UTIL.getConnection().getTable(TableName.valueOf(tablename))) {
        for (int i = start; i <= end; i++) {
          Put p = new Put(Bytes.toBytes(String.format("tableName,rowKey%d,region%d", i, i)));
          p.addColumn(cf, col, Bytes.toBytes("Value" + i));
          table.put(p);
        }
      } catch (IOException e) {
        LOG.warn("Caught IOException while PutThread operation", e);
      }
    }
  }

  private class GetThread extends Thread {
    int start;
    int end;

    GetThread(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public void run() {
      try (Table table = UTIL.getConnection().getTable(TableName.valueOf(tablename))) {
        for (int i = start; i <= end; i++) {
          Get get = new Get(Bytes.toBytes(String.format("tableName,rowKey%d,region%d", i, i)));
          table.get(get);
        }
      } catch (IOException e) {
        LOG.warn("Caught IOException while GetThread operation", e);
      }
    }
  }
}
