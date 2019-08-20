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

package org.apache.hadoop.hbase.tool;

import com.google.common.collect.Iterables;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
@Category({MediumTests.class})
public class TestCanaryTool {

  private HBaseTestingUtility testingUtility;
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] COLUMN = Bytes.toBytes("col");

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
    LogManager.getRootLogger().addAppender(mockAppender);
  }

  @After
  public void tearDown() throws Exception {
    testingUtility.shutdownMiniCluster();
    LogManager.getRootLogger().removeAppender(mockAppender);
  }

  @Mock
  Appender mockAppender;

  @Test
  public void testBasicZookeeperCanaryWorks() throws Exception {
    final String[] args = { "-t", "10000", "-zookeeper" };
    testZookeeperCanaryWithArgs(args);
  }

  @Test
  public void testZookeeperCanaryPermittedFailuresArgumentWorks() throws Exception {
    final String[] args = { "-t", "10000", "-zookeeper", "-treatFailureAsError", "-permittedZookeeperFailures", "1" };
    testZookeeperCanaryWithArgs(args);
  }

  @Test
  public void testBasicCanaryWorks() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    HTable table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i=0; i<1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-t", "10000", tableName.getNameAsString() };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    assertEquals("verify no read error count", 0, canary.getReadFailures().size());
    assertEquals("verify no write error count", 0, canary.getWriteFailures().size());
    verify(sink, atLeastOnce()).publishReadTiming(isA(ServerName.class), isA(HRegionInfo.class), isA(HColumnDescriptor.class), anyLong());
  }

  @Test
  public void testCanaryRegionTaskResult() throws Exception {
    TableName tableName = TableName.valueOf("testCanaryRegionTaskResult");
    HTable table = testingUtility.createTable(tableName, new byte[][]{FAMILY});
    // insert some test rows
    for (int i = 0; i < 1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-t", "10000", "testCanaryRegionTaskResult" };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));

    assertTrue("verify read success count > 0", sink.getReadSuccessCount() > 0);
    assertTrue("verify write success count > 0", sink.getWriteSuccessCount() > 0);
    verify(sink, atLeastOnce()).publishReadTiming(isA(ServerName.class), isA(HRegionInfo.class),
      isA(HColumnDescriptor.class), anyLong());
    verify(sink, atLeastOnce()).publishWriteTiming(isA(ServerName.class), isA(HRegionInfo.class),
      isA(HColumnDescriptor.class), anyLong());

    assertTrue("canary should expect to scan at least 1 region",
        sink.getTotalExpectedRegions() > 0);
    Map<String, CanaryTool.RegionTaskResult> regionMap = sink.getRegionMap();
    assertFalse("verify region map has size > 0", regionMap.isEmpty());

    for (String regionName : regionMap.keySet()) {
      CanaryTool.RegionTaskResult res = regionMap.get(regionName);
      assertNotNull("verify each expected region has a RegionTaskResult object in the map", res);
      assertNotNull("verify getRegionNameAsString()", regionName);
      assertNotNull("verify getRegionInfo()", res.getRegionInfo());
      assertNotNull("verify getTableName()", res.getTableName());
      assertNotNull("verify getTableNameAsString()", res.getTableNameAsString());
      assertNotNull("verify getServerName()", res.getServerName());
      assertNotNull("verify getServerNameAsString()", res.getServerNameAsString());

      if (regionName.contains(CanaryTool.DEFAULT_WRITE_TABLE_NAME.getNameAsString())) {
        assertTrue("write to region " + regionName + " succeeded", res.isWriteSuccess());
        assertTrue("write took some time", res.getWriteLatency() > -1);
      } else {
        assertTrue("read from region " + regionName + " succeeded", res.isReadSuccess());
        assertTrue("read took some time", res.getReadLatency() > -1);
      }
    }
  }

  @Test
  @Ignore("Intermittent argument matching failures, see HBASE-18813")
  public void testReadTableTimeouts() throws Exception {
    final TableName [] tableNames = new TableName[2];
    tableNames[0] = TableName.valueOf("testReadTableTimeouts1");
    tableNames[1] = TableName.valueOf("testReadTableTimeouts2");
    // Create 2 test tables.
    for (int j = 0; j<2; j++) {
      Table table = testingUtility.createTable(tableNames[j], new byte[][] { FAMILY });
      // insert some test rows
      for (int i=0; i<1000; i++) {
        byte[] iBytes = Bytes.toBytes(i + j);
        Put p = new Put(iBytes);
        p.addColumn(FAMILY, COLUMN, iBytes);
        table.put(p);
      }
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String configuredTimeoutStr = tableNames[0].getNameAsString() + "=" + Long.MAX_VALUE + "," +
      tableNames[1].getNameAsString() + "=0";
    String[] args = { "-readTableTimeouts", configuredTimeoutStr, tableNames[0].getNameAsString(), tableNames[1].getNameAsString()};
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
    verify(sink, times(tableNames.length)).initializeAndGetReadLatencyForTable(isA(String.class));
    for (int i=0; i<2; i++) {
      assertNotEquals("verify non-null read latency", null, sink.getReadLatencyMap().get(tableNames[i].getNameAsString()));
      assertNotEquals("verify non-zero read latency", 0L, sink.getReadLatencyMap().get(tableNames[i].getNameAsString()));
    }
    // One table's timeout is set for 0 ms and thus, should lead to an error.
    verify(mockAppender, times(1)).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("exceeded the configured read timeout.");
      }
    }));
    verify(mockAppender, times(2)).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Configured read timeout");
      }
    }));
  }

  @Test
  @Ignore("Intermittent argument matching failures, see HBASE-18813")
  public void testWriteTableTimeout() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-writeTableTimeout", String.valueOf(Long.MAX_VALUE)};
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
    assertNotEquals("verify non-null write latency", null, sink.getWriteLatency());
    assertNotEquals("verify non-zero write latency", 0L, sink.getWriteLatency());
    verify(mockAppender, times(1)).doAppend(argThat(
        new ArgumentMatcher<LoggingEvent>() {
          @Override
          public boolean matches(Object argument) {
            return ((LoggingEvent) argument).getRenderedMessage().contains("Configured write timeout");
          }
        }));
  }

  //no table created, so there should be no regions
  @Test
  public void testRegionserverNoRegions() throws Exception {
    runRegionserverCanary();
    verify(mockAppender).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }

  //by creating a table, there shouldn't be any region servers not serving any regions
  @Test
  public void testRegionserverWithRegions() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    testingUtility.createTable(tableName, new byte[][] { FAMILY });
    runRegionserverCanary();
    verify(mockAppender, never()).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }

  @Test
  public void testRawScanConfig() throws Exception {
    TableName tableName = TableName.valueOf("testTableRawScan");
    Table table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i=0; i<1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-t", "10000", "testTableRawScan" };
    org.apache.hadoop.conf.Configuration conf =
      new org.apache.hadoop.conf.Configuration(testingUtility.getConfiguration());
    conf.setBoolean(HConstants.HBASE_CANARY_READ_RAW_SCAN_KEY, true);
    ToolRunner.run(conf, canary, args);
    verify(sink, atLeastOnce())
        .publishReadTiming(isA(ServerName.class), isA(HRegionInfo.class), isA(HColumnDescriptor.class), anyLong());
    assertEquals("verify no read error count", 0, canary.getReadFailures().size());
  }

  private void runRegionserverCanary() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool canary = new CanaryTool(executor, new CanaryTool.RegionServerStdOutSink());
    String[] args = { "-t", "10000", "-regionserver"};
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
    assertEquals("verify no read error count", 0, canary.getReadFailures().size());
  }

  private void testZookeeperCanaryWithArgs(String[] args) throws Exception {
    Integer port =
      Iterables.getOnlyElement(testingUtility.getZkCluster().getClientPortList(), null);
    testingUtility.getConfiguration().set(HConstants.ZOOKEEPER_QUORUM,
      "localhost:" + port + "/hbase");
    ExecutorService executor = new ScheduledThreadPoolExecutor(2);
    CanaryTool.ZookeeperStdOutSink sink = spy(new CanaryTool.ZookeeperStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));

    String baseZnode = testingUtility.getConfiguration()
      .get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    verify(sink, atLeastOnce())
      .publishReadTiming(eq(baseZnode), eq("localhost:" + port), anyLong());
  }
}
