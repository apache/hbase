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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;

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
    Integer port =
        Iterables.getOnlyElement(testingUtility.getZkCluster().getClientPortList(), null);
    testingUtility.getConfiguration().set(HConstants.ZOOKEEPER_QUORUM,
        "localhost:" + port + "/hbase");
    ExecutorService executor = new ScheduledThreadPoolExecutor(2);
    Canary.ZookeeperStdOutSink sink = spy(new Canary.ZookeeperStdOutSink());
    Canary canary = new Canary(executor, sink);
    String[] args = { "-t", "10000", "-zookeeper" };
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);

    String baseZnode = testingUtility.getConfiguration()
        .get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    verify(sink, atLeastOnce())
        .publishReadTiming(eq(baseZnode), eq("localhost:" + port), anyLong());
  }

  @Test
  public void testBasicCanaryWorks() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    Table table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i=0; i<1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    Canary.RegionServerStdOutSink sink = spy(new Canary.RegionServerStdOutSink());
    Canary canary = new Canary(executor, sink);
    String[] args = { "-t", "10000", "testTable" };
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
    verify(sink, atLeastOnce())
        .publishReadTiming(isA(HRegionInfo.class), isA(HColumnDescriptor.class), anyLong());
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
    Canary.RegionServerStdOutSink sink = spy(new Canary.RegionServerStdOutSink());
    Canary canary = new Canary(executor, sink);
    String[] args = { "-t", "10000", "testTableRawScan" };
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(testingUtility.getConfiguration());
    conf.setBoolean(HConstants.HBASE_CANARY_READ_RAW_SCAN_KEY, true);
    ToolRunner.run(conf, canary, args);
    verify(sink, atLeastOnce())
        .publishReadTiming(isA(HRegionInfo.class), isA(HColumnDescriptor.class), anyLong());
  }
  
  private void runRegionserverCanary() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    Canary canary = new Canary(executor, new Canary.RegionServerStdOutSink());
    String[] args = { "-t", "10000", "-regionserver"};
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
  }

}

