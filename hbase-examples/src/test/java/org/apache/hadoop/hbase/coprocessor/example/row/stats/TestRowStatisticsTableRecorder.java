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
package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import static org.apache.hadoop.hbase.util.TestRegionSplitCalculator.TEST_UTIL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.recorder.RowStatisticsTableRecorder;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.utils.RowStatisticsTableUtil;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.impl.CounterImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRowStatisticsTableRecorder {

  private static final NamespaceDescriptor NAMESPACE_DESCRIPTOR =
    NamespaceDescriptor.create(RowStatisticsTableUtil.NAMESPACE).build();
  private static final byte[] FULL_REGION_NAME = Bytes.toBytes("fullRegionName");
  private static SingleProcessHBaseCluster cluster;
  private static Connection connection;
  private RowStatisticsImpl rowStatistics;
  private Counter counter;

  @BeforeClass
  public static void setUpClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1);
    connection = ConnectionFactory.createConnection(cluster.getConf());
    connection.getAdmin().createNamespace(NAMESPACE_DESCRIPTOR);
    // need this table to write to
    TEST_UTIL.createTable(RowStatisticsTableUtil.NAMESPACED_TABLE_NAME, RowStatisticsTableUtil.CF);
  }

  @Before
  public void setup() {
    rowStatistics = mock(RowStatisticsImpl.class);
    counter = new CounterImpl();
  }

  @Test
  public void itReturnsNullRecorderOnFailedBufferedMutator() throws IOException {
    Connection badConnection = mock(Connection.class);
    Configuration conf = mock(Configuration.class);
    when(badConnection.getConfiguration()).thenReturn(conf);
    when(badConnection.getBufferedMutator(any(BufferedMutatorParams.class)))
      .thenThrow(IOException.class);
    RowStatisticsTableRecorder recorder =
      RowStatisticsTableRecorder.forClusterConnection(badConnection, counter, counter);
    assertNull(recorder);
  }

  @Test
  public void itDoesNotIncrementCounterWhenRecordSucceeds() throws IOException {
    RowStatisticsTableRecorder recorder =
      RowStatisticsTableRecorder.forClusterConnection(connection, counter, counter);
    assertNotNull(recorder);
    recorder.record(rowStatistics, Optional.of(FULL_REGION_NAME));
    assertEquals(counter.getCount(), 0);
  }

  @Test
  public void itIncrementsCounterWhenRecordFails() throws IOException {
    RowStatisticsTableRecorder recorder =
      RowStatisticsTableRecorder.forClusterConnection(connection, counter, counter);
    assertNotNull(recorder);
    recorder.close();
    recorder.record(rowStatistics, Optional.of(FULL_REGION_NAME));
    assertEquals(counter.getCount(), 1);
  }
}
