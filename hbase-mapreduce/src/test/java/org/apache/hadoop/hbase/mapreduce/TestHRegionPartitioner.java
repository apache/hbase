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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MapReduceTests.TAG)
@Tag(MediumTests.TAG)
public class TestHRegionPartitioner {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterAll
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test HRegionPartitioner
   */
  @Test
  public void testHRegionPartitioner(TestInfo testInfo) throws Exception {

    byte[][] families = { Bytes.toBytes("familyA"), Bytes.toBytes("familyB") };

    String tableName = testInfo.getTestMethod().get().getName();
    UTIL.createTable(TableName.valueOf(tableName), families, 1, Bytes.toBytes("aa"),
      Bytes.toBytes("cc"), 3);

    HRegionPartitioner<Long, Long> partitioner = new HRegionPartitioner<>();
    Configuration configuration = UTIL.getConfiguration();
    configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    partitioner.setConf(configuration);
    ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes("bb"));

    assertEquals(1, partitioner.getPartition(writable, 10L, 3));
    assertEquals(0, partitioner.getPartition(writable, 10L, 1));
  }

  @Test
  public void testHRegionPartitionerMoreRegions(TestInfo testInfo) throws Exception {
    byte[][] families = { Bytes.toBytes("familyA"), Bytes.toBytes("familyB") };

    String tableNameStr = testInfo.getTestMethod().get().getName();
    TableName tableName = TableName.valueOf(tableNameStr);
    UTIL.createTable(tableName, families, 1, Bytes.toBytes("aa"), Bytes.toBytes("cc"), 5);

    Configuration configuration = UTIL.getConfiguration();
    int numberOfRegions = UTIL.getMiniHBaseCluster().getRegions(tableName).size();
    assertEquals(5, numberOfRegions);

    HRegionPartitioner<Long, Long> partitioner = new HRegionPartitioner<>();
    configuration.set(TableOutputFormat.OUTPUT_TABLE, tableNameStr);
    partitioner.setConf(configuration);

    // Get some rowKey for the lastRegion
    ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes("df"));

    // getPartition should return 4 since number of partition = number of reduces.
    assertEquals(4, partitioner.getPartition(writable, 10L, 5));
  }
}
